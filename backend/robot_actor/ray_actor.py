"""Robot processes as Ray actors (design doc phase 2).

One stateful actor per robot, as in the original backend/runtime.py
(RayRobotRuntime) and LaDe's _DroneActor: the actor owns the robot's state and
plan; the main process dispatches, sends each robot its changes and asks every
actor to advance one second, then collects all results with one ray.get. The
logic is RobotProcess from local.py, so the local pool, this Ray pool and the
sequential engine are directly comparable (tests/test_robot_process_ray.py).

Each actor can also own the robot's graph writer, written the same way as
ray_medium.MotionActor, so per-robot graph writing keeps working.
"""
from __future__ import annotations

import os
from pathlib import Path
import time
import json
from collections import deque

from .local import RobotProcess, RobotProcessSplit

ROOT = Path(__file__).resolve().parents[2]


class RobotActor(RobotProcess):
    """RobotProcess plus this robot's own graph writer (optional)."""

    def __init__(self, robot_id, robot, energy, charge_target_soc, graph=None):
        super().__init__(robot_id, robot, energy, charge_target_soc)
        self.writer, self.last_graph_error = None, None
        self.context_client = None
        self.context_config = None
        if graph:
            from ..warehouse_runtime_graph import Neo4jRuntimeGraphWriter
            config = dict(graph)
            config.pop('schema_ready', None)
            self.writer = Neo4jRuntimeGraphWriter(**config)
            self.context_config = {k: config[k] for k in ('uri', 'user', 'password', 'database')}
            self.writer.schema_ready = True  # the main process bootstraps constraints once

    def write_graph(self, snapshot):
        if self.writer is None:
            return 'robot graph writer is not configured'
        for attempt in range(5):
            try:
                self.writer.record_robot_snapshot(snapshot)
                if 'current_robot' in snapshot:
                    self.sync(robot=snapshot['current_robot'])
                    self.writer.record_robot_current_state(snapshot, self.robot)
                self.last_graph_error = None
                break
            except Exception as exc:
                transient = 'TransientError' in type(exc).__name__ or 'DeadlockDetected' in str(exc)
                if transient and attempt < 4:
                    time.sleep(0.02 * (attempt + 1))
                    continue
                self.last_graph_error = str(exc)
                break
        return self.last_graph_error

    def query_context(self, run_id, tick=None):
        began = time.perf_counter()
        try:
            if self.context_client is None:
                if self.context_config is None:
                    raise RuntimeError('robot context requires a graph connection')
                from ..query_c import RobotContextClient
                self.context_client = RobotContextClient(self.context_config)
            return self.context_client.query(run_id, self.robot_id, tick=tick)
        except Exception as exc:
            # Observations do not choose tasks or change the physical scheduler.
            return {'robot_id': self.robot_id, 'pid': os.getpid(), 'tick': tick,
                    'total_ms': (time.perf_counter() - began) * 1000,
                    'pack_ms': {}, 'pack_rows': {}, 'missing': [],
                    'error': type(exc).__name__ + ': ' + str(exc)}

    def close(self):
        if self.context_client is not None:
            self.context_client.close()
            self.context_client = None
        if self.writer is not None:
            self.writer.close()
            self.writer = None


class RayRobotPool:
    """One RobotActor per robot; step_all fans out and collects with one ray.get."""

    def __init__(self, robots, energy, charge_target_soc, *, address=None, actor_cpus=0.1,
                 namespace='warehouse-robot-actors', graph=None):
        import ray
        self.ray = ray
        if not ray.is_initialized():
            ray.init(address=address or os.environ.get('RAY_ADDRESS') or None, namespace=namespace,
                     ignore_reinit_error=True, log_to_driver=False,
                     runtime_env={'env_vars': {'PYTHONPATH': str(ROOT)}})
        actor_class = ray.remote(RobotActor)
        self.actors = {owner: actor_class.options(num_cpus=actor_cpus).remote(owner, robot, energy,
                                                                            charge_target_soc, graph)
                       for owner, robot in robots.items()}

    def step_all(self, now, calls):
        owners = list(calls)
        refs = [self.actors[owner].step.remote(now, *calls[owner]) for owner in owners]
        return dict(zip(owners, self.ray.get(refs)))

    def redo(self, owner, now, robot, plan, clear_plan, persistent_fault):
        return self.ray.get(self.actors[owner].redo.remote(now, robot, plan, clear_plan, persistent_fault))

    def write_graph_all(self, snapshots):
        return {owner: self.actors[owner].write_graph.remote(snapshot) for owner, snapshot in snapshots.items()}

    def write_graph(self, owner, snapshot):
        return self.ray.get(self.actors[owner].write_graph.remote(snapshot))

    def snapshots(self):
        owners = list(self.actors)
        return dict(zip(owners, self.ray.get([self.actors[o].snapshot.remote() for o in owners])))

    def query_context_all(self, run_id, tick=None):
        return self.ray.get([self.actors[o].query_context.remote(run_id, tick) for o in sorted(self.actors)])

    def close(self):
        for actor in self.actors.values():
            try:
                self.ray.kill(actor)
            except Exception:
                pass
        self.actors = {}

    def __del__(self):
        if getattr(self, 'actors', None):
            self.close()


class RayRobotProcessSplit(RobotProcessSplit):
    """Split engine whose robot processes are Ray actors.

    With robot_graph=True each actor writes its own Robot/AgentState/Execution/
    RuntimeEvent subgraph through the existing lys writer (same snapshots as the
    MotionActor path, plus the full event payload). Writes are issued after each
    tick and collected in batches in memory mode; neo4j mode waits each tick.
    A failed snapshot is resent (writes are MERGE,
    so a resend never duplicates). flush_robot_graph() waits for every issued
    write and returns the receipt used for the Step marker.
    """
    ray_address = None
    ray_actor_cpus = 0.1
    GRAPH_BATCH_TICKS = 8
    GRAPH_RETRIES = 3

    def __init__(self, *args, robot_graph=False, graph_state_interval=4,
                 kg_state_source='memory', global_writer_url=None, robot_context_interval=10, **kwargs):
        if type(robot_context_interval) is not int or not 0 <= robot_context_interval <= 3600:
            raise ValueError('robot_context_interval must be within 0..3600')
        self.robot_context_interval = robot_context_interval
        self._context_tick = None
        self.context_samples = deque(maxlen=10000)
        self.context_log_path = None
        self.context_errors = 0
        if kg_state_source not in ('memory', 'neo4j'):
            raise ValueError('kg_state_source must be memory or neo4j')
        if kg_state_source == 'neo4j' and (not robot_graph or not global_writer_url):
            raise ValueError('neo4j state requires --robot-processes ray --robot-graph --global-writer-url')
        self.kg_state_source = kg_state_source
        self.global_writer_url = global_writer_url
        self._kg_reader = self._step_client = None
        self._published_robots = None
        self._global_cursor = 0
        self._global_tick = -1
        self.kg_error = None
        self.kg_timing = {}
        self.global_record_identity = None
        self._robot_graph, self._graph_state_interval = bool(robot_graph), graph_state_interval
        self._graph_config, self._graph_cursor, self._graph_pending = None, 0, []
        self.graph_receipt = {'tick': None, 'robots': []}
        self.graph_errors = []
        self.graph_error_count = 0
        super().__init__(*args, **kwargs)
        self._kg_fields = set(next(iter(self.robots.values())))
        self.run_metadata['kg_state_source'] = kg_state_source
        self.run_metadata['robot_context_interval'] = robot_context_interval
        if self._robot_graph:
            self.run_metadata['robot_graph_writer'] = 'robot-actors'
            self.run_metadata['robot_graph_interval'] = graph_state_interval

    def _make_robot_pool(self):
        if self._robot_graph and self._graph_config is None:
            from ..ray_medium.sim import bootstrap_robot_graph
            self._graph_config = bootstrap_robot_graph(self, self._graph_state_interval)
            if self.robot_context_interval:
                from ..warehouse_runtime_graph import Neo4jRuntimeGraphWriter
                bootstrap = Neo4jRuntimeGraphWriter(**self._graph_config)
                try:
                    bootstrap.ensure_context_schema()
                finally:
                    bootstrap.close()
        return RayRobotPool(self.robots, self.energy, self.config.charge_target_soc,
                            address=self.ray_address, actor_cpus=self.ray_actor_cpus, graph=self._graph_config)

    def step(self):
        if self.kg_error:
            raise RuntimeError(self.kg_error)
        self.kg_timing = {'advance_ms': 0., 'write_ms': 0., 'wait_step_ms': 0., 'read_ms': 0., 'dispatch_ms': 0.}
        began = time.perf_counter()
        try:
            super().step()
            self.kg_timing['advance_ms'] = max(0., (time.perf_counter() - began) * 1000 -
                sum(self.kg_timing.get(k, 0.) for k in ('write_ms', 'wait_step_ms', 'read_ms', 'dispatch_ms', 'context_ms')))
            if self._robot_graph:
                self._emit_robot_graph()
            if self.global_writer_url:
                self._submit_global_step()
        except Exception as exc:
            if self.kg_state_source == 'neo4j':
                self.running = False
                self.kg_error = f'KG dispatch paused at tick {self.tick}: {exc}'
            raise

    def _prepare_kg_dispatch(self):
        from .kg_state import KGStateReader
        # Bootstrap tick zero (or a restored tick) before the first graph read.
        if self._kg_reader is None:
            self._robot_pool()
            config = {k: self._graph_config[k] for k in ('uri', 'user', 'password', 'database')}
            self._kg_reader = KGStateReader(**config)
        if self._published_robots is None or self._published_robots != self.robots:
            # Main-process marker/disturbance changes must reach the actors and
            # graph before reading. This is an input update, never a read fallback.
            self._emit_robot_graph()
        if self._global_tick < self.tick:
            self._submit_global_step()

    def _after_kg_state_read(self):
        if (not self.robot_context_interval or self.tick % self.robot_context_interval
                or self._context_tick == self.tick):
            return
        began = time.perf_counter()
        try:
            rows = self._robot_pool().query_context_all(self.run_id, self.tick)
            self.context_errors += sum(bool(row.get('error')) for row in rows)
            self.context_samples.extend(rows)
            self._context_tick = self.tick
            if self.context_log_path:
                with Path(self.context_log_path).open('a', encoding='utf-8') as stream:
                    stream.write(json.dumps({'tick': self.tick, 'robots': rows}, sort_keys=True) + '\n')
        except Exception:
            self.context_errors += 1
        finally:
            self.kg_timing['context_ms'] = (time.perf_counter() - began) * 1000

    def _reset_after_restore(self):
        if self._step_client is not None:
            try:
                self._step_client.flush(10.)
            finally:
                self._step_client.close()
                self._step_client = None
        if self._kg_reader is not None:
            self._kg_reader.close()
            self._kg_reader = None
        self._graph_pending, self._graph_cursor, self._global_cursor = [], 0, 0
        self._published_robots, self._context_tick = None, None
        self._global_tick = self.tick - 1
        self.graph_receipt, self.graph_errors = {'tick': None, 'robots': []}, []
        self.kg_error = None

    def _validate_restore(self, state):
        if self.kg_state_source != 'neo4j':
            return
        target = int(state['tick'])
        if self.tick > target:
            raise ValueError('same-run graph rewind is unsupported; restore the latest boundary or use a new run')
        from .kg_state import KGStateReader
        config = self._graph_config or {
            'uri': os.environ.get('WAREHOUSE_NEO4J_URI') or os.environ.get('NEO4J_URI'),
            'user': os.environ.get('WAREHOUSE_NEO4J_USER') or os.environ.get('NEO4J_USER', 'neo4j'),
            'password': os.environ.get('WAREHOUSE_NEO4J_PASSWORD') or os.environ.get('NEO4J_PASSWORD'),
            'database': os.environ.get('WAREHOUSE_NEO4J_DATABASE') or os.environ.get('NEO4J_DATABASE', 'neo4j')}
        reader = KGStateReader(**{k: config[k] for k in ('uri', 'user', 'password', 'database')})
        try:
            from neo4j import Query
            with reader.driver.session(database=reader.database, default_access_mode='READ') as session:
                row = session.run(Query('MATCH (r:Run {id:$run}) '
                    'OPTIONAL MATCH (e:Execution {run_id:$run}) '
                    'RETURN r.latest_step AS step,r.updated_tick AS updated,max(e.current_owner_tick) AS owner',
                    timeout=2.), run=self.run_id).single()
            if row and any(row[k] is not None and row[k] > target for k in ('step', 'updated', 'owner')):
                raise ValueError('graph is ahead of checkpoint; same-run rewind requires explicit reconciliation')
        finally:
            reader.close()

    def frame(self):
        value = super().frame()
        latest = getattr(self._kg_reader, 'latest_step', None)
        value['robotProcesses'] = 'ray'
        value['robotGraph'] = {'latestCompleteStep': latest, 'currentTick': self.tick,
            'lag': max(0, self.tick - latest) if latest is not None else None,
            'errors': self.graph_error_count, 'lastError': self.kg_error,
            'contextErrors': self.context_errors, 'contextSamples': len(self.context_samples)}
        return value

    def _submit_global_step(self):
        from ..global_record_client import GlobalRecordClient, BufferFull
        from ..global_writer import build_global_record
        if self._global_tick >= self.tick:
            return
        began = time.perf_counter()
        receipt = self.flush_robot_graph()
        received = receipt['robots'] if receipt['tick'] == self.tick else []
        complete = len(received) == 100 and set(received) == set(self.robots)
        if self.kg_state_source == 'neo4j' and not complete:
            raise RuntimeError(f'incomplete robot receipts at tick {self.tick}')
        if self._step_client is None:
            self._step_client = GlobalRecordClient(self.global_writer_url, self.run_id, batch_size=128)
        pending = self.pending_events
        if len(pending) < self._global_cursor:
            self._global_cursor = 0
        fresh = pending[self._global_cursor:]
        complete_run = (self.elapsed >= self.manifest['duration_seconds'] and
                        self.metrics['completed'] == len(self.tasks) and not self.assists)
        record = build_global_record(self, fresh, status='complete' if complete_run else 'running',
            identity=self.global_record_identity, commit_seq=self.tick,
            include_dispatch=self.tick % 5 == 0)
        record['step'] = {'tick': self.tick, 'expected_robot_count': 100,
                          'received_robot_ids': received, 'complete': complete, 'source': 'ray-robot-actors'}
        try:
            self._step_client.submit(record)
        except BufferFull:
            self._step_client.flush(2.)
            self._step_client.submit(record)
        self._global_cursor, self._global_tick = len(pending), self.tick
        self.kg_timing['write_ms'] = self.kg_timing.get('write_ms', 0.) + (time.perf_counter() - began) * 1000

    def close(self):
        try:
            if self._step_client is not None:
                try:
                    self._step_client.flush(10.)
                finally:
                    self._step_client.close()
                    self._step_client = None
        finally:
            if self._kg_reader is not None:
                self._kg_reader.close()
                self._kg_reader = None
            super().close()

    def _emit_robot_graph(self):
        began = time.perf_counter()
        from ..ray_medium.sim import robot_graph_snapshots
        pending = self.pending_events
        if len(pending) < self._graph_cursor:
            self._graph_cursor = 0  # pending events were committed and cleared
        fresh, self._graph_cursor = pending[self._graph_cursor:], len(pending)
        snapshots = robot_graph_snapshots(self, fresh, self.frame().get('backend', 'warehouse-robot-actors'))
        for owner, snapshot in snapshots.items():
            snapshot['current_robot'] = dict(self.robots[owner])
        self._graph_pending.append((self.tick, snapshots, self._robot_pool().write_graph_all(snapshots)))
        if self.kg_state_source == 'neo4j' or len(self._graph_pending) >= self.GRAPH_BATCH_TICKS:
            self._collect_robot_graph()
        self._published_robots = {i: dict(r) for i, r in self.robots.items()}
        self.kg_timing['write_ms'] = self.kg_timing.get('write_ms', 0.) + (time.perf_counter() - began) * 1000

    def _collect_robot_graph(self):
        pool = self._robot_pool()
        for tick, snapshots, refs in self._graph_pending:
            failed = [owner for owner, error in zip(refs, pool.ray.get(list(refs.values()))) if error]
            for _ in range(self.GRAPH_RETRIES):
                if not failed:
                    break
                failed = [owner for owner in failed if pool.write_graph(owner, snapshots[owner])]
            if failed:
                self.graph_error_count = getattr(self, 'graph_error_count', 0) + len(failed)
                self.graph_errors = (self.graph_errors + [{'tick': tick, 'robots': failed}])[-20:]
            self.graph_receipt = {'tick': tick, 'robots': sorted(set(snapshots) - set(failed))}
            if failed and self.kg_state_source == 'neo4j':
                raise RuntimeError(f'robot graph writes failed at tick {tick}: {failed}')
        self._graph_pending = []

    def flush_robot_graph(self):
        """Wait for every issued robot write; return the receipt of the last tick."""
        if self._robot_graph:
            self._collect_robot_graph()
        return dict(self.graph_receipt)
