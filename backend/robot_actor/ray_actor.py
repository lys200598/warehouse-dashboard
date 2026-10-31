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
import threading
from collections import deque
from copy import deepcopy

from .local import RobotProcess, RobotProcessSplit
from .graph_async import GraphWaitCancelled

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
                    # Graph work may overlap newer motion. Never replace the
                    # live actor state with an older queued graph snapshot.
                    self.writer.record_robot_current_state(snapshot, snapshot['current_robot'])
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
        actor_class = ray.remote(concurrency_groups={'graph': 1})(RobotActor)
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
        return {owner: self.actors[owner].write_graph.options(concurrency_group='graph').remote(snapshot)
                for owner, snapshot in snapshots.items()}

    def write_graph(self, owner, snapshot):
        return self.ray.get(self.actors[owner].write_graph.options(concurrency_group='graph').remote(snapshot))

    def snapshots(self):
        owners = list(self.actors)
        return dict(zip(owners, self.ray.get([self.actors[o].snapshot.remote() for o in owners])))

    def query_context_all(self, run_id, tick=None, concurrency_group=None):
        methods = [self.actors[o].query_context for o in sorted(self.actors)]
        if concurrency_group is not None:
            methods = [method.options(concurrency_group=concurrency_group) for method in methods]
        return self.ray.get([method.remote(run_id, tick) for method in methods])

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
                 kg_state_source='memory', global_writer_url=None, robot_context_interval=10,
                 robot_graph_async=True, robot_graph_backlog=120, **kwargs):
        if type(robot_graph_async) is not bool:
            raise ValueError('robot_graph_async must be a boolean')
        if type(robot_graph_backlog) is not int or not 1 <= robot_graph_backlog <= 128:
            raise ValueError('robot_graph_backlog must be within 1..128')
        self.robot_graph_async = bool(robot_graph_async and robot_graph and global_writer_url
                                      and kg_state_source == 'memory')
        self.robot_graph_backlog = robot_graph_backlog
        self._async_graph_queue = None
        self._async_graph_stats = {}
        self._async_event_source = None
        self._async_metrics_lock = threading.Lock()
        self._async_phase_windows = {}
        self._async_latest_step = None
        self._async_write_refs = []
        self._async_max_lag = 0
        self._graph_pause_events = []
        self._graph_pause_started = None
        self.graph_timing_log_path = None
        self._async_step_driver = None
        self.graph_backpressure_callback = None
        self.graph_wait_cancel_event = None
        if type(robot_context_interval) is not int or not 0 <= robot_context_interval <= 3600:
            raise ValueError('robot_context_interval must be within 0..3600')
        self.robot_context_interval = robot_context_interval
        self._context_tick = None
        self.context_samples = deque(maxlen=10000)
        self.context_log_path = None
        self.context_errors = 0
        self._context_lock = threading.Lock()
        self._async_context_windows = {}
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
        self.run_metadata['robot_graph_async'] = self.robot_graph_async
        self.run_metadata['robot_graph_backlog'] = robot_graph_backlog
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
            if self.robot_graph_async:
                self._ensure_async_graph_queue()
                if self._async_graph_queue.snapshot()['confirmed_tick'] < 0 and self._global_tick < self.tick:
                    self._enqueue_async_graph_frame()  # include the initial/restored boundary
                # This runs before dispatch/energy/motion; never replay a step
                # just because its graph writes are still pending.
                waiting = time.perf_counter()
                self._async_graph_queue.reserve(self.graph_wait_cancel_event)
                self.kg_timing['backpressure_ms'] = (time.perf_counter() - waiting) * 1000
            super().step()
            self.kg_timing['advance_ms'] = max(0., (time.perf_counter() - began) * 1000 -
                sum(self.kg_timing.get(k, 0.) for k in ('write_ms', 'wait_step_ms', 'read_ms', 'dispatch_ms', 'context_ms', 'backpressure_ms')))
            if self.robot_graph_async:
                self._enqueue_async_graph_frame()
            elif self._robot_graph:
                self._emit_robot_graph()
            if self.global_writer_url and not self.robot_graph_async:
                self._submit_global_step()
        except GraphWaitCancelled:
            raise  # safe pre-dispatch yield for pause/shutdown; never latch kg_error
        except Exception as exc:
            if self.kg_state_source == 'neo4j' or self.robot_graph_async:
                self.running = False
                self.kg_error = f'KG dispatch paused at tick {self.tick}: {exc}'
            raise

    def _ensure_async_graph_queue(self):
        if self._async_graph_queue is None:
            from .graph_async import AsyncRobotGraphQueue
            self._robot_pool()  # initialize Ray/schema on the simulation owner
            self._async_graph_queue = AsyncRobotGraphQueue(
                self._process_async_graph_frame, capacity=self.robot_graph_backlog,
                on_wait=self._graph_backpressure_changed)

    def _graph_backpressure_changed(self, stats):
        with self._async_metrics_lock:
            now = time.monotonic()
            utc = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
            if stats['waiting']:
                self._graph_pause_started = now
                self._graph_pause_events.append({'tick': self.tick + 1, 'started_at_utc': utc,
                    'ended_at_utc': None, 'duration_seconds': None, 'status': 'waiting'})
            elif self._graph_pause_started is not None:
                event = self._graph_pause_events[-1]
                event.update(ended_at_utc=utc, duration_seconds=now - self._graph_pause_started,
                    status='cancelled' if self.graph_wait_cancel_event is not None and
                    self.graph_wait_cancel_event.is_set() else ('caught_up' if not stats['pending'] else 'failed'))
                self._graph_pause_started = None
        if self.graph_backpressure_callback is not None:
            self.graph_backpressure_callback(stats)

    def _enqueue_async_graph_frame(self):
        from ..ray_medium.sim import robot_graph_snapshots
        from ..global_writer import build_global_record
        pending = self.pending_events
        if pending is not self._async_event_source:
            # The journal replaces pending_events on save. Equal list lengths
            # do not mean it is the same batch of events.
            self._graph_cursor = self._global_cursor = 0
            self._async_event_source = pending
        if len(pending) < self._graph_cursor:
            self._graph_cursor = 0
        if len(pending) < self._global_cursor:
            self._global_cursor = 0
        snapshots = robot_graph_snapshots(self, pending[self._graph_cursor:],
                                         self.frame().get('backend', 'warehouse-robot-actors'))
        for owner, snapshot in snapshots.items():
            snapshot['current_robot'] = deepcopy(self.robots[owner])
        complete_run = (self.elapsed >= self.manifest['duration_seconds'] and
                        self.metrics['completed'] == len(self.tasks) and not self.assists)
        record = build_global_record(self, pending[self._global_cursor:],
            status='complete' if complete_run else 'running', identity=self.global_record_identity,
            commit_seq=self.tick, include_dispatch=self.tick % 5 == 0) if self.global_writer_url else None
        self._async_graph_queue.submit({'tick': self.tick, 'snapshots': snapshots, 'record': record})
        with self._async_metrics_lock:
            # Before initial Step(0) exists, elapsed-time lag is measured from
            # boundary zero; pendingSteps also counts the initial frame itself.
            self._async_max_lag = max(self._async_max_lag, self.tick - (self._async_latest_step or 0))
        self._graph_cursor = self._global_cursor = len(pending)
        self._global_tick = self.tick

    def _process_async_graph_frame(self, frame):
        timing = {'tick': frame['tick'], 'started_at_utc': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
                  'write_ms': 0., 'wait_step_ms': 0., 'context_ms': 0.}
        try:
            self._write_and_confirm_async_graph_frame(frame, timing)
        except Exception as exc:
            timing['error'] = f'{type(exc).__name__}: {exc}'
            raise
        finally:
            timing['ended_at_utc'] = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
            with self._async_metrics_lock:
                window = self._async_phase_windows.setdefault(max(0, frame['tick'] - 1) // 7200, {})
                for name in ('write_ms', 'wait_step_ms', 'context_ms'):
                    window.setdefault(name, []).append(timing[name])
            if self.graph_timing_log_path:
                with Path(self.graph_timing_log_path).open('a', encoding='utf-8') as stream:
                    stream.write(json.dumps(timing, sort_keys=True) + '\n')

    def _write_and_confirm_async_graph_frame(self, frame, timing):
        # One coordinator processes one tick at a time. Graph methods have a
        # separate single-thread group from motion and never touch actor state.
        pool, snapshots, tick = self._robot_pool(), frame['snapshots'], frame['tick']
        began = time.perf_counter()
        refs = pool.write_graph_all(snapshots)
        with self._async_metrics_lock:
            self._async_write_refs = list(refs.values())
        failed = [owner for owner, error in zip(refs, pool.ray.get(list(refs.values()))) if error]
        with self._async_metrics_lock:
            self._async_write_refs = []  # ray.get returned every write, including error receipts
        for _ in range(self.GRAPH_RETRIES):
            if not failed:
                break
            failed = [owner for owner in failed if pool.write_graph(owner, snapshots[owner])]
        timing['write_ms'] = (time.perf_counter() - began) * 1000
        if failed:
            self.graph_error_count += len(failed)
            self.graph_errors = (self.graph_errors + [{'tick': tick, 'robots': failed}])[-20:]
            raise RuntimeError(f'robot graph writes failed at tick {tick}: {failed}')
        received = sorted(snapshots)
        if len(received) != 100:
            raise RuntimeError(f'incomplete robot receipts at tick {tick}: {len(received)}')
        self.graph_receipt = {'tick': tick, 'robots': received}
        if frame['record'] is not None:
            from ..global_record_client import GlobalRecordClient
            from neo4j import GraphDatabase, Query
            from neo4j.exceptions import ServiceUnavailable, SessionExpired, TransientError
            if self._step_client is None:
                self._step_client = GlobalRecordClient(self.global_writer_url, self.run_id, batch_size=128)
            record = frame['record']
            record['step'] = {'tick': tick, 'expected_robot_count': 100,
                              'received_robot_ids': received, 'complete': True, 'source': 'ray-robot-actors'}
            wait_began = time.perf_counter()
            self._step_client.submit(record)
            if self._async_step_driver is None:
                config = self._graph_config
                self._async_step_driver = GraphDatabase.driver(config['uri'], auth=(config['user'], config['password']),
                    connection_timeout=2., connection_acquisition_timeout=2., max_connection_pool_size=1)
            while True:
                try:
                    self._step_client.flush(.25)
                except TimeoutError:
                    continue  # accepted delivery remains alive; never re-submit this tick
                try:
                    with self._async_step_driver.session(database=self._graph_config['database'],
                                                         default_access_mode='READ') as session:
                        row = session.run(Query('MATCH (r:Run {id:$run}) RETURN r.latest_step AS tick', timeout=2.),
                                          run=self.run_id).single()
                    if row and row['tick'] is not None and row['tick'] >= tick:
                        with self._async_metrics_lock:
                            self._async_latest_step = row['tick']
                        break  # HTTP202 alone never releases a backlog slot
                except (ServiceUnavailable, SessionExpired, TransientError):
                    pass
                time.sleep(.05)
            timing['wait_step_ms'] = (time.perf_counter() - wait_began) * 1000
            if self.robot_context_interval and tick % self.robot_context_interval == 0:
                began = time.perf_counter()
                try:
                    rows = pool.query_context_all(self.run_id, tick, concurrency_group='graph')
                    self._record_context_rows(rows, tick)
                except Exception:
                    with self._context_lock:
                        self.context_errors += 1
                finally:
                    with self._context_lock:
                        self._async_context_windows.setdefault(max(0, tick - 1) // 7200, []).append(
                            (time.perf_counter() - began) * 1000)
                    timing['context_ms'] = (time.perf_counter() - began) * 1000

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
            self._record_context_rows(rows, self.tick)
        except Exception:
            self.context_errors += 1
        finally:
            self.kg_timing['context_ms'] = (time.perf_counter() - began) * 1000

    def _record_context_rows(self, rows, tick):
        with self._context_lock:
            self.context_errors += sum(bool(row.get('error')) for row in rows)
            self.context_samples.extend(rows)
            self._context_tick = tick
            if self.context_log_path:
                with Path(self.context_log_path).open('a', encoding='utf-8') as stream:
                    stream.write(json.dumps({'tick': tick, 'robots': rows}, sort_keys=True) + '\n')

    def async_context_windows(self):
        with self._context_lock:
            return deepcopy(self._async_context_windows)

    def graph_backpressure_status(self):
        return (self._async_graph_queue.snapshot() if self._async_graph_queue is not None
                else dict(self._async_graph_stats))

    def async_graph_timing_windows(self):
        with self._async_metrics_lock:
            return deepcopy(self._async_phase_windows)

    def graph_pause_events(self):
        with self._async_metrics_lock:
            events = deepcopy(self._graph_pause_events)
            if events and self._graph_pause_started is not None:
                events[-1]['duration_seconds'] = time.monotonic() - self._graph_pause_started
            return events

    def graph_inflight_active(self):
        """Conservative cleanup fence after a close error; never creates a new pool."""
        queue = self._async_graph_queue
        if queue is not None and queue.inflight_active():
            return True
        with self._async_metrics_lock:
            refs = list(self._async_write_refs)
            latest = self._async_latest_step
        refs.extend(ref for _, _, pending in self._graph_pending for ref in pending.values())
        if refs:
            pool = getattr(self, '_pool', None)
            if pool is None:
                return True
            try:
                _, not_ready = pool.ray.wait(refs, num_returns=len(refs), timeout=0)
            except Exception:
                return True
            if not_ready:
                return True
        if self._step_client is not None:
            health = self._step_client.health()
            if health['buffer_length'] and (health['worker_alive'] or health['closed']):
                return True
            if health['last_sent_tick'] >= 0 and (latest is None or latest < health['last_sent_tick']):
                return True  # HTTP202 may still have an uncommitted server-side write
        return False

    def _reset_after_restore(self):
        self._close_async_graph_queue()
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
        if getattr(self, 'robot_graph_async', False):
            self.flush_robot_graph()
        if self.kg_state_source != 'neo4j' and not getattr(self, 'robot_graph_async', False):
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
        with self._context_lock:
            context_errors, context_samples = self.context_errors, len(self.context_samples)
        value['robotProcesses'] = 'ray'
        value['robotGraph'] = {'latestCompleteStep': latest, 'currentTick': self.tick,
            'lag': max(0, self.tick - latest) if latest is not None else None,
            'errors': self.graph_error_count, 'lastError': self.kg_error,
            'contextErrors': context_errors, 'contextSamples': context_samples}
        stats = self.graph_backpressure_status()
        with self._async_metrics_lock:
            actual_step, max_lag = self._async_latest_step, self._async_max_lag
        if self.robot_graph_async and self.global_writer_url:
            latest = actual_step
            value['robotGraph'].update(latestCompleteStep=latest,
                lag=max(0, self.tick - latest) if latest is not None else None)
        value['robotGraph'].update({'async': self.robot_graph_async, 'pendingSteps': stats.get('pending', 0),
            'maxLagSteps': max_lag, 'pauseCount': stats.get('pause_count', 0),
            'pauseSeconds': stats.get('pause_seconds', 0.), 'backpressurePaused': stats.get('waiting', False)})
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
        self._close_async_graph_queue()  # accepted graph work must finish before actors are killed
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

    def _close_async_graph_queue(self):
        if self._async_graph_queue is not None:
            self._async_graph_queue.close(120.)
            self._async_graph_stats = self._async_graph_queue.snapshot()
            self._async_graph_queue = None
        if self._async_step_driver is not None:
            self._async_step_driver.close()
            self._async_step_driver = None

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

    def flush_robot_graph(self, timeout=120.):
        """Wait for every issued robot write; return the receipt of the last tick."""
        if self._async_graph_queue is not None:
            self._async_graph_queue.flush(timeout)
        if self._robot_graph:
            self._collect_robot_graph()
        return dict(self.graph_receipt)
