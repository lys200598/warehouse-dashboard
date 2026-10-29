"""V5 medium simulation with per-robot Ray motion and graph actors.

The central V5 coordinator remains authoritative for dispatch, SIPP/traffic
reservations, finite swapping, inventory and energy admission.  Each Ray actor
executes one robot's motion segment and, when graph writing is enabled, writes
only its own Robot/AgentState/Execution/RuntimeEvent subgraph.
"""
from __future__ import annotations

import math
import os
import time

from ..knowledge_runtime.sim import KnowledgeSimulation
from ..wheeled.sim import WheeledConfig, WheeledMixin
from .actors import MotionActor, RayMotionPool


class RayWideMixin:
    def __init__(self, *args, ray_address: str | None = None, ray_actor_cpus: float = 0.1,
                 robot_graph: bool = False, graph_state_interval: int = 4, **kwargs):
        if isinstance(ray_actor_cpus, bool) or not isinstance(ray_actor_cpus, (int, float)) or not math.isfinite(ray_actor_cpus) or not 0 < ray_actor_cpus <= 1:
            raise ValueError('ray_actor_cpus must be finite within (0,1]')
        if type(graph_state_interval) is not int or not 1 <= graph_state_interval <= 300:
            raise ValueError('graph_state_interval must be an integer within 1..300')
        self.motion_executor = None
        self._ray_actors = []
        self._robot_graph = bool(robot_graph)
        self._graph_refs = []
        self._graph_errors = []
        self._graph_event_cursor = 0
        self._graph_state_interval = graph_state_interval
        super().__init__(*args, **kwargs)
        import ray
        if not ray.is_initialized():
            ray.init(address=ray_address or os.environ.get('RAY_ADDRESS') or None,
                     ignore_reinit_error=True, namespace='warehouse-medium-ray')
        graph_config = self._bootstrap_robot_graph() if self._robot_graph else None
        self._ray_actors = [
            MotionActor.options(num_cpus=ray_actor_cpus).remote(graph=graph_config)
            for _ in sorted(self.robots)
        ]
        self.motion_executor = RayMotionPool(self, self._ray_actors)
        self.run_metadata['initial_backend'] = 'ray'
        self.run_metadata['ray_actor_count'] = len(self._ray_actors)
        self.run_metadata['ray_actor_cpus'] = ray_actor_cpus
        self.run_metadata['ray_scheduler'] = 'v5-central-dispatch-and-reservations'
        self.run_metadata['robot_graph_writer'] = 'ray-actors' if self._robot_graph else 'central'
        if self._robot_graph:
            self.run_metadata['robot_graph_interval'] = graph_state_interval

    def _bootstrap_robot_graph(self):
        from ..warehouse_runtime_graph import Neo4jRuntimeGraphWriter
        uri = os.environ.get('WAREHOUSE_NEO4J_URI') or os.environ.get('NEO4J_URI') or 'bolt://127.0.0.1:47687'
        user = os.environ.get('WAREHOUSE_NEO4J_USER') or os.environ.get('NEO4J_USER') or 'neo4j'
        password = os.environ.get('WAREHOUSE_NEO4J_PASSWORD') or os.environ.get('NEO4J_PASSWORD')
        database = os.environ.get('WAREHOUSE_NEO4J_DATABASE') or os.environ.get('NEO4J_DATABASE') or 'neo4j'
        dataset_id = os.environ.get('WAREHOUSE_NEO4J_DATASET_ID') or None
        if not password:
            raise ValueError('--robot-graph requires WAREHOUSE_NEO4J_PASSWORD')
        bootstrap = Neo4jRuntimeGraphWriter(
            uri=uri, user=user, password=password, database=database,
            dataset_id=dataset_id, state_interval_ticks=self._graph_state_interval)
        try:
            bootstrap.ensure_schema()
            bootstrap.ensure_run({
                'id': str(self.run_id),
                'backend': 'warehouse-real-scale-v1-ray',
                'started_at_ms': int(time.time() * 1000),
                'robot_count': len(self.robots),
                'max_active_robots': int(self.config.max_active_robots),
                'dataset_id': dataset_id,
            })
        finally:
            bootstrap.close()
        return {
            'uri': uri, 'user': user, 'password': password, 'database': database,
            'dataset_id': dataset_id, 'state_interval_ticks': self._graph_state_interval,
        }

    def step(self):
        super().step()
        if self._robot_graph:
            self._emit_robot_graph()

    def _emit_robot_graph(self):
        import ray
        pending = self.pending_events
        if len(pending) < self._graph_event_cursor:
            self._graph_event_cursor = 0
        fresh = pending[self._graph_event_cursor:]
        self._graph_event_cursor = len(pending)
        by_robot = {}
        for event in fresh:
            robot_id = event.get('robot_id')
            if robot_id is not None:
                by_robot.setdefault(int(robot_id), []).append(event)
        backend = self.frame().get('backend', 'warehouse-real-scale-v1-ray')
        now_ms = int(time.time() * 1000)
        for robot_id, robot in sorted(self.robots.items()):
            events = []
            for event in by_robot.get(robot_id, []):
                events.append({
                    'id': event.get('event_seq', event.get('event_id')),
                    'tick': event.get('tick', self.tick),
                    'type': event.get('kind', 'runtime_event'),
                    'robot_id': robot_id,
                    'task_id': event.get('task_id'),
                    'cargo_id': event.get('goods_id'),
                    'rack_id': event.get('rack_id'),
                    'slot_id': event.get('location_id'),
                    'text': str(event.get('kind', 'runtime_event')),
                    'time': event.get('sim_time', self.elapsed),
                })
            snapshot = {
                'runId': str(self.run_id),
                'backend': backend,
                'tick': int(self.tick),
                'generatedAtMs': now_ms,
                'robotCount': len(self.robots),
                'maxActiveRobots': int(self.config.max_active_robots),
                'robots': [{
                    'id': int(robot_id),
                    'type': {'key': str(robot.get('robot_type_key', 'X3'))},
                    'stage': str(robot.get('stage', 'idle')),
                    'battery': float(robot.get('battery', 0.0)),
                    'x': float(robot.get('x', 0.0)),
                    'y': float(robot.get('y', 0.0)),
                    'taskId': robot.get('task_id'),
                    'cargoId': robot.get('cargo_id'),
                }],
                'events': events,
            }
            self._graph_refs.append(self.motion_executor.write_graph(robot_id, snapshot))
        if len(self._graph_refs) >= 256:
            self._flush_graph_refs()

    def _flush_graph_refs(self):
        import ray
        if not self._graph_refs:
            return
        refs, self._graph_refs = self._graph_refs, []
        for result in ray.get(refs):
            if result:
                self._graph_errors.append(result)
        if len(self._graph_errors) > 20:
            self._graph_errors = self._graph_errors[-20:]

    def flush_robot_graph(self):
        self._flush_graph_refs()

    def frame(self):
        value = super().frame()
        value['backend'] = 'warehouse-real-scale-v1-ray'
        value['rayActorCount'] = len(self._ray_actors)
        value['robotGraphWriter'] = 'ray-actors' if self._robot_graph else 'central'
        value['robotGraphInterval'] = self._graph_state_interval if self._robot_graph else None
        value['robotGraphErrors'] = len(self._graph_errors)
        value['activation'] = {
            **value.get('activation', {}),
            'scope': 'central V5 dispatch; per-robot motion and graph writes execute on Ray actors',
        }
        return value

    def close(self):
        import ray
        if self._robot_graph:
            self.flush_robot_graph()
        for actor in getattr(self, '_ray_actors', []):
            try:
                ray.kill(actor, no_restart=True)
            except Exception:
                pass
        self._ray_actors = []
        self.motion_executor = None


class RayWheeledKnowledgeSimulation(RayWideMixin, WheeledMixin, KnowledgeSimulation):
    """V5 wheeled medium + 100 Ray motion/graph actors; no scheduler replacement."""
