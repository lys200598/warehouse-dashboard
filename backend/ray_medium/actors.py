"""Ray actors that compute one robot motion segment per simulation tick."""
from __future__ import annotations

import ray

from ..scheduling_v3.sim import position


@ray.remote
class MotionActor:
    """Per-robot motion executor and owner of that robot's graph writer.

    The V5 coordinator still owns scheduling/inventory; this actor receives
    only the committed snapshot for its own robot and writes its own Robot,
    AgentState, Execution and RuntimeEvent subgraph.
    """

    def __init__(self, graph=None):
        self.writer = None
        if graph:
            from ..warehouse_runtime_graph import Neo4jRuntimeGraphWriter
            config = dict(graph)
            config.pop('schema_ready', None)
            self.writer = Neo4jRuntimeGraphWriter(**config)
            # The server bootstraps constraints once before fan-out.
            self.writer.schema_ready = True
        self.last_graph_error = None

    def motion(self, part: dict, at: float):
        return position(part, at), position(part, at + 1)

    def write_graph(self, snapshot: dict):
        if self.writer is None:
            return None
        try:
            self.writer.record_robot_snapshot(snapshot)
            self.last_graph_error = None
        except Exception as exc:
            self.last_graph_error = str(exc)
        return self.last_graph_error

    def close(self):
        if self.writer is not None:
            self.writer.close()
            self.writer = None


class RayMotionPool:
    def __init__(self, sim, actors):
        self.sim = sim
        self.actors = actors

    def __call__(self, now: float):
        calls = {}
        for owner, job in self.sim.jobs.items():
            at = now - job['start']
            idx = job['phase_index']
            parts = job['parts']
            while idx < len(parts) and at >= parts[idx]['end']:
                idx += 1
            if idx == len(parts):
                # The central V5 loop raises the same unprocessed-completion
                # assertion; no segment exists for this job this tick.
                continue
            calls[owner] = self.actors[owner - 1].motion.remote(parts[idx], at)
        if not calls:
            return {}
        owners = list(calls)
        results = ray.get([calls[owner] for owner in owners])
        return {owner: result for owner, result in zip(owners, results)}

    def write_graph(self, owner: int, snapshot: dict):
        return self.actors[owner - 1].write_graph.remote(snapshot)
