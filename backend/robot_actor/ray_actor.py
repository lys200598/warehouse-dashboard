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

from .local import RobotProcess, RobotProcessSplit

ROOT = Path(__file__).resolve().parents[2]


class RobotActor(RobotProcess):
    """RobotProcess plus this robot's own graph writer (optional)."""

    def __init__(self, robot_id, robot, energy, charge_target_soc, graph=None):
        super().__init__(robot_id, robot, energy, charge_target_soc)
        self.writer, self.last_graph_error = None, None
        if graph:
            from ..warehouse_runtime_graph import Neo4jRuntimeGraphWriter
            config = dict(graph)
            config.pop('schema_ready', None)
            self.writer = Neo4jRuntimeGraphWriter(**config)
            self.writer.schema_ready = True  # the main process bootstraps constraints once

    def write_graph(self, snapshot):
        if self.writer is None:
            return None
        for attempt in range(5):
            try:
                self.writer.record_robot_snapshot(snapshot)
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

    def close(self):
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

    def snapshots(self):
        owners = list(self.actors)
        return dict(zip(owners, self.ray.get([self.actors[o].snapshot.remote() for o in owners])))

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
    """Split engine whose robot processes are Ray actors."""
    ray_address = None
    ray_actor_cpus = 0.1

    def _make_robot_pool(self):
        return RayRobotPool(self.robots, self.energy, self.config.charge_target_soc,
                            address=self.ray_address, actor_cpus=self.ray_actor_cpus)
