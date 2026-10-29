from __future__ import annotations

import time
from typing import Any, Protocol

from .domain import RobotAgent


class RobotRuntime(Protocol):
    name: str

    def replace(self, snapshots: list[dict[str, Any]]) -> None: ...
    def snapshots(self) -> list[dict[str, Any]]: ...
    def assign(self, robot_id: int, assignment: dict[str, Any]) -> dict[str, Any]: ...
    def reroute_many(self, paths: dict[int, list[dict[str, float]]]) -> list[dict[str, Any]]: ...
    def admit_pickups(self, robots: list[int]) -> list[dict[str, Any]]: ...
    def propose_all(self, delta_seconds: float, sim_time: float, tick_id: int) -> list[dict[str, Any]]: ...
    def commit_all(self, proposals: list[dict[str, Any]], decisions: dict[int, bool]) -> list[dict[str, Any]]: ...
    def configure_graph_writer(self, config: dict[str, Any]) -> list[dict[str, Any]]: ...
    def close(self) -> None: ...


class LocalRobotRuntime:
    name = "local"

    def __init__(self):
        self.agents: dict[int, RobotAgent] = {}

    def replace(self, snapshots: list[dict[str, Any]]) -> None:
        self.agents = {
            int(payload["state"]["robot_id"]): RobotAgent.from_snapshot(payload)
            for payload in snapshots
        }

    def snapshots(self) -> list[dict[str, Any]]:
        return [self.agents[robot_id].snapshot() for robot_id in sorted(self.agents)]

    def assign(self, robot_id: int, assignment: dict[str, Any]) -> dict[str, Any]:
        return self.agents[robot_id].assign(assignment)

    def reroute_many(self, paths):
        return [self.agents[robot].reroute(path) for robot,path in paths.items()]

    def admit_pickups(self, robots):
        return [self.agents[robot].admit_pickup() for robot in robots]

    def propose_all(self, delta_seconds: float, sim_time: float, tick_id: int) -> list[dict[str, Any]]:
        return [
            self.agents[robot_id].propose_step(delta_seconds, sim_time, tick_id)
            for robot_id in sorted(self.agents)
        ]

    def commit_all(
        self,
        proposals: list[dict[str, Any]],
        decisions: dict[int, bool],
    ) -> list[dict[str, Any]]:
        return [
            self.agents[int(proposal["robot_id"])].commit_step(
                proposal,
                decisions[int(proposal["robot_id"])],
            )
            for proposal in proposals
        ]

    def configure_graph_writer(self, config: dict[str, Any]) -> list[dict[str, Any]]:
        """Local mode deliberately has no worker-owned graph writer.

        The dashboard's realtime graph mode is an actor isolation feature, not
        a disguised in-process writer.  Server startup rejects this mode before
        reaching this method; keeping it here makes the runtime protocol total.
        """
        raise RuntimeError("robot-actor graph writing requires the Ray backend")

    def close(self) -> None:
        self.agents.clear()


class _RayRobotActor:
    def __init__(self, snapshot: dict[str, Any]):
        self.agent = RobotAgent.from_snapshot(snapshot)
        self.graph_writer = None
        self.graph_config: dict[str, Any] | None = None
        self.graph_error: str | None = None

    def snapshot(self) -> dict[str, Any]:
        return self.agent.snapshot()

    def assign(self, assignment: dict[str, Any]) -> dict[str, Any]:
        result = self.agent.assign(assignment)
        if result["accepted"]:
            event = {
                "id": f"{assignment.get('graph_tick', 0)}:{self.agent.state.robot_id}:task_assigned:{assignment['task_id']}",
                "tick": int(assignment.get("graph_tick", 0)),
                "type": "task_assigned",
                "robot_id": self.agent.state.robot_id,
                "task_id": assignment["task_id"],
                "cargo_id": assignment["cargo_id"],
                "rack_id": assignment.get("rack_id"),
                "slot_id": assignment.get("slot_id"),
                "text": "机器人收到主协调器派发的入库任务",
                "time": 0.0,
            }
            self._write_graph(int(assignment.get("graph_tick", 0)), [event])
        result["graph_error"] = self.graph_error
        return result

    def reroute(self, path):
        return self.agent.reroute(path)

    def admit_pickup(self):
        return self.agent.admit_pickup()

    def propose_step(self, delta_seconds: float, sim_time: float, tick_id: int) -> dict[str, Any]:
        return self.agent.propose_step(delta_seconds, sim_time, tick_id)

    def commit_step(self, proposal: dict[str, Any], accepted: bool) -> dict[str, Any]:
        result = self.agent.commit_step(proposal, accepted)
        self._write_graph(int(proposal["tick_id"]), result["events"])
        result["graph_error"] = self.graph_error
        return result

    def configure_graph_writer(self, config: dict[str, Any]) -> dict[str, Any]:
        """Enable a private Neo4j writer inside this Ray worker process."""
        from .warehouse_runtime_graph import Neo4jRuntimeGraphWriter

        self.graph_config = dict(config)
        self.graph_writer = Neo4jRuntimeGraphWriter(
            uri=config["uri"], user=config["user"], password=config.get("password"),
            database=config.get("database", "neo4j"), dataset_id=config.get("dataset_id"),
            state_interval_ticks=int(config.get("state_interval_ticks", 4)),
        )
        # Schema is bootstrapped once by the server control plane before actor
        # fan-out; fall back to the writer's lazy bootstrap when called alone.
        self.graph_writer.schema_ready = bool(config.get("schema_ready", False))
        # A baseline makes the initial assignments visible even though those
        # assignments were made while the coordinator was constructing actors.
        self._write_graph(int(config.get("tick", 0)), [])
        return {"robot_id": self.agent.state.robot_id, "error": self.graph_error}

    def close(self) -> None:
        if self.graph_writer is not None:
            self.graph_writer.close()
            self.graph_writer = None

    def _write_graph(self, tick: int, events: list[dict[str, Any]]) -> None:
        if self.graph_writer is None or self.graph_config is None:
            return
        state = self.agent.state
        snapshot = {
            "runId": self.graph_config["run_id"],
            "backend": "ray-robot-actors",
            "tick": tick,
            "generatedAtMs": int(time.time() * 1000),
            "robotCount": int(self.graph_config.get("robot_count", 1)),
            "maxActiveRobots": int(self.graph_config["max_active_robots"]),
            "robots": [{
                "id": state.robot_id,
                "type": {"key": state.robot_type_key},
                "stage": state.stage,
                "battery": state.battery,
                "x": state.x,
                "y": state.y,
                "taskId": state.task_id,
                "cargoId": state.cargo_id,
            }],
            "events": events,
        }
        try:
            self.graph_writer.record_snapshot(snapshot)
            self.graph_error = None
        except Exception as exc:
            # Graph degradation must not make a robot lose its authoritative
            # dispatch/receipt path.  The error is returned with its receipt.
            self.graph_error = f"{type(exc).__name__}: {exc}"


class RayRobotRuntime:
    name = "ray"

    def __init__(
        self,
        *,
        address: str | None = None,
        num_cpus: float | None = None,
        actor_cpus: float = 0.1,
        node_ip_address: str | None = None,
    ):
        try:
            import ray
        except ImportError as exc:
            raise RuntimeError(
                "Ray backend requires the 'ray' package; install requirements.txt first"
            ) from exc
        self.ray = ray
        if not ray.is_initialized():
            init_kwargs: dict[str, Any] = {"ignore_reinit_error": True}
            if address:
                init_kwargs["address"] = address
            else:
                if num_cpus is not None:
                    init_kwargs["num_cpus"] = num_cpus
                # Local dashboard mode must not require public DNS/Internet to
                # discover an address.  Cluster attachment keeps Ray's own
                # address discovery by passing --ray-address instead.
                if node_ip_address:
                    init_kwargs["_node_ip_address"] = node_ip_address
            ray.init(**init_kwargs)
        self.actor_class = ray.remote(
            num_cpus=actor_cpus,
            max_concurrency=1,
            max_restarts=0,
        )(_RayRobotActor)
        self.actors: dict[int, Any] = {}

    def replace(self, snapshots: list[dict[str, Any]]) -> None:
        self._kill_actors()
        self.actors = {
            int(payload["state"]["robot_id"]): self.actor_class.remote(payload)
            for payload in snapshots
        }

    def snapshots(self) -> list[dict[str, Any]]:
        refs = [self.actors[robot_id].snapshot.remote() for robot_id in sorted(self.actors)]
        return self.ray.get(refs)

    def assign(self, robot_id: int, assignment: dict[str, Any]) -> dict[str, Any]:
        return self.ray.get(self.actors[robot_id].assign.remote(assignment))

    def reroute_many(self, paths):
        return self.ray.get([self.actors[robot].reroute.remote(path) for robot,path in paths.items()])

    def admit_pickups(self, robots):
        return self.ray.get([self.actors[robot].admit_pickup.remote() for robot in robots])

    def propose_all(self, delta_seconds: float, sim_time: float, tick_id: int) -> list[dict[str, Any]]:
        refs = [
            self.actors[robot_id].propose_step.remote(delta_seconds, sim_time, tick_id)
            for robot_id in sorted(self.actors)
        ]
        return self.ray.get(refs)

    def commit_all(
        self,
        proposals: list[dict[str, Any]],
        decisions: dict[int, bool],
    ) -> list[dict[str, Any]]:
        refs = [
            self.actors[int(proposal["robot_id"])].commit_step.remote(
                proposal,
                decisions[int(proposal["robot_id"])],
            )
            for proposal in proposals
        ]
        return self.ray.get(refs)

    def configure_graph_writer(self, config: dict[str, Any]) -> list[dict[str, Any]]:
        refs = [
            self.actors[robot_id].configure_graph_writer.remote(config)
            for robot_id in sorted(self.actors)
        ]
        return self.ray.get(refs)

    def close(self) -> None:
        self._kill_actors()

    def _kill_actors(self) -> None:
        for actor in self.actors.values():
            try:
                actor.close.remote()
                self.ray.kill(actor, no_restart=True)
            except Exception:
                pass
        self.actors = {}


def build_runtime(
    backend: str,
    *,
    ray_address: str | None = None,
    ray_num_cpus: float | None = None,
    ray_actor_cpus: float = 0.1,
    ray_node_ip_address: str | None = None,
) -> RobotRuntime:
    if backend == "local":
        return LocalRobotRuntime()
    if backend == "ray":
        return RayRobotRuntime(
            address=ray_address,
            num_cpus=ray_num_cpus,
            actor_cpus=ray_actor_cpus,
            node_ip_address=ray_node_ip_address,
        )
    raise ValueError(f"unsupported backend: {backend}")
