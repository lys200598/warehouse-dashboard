from __future__ import annotations

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

    def close(self) -> None:
        self.agents.clear()


class _RayRobotActor:
    def __init__(self, snapshot: dict[str, Any]):
        self.agent = RobotAgent.from_snapshot(snapshot)

    def snapshot(self) -> dict[str, Any]:
        return self.agent.snapshot()

    def assign(self, assignment: dict[str, Any]) -> dict[str, Any]:
        return self.agent.assign(assignment)

    def reroute(self, path):
        return self.agent.reroute(path)

    def admit_pickup(self):
        return self.agent.admit_pickup()

    def propose_step(self, delta_seconds: float, sim_time: float, tick_id: int) -> dict[str, Any]:
        return self.agent.propose_step(delta_seconds, sim_time, tick_id)

    def commit_step(self, proposal: dict[str, Any], accepted: bool) -> dict[str, Any]:
        return self.agent.commit_step(proposal, accepted)


class RayRobotRuntime:
    name = "ray"

    def __init__(
        self,
        *,
        address: str | None = None,
        num_cpus: float | None = None,
        actor_cpus: float = 0.1,
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
            elif num_cpus is not None:
                init_kwargs["num_cpus"] = num_cpus
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

    def close(self) -> None:
        self._kill_actors()

    def _kill_actors(self) -> None:
        for actor in self.actors.values():
            try:
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
) -> RobotRuntime:
    if backend == "local":
        return LocalRobotRuntime()
    if backend == "ray":
        return RayRobotRuntime(
            address=ray_address,
            num_cpus=ray_num_cpus,
            actor_cpus=ray_actor_cpus,
        )
    raise ValueError(f"unsupported backend: {backend}")
