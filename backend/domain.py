from __future__ import annotations

from copy import deepcopy
from dataclasses import asdict, dataclass, field
import math
import os
from typing import Any


WORLD_WIDTH = 32.0
WORLD_HEIGHT = 22.0
SHELF_COLUMNS = 6
SHELF_DEPTH_ROWS = 2
PACKAGE_WIDTH = 0.28
PACKAGE_DEPTH = 0.24

ROBOT_TYPES: dict[str, dict[str, Any]] = {
    "A1": {
        "key": "A1",
        "name": "基础型 A1",
        "maxLevel": 1,
        "maxHeight": 0.55,
        "extension": 0.18,
        "accent": "#d6e0dc",
    },
    "R2": {
        "key": "R2",
        "name": "增程型 R2",
        "maxLevel": 2,
        "maxHeight": 1.05,
        "extension": 0.52,
        "accent": "#c9f46c",
    },
    "X3": {
        "key": "X3",
        "name": "长臂型 X3",
        "maxLevel": 3,
        "maxHeight": 1.55,
        "extension": 0.92,
        "accent": "#68c7ff",
    },
}

MOVING_STAGES = {"toQueue", "toPick", "toRack", "returning"}


def robot_type_key(robot_id: int) -> str:
    group = robot_id % 10
    if group < 4:
        return "A1"
    if group < 8:
        return "R2"
    return "X3"


class DeterministicRandom:
    """Small checkpoint-friendly xorshift PRNG."""

    def __init__(self, seed: int):
        self.state = seed & 0xFFFFFFFF or 0x6D2B79F5

    def random(self) -> float:
        value = self.state
        value ^= (value << 13) & 0xFFFFFFFF
        value ^= value >> 17
        value ^= (value << 5) & 0xFFFFFFFF
        self.state = value & 0xFFFFFFFF
        return self.state / 4294967296


@dataclass
class RobotState:
    robot_id: int
    robot_type_key: str
    x: float
    y: float
    heading: float = 0.0
    stage: str = "idle"
    speed: float = 1.0
    battery: float = 100.0
    carrying: bool = False
    action_timer: float = 0.0
    action_duration: float = 1.0
    idle_timer: float = 0.0
    task_round: int = 0
    rack_id: int | None = None
    slot_id: str | None = None
    zone: str = "A"
    rack_x: float = 8.0
    rack_y: float = 4.3
    target_x: float = 8.0
    target_y: float = 6.02
    level: int = 1
    bay: int = 1
    depth_row: int = 0
    task_id: str | None = None
    cargo_id: str | None = None
    task_type: str = "INBOUND"
    priority: str = "normal"
    alert: str = ""
    path: list[dict[str, float]] = field(default_factory=list)
    path_index: int = 0
    stage_distance: float = 1.0
    distance_travelled: float = 0.0
    phase: float = 0.0
    blocked_steps: int = 0
    revision: int = 0
    route_planned: bool = False
    pickup_x: float | None = None
    pickup_y: float | None = None
    queue_x: float | None = None
    queue_y: float | None = None

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "RobotState":
        fields = cls.__dataclass_fields__
        return cls(**{key: deepcopy(value) for key, value in payload.items() if key in fields})


def pickup_point(robot_id: int, layout: str = 'edge') -> dict[str, float]:
    if layout == 'corner':
        station = (robot_id-1)%8
        return {'x':1.4+(station%4)*1.05,'y':19.3+(station//4)*1.05}
    # Five shallow rows of twenty stations keep initial robot positions unique.
    column = (robot_id - 1) % 20
    row = (robot_id - 1) // 20
    return {"x": 1.55 + column * 1.48, "y": 18.9 + row * 0.46}


def outbound_point(robot_id: int) -> dict[str, float]:
    column = (robot_id - 1) % 20
    row = (robot_id - 1) // 20
    return {"x": 1.55 + column * 1.48, "y": 0.9 + row * 0.46}


def inbound_queue_point(robot_id: int) -> dict[str,float]:
    index=robot_id-1
    return {'x':.9+(index%10)*.5,'y':13.0+(index//10)*.5}


def compact_path(path: list[dict[str, float]]) -> list[dict[str, float]]:
    compacted: list[dict[str, float]] = []
    for point in path:
        if not compacted or math.hypot(
            point["x"] - compacted[-1]["x"], point["y"] - compacted[-1]["y"]
        ) > 0.08:
            compacted.append({"x": float(point["x"]), "y": float(point["y"])})
    return compacted


def build_route(
    from_x: float,
    from_y: float,
    destination: dict[str, float],
    kind: str,
    robot_id: int,
) -> list[dict[str, float]]:
    path = [{"x": from_x, "y": from_y}]
    if kind == "pickup":
        # Each robot owns a north/south feeder column between dispatch and intake.
        path.append(destination)
    elif kind == "rack":
        return_lane_x = pickup_point(robot_id)["x"] + 0.5
        approach_y = destination["y"] + 0.5 + (robot_id % 4) * 0.45
        path.extend(
            [
                {"x": return_lane_x, "y": from_y},
                {"x": return_lane_x, "y": approach_y},
                {"x": destination["x"], "y": approach_y},
                destination,
            ]
        )
    else:
        return_lane_x = outbound_point(robot_id)["x"] + 0.5
        departure_y = from_y - 0.5 - (robot_id % 4) * 0.45
        path.extend(
            [
                {"x": return_lane_x, "y": departure_y},
                {"x": return_lane_x, "y": destination["y"]},
                destination,
            ]
        )
    return compact_path(path)


def route_length(path: list[dict[str, float]]) -> float:
    return sum(
        math.hypot(path[index]["x"] - path[index - 1]["x"], path[index]["y"] - path[index - 1]["y"])
        for index in range(1, len(path))
    )


class RobotAgent:
    """State machine owned by one local worker or one Ray actor."""

    def __init__(self, state: RobotState, rng_state: int):
        self.state = state
        self.rng = DeterministicRandom(rng_state)

    @classmethod
    def create(cls, robot_id: int, seed: int) -> "RobotAgent":
        rng = DeterministicRandom(seed + robot_id * 7919)
        home = outbound_point(robot_id)
        state = RobotState(
            robot_id=robot_id,
            robot_type_key=robot_type_key(robot_id),
            x=home["x"],
            y=home["y"],
            speed=0.86 + rng.random() * 0.36,
            battery=72.0 + rng.random() * 26.0,
            phase=rng.random() * math.pi * 2,
            idle_timer=(robot_id % 20) * 0.18,
        )
        return cls(state, rng.state)

    @classmethod
    def from_snapshot(cls, payload: dict[str, Any]) -> "RobotAgent":
        return cls(
            RobotState.from_dict(payload["state"]),
            int(payload.get("rng_state", 1)),
        )

    def snapshot(self) -> dict[str, Any]:
        return {
            "state": asdict(self.state),
            "rng_state": self.rng.state,
            "worker_pid": os.getpid(),
        }

    def assign(self, assignment: dict[str, Any]) -> dict[str, Any]:
        if self.state.stage != "idle" or self.state.task_id is not None:
            return {"accepted": False, "robot": self.snapshot()}
        state = self.state
        state.task_round += 1
        state.task_id = assignment["task_id"]
        state.cargo_id = assignment["cargo_id"]
        state.task_type = assignment.get("task_type", "INBOUND")
        state.priority = assignment.get("priority", "normal")
        station = assignment.get('pickup') or pickup_point(state.robot_id)
        state.pickup_x,state.pickup_y = station['x'],station['y']
        queue=assignment.get('queue')
        state.queue_x,state.queue_y = (queue['x'],queue['y']) if queue else (None,None)
        state.rack_id = int(assignment["rack_id"])
        state.slot_id = assignment["slot_id"]
        state.zone = assignment["zone"]
        state.rack_x = float(assignment["rack_x"])
        state.rack_y = float(assignment["rack_y"])
        state.target_x = float(assignment["target_x"])
        state.target_y = float(assignment["target_y"])
        state.level = int(assignment["level"])
        state.bay = int(assignment["bay"])
        state.depth_row = int(assignment["depth_row"])
        state.alert = ""
        state.blocked_steps = 0
        self._set_moving_stage('toQueue' if queue else "toPick")
        state.revision += 1
        return {"accepted": True, "robot": self.snapshot()}

    def propose_step(self, delta_seconds: float, sim_time: float, tick_id: int) -> dict[str, Any]:
        preview = RobotAgent.from_snapshot(self.snapshot())
        start = {"x": self.state.x, "y": self.state.y}
        events = preview._advance(delta_seconds, sim_time, tick_id)
        proposed = preview.snapshot()
        end_state = proposed["state"]
        moved = math.hypot(end_state["x"] - start["x"], end_state["y"] - start["y"]) > 1e-7
        return {
            "robot_id": self.state.robot_id,
            "base_revision": self.state.revision,
            "tick_id": tick_id,
            "moving": moved,
            "stage": self.state.stage,
            "from": start,
            "to": {"x": end_state["x"], "y": end_state["y"]},
            "priority": self.state.priority,
            "blocked_steps": self.state.blocked_steps,
            "proposed": proposed,
            "events": events,
        }

    def reroute(self, path: list[dict[str, float]]) -> dict[str, Any]:
        if self.state.stage not in MOVING_STAGES or len(path) < 2:
            return self.snapshot()
        if math.hypot(path[0]["x"]-self.state.x,path[0]["y"]-self.state.y) > 1e-7:
            raise ValueError("reroute must begin at the committed position")
        destination = self.state.path[-1]
        if math.hypot(path[-1]["x"]-destination["x"],path[-1]["y"]-destination["y"]) > 1e-7:
            raise ValueError("reroute cannot change the task destination")
        self.state.path = deepcopy(path)
        self.state.path_index = 1
        self.state.stage_distance = max(.001, route_length(path))
        self.state.distance_travelled = 0.
        self.state.route_planned = True
        self.state.revision += 1
        return self.snapshot()

    def admit_pickup(self):
        if self.state.stage != 'queued':
            raise ValueError('pickup admission requires a queued robot')
        self._set_moving_stage('toPick')
        self.state.revision += 1
        return self.snapshot()

    def commit_step(self, proposal: dict[str, Any], accepted: bool) -> dict[str, Any]:
        if proposal["base_revision"] != self.state.revision:
            raise RuntimeError(
                f"stale proposal for robot {self.state.robot_id}: "
                f"base={proposal['base_revision']} current={self.state.revision}"
            )
        events: list[dict[str, Any]] = []
        if accepted:
            restored = RobotAgent.from_snapshot(proposal["proposed"])
            self.state = restored.state
            self.rng = restored.rng
            self.state.blocked_steps = 0
            if self.state.alert == "路径暂时阻塞":
                self.state.alert = ""
            events = proposal["events"]
        else:
            self.state.blocked_steps += 1
            self.state.alert = "路径暂时阻塞"
            if self.state.blocked_steps == 5 or self.state.blocked_steps % 25 == 0:
                events.append(
                    self._event(
                        proposal["tick_id"],
                        "path_blocked",
                        "路径暂时阻塞，等待通道释放",
                    )
                )
        self.state.revision += 1
        return {"robot": self.snapshot(), "events": events, "accepted": accepted}

    def _advance(self, delta_seconds: float, sim_time: float, tick_id: int) -> list[dict[str, Any]]:
        state = self.state
        events: list[dict[str, Any]] = []
        state.phase += delta_seconds * (4.5 + state.speed * 2.2)

        if state.stage in MOVING_STAGES:
            arrived = self._move_along_path(delta_seconds * state.speed)
            state.battery = max(4.0, state.battery - delta_seconds * 0.0035)
            if arrived:
                if state.stage == 'toQueue':
                    state.stage='queued'
                    events.append(self._event(tick_id,'inbound_queued','抵达入库等候位，等待工位准入'))
                elif state.stage == "toPick":
                    state.stage = "picking"
                    state.action_duration = 2.1 + self.rng.random()
                    state.action_timer = 0.0
                    text = "抵达入库台，开始识别货物" if state.task_type == "INBOUND" else "抵达货位，开始拣取货物"
                    events.append(self._event(tick_id, "pickup_started", text))
                elif state.stage == "toRack":
                    state.stage = "placing"
                    state.action_duration = 4.4
                    state.action_timer = 0.0
                    text = "抵达目标货架，开始伸臂上架" if state.task_type == "INBOUND" else "抵达出库台，开始交接货物"
                    events.append(self._event(tick_id, "placement_started", text))
                else:
                    state.stage = "charging" if state.battery < 28.0 else "idle"
                    state.idle_timer = 0.0 if state.stage == "charging" else 1.5 + self.rng.random() * 5.0
                    state.task_id = None
                    state.priority = "normal"
                    events.append(self._event(tick_id, "robot_available", "已返回调度区"))
            return events

        if state.stage in {"picking", "placing"}:
            state.action_timer += delta_seconds
            if state.action_timer >= state.action_duration:
                if state.stage == "picking":
                    state.carrying = True
                    self._set_moving_stage("toRack")
                    events.append(self._event(tick_id, "task_picked_up", f"已抓取货物 {state.cargo_id}"))
                else:
                    state.carrying = False
                    events.append(
                        self._event(
                            tick_id,
                            "task_completed",
                            (
                                f"已完成 {state.zone}-{state.rack_id:02d} / C{state.bay} / L{state.level} 上架"
                                if state.task_type == "INBOUND"
                                else f"已完成货物 {state.cargo_id} 出库交接"
                            ),
                        )
                    )
                    self._set_moving_stage("returning")
            return events

        if state.stage == "charging":
            state.battery = min(100.0, state.battery + delta_seconds * 2.8)
            if state.battery >= 88.0:
                state.stage = "idle"
                state.idle_timer = 0.0
                events.append(self._event(tick_id, "charging_completed", "充电完成，等待派单"))
            return events

        if state.stage == "warning":
            state.action_timer = max(0.0, state.action_timer - delta_seconds)
            if state.action_timer == 0:
                state.stage = "idle"
                state.alert = ""
            return events

        if state.stage == 'queued':
            return events

        state.idle_timer = max(0.0, state.idle_timer - delta_seconds)
        return events

    def _set_moving_stage(self, stage_name: str) -> None:
        state = self.state
        state.stage = stage_name
        state.route_planned = False
        if stage_name == 'toQueue':
            destination={'x':state.queue_x,'y':state.queue_y}
            kind='pickup'
        elif stage_name == "toPick":
            destination = ({'x':state.pickup_x,'y':state.pickup_y} if state.pickup_x is not None else pickup_point(state.robot_id))
            kind = "pickup"
        elif stage_name == "toRack":
            destination = {"x": state.target_x, "y": state.target_y}
            kind = "rack" if state.task_type == "INBOUND" else "outbound"
        else:
            destination = outbound_point(state.robot_id)
            kind = "outbound"
        state.path = build_route(state.x, state.y, destination, kind, state.robot_id)
        state.path_index = 1
        state.stage_distance = max(0.001, route_length(state.path))
        state.distance_travelled = 0.0

    def _move_along_path(self, amount: float) -> bool:
        state = self.state
        while amount > 0 and state.path_index < len(state.path):
            target = state.path[state.path_index]
            dx = target["x"] - state.x
            dy = target["y"] - state.y
            distance = math.hypot(dx, dy)
            if distance < 0.015:
                state.x = target["x"]
                state.y = target["y"]
                state.path_index += 1
                continue
            step = min(amount, distance)
            state.x += dx / distance * step
            state.y += dy / distance * step
            state.heading = math.atan2(dy, dx)
            state.distance_travelled += step
            amount -= step
            if step >= distance - 0.001:
                state.path_index += 1
                # One straight segment per commit: the traffic arbiter's swept
                # segment must describe the actual movement, including corners.
                break
        return state.path_index >= len(state.path)

    def _event(self, tick_id: int, event_type: str, text: str) -> dict[str, Any]:
        state = self.state
        return {
            "id": f"{tick_id}:{state.robot_id}:{event_type}",
            "tick": tick_id,
            "type": event_type,
            "robot_id": state.robot_id,
            "task_id": state.task_id,
            "cargo_id": state.cargo_id,
            "rack_id": state.rack_id,
            "slot_id": state.slot_id,
            "text": text,
        }
