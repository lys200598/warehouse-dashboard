from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import random
import time
from typing import Any
from uuid import uuid4

from .domain import (
    PACKAGE_DEPTH,
    PACKAGE_WIDTH,
    ROBOT_TYPES,
    SHELF_COLUMNS,
    SHELF_DEPTH_ROWS,
    RobotAgent,
)
from .dataset import load_initial_inventory, load_warehouse_tasks
from .runtime import RobotRuntime
from .traffic import TrafficManager


@dataclass
class SimulationConfig:
    robot_count: int = 100
    max_active_robots: int = 20
    seed: int = 20261013
    step_seconds: float = 0.25
    flow_rate: float = 48.0
    traffic_cell_size: float = 0.12
    dataset_path: str | None = None

    def validate(self) -> None:
        if not 1 <= self.robot_count <= 500:
            raise ValueError("robot_count must be between 1 and 500")
        if self.max_active_robots < 1:
            raise ValueError("max_active_robots must be positive")
        if not 0 < self.step_seconds <= 5:
            raise ValueError("step_seconds must be in (0, 5]")
        if self.flow_rate < 0:
            raise ValueError("flow_rate cannot be negative")


class SimulationCoordinator:
    """Authoritative clock, scheduler, warehouse state, and actor barrier."""

    CHECKPOINT_SCHEMA_VERSION = 1

    def __init__(self, runtime: RobotRuntime, config: SimulationConfig | None = None):
        self.runtime = runtime
        self.config = config or SimulationConfig()
        self.config.validate()
        self.traffic = TrafficManager(self.config.traffic_cell_size)
        self._initialize_new_run()

    def _initialize_new_run(self) -> None:
        self.run_id = f"warehouse-{uuid4().hex[:12]}"
        self.tick = 0
        self.elapsed = 0.0
        self.completed = 0
        self.running = True
        self.speed = 1.0
        self.task_sequence = 0
        self.task_credit = 0.0
        self.pending_priority = 0
        self.racks = self._create_racks()
        self.tasks: dict[str, dict[str, Any]] = {}
        self.events: list[dict[str, Any]] = []
        self.processed_event_ids: set[str] = set()
        robots = [
            RobotAgent.create(robot_id, self.config.seed).snapshot()
            for robot_id in range(1, self.config.robot_count + 1)
        ]
        self.runtime.replace(robots)
        self.robot_snapshots = self.runtime.snapshots()
        self.dataset_records = (
            load_warehouse_tasks(self.config.dataset_path) if self.config.dataset_path else []
        )
        self.dataset_initial_inventory = load_initial_inventory(self.config.dataset_path) if self.config.dataset_path else []
        if self.dataset_initial_inventory:
            self._apply_dataset_initial_inventory(self.dataset_initial_inventory)
        self.dataset_cursor = 0
        self.pending_dataset_tasks: list[dict[str, Any]] = []
        if self.dataset_records:
            self._record_system_event(f"已加载 CSV 任务集，共 {len(self.dataset_records)} 条")
        else:
            self._seed_initial_backlog()
            self._record_system_event("仿真已启动，初始任务队列已创建")

    def _apply_dataset_initial_inventory(self, inventory: list[dict[str, str]]) -> None:
        slots = {slot["slot_id"]: slot for rack in self.racks for slot in rack["slots"]}
        unknown = [row["location_id"] for row in inventory if row["location_id"] not in slots]
        if unknown:
            raise ValueError(f"initial inventory refers to unknown simulation slot: {unknown[0]}")
        for slot in slots.values():
            slot.update({"occupied": False, "reserved_by": None, "cargo_id": None, "placed_at": None})
        for row in inventory:
            slot = slots[row["location_id"]]
            if slot["occupied"]:
                raise ValueError(f"duplicate initial inventory slot: {row['location_id']}")
            slot.update({"occupied": True, "cargo_id": row["goods_id"]})

    def _create_racks(self) -> list[dict[str, Any]]:
        rng = random.Random(self.config.seed)
        racks: list[dict[str, Any]] = []
        rack_id = 1
        for column, x in enumerate([8.0, 13.0, 18.0, 23.0, 28.0]):
            for row, y in enumerate([4.3, 9.4, 14.5]):
                slots = []
                for level in range(1, 4):
                    for depth_row in range(SHELF_DEPTH_ROWS):
                        for bay in range(SHELF_COLUMNS):
                            occupied = rng.random() > 0.48
                            slot_id = f"R{rack_id:02d}-L{level}-C{bay + 1}-D{depth_row + 1}"
                            slots.append(
                                {
                                    "slot_id": slot_id,
                                    "rack_id": rack_id,
                                    "level": level,
                                    "bay": bay,
                                    "depth_row": depth_row,
                                    "occupied": occupied,
                                    "reserved_by": None,
                                    "cargo_id": f"ST-{rack_id:02d}{level}{bay + 1}{depth_row + 1}" if occupied else None,
                                    "color_index": rng.randrange(4),
                                    "placed_at": None,
                                }
                            )
                racks.append(
                    {
                        "id": rack_id,
                        "zone": chr(65 + row),
                        "x": x,
                        "y": y,
                        "width": 2.8,
                        "depth": 1.15,
                        "column": column,
                        "row": row,
                        "slots": slots,
                    }
                )
                rack_id += 1
        return racks

    def _seed_initial_backlog(self) -> None:
        initial = sorted(
            self.robot_snapshots,
            key=lambda item: int(item["state"]["robot_id"]),
            reverse=True,
        )[: min(self.config.max_active_robots, self.config.robot_count)]
        for payload in initial:
            self._assign_task(int(payload["state"]["robot_id"]), priority="normal")

    def step_once(self) -> dict[str, Any]:
        if not self.running:
            return self.snapshot()

        delta_seconds = self.config.step_seconds * self.speed
        self.tick += 1
        self.elapsed += delta_seconds
        self.task_credit = min(
            float(self.config.robot_count),
            self.task_credit + delta_seconds * self.config.flow_rate / 3600.0,
        )
        self._release_dataset_tasks()
        self._assign_waiting_robots()

        proposals = self.runtime.propose_all(delta_seconds, self.elapsed, self.tick)
        traffic_decision = self.traffic.resolve(proposals)
        results = self.runtime.commit_all(proposals, traffic_decision.accepted)
        self.robot_snapshots = [result["robot"] for result in results]

        for result in results:
            for event in result["events"]:
                self._process_robot_event(event)

        return self.snapshot(blocked_robot_ids=traffic_decision.blocked_robot_ids)

    def _assign_waiting_robots(self) -> None:
        ready = [
            payload
            for payload in self.robot_snapshots
            if payload["state"]["stage"] == "idle"
            and payload["state"]["task_id"] is None
            and payload["state"]["idle_timer"] <= 0
        ]
        active_count = sum(
            payload["state"]["task_id"] is not None
            for payload in self.robot_snapshots
        )
        capacity = max(0, min(self.config.max_active_robots, self.config.robot_count) - active_count)
        if capacity == 0:
            return
        ready.sort(
            key=lambda item: (
                int(item["state"]["task_round"]),
                -int(item["state"]["robot_id"]),
            )
        )
        ready = ready[:capacity]

        if self.dataset_records:
            self._assign_dataset_tasks(ready)
            return

        while ready and self.pending_priority > 0:
            payload = ready.pop(0)
            if self._assign_task(int(payload["state"]["robot_id"]), priority="high"):
                self.pending_priority -= 1

        while ready and self.task_credit >= 1.0:
            payload = ready.pop(0)
            if self._assign_task(int(payload["state"]["robot_id"]), priority="normal"):
                self.task_credit -= 1.0

    def _release_dataset_tasks(self) -> None:
        while (
            self.dataset_cursor < len(self.dataset_records)
            and self.dataset_records[self.dataset_cursor]["release_offset"] <= self.elapsed
        ):
            self.pending_dataset_tasks.append(self.dataset_records[self.dataset_cursor])
            self.dataset_cursor += 1

    def _assign_dataset_tasks(self, ready: list[dict[str, Any]]) -> None:
        while ready and self.pending_dataset_tasks:
            selection: tuple[int, int] | None = None
            for task_index, record in enumerate(self.pending_dataset_tasks):
                compatible = [
                    index for index, payload in enumerate(ready)
                    if self._robot_can_handle_dataset_task(payload, record)
                ]
                if compatible:
                    preferred = next(
                        (
                            index for index in compatible
                            if f"HR-{int(ready[index]['state']['robot_id']):03d}" == record["robot_id"]
                        ),
                        compatible[0],
                    )
                    selection = (task_index, preferred)
                    break
            if selection is None:
                return
            task_index, robot_index = selection
            record = self.pending_dataset_tasks.pop(task_index)
            payload = ready.pop(robot_index)
            if not self._assign_dataset_task(int(payload["state"]["robot_id"]), record):
                self.pending_dataset_tasks.insert(task_index, record)
                return

    def _robot_can_handle_dataset_task(self, payload: dict[str, Any], record: dict[str, Any]) -> bool:
        robot_type = ROBOT_TYPES[payload["state"]["robot_type_key"]]
        is_outbound = record.get("task_type") == "OUTBOUND"
        return int(record["level"]) <= int(robot_type["maxLevel"]) and any(
            slot["occupied"] == is_outbound and slot["reserved_by"] is None and slot["level"] <= robot_type["maxLevel"]
            for rack in self.racks for slot in rack["slots"]
        )

    def _assign_dataset_task(self, robot_id: int, record: dict[str, Any]) -> bool:
        current = self._robot_snapshot(robot_id)
        if current is None:
            return False
        robot_type = ROBOT_TYPES[current["state"]["robot_type_key"]]
        is_outbound = record.get("task_type") == "OUTBOUND"
        available = [
            (rack, slot)
            for rack in self.racks for slot in rack["slots"]
            if slot["occupied"] == is_outbound and slot["reserved_by"] is None and slot["level"] <= robot_type["maxLevel"]
        ]
        if not available:
            return False
        requested = next(
            (
                (rack, slot) for rack, slot in available
                if rack["id"] == record["rack_id"] and slot["slot_id"] == record["slot_id"]
            ),
            None,
        )
        rack, slot = requested or available[(robot_id + self.task_sequence) % len(available)]
        self.task_sequence += 1
        slot["reserved_by"] = robot_id
        assignment = self._assignment_payload(
            task_id=record["task_id"], cargo_id=record["cargo_id"], priority=record["priority"], rack=rack, slot=slot,
        )
        result = self.runtime.assign(robot_id, assignment)
        if not result["accepted"]:
            slot["reserved_by"] = None
            return False
        self.tasks[record["task_id"]] = {
            **assignment, "robot_id": robot_id, "status": "assigned", "created_at": self.elapsed,
            "task_type": record.get("task_type", "putaway"),
            "completed_at": None, "source_task": record["source"], "requested_robot_id": record["robot_id"],
            "requested_rack_id": record["rack_id"], "requested_slot_id": record["slot_id"],
        }
        self._replace_robot_snapshot(result["robot"])
        self._record_event({
            "id": f"{self.tick}:{robot_id}:dataset_task_assigned:{record['task_id']}", "tick": self.tick,
            "type": "dataset_task_assigned", "robot_id": robot_id, "task_id": record["task_id"],
            "cargo_id": record["cargo_id"], "rack_id": rack["id"], "slot_id": slot["slot_id"],
            "text": "已从 CSV 任务集分配上架任务",
        })
        return True

    def _assign_task(self, robot_id: int, priority: str) -> bool:
        current = self._robot_snapshot(robot_id)
        if current is None:
            return False
        state = current["state"]
        robot_type = ROBOT_TYPES[state["robot_type_key"]]
        available: list[tuple[dict[str, Any], dict[str, Any]]] = []
        for rack in self.racks:
            for slot in rack["slots"]:
                if (
                    slot["level"] <= robot_type["maxLevel"]
                    and not slot["occupied"]
                    and slot["reserved_by"] is None
                ):
                    available.append((rack, slot))
        preferred = [item for item in available if item[1]["level"] == robot_type["maxLevel"]]
        if preferred:
            available = preferred
        if not available:
            return False

        next_sequence = self.task_sequence + 1
        rack, slot = available[(robot_id * 7 + next_sequence * 5) % len(available)]
        self.task_sequence = next_sequence
        task_id = f"TK-{self.task_sequence:06d}"
        cargo_id = f"BX-{4000 + self.task_sequence:06d}"
        slot["reserved_by"] = robot_id
        assignment = self._assignment_payload(
            task_id=task_id,
            cargo_id=cargo_id,
            priority=priority,
            rack=rack,
            slot=slot,
        )
        result = self.runtime.assign(robot_id, assignment)
        if not result["accepted"]:
            slot["reserved_by"] = None
            return False

        self.tasks[task_id] = {
            **assignment,
            "robot_id": robot_id,
            "status": "assigned",
            "created_at": self.elapsed,
            "completed_at": None,
        }
        self._replace_robot_snapshot(result["robot"])
        self._record_event(
            {
                "id": f"{self.tick}:{robot_id}:task_assigned:{task_id}",
                "tick": self.tick,
                "type": "task_assigned",
                "robot_id": robot_id,
                "task_id": task_id,
                "cargo_id": cargo_id,
                "rack_id": rack["id"],
                "slot_id": slot["slot_id"],
                "text": f"收到{'优先' if priority == 'high' else ''}任务，前往入库台",
            }
        )
        return True

    def _assignment_payload(
        self,
        *,
        task_id: str,
        cargo_id: str,
        priority: str,
        rack: dict[str, Any],
        slot: dict[str, Any],
    ) -> dict[str, Any]:
        cell_width = (rack["width"] - 0.2) / SHELF_COLUMNS
        slot_x = rack["x"] + 0.1 + slot["bay"] * cell_width + (cell_width - PACKAGE_WIDTH) / 2
        return {
            "task_id": task_id,
            "cargo_id": cargo_id,
            "priority": priority,
            "rack_id": rack["id"],
            "slot_id": slot["slot_id"],
            "zone": rack["zone"],
            "rack_x": rack["x"],
            "rack_y": rack["y"],
            "target_x": slot_x + PACKAGE_WIDTH / 2,
            "target_y": rack["y"] + rack["depth"] + 0.57,
            "level": slot["level"],
            "bay": slot["bay"] + 1,
            "depth_row": slot["depth_row"],
        }

    def _process_robot_event(self, event: dict[str, Any]) -> None:
        event_type = event["type"]
        task_id = event.get("task_id")
        task = self.tasks.get(task_id) if task_id else None
        if task and event_type == "task_picked_up":
            task["status"] = "picked_up"
        elif task and event_type == "placement_started":
            task["status"] = "placing"
        elif task and event_type == "task_completed" and task["status"] != "completed":
            task["status"] = "completed"
            task["completed_at"] = self.elapsed
            slot = self._slot_by_id(task["slot_id"])
            if slot is not None:
                slot["occupied"] = task.get("task_type") != "OUTBOUND"
                slot["reserved_by"] = None
                slot["cargo_id"] = task["cargo_id"] if slot["occupied"] else None
                slot["color_index"] = (int(task["robot_id"]) + self.task_sequence) % 4
                slot["placed_at"] = self.elapsed
            self.completed += 1
        self._record_event(event)

    def inject_priority_task(self) -> dict[str, Any]:
        previous_task_ids = set(self.tasks)
        self.pending_priority += 1
        self._assign_waiting_robots()
        assigned = next(
            (
                task
                for task in reversed(list(self.tasks.values()))
                if task["task_id"] not in previous_task_ids
                and task["priority"] == "high"
                and task["status"] == "assigned"
            ),
            None,
        )
        if assigned:
            return {
                "status": "assigned",
                "task_id": assigned["task_id"],
                "robot_id": assigned["robot_id"],
            }
        return {"status": "queued", "pending_priority": self.pending_priority}

    def control(self, action: str, value: float | None = None) -> dict[str, Any]:
        if action == "pause":
            self.running = False
            self._record_system_event("仿真已暂停")
        elif action == "resume":
            self.running = True
            self._record_system_event("仿真已继续")
        elif action == "set_speed":
            if value not in {0.5, 1.0, 2.0, 4.0}:
                raise ValueError("speed must be one of 0.5, 1, 2, 4")
            self.speed = float(value)
            self._record_system_event(f"仿真速度已切换为 {self.speed:g}×")
        elif action == "set_flow_rate":
            if value is None or not 0 <= value <= 1000:
                raise ValueError("flow rate must be between 0 and 1000")
            self.config.flow_rate = float(value)
        elif action == "reset":
            self._initialize_new_run()
        else:
            raise ValueError(f"unsupported control action: {action}")
        return self.snapshot()

    def snapshot(self, blocked_robot_ids: list[int] | None = None) -> dict[str, Any]:
        generated_at_ms = int(time.time() * 1000)
        robots = [self._public_robot(payload) for payload in self.robot_snapshots]
        worker_pids = sorted({int(payload.get("worker_pid", 0)) for payload in self.robot_snapshots})
        return {
            "schemaVersion": 1,
            "runId": self.run_id,
            "backend": self.runtime.name,
            "tick": self.tick,
            "elapsed": round(self.elapsed, 3),
            "completed": self.completed,
            "running": self.running,
            "speed": self.speed,
            "flowRate": self.config.flow_rate,
            "maxActiveRobots": min(self.config.max_active_robots, self.config.robot_count),
            "trafficSafetyRadius": self.config.traffic_cell_size,
            "dataset": {
                "enabled": bool(self.dataset_records), "total": len(self.dataset_records),
                "released": self.dataset_cursor, "queued": len(self.pending_dataset_tasks),
            },
            "generatedAtMs": generated_at_ms,
            "robots": robots,
            "racks": [self._public_rack(rack) for rack in self.racks],
            "events": self.events[:100],
            "blockedRobotIds": blocked_robot_ids or [],
            "runtime": {
                "workerProcessCount": len(worker_pids),
                "workerPids": worker_pids,
            },
        }

    def checkpoint_payload(self) -> dict[str, Any]:
        return {
            "schema_version": self.CHECKPOINT_SCHEMA_VERSION,
            "saved_at": datetime.now(timezone.utc).isoformat(),
            "run_id": self.run_id,
            "config": asdict(self.config),
            "tick": self.tick,
            "elapsed": self.elapsed,
            "completed": self.completed,
            "running": self.running,
            "speed": self.speed,
            "task_sequence": self.task_sequence,
            "task_credit": self.task_credit,
            "pending_priority": self.pending_priority,
            "racks": self.racks,
            "tasks": self.tasks,
            "events": self.events,
            "processed_event_ids": sorted(self.processed_event_ids),
            "dataset_records": self.dataset_records,
            "dataset_cursor": self.dataset_cursor,
            "pending_dataset_tasks": self.pending_dataset_tasks,
            "robots": self.runtime.snapshots(),
        }

    def restore_checkpoint(self, payload: dict[str, Any]) -> None:
        if payload.get("schema_version") != self.CHECKPOINT_SCHEMA_VERSION:
            raise ValueError("unsupported checkpoint schema version")
        saved_config = SimulationConfig(**payload["config"])
        saved_config.validate()
        if saved_config.robot_count != self.config.robot_count:
            raise ValueError(
                f"checkpoint robot_count={saved_config.robot_count} does not match "
                f"configured robot_count={self.config.robot_count}"
            )
        self.config = saved_config
        self.traffic = TrafficManager(self.config.traffic_cell_size)
        self.run_id = payload["run_id"]
        self.tick = int(payload["tick"])
        self.elapsed = float(payload["elapsed"])
        self.completed = int(payload["completed"])
        self.running = bool(payload["running"])
        self.speed = float(payload["speed"])
        self.task_sequence = int(payload["task_sequence"])
        self.task_credit = float(payload["task_credit"])
        self.pending_priority = int(payload.get("pending_priority", 0))
        self.racks = payload["racks"]
        self.tasks = payload["tasks"]
        self.events = payload.get("events", [])
        self.processed_event_ids = set(payload.get("processed_event_ids", []))
        self.dataset_records = payload.get("dataset_records") or (
            load_warehouse_tasks(self.config.dataset_path) if self.config.dataset_path else []
        )
        self.dataset_cursor = int(payload.get("dataset_cursor", 0))
        self.pending_dataset_tasks = payload.get("pending_dataset_tasks", [])
        self.runtime.replace(payload["robots"])
        self.robot_snapshots = self.runtime.snapshots()

    def close(self) -> None:
        self.runtime.close()

    def record_system_event(self, text: str) -> None:
        self._record_system_event(text)

    def _robot_snapshot(self, robot_id: int) -> dict[str, Any] | None:
        return next(
            (payload for payload in self.robot_snapshots if int(payload["state"]["robot_id"]) == robot_id),
            None,
        )

    def _replace_robot_snapshot(self, replacement: dict[str, Any]) -> None:
        robot_id = int(replacement["state"]["robot_id"])
        for index, payload in enumerate(self.robot_snapshots):
            if int(payload["state"]["robot_id"]) == robot_id:
                self.robot_snapshots[index] = replacement
                return
        self.robot_snapshots.append(replacement)

    def _slot_by_id(self, slot_id: str) -> dict[str, Any] | None:
        for rack in self.racks:
            for slot in rack["slots"]:
                if slot["slot_id"] == slot_id:
                    return slot
        return None

    def _public_robot(self, payload: dict[str, Any]) -> dict[str, Any]:
        state = payload["state"]
        return {
            "id": state["robot_id"],
            "type": ROBOT_TYPES[state["robot_type_key"]],
            "x": round(state["x"], 4),
            "y": round(state["y"], 4),
            "heading": state["heading"],
            "stage": state["stage"],
            "speed": state["speed"],
            "battery": round(state["battery"], 3),
            "carrying": state["carrying"],
            "actionTimer": state["action_timer"],
            "actionDuration": state["action_duration"],
            "idleTimer": state["idle_timer"],
            "taskRound": state["task_round"],
            "rackId": state["rack_id"],
            "slotId": state["slot_id"],
            "level": state["level"],
            "bay": state["bay"],
            "depthRow": state["depth_row"],
            "taskId": state["task_id"],
            "cargoId": state["cargo_id"],
            "priority": state["priority"],
            "alert": state["alert"],
            "blocked": state["blocked_steps"] > 0,
            "path": state["path"],
            "pathIndex": state["path_index"],
            "stageDistance": state["stage_distance"],
            "distanceTravelled": state["distance_travelled"],
            "phase": state["phase"],
            "workerPid": payload.get("worker_pid"),
        }

    @staticmethod
    def _public_rack(rack: dict[str, Any]) -> dict[str, Any]:
        return {
            **{key: rack[key] for key in ("id", "zone", "x", "y", "width", "depth", "column", "row")},
            "slots": [
                {
                    "slotId": slot["slot_id"],
                    "rackId": slot["rack_id"],
                    "level": slot["level"],
                    "bay": slot["bay"],
                    "depthRow": slot["depth_row"],
                    "occupied": slot["occupied"],
                    "reservedBy": slot["reserved_by"],
                    "cargoId": slot["cargo_id"],
                    "colorIndex": slot["color_index"],
                    "placedAt": slot["placed_at"],
                }
                for slot in rack["slots"]
            ],
        }

    def _record_event(self, event: dict[str, Any]) -> None:
        event_id = event["id"]
        if event_id in self.processed_event_ids:
            return
        self.processed_event_ids.add(event_id)
        robot_id = event.get("robot_id")
        enriched = {
            **event,
            "time": self._format_clock(self.elapsed),
            "robot": f"HR-{int(robot_id):03d}" if robot_id else "SYSTEM",
        }
        self.events.insert(0, enriched)
        del self.events[100:]
        if len(self.processed_event_ids) > 1000:
            self.processed_event_ids = {item["id"] for item in self.events}

    def _record_system_event(self, text: str) -> None:
        self._record_event(
            {
                "id": f"{self.run_id}:{self.tick}:system:{len(self.events)}",
                "tick": self.tick,
                "type": "system",
                "robot_id": None,
                "text": text,
            }
        )

    @staticmethod
    def _format_clock(elapsed_seconds: float) -> str:
        total = 8 * 3600 + int(elapsed_seconds)
        return f"{(total // 3600) % 24:02d}:{(total % 3600) // 60:02d}:{total % 60:02d}"
