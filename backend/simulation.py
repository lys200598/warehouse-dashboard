from __future__ import annotations

from dataclasses import asdict, dataclass
from copy import deepcopy
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
    pickup_point,
    inbound_queue_point,
)
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
    inbound_layout: str = 'edge'
    initial_occupancy: float = .52

    def validate(self) -> None:
        if not 1 <= self.robot_count <= 500:
            raise ValueError("robot_count must be between 1 and 500")
        if self.max_active_robots < 1:
            raise ValueError("max_active_robots must be positive")
        if not 0 < self.step_seconds <= 5:
            raise ValueError("step_seconds must be in (0, 5]")
        if self.flow_rate < 0:
            raise ValueError("flow_rate cannot be negative")
        if self.inbound_layout not in {'edge','corner'}:
            raise ValueError('inbound_layout must be edge or corner')
        if not 0 <= self.initial_occupancy <= 1:
            raise ValueError('initial_occupancy must be in 0..1')


class SimulationCoordinator:
    """Authoritative clock, scheduler, warehouse state, and actor barrier."""

    CHECKPOINT_SCHEMA_VERSION = 1

    def __init__(self, runtime: RobotRuntime, config: SimulationConfig | None = None, *, knowledge_store=None, initialize=True, run_id=None, metadata=None):
        self.runtime = runtime
        self.knowledge_store = knowledge_store
        self.requested_run_id = run_id
        self.metadata = metadata or {}
        self.config = config or SimulationConfig()
        self.config.validate()
        self.traffic = TrafficManager(self.config.traffic_cell_size)
        if initialize:
            self._initialize_new_run()

    def _initialize_new_run(self) -> None:
        self.run_id = self.requested_run_id or f"warehouse-{uuid4().hex[:12]}"
        self.requested_run_id = None
        self.knowledge_seq = 0
        self.event_sequence = 0
        self.pending_events = []
        if self.knowledge_store is not None:
            from .knowledge import source_fingerprint
            self.run_metadata = {"source_sha256": source_fingerprint(), "domain": "warehouse-inbound",
                                 "time_unit": "simulated_seconds", "coordinate_unit": "dashboard-unit",
                                 "schema_version": 1, "dataset_origin": "live-simulation"}
            self.run_metadata['initial_backend']=self.runtime.name
            self.run_metadata.update(self.metadata)
        else:
            self.run_metadata = {}
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
        self._seed_initial_backlog()
        self._record_system_event("仿真已启动，初始任务队列已创建")
        self.persist_knowledge()

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
                            occupied = rng.random() > 1.-self.config.initial_occupancy
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
        self._assign_waiting_robots()
        self._admit_pickups()

        unroutable = self._reroute_blocked()

        proposals = self.runtime.propose_all(delta_seconds, self.elapsed, self.tick)
        traffic_decision = self.traffic.resolve(proposals)
        for robot_id in unroutable:
            traffic_decision.accepted[robot_id] = False
        if unroutable:
            # Propagate new stationary reservations after fail-closed route failures.
            stopped = [p for p in proposals if not traffic_decision.accepted[p['robot_id']]]
            from .traffic import swept_distance
            changed = True
            while changed:
                changed = False
                for p in proposals:
                    if p['moving'] and traffic_decision.accepted[p['robot_id']] and any(
                        p['robot_id'] != q['robot_id'] and swept_distance(p['from'],p['to'],q['from'],q['from']) < self.config.traffic_cell_size-1e-9
                        for q in stopped):
                        traffic_decision.accepted[p['robot_id']] = False
                        stopped.append(p)
                        changed = True
            traffic_decision.blocked_robot_ids = sorted(k for k,v in traffic_decision.accepted.items() if not v)
        results = self.runtime.commit_all(proposals, traffic_decision.accepted)
        self.robot_snapshots = [result["robot"] for result in results]

        for result in results:
            for event in result["events"]:
                self._process_robot_event(event)

        self.persist_knowledge()
        return self.snapshot(blocked_robot_ids=traffic_decision.blocked_robot_ids)

    def _reroute_blocked(self):
        from .routing import detour
        states = [p["state"] for p in self.robot_snapshots]
        paths = {}
        initial = [s for s in states if s['stage'] in {'toQueue','toPick','toRack','returning'} and not s.get('route_planned')]
        candidates = sorted((s for s in states if s.get('route_planned') and s["stage"] in {"toQueue","toPick","toRack","returning"}
                             and s["blocked_steps"] >= 4 and (s["blocked_steps"]-4)%8 == 0),
                            key=lambda s:(-s["blocked_steps"],s["robot_id"]))[:8]
        boxes = [(r['x'],r['y'],r['x']+r['width'],r['y']+r['depth']) for r in self.racks]
        failed = set()
        for state in initial+candidates:
            obstacles = [(s["x"],s["y"]) for s in states if s["robot_id"] != state["robot_id"]]
            goal = state["path"][-1]
            path = detour((state["x"],state["y"]),(goal["x"],goal["y"]),
                          [] if not state.get('route_planned') else obstacles,
                          self.config.traffic_cell_size,boxes=boxes)
            if path:
                paths[state["robot_id"]] = path
            elif not state.get('route_planned'):
                failed.add(state['robot_id'])
        if paths:
            for replacement in self.runtime.reroute_many(paths):
                self._replace_robot_snapshot(replacement)
        return failed

    def _admit_pickups(self):
        if self.config.inbound_layout != 'corner':
            return
        import math
        states=[p['state'] for p in self.robot_snapshots]
        admitted=[]
        occupied=set()
        for s in sorted(states,key=lambda s:s['robot_id']):
            if s['stage'] != 'queued':
                continue
            station=(s['pickup_x'],s['pickup_y'])
            if station in occupied:
                continue
            if any(o['robot_id'] != s['robot_id'] and (
                (o['stage'] in {'toPick','picking'} and (o['pickup_x'],o['pickup_y'])==station)
                or math.hypot(o['x']-station[0],o['y']-station[1])<.65) for o in states):
                continue
            admitted.append(s['robot_id']);occupied.add(station)
        if admitted:
            for replacement in self.runtime.admit_pickups(admitted):
                self._replace_robot_snapshot(replacement)
                s=replacement['state']
                self._record_event({'id':f'{self.tick}:{s["robot_id"]}:pickup_admitted:{s["task_id"]}',
                    'tick':self.tick,'type':'pickup_admitted','robot_id':s['robot_id'],'task_id':s['task_id'],
                    'cargo_id':s['cargo_id'],'slot_id':s['slot_id'],'text':'主进程准入入库工位'})

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

        while ready and self.pending_priority > 0:
            payload = ready.pop(0)
            if self._assign_task(int(payload["state"]["robot_id"]), priority="high"):
                self.pending_priority -= 1

        while ready and self.task_credit >= 1.0:
            payload = ready.pop(0)
            if self._assign_task(int(payload["state"]["robot_id"]), priority="normal"):
                self.task_credit -= 1.0

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
            robot_id=robot_id,
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
        robot_id: int,
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
            "pickup": pickup_point(robot_id, self.config.inbound_layout),
            "queue": inbound_queue_point(robot_id) if self.config.inbound_layout == 'corner' else None,
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
                slot["occupied"] = True
                slot["reserved_by"] = None
                slot["cargo_id"] = task["cargo_id"]
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
            self.persist_knowledge()
            return {
                "status": "assigned",
                "task_id": assigned["task_id"],
                "robot_id": assigned["robot_id"],
            }
        self.persist_knowledge()
        return {"status": "queued", "pending_priority": self.pending_priority}

    def inject_task_batch(self,count=100):
        if type(count) is not int or not 1<=count<=100:
            raise ValueError('batch count must be an integer in 1..100')
        previous=set(self.tasks)
        self.pending_priority+=count
        self._assign_waiting_robots()
        assigned=[t['task_id'] for key,t in self.tasks.items() if key not in previous]
        self._record_system_event(f'新增 {count} 项入库请求，已派发 {len(assigned)} 项')
        self.persist_knowledge()
        return {'requested':count,'assigned':len(assigned),'task_ids':assigned,'pending_priority':self.pending_priority}

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
            self._record_system_event(f"任务流量已切换为 {self.config.flow_rate:g} 单/时")
        elif action == "reset":
            self._initialize_new_run()
        else:
            raise ValueError(f"unsupported control action: {action}")
        if action != "reset":
            self.persist_knowledge()
        return self.snapshot()

    def snapshot(self, blocked_robot_ids: list[int] | None = None) -> dict[str, Any]:
        generated_at_ms = int(time.time() * 1000)
        robots = [self._public_robot(payload) for payload in self.robot_snapshots]
        worker_pids = sorted({int(payload.get("worker_pid", 0)) for payload in self.robot_snapshots})
        return {
            "schemaVersion": 1,
            "scene": {
                "inboundLayout": self.config.inbound_layout,
                "name": "角落集中入库 · 8 工位" if self.config.inbound_layout == 'corner' else "长边分布入库 · 100 工位",
                "model": "2d-kinematic-with-shelf-obstacles",
                "inbound": ({"x":.6,"y":18.55,"width":5.2,"depth":2.7} if self.config.inbound_layout == 'corner'
                            else {"x":.6,"y":18.55,"width":30.8,"depth":2.7}),
                "stations": [dict(id=i,**pickup_point(i,self.config.inbound_layout))
                             for i in range(1,9 if self.config.inbound_layout == 'corner' else self.config.robot_count+1)],
                "queueArea": {'x':.6,'y':12.7,'width':5.2,'depth':5.2} if self.config.inbound_layout=='corner' else None,
                "queuePoints": [dict(id=i,**inbound_queue_point(i)) for i in range(1,self.config.robot_count+1)] if self.config.inbound_layout=='corner' else [],
            },
            "metrics": {
                "assignedRobots": sum(p['state']['task_id'] is not None and self.tasks.get(p['state']['task_id'],{}).get('status') != 'completed' for p in self.robot_snapshots),
                "inProgressTasks": sum(t['status'] != 'completed' for t in self.tasks.values()),
                "occupiedSlots": sum(s['occupied'] for r in self.racks for s in r['slots']),
                "freeSlots": sum(not s['occupied'] and s['reserved_by'] is None for r in self.racks for s in r['slots']),
                "blockedRobots": sum(p['state']['blocked_steps'] > 0 for p in self.robot_snapshots),
                "completedRobotIds": sorted({t['robot_id'] for t in self.tasks.values() if t['status'] == 'completed'}),
            },
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
            "generatedAtMs": generated_at_ms,
            "robots": robots,
            "racks": [self._public_rack(rack) for rack in self.racks],
            "events": self.events[:100],
            "blockedRobotIds": blocked_robot_ids or [],
            "runtime": {
                "workerProcessCount": len(worker_pids),
                "workerPids": worker_pids,
            },
            "knowledge": self.knowledge_status(),
        }

    def checkpoint_payload(self) -> dict[str, Any]:
        return {
            "schema_version": self.CHECKPOINT_SCHEMA_VERSION,
            "knowledge_seq": self.knowledge_seq,
            "event_sequence": self.event_sequence,
            "run_metadata": self.run_metadata,
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
            # These are the successful barrier results, including every accepted
            # assignment/reroute. Do not issue a third 100-Actor RPC barrier merely
            # to re-read the exact same committed state at each journal commit.
            "robots": deepcopy(self.robot_snapshots),
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
        self.knowledge_seq = int(payload.get("knowledge_seq", 0))
        self.event_sequence = int(payload.get("event_sequence", 0))
        self.run_metadata = payload.get("run_metadata", {})
        self.pending_events = []
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
        self.runtime.replace(payload["robots"])
        self.robot_snapshots = self.runtime.snapshots()

    def close(self) -> None:
        self.runtime.close()

    def knowledge_status(self):
        return self.knowledge_store.status(self.run_id) if self.knowledge_store else {"backend": "none"}

    def persist_knowledge(self):
        if self.knowledge_store is None:
            self.pending_events.clear()
            return
        self.knowledge_seq += 1
        try:
            self.knowledge_store.append(self.checkpoint_payload(), self.pending_events)
        except Exception:
            self.knowledge_seq -= 1
            raise
        self.pending_events.clear()

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
        self.event_sequence += 1
        robot_id = event.get("robot_id")
        enriched = {
            **event,
            "event_id": f"{self.run_id}:{self.event_sequence}",
            "event_seq": self.event_sequence,
            "run_id": self.run_id,
            "sim_time": self.elapsed,
            "time": self._format_clock(self.elapsed),
            "robot": f"HR-{int(robot_id):03d}" if robot_id else "SYSTEM",
        }
        self.events.insert(0, enriched)
        if self.knowledge_store is not None:
            self.pending_events.append(enriched)
        del self.events[100:]
        if len(self.processed_event_ids) > 1000:
            self.processed_event_ids = {item["id"] for item in self.events}

    def _record_system_event(self, text: str) -> None:
        self._record_event(
            {
                "id": f"{self.run_id}:{self.tick}:system:{self.event_sequence + 1}",
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
