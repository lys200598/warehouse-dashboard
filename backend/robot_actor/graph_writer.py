"""One robot's C graph writer, independent of Ray and the simulation.

Integration: bootstrap Run and RUNTIME_SCHEMA centrally, then construct one
RobotGraphWriter per (run_id, robot_id). Call record(tick, robot, raw_events)
after marker handling; acknowledge the step only after it returns. On failure,
retry flush() (or record the identical step) before acknowledging. enqueue()
can retain subsequent steps while the caller schedules retries/backoff.

The queue is memory-only: callers need their own durable source to replay after
process restart. Calls must be serialized by the owning robot process. Existing
C id constraints are required; competing owners for one robot are unsupported.
A transaction locks Robot before checking its tick, so an older replay cannot
roll current state back. History and events are immutable by business ID; a
changed payload raises rather than silently overwriting an acknowledged fact.
"""
from collections import deque
from copy import deepcopy
from hashlib import sha256
import json

from ..warehouse_runtime_graph import RuntimeGraphUnavailable


_RUN = "MATCH (run:Run {id: $run_id}) RETURN run.id AS id"
_LOCK_ROBOT = """
MERGE (robot:Robot {id: $robot_id})
SET robot.updated_tick = coalesce(robot.updated_tick, -1)
RETURN robot.updated_tick AS tick, robot.graph_writer_payload_sha256 AS checksum
"""
_WRITE_CURRENT = """
MATCH (run:Run {id: $run_id})
MERGE (robot:Robot {id: $robot.id})
SET robot += $robot
MERGE (robot)-[:IN_RUN]->(run)
"""
_WRITE_HISTORY = """
MATCH (run:Run {id: $run_id}), (robot:Robot {id: $robot_id})
MERGE (state:AgentState {id: $state.id})
ON CREATE SET state += $state
MERGE (state)-[:IN_RUN]->(run)
MERGE (state)-[:OF_ROBOT]->(robot)
RETURN state.payload_sha256 AS checksum
"""
_WRITE_EVENTS = """
MATCH (run:Run {id: $run_id}), (robot:Robot {id: $robot_id})
UNWIND $events AS row
MERGE (event:RuntimeEvent {id: row.id})
ON CREATE SET event += row
MERGE (event)-[:IN_RUN]->(run)
MERGE (event)-[:ABOUT_ROBOT]->(robot)
RETURN event.id AS id, event.payload_sha256 AS checksum
"""
_LINK_EXECUTIONS = """
MATCH (run:Run {id: $run_id}), (robot:Robot {id: $robot_id})
UNWIND $task_ids AS task_id
MERGE (execution:Execution {id: $run_id + ':execution:' + task_id})
ON CREATE SET execution.run_id = $run_id, execution.task_id = task_id
MERGE (execution)-[:IN_RUN]->(run)
MERGE (execution)-[:ASSIGNED_TO]->(robot)
"""
_LINK_EVENT_EXECUTIONS = """
UNWIND $events AS row
MATCH (event:RuntimeEvent {id: row.id}),
      (execution:Execution {id: $run_id + ':execution:' + row.task_id})
MERGE (event)-[:ABOUT_EXECUTION]->(execution)
"""


def _json(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":"), allow_nan=False)


def _checksum(value):
    return sha256(_json(value).encode("utf-8")).hexdigest()


def _tick(value):
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError("tick must be a non-negative integer")
    return value


class RobotGraphWriter:
    """Use an injected Neo4j driver; its lifecycle belongs to the caller.

    robot accepts local.py's raw robot dict or the C dashboard robot shape.
    Events must retain their source id/event_id/event_seq; never generate an ID
    from list position or tick alone. payload_json preserves nested/extra fields.
    Current task progress is in Robot; historical progress is in AgentState and
    full RuntimeEvent payloads. Shared Execution ownership/status and Step/Run
    completion markers belong to the global writer, not this class.
    """

    def __init__(self, *, driver, run_id, robot_id, database="neo4j"):
        if not isinstance(run_id, str) or not run_id:
            raise ValueError("run_id must be a non-empty string")
        if isinstance(robot_id, bool) or not isinstance(robot_id, int) or robot_id < 0:
            raise ValueError("robot_id must be a non-negative integer")
        self.driver, self.database = driver, database
        self.run_id, self.robot_id = run_id, robot_id
        self.robot_node_id = f"{run_id}:robot:{robot_id}"
        self._pending = deque()
        self._last_enqueued = None
        self.last_confirmed_tick = None
        self.last_error = None
        self.failure_count = 0

    @property
    def pending_count(self):
        return len(self._pending)

    def enqueue(self, tick, robot, events=()):
        """Copy a complete step into memory; invalid/conflicting input is rejected.

        Steps may have gaps but must increase. Identical resubmission of a pending
        step or the most recently confirmed step is allowed. Older confirmed
        steps must be replayed through a fresh writer, not silently discarded.
        No queue eviction or automatic retry loop hides a failed step.
        """
        tick = _tick(tick)
        robot, events = deepcopy(robot), deepcopy(list(events))
        source_id = robot.get("id", robot.get("robot_id", self.robot_id))
        if source_id != self.robot_id:
            raise ValueError("snapshot belongs to a different robot")
        checksum = _checksum({"tick": tick, "robot": robot, "events": events})
        for pending in self._pending:
            if pending["tick"] == tick:
                if pending["checksum"] != checksum:
                    raise ValueError("same robot tick has different content")
                return
        if self._last_enqueued is not None and tick <= self._last_enqueued[0]:
            if (tick, checksum) == self._last_enqueued:
                return
            raise ValueError("steps must increase; conflicting or stale submission")
        robot_type = robot.get("type", robot.get("robot_type_key", "X3"))
        if isinstance(robot_type, dict):
            robot_type = robot_type["key"]
        row = {
            "id": self.robot_node_id, "run_id": self.run_id, "robot_id": self.robot_id,
            "type": robot_type, "stage": robot.get("stage", "idle"),
            "battery": robot.get("battery"), "x": robot.get("x"), "y": robot.get("y"),
            "task_id": robot.get("task_id", robot.get("taskId")),
            "cargo_id": robot.get("cargo_id", robot.get("cargoId")),
            "updated_tick": tick, "payload_json": _json(robot),
            "graph_writer_payload_sha256": checksum,
        }
        event_rows = {}
        for event in events:
            identity = event.get("event_id", event.get("id"))
            if identity is None and event.get("event_seq") is not None:
                sequence = event["event_seq"]
                if isinstance(sequence, bool) or not isinstance(sequence, (str, int)):
                    raise ValueError("event_seq must be a stable string or integer")
                identity = f"v5:{sequence}"
            if isinstance(identity, bool) or not isinstance(identity, (str, int)) or identity == "":
                raise ValueError("each event needs a stable id, event_id or event_seq")
            event_tick = _tick(event.get("tick", tick))
            if event_tick > tick:
                raise ValueError("event tick is ahead of the robot snapshot")
            # Neo4j 属性不能直接存 dict；原始 JSON 完整保留交接双方、原因与嵌套字段。
            erow = {key: value for key, value in event.items()
                    if value is None or isinstance(value, (str, int, float, bool))}
            erow.update(
                id=f"{self.run_id}:event:{identity}", event_id=str(identity),
                run_id=self.run_id, tick=event_tick,
                type=event.get("type", event.get("kind", "runtime_event")),
                robot_node_id=f"{self.run_id}:robot:{event.get('robot_id', self.robot_id)}",
                task_id=event.get("task_id"),
                cargo_id=event.get("cargo_id", event.get("goods_id")),
                slot_id=event.get("slot_id", event.get("location_id")),
                payload_json=_json(event), payload_sha256=_checksum(event),
            )
            previous = event_rows.get(erow["id"])
            if previous is not None and previous != erow:
                raise ValueError("same event id has different content")
            event_rows[erow["id"]] = erow
        state = {
            **{key: value for key, value in row.items()
               if key not in {"updated_tick", "graph_writer_payload_sha256"}},
            "id": f"{self.robot_node_id}:state:{tick}", "robot_node_id": self.robot_node_id,
            "tick": tick, "payload_sha256": _checksum(robot),
        }
        self._pending.append({"tick": tick, "checksum": checksum, "robot": row,
                              "state": state, "events": list(event_rows.values())})
        self._last_enqueued = (tick, checksum)

    def record(self, tick, robot, events=()):
        """Enqueue and flush in order; failure leaves this and later steps queued."""
        self.enqueue(tick, robot, events)
        return self.flush()

    def flush(self):
        """Retry the exact oldest batch; confirm only after transaction/session exit.

        所有节点、关系在同一事务中提交。断连时提交结果可能未知，仍保留原批次，
        下次按同一业务 ID 重放；成功确认前绝不出队，也不推进确认步号。
        """
        while self._pending:
            batch = self._pending[0]
            try:
                with self.driver.session(database=self.database) as session:
                    with session.begin_transaction() as tx:
                        self._write(tx, batch)
                        tx.commit()
            except Exception as exc:
                self.last_error = exc
                self.failure_count += 1
                raise RuntimeGraphUnavailable(
                    f"robot {self.robot_id} graph tick {batch['tick']} unconfirmed; "
                    f"{self.pending_count} pending batch(es)"
                ) from exc
            self._pending.popleft()
            self.last_confirmed_tick = batch["tick"]
            self.last_error = None
        return self.last_confirmed_tick

    def _write(self, tx, batch):
        args = {"run_id": self.run_id, "robot_id": self.robot_node_id}
        if tx.run(_RUN, run_id=self.run_id).single() is None:
            raise ValueError("Run must be bootstrapped before robot writes")
        current = tx.run(_LOCK_ROBOT, robot_id=self.robot_node_id).single()
        if current["tick"] == batch["tick"] and current["checksum"] != batch["checksum"]:
            raise ValueError("persisted robot tick has different content")
        if current["tick"] < batch["tick"]:
            tx.run(_WRITE_CURRENT, run_id=self.run_id, robot=batch["robot"]).consume()
        # 当前值每步更新；历史只在 0、4、8……步采样，不额外插入第 1 步。
        if batch["tick"] % 4 == 0:
            result = tx.run(_WRITE_HISTORY, **args, state=batch["state"]).single()
            if result["checksum"] != batch["state"]["payload_sha256"]:
                raise ValueError("persisted history has different content")
        events = batch["events"]
        if events:
            result = tx.run(_WRITE_EVENTS, **args, events=events)
            actual = {record["id"]: record["checksum"] for record in result}
            if actual != {row["id"]: row["payload_sha256"] for row in events}:
                raise ValueError("persisted event id has different content")
        tasks = sorted({str(task) for task in
                        [batch["robot"]["task_id"], *(e["task_id"] for e in events)] if task is not None})
        if tasks:
            # ASSIGNED_TO 仅表示历史参与，不覆盖跨机器人共享的当前负责人或执行状态。
            tx.run(_LINK_EXECUTIONS, **args, task_ids=tasks).consume()
        if events:
            tx.run(_LINK_EVENT_EXECUTIONS, run_id=self.run_id,
                   events=[e for e in events if e["task_id"] is not None]).consume()
