"""Batched persistence of live simulation facts into the warehouse Neo4j graph."""

from __future__ import annotations

import time
from typing import Any, Callable, Iterable


RUNTIME_SCHEMA = (
    "CREATE CONSTRAINT warehouse_runtime_run_id IF NOT EXISTS FOR (n:Run) REQUIRE n.id IS UNIQUE",
    "CREATE CONSTRAINT warehouse_runtime_robot_id IF NOT EXISTS FOR (n:Robot) REQUIRE n.id IS UNIQUE",
    "CREATE CONSTRAINT warehouse_runtime_execution_id IF NOT EXISTS FOR (n:Execution) REQUIRE n.id IS UNIQUE",
    "CREATE CONSTRAINT warehouse_runtime_state_id IF NOT EXISTS FOR (n:AgentState) REQUIRE n.id IS UNIQUE",
    "CREATE CONSTRAINT warehouse_runtime_event_id IF NOT EXISTS FOR (n:RuntimeEvent) REQUIRE n.id IS UNIQUE",
    "CREATE INDEX warehouse_runtime_state_run_tick IF NOT EXISTS FOR (n:AgentState) ON (n.run_id, n.tick)",
    "CREATE INDEX warehouse_runtime_state_robot_tick IF NOT EXISTS FOR (n:AgentState) ON (n.robot_node_id, n.tick)",
    "CREATE INDEX warehouse_runtime_execution_run_task IF NOT EXISTS FOR (n:Execution) ON (n.run_id, n.task_id)",
    "CREATE INDEX warehouse_runtime_event_run_task IF NOT EXISTS FOR (n:RuntimeEvent) ON (n.run_id, n.task_id)",
)


class RuntimeGraphUnavailable(RuntimeError):
    pass


def v5_runtime_snapshot(sim: Any, events: Iterable[dict[str, Any]]) -> dict[str, Any]:
    """Adapt one committed V5 state into the dashboard graph contract.

    V5 remains the owner of layout, scheduling, charging and inventory.  This
    adapter is deliberately read-only: it only projects V5's already committed
    state into the separately queryable ``Run/Robot/AgentState`` graph model.
    """
    robots = []
    for robot_id, robot in sorted(sim.robots.items()):
        robots.append({
            "id": int(robot_id),
            "type": {"key": str(robot.get("robot_type_key", "X3"))},
            "stage": str(robot.get("stage", "idle")),
            "battery": float(robot.get("battery", 0.0)),
            "x": float(robot.get("x", 0.0)),
            "y": float(robot.get("y", 0.0)),
            "taskId": robot.get("task_id"),
            "cargoId": robot.get("cargo_id"),
        })
    mapped_events = []
    for event in events:
        # ``event_id`` already embeds the V5 run ID.  Keep a stable, local
        # sequence here because the writer scopes it under ``runId`` itself.
        event_id = event.get("event_seq", event.get("event_id"))
        mapped_events.append({
            "id": f"v5:{event_id}",
            "tick": event.get("tick", sim.tick),
            "type": event.get("kind", "runtime_event"),
            "robot_id": event.get("robot_id"),
            "task_id": event.get("task_id"),
            "cargo_id": event.get("goods_id"),
            "rack_id": event.get("rack_id"),
            "slot_id": event.get("location_id"),
            "text": str(event.get("kind", "runtime_event")),
            "time": event.get("sim_time", sim.elapsed),
        })
    return {
        "runId": str(sim.run_id),
        "backend": str(sim.run_metadata.get("backend", "warehouse-v5")),
        "tick": int(sim.tick),
        "generatedAtMs": int(time.time() * 1000),
        "maxActiveRobots": len(robots),
        "robots": robots,
        "events": mapped_events,
    }


class Neo4jRuntimeGraphWriter:
    """Persist bounded batches; callers may safely drop intermediate snapshots."""

    def __init__(self, *, uri: str, user: str, password: str | None, database: str = "neo4j", dataset_id: str | None = None, state_interval_ticks: int = 4, driver: Any | None = None, driver_factory: Callable[..., Any] | None = None):
        if state_interval_ticks < 1:
            raise ValueError("state_interval_ticks must be positive")
        self.uri, self.user, self.password, self.database, self.dataset_id = uri, user, password, database, dataset_id
        self.state_interval_ticks = state_interval_ticks
        self.driver, self.driver_factory = driver, driver_factory
        self.schema_ready = False
        self.last_run_id = ""
        self.exported_events: set[str] = set()

    @property
    def configured(self) -> bool:
        return bool(self.password) or self.driver is not None

    def close(self) -> None:
        if self.driver is not None:
            self.driver.close()
            self.driver = None

    def record_snapshot(self, snapshot: dict[str, Any]) -> None:
        """Synchronously persist one snapshot; intended for a background worker."""
        if not self.configured:
            raise RuntimeGraphUnavailable("WAREHOUSE_NEO4J_PASSWORD is not configured")
        run_id, tick = str(snapshot["runId"]), int(snapshot["tick"])
        if run_id != self.last_run_id:
            self.last_run_id, self.exported_events = run_id, set()
        driver = self._get_driver()
        try:
            with driver.session(database=self.database) as session:
                if not self.schema_ready:
                    for query in RUNTIME_SCHEMA:
                        session.run(query).consume()
                    self.schema_ready = True
                event_rows = self._event_rows(snapshot, run_id)
                write_state = tick % self.state_interval_ticks == 0 or tick == 1
                robots = self._robot_rows(snapshot, run_id) if write_state else []
                execution_rows = self._execution_rows(robots, event_rows, tick)
                if write_state:
                    session.run(_WRITE_RUN_AND_ROBOTS, run=self._run_row(snapshot), robots=robots).consume()
                    if self.dataset_id:
                        session.run(_LINK_RUN_DATASET, run_id=run_id, dataset_id=self.dataset_id).consume()
                    session.run(_WRITE_AGENT_STATES, run_id=run_id, states=self._state_rows(robots, tick)).consume()
                if execution_rows:
                    session.run(_WRITE_EXECUTIONS, run_id=run_id, dataset_id=self.dataset_id, executions=execution_rows).consume()
                    if self.dataset_id:
                        session.run(_LINK_EXECUTIONS_TO_TASKS, dataset_id=self.dataset_id, executions=execution_rows).consume()
                if event_rows:
                    session.run(_WRITE_EVENTS, run_id=run_id, events=event_rows).consume()
                    robot_events = [row for row in event_rows if row["robot_node_id"]]
                    task_events = [row for row in event_rows if row["task_id"]]
                    if robot_events:
                        session.run(_LINK_EVENTS_TO_ROBOTS, events=robot_events).consume()
                    if task_events:
                        session.run(_LINK_EVENTS_TO_EXECUTIONS, events=task_events).consume()
                    self._update_execution_statuses(session, event_rows)
                    self.exported_events.update(row["id"] for row in event_rows)
                    if len(self.exported_events) > 2_000:
                        self.exported_events = set(sorted(self.exported_events)[-1_000:])
        except Exception as exc:
            raise RuntimeGraphUnavailable(f"Neo4j runtime graph write failed: {exc}") from exc

    @staticmethod
    def _update_execution_statuses(session: Any, events: list[dict[str, Any]]) -> None:
        """Apply terminal/status transitions without conditional Cypher clauses."""
        transitions = {
            "task_assigned": ("assigned", "assigned_tick"),
            "dataset_task_assigned": ("assigned", "assigned_tick"),
            "task_picked_up": ("carrying", "picked_tick"),
            "picked": ("carrying", "picked_tick"),
            "task_completed": ("completed", "completed_tick"),
            "placed": ("completed", "completed_tick"),
            "shipped": ("completed", "completed_tick"),
        }
        for event_type, (status, tick_field) in transitions.items():
            rows = [row for row in events if row["type"] == event_type and row["task_id"]]
            if rows:
                session.run(_execution_status_query(tick_field), events=rows, status=status).consume()

    def _get_driver(self) -> Any:
        if self.driver is not None:
            return self.driver
        if not self.password:
            raise RuntimeGraphUnavailable("WAREHOUSE_NEO4J_PASSWORD is not configured")
        if self.driver_factory is None:
            try:
                from neo4j import GraphDatabase
            except ImportError as exc:
                raise RuntimeGraphUnavailable("neo4j Python driver is not installed") from exc
            self.driver_factory = GraphDatabase.driver
        self.driver = self.driver_factory(self.uri, auth=(self.user, self.password))
        return self.driver

    def _run_row(self, snapshot: dict[str, Any]) -> dict[str, Any]:
        return {"id": str(snapshot["runId"]), "backend": snapshot["backend"], "started_at_ms": snapshot["generatedAtMs"], "robot_count": len(snapshot["robots"]), "max_active_robots": snapshot["maxActiveRobots"], "dataset_id": self.dataset_id}

    @staticmethod
    def _robot_rows(snapshot: dict[str, Any], run_id: str) -> list[dict[str, Any]]:
        return [{"id": f"{run_id}:robot:{robot['id']}", "run_id": run_id, "robot_id": int(robot["id"]), "type": robot["type"]["key"], "stage": robot["stage"], "battery": robot["battery"], "x": robot["x"], "y": robot["y"], "task_id": robot.get("taskId"), "cargo_id": robot.get("cargoId"), "updated_tick": snapshot["tick"]} for robot in snapshot["robots"]]

    @staticmethod
    def _state_rows(robots: list[dict[str, Any]], tick: int) -> list[dict[str, Any]]:
        return [{"id": f"{row['id']}:state:{tick}", "run_id": row["run_id"], "robot_node_id": row["id"], "tick": tick, "stage": row["stage"], "battery": row["battery"], "x": row["x"], "y": row["y"], "task_id": row["task_id"], "cargo_id": row["cargo_id"]} for row in robots]

    def _event_rows(self, snapshot: dict[str, Any], run_id: str) -> list[dict[str, Any]]:
        rows = []
        for event in snapshot.get("events", []):
            event_id = f"{run_id}:event:{event['id']}"
            if event_id in self.exported_events:
                continue
            robot_id = event.get("robot_id")
            rows.append({"id": event_id, "event_id": event["id"], "run_id": run_id, "tick": event.get("tick"), "type": event.get("type"), "text": event.get("text"), "time": event.get("time"), "robot_node_id": f"{run_id}:robot:{robot_id}" if robot_id else None, "task_id": event.get("task_id"), "cargo_id": event.get("cargo_id"), "rack_id": event.get("rack_id"), "slot_id": event.get("slot_id")})
        return rows

    @staticmethod
    def _execution_rows(robots: list[dict[str, Any]], events: list[dict[str, Any]], tick: int) -> list[dict[str, Any]]:
        rows: dict[str, dict[str, Any]] = {}
        for row in robots:
            if row["task_id"]:
                key = f"{row['run_id']}:execution:{row['task_id']}"
                rows[key] = {"id": key, "task_id": row["task_id"], "run_id": row["run_id"], "robot_node_id": row["id"], "cargo_id": row["cargo_id"], "status": row["stage"], "updated_tick": tick}
        for event in events:
            if event.get("task_id") and event.get("robot_node_id"):
                key = f"{event['run_id']}:execution:{event['task_id']}"
                rows[key] = {"id": key, "task_id": event["task_id"], "run_id": event["run_id"], "robot_node_id": event["robot_node_id"], "cargo_id": event.get("cargo_id"), "status": event["type"], "updated_tick": event.get("tick")}
        return list(rows.values())


_WRITE_RUN_AND_ROBOTS = """
MERGE (run:Run {id: $run.id}) SET run += $run
WITH run UNWIND $robots AS row
MERGE (robot:Robot {id: row.id}) SET robot += row
MERGE (robot)-[:IN_RUN]->(run)
"""
_LINK_RUN_DATASET = """
MATCH (run:Run {id: $run_id}), (dataset:Dataset {id: $dataset_id})
MERGE (run)-[:USES_DATASET]->(dataset)
"""
_WRITE_AGENT_STATES = """
MATCH (run:Run {id: $run_id})
UNWIND $states AS row
MATCH (robot:Robot {id: row.robot_node_id})
MERGE (state:AgentState {id: row.id}) SET state += row
MERGE (state)-[:IN_RUN]->(run)
MERGE (state)-[:OF_ROBOT]->(robot)
"""
_WRITE_EXECUTIONS = """
MATCH (run:Run {id: $run_id})
UNWIND $executions AS row
MATCH (robot:Robot {id: row.robot_node_id})
MERGE (execution:Execution {id: row.id}) SET execution += row, execution.dataset_id = $dataset_id
MERGE (execution)-[:IN_RUN]->(run)
MERGE (execution)-[:ASSIGNED_TO]->(robot)
"""
_LINK_EXECUTIONS_TO_TASKS = """
UNWIND $executions AS row
MATCH (execution:Execution {id: row.id}),
      (:Dataset {id: $dataset_id})-[:INCLUDES_TASK]->(task:WarehouseTask {task_id: row.task_id})
MERGE (execution)-[:EXECUTES]->(task)
"""
_WRITE_EVENTS = """
MATCH (run:Run {id: $run_id})
UNWIND $events AS row
MERGE (event:RuntimeEvent {id: row.id}) SET event += row
MERGE (event)-[:IN_RUN]->(run)
"""
_LINK_EVENTS_TO_ROBOTS = """
UNWIND $events AS row
MATCH (event:RuntimeEvent {id: row.id}), (robot:Robot {id: row.robot_node_id})
MERGE (event)-[:ABOUT_ROBOT]->(robot)
"""
_LINK_EVENTS_TO_EXECUTIONS = """
UNWIND $events AS row
MATCH (event:RuntimeEvent {id: row.id}),
      (execution:Execution {id: row.run_id + ':execution:' + row.task_id})
MERGE (event)-[:ABOUT_EXECUTION]->(execution)
"""


def _execution_status_query(tick_field: str) -> str:
    if tick_field not in {"assigned_tick", "picked_tick", "completed_tick"}:
        raise ValueError("unsupported execution status field")
    return f"""
    UNWIND $events AS row
    MATCH (execution:Execution {{id: row.run_id + ':execution:' + row.task_id}})
    SET execution.status = $status, execution.{tick_field} = row.tick, execution.updated_tick = row.tick
    """
