"""Bounded read queries for the persistent Warehouse100 Neo4j graph.

The driver is created only on the first request. Keeping this adapter separate
from ``knowledge.py`` prevents the UI demo's in-memory data from being
mistaken for the persistent graph.
"""

from __future__ import annotations

from typing import Any, Callable
import time


class WarehouseGraphUnavailable(RuntimeError):
    """The persistent graph has not been configured or cannot be reached."""


class Neo4jWarehouseGraph:
    QUERY_PRESETS = ("run-progress", "task-trace", "robot-trace", "replay-tick")

    def query_preset(self, preset: str, *, task_id: str = "", run_id: str = "", robot_id: int = 1, tick: int = 0) -> dict[str, Any]:
        """Run only a named, bounded read query; never accept arbitrary Cypher."""
        if preset not in self.QUERY_PRESETS:
            raise ValueError(f"unsupported graph query preset: {preset}")
        started = time.perf_counter()
        if preset == "run-progress":
            if not run_id:
                raise ValueError("run_id is required for run-progress")
            result = self.run_write_progress(run_id)
        elif preset == "task-trace":
            if not task_id:
                raise ValueError("task_id is required for task-trace")
            result = self.task_execution_trace(task_id)
        elif preset == "robot-trace":
            if not run_id or robot_id < 1:
                raise ValueError("run_id and a positive robot_id are required for robot-trace")
            result = self.robot_trace_context(run_id, robot_id, tick=tick, history_limit=50)
        else:
            if not run_id or tick < 0:
                raise ValueError("run_id and a non-negative tick are required for replay-tick")
            result = {"runId": run_id, "tick": tick, "states": self.replay_states(run_id, tick)}
        return {
            "preset": preset,
            "result": result,
            "timing": {"backend_query_ms": round((time.perf_counter() - started) * 1000, 3)},
        }

    def task_list(self, limit: int = 8, dataset_id: str | None = None) -> list[dict[str, Any]]:
        dataset_id = self._resolve_dataset_id(dataset_id)
        return self._query("""MATCH (d:Dataset)-[:INCLUDES_TASK]->(t:WarehouseTask)
        WHERE $dataset_id IS NULL OR d.id = $dataset_id
        WITH t, d ORDER BY t.release_time LIMIT $limit
        OPTIONAL MATCH (e:Execution {task_id:t.task_id, dataset_id:d.id})-[:ASSIGNED_TO]->(r:Robot)
        WITH t, e, r ORDER BY coalesce(e.updated_tick,-1) DESC
        WITH t, head(collect({status:e.status, robot_id:r.robot_id})) AS latest
        RETURN t.task_id AS task_id,t.task_type AS task_type,t.goods_id AS goods_id,t.source_id AS source_id,t.target_id AS target_id,
        coalesce(latest.status,'待执行') AS status,latest.robot_id AS robot_id ORDER BY t.release_time""", dataset_id=dataset_id, limit=max(1,min(limit,30)))
    def task_execution_trace(self, task_id: str, dataset_id: str | None = None) -> dict[str, Any]:
        dataset_id = self._resolve_dataset_id(dataset_id)
        graph = self.task_subgraph(task_id, dataset_id)
        if graph is None:
            return {"taskId": task_id, "static": None, "executions": [], "events": []}
        executions = self._query("""MATCH (e:Execution {task_id:$task_id, dataset_id:$dataset_id})-[:ASSIGNED_TO]->(r:Robot)
        RETURN e { .id,.run_id,.status,.assigned_tick,.picked_tick,.completed_tick,.updated_tick,.cargo_id } AS execution,
               r { .robot_id,.battery,.stage } AS robot ORDER BY e.updated_tick DESC LIMIT 10""", task_id=task_id, dataset_id=dataset_id)
        events = self._query("""MATCH (event:RuntimeEvent {task_id:$task_id})-[:ABOUT_EXECUTION]->(:Execution {dataset_id:$dataset_id})
        OPTIONAL MATCH (event)-[:ABOUT_ROBOT]->(r:Robot)
        RETURN event { .type,.text,.tick,.time } AS event, r.robot_id AS robot_id ORDER BY event.tick LIMIT 20""", task_id=task_id, dataset_id=dataset_id)
        return {"taskId": task_id, "static": graph, "executions": executions, "events": events}

    def run_stats(self, run_id: str, limit: int = 200) -> dict[str, Any]:
        rows = self._query("""MATCH (:Run {id:$run_id})<-[:IN_RUN]-(stat:RunStat)
        RETURN stat {.*} AS stat ORDER BY stat.commit_seq DESC LIMIT $limit""",
                           run_id=run_id, limit=max(1, min(limit, 500)))
        return {'runId': run_id, 'stats': [row['stat'] for row in rows]}

    def dispatch_state(self, run_id: str, commit_seq: int | None = None) -> dict[str, Any] | None:
        rows = self._query("""MATCH (:Run {id:$run_id})<-[:IN_RUN]-(state:DispatchState {run_id:$run_id})
        WHERE $commit_seq IS NULL OR state.commit_seq = $commit_seq
        RETURN state {.*} AS state ORDER BY state.commit_seq DESC LIMIT 1""",
                           run_id=run_id, commit_seq=commit_seq)
        return rows[0]['state'] if rows else None

    def global_events(self, run_id: str, limit: int = 200) -> list[dict[str, Any]]:
        rows = self._query("""MATCH (event:RuntimeEvent {run_id:$run_id})
        WHERE event.robot_node_id IS NULL
        RETURN event {.*} AS event ORDER BY event.tick DESC, event.id DESC LIMIT $limit""",
                           run_id=run_id, limit=max(1, min(limit, 500)))
        return [row['event'] for row in rows]

    def assists(self, run_id: str, limit: int = 200) -> list[dict[str, Any]]:
        rows = self._query("""MATCH (assist:Assist {run_id:$run_id})
        RETURN assist {.*} AS assist ORDER BY assist.start_tick DESC, assist.id DESC LIMIT $limit""",
                           run_id=run_id, limit=max(1, min(limit, 500)))
        return [row['assist'] for row in rows]

    def robot_trace_context(self, run_id: str, robot_id: int, tick: int | None = None, history_limit: int = 20) -> dict[str, Any]:
        """Return a bounded, time-scoped robot trace rather than raw graph fragments."""
        history_limit = max(2, min(history_limit, 100))
        node_id = f"{run_id}:robot:{robot_id}"
        # ``0`` is the UI's "latest" default.  A positive tick asks for the
        # most recent sampled state at or before that point in time.
        requested_tick = tick if tick is not None and tick > 0 else 2_147_483_647
        latest_rows = self._query("""MATCH (s:AgentState {run_id:$run_id, robot_node_id:$robot_node_id})
        WHERE s.tick <= $tick
        RETURN s { .tick,.stage,.battery,.x,.y,.task_id,.cargo_id } AS state
        ORDER BY s.tick DESC LIMIT 1""", run_id=run_id, robot_node_id=node_id, tick=requested_tick)
        trajectory_rows = self._query("""MATCH (s:AgentState {run_id:$run_id, robot_node_id:$robot_node_id})
        WHERE s.tick <= $tick
        RETURN s { .tick,.stage,.battery,.x,.y,.task_id,.cargo_id } AS state
        ORDER BY s.tick DESC LIMIT $limit""", run_id=run_id, robot_node_id=node_id, tick=requested_tick, limit=history_limit)
        executions = self._query("""MATCH (e:Execution {run_id:$run_id})-[:ASSIGNED_TO]->(:Robot {id:$robot_node_id})
        OPTIONAL MATCH (e)-[:EXECUTES]->(task:WarehouseTask)
        RETURN e { .task_id,.status,.assigned_tick,.picked_tick,.completed_tick,.updated_tick } AS execution,
               task { .task_type,.goods_id,.source_id,.target_id,.due_time } AS task
        ORDER BY e.updated_tick DESC LIMIT 20""", run_id=run_id, robot_node_id=node_id)
        events = self._query("""MATCH (e:RuntimeEvent {run_id:$run_id})-[:ABOUT_ROBOT]->(:Robot {id:$robot_node_id})
        WHERE e.tick <= $tick
        RETURN e { .type,.text,.tick,.time,.task_id,.cargo_id } AS event
        ORDER BY e.tick DESC LIMIT 20""", run_id=run_id, robot_node_id=node_id, tick=requested_tick)
        latest = latest_rows[0]["state"] if latest_rows else None
        return {"runId": run_id, "robotId": robot_id, "requestedTick": None if requested_tick == 2_147_483_647 else requested_tick,
                "latestState": latest, "stateAgeTicks": None if latest is None or requested_tick == 2_147_483_647 else requested_tick - latest["tick"],
                "trajectory": [row["state"] for row in reversed(trajectory_rows)], "executions": executions, "events": [row["event"] for row in reversed(events)]}
    def run_write_progress(self, run_id: str) -> dict[str, Any]:
        rows = self._query("""MATCH (run:Run {id:$run_id})
        OPTIONAL MATCH (run)<-[:IN_RUN]-(state:AgentState)
        WITH run, count(state) AS states, max(state.tick) AS latest_tick
        RETURN run.id AS run_id,
        COUNT { (run)<-[:IN_RUN]-(:Robot) } AS robots,
        COUNT { (run)<-[:IN_RUN]-(:Execution) } AS executions,
        states AS states,
        COUNT { (run)<-[:IN_RUN]-(:RuntimeEvent) } AS events,
        latest_tick AS latest_tick""", run_id=run_id)
        return rows[0] if rows else {"run_id": run_id, "robots": 0, "executions": 0, "states": 0, "events": 0, "latest_tick": None}
    def replay_states(self, run_id: str, tick: int) -> list[dict[str, Any]]:
        rows = self._query("""MATCH (s:AgentState {run_id:$run_id, tick:$tick})-[:OF_ROBOT]->(r:Robot)
        RETURN s { .stage,.battery,.x,.y,.task_id,.cargo_id } AS state,
               coalesce(s.robot_id,r.robot_id) AS robot_id ORDER BY robot_id""", run_id=run_id, tick=tick)
        return [{**row["state"], "robot_id": row.get("robot_id", row["state"].get("robot_id"))} for row in rows]

    def runtime_state_trace(self, run_id: str, tick: int | None = None, robot_id: int | None = None, limit: int = 100) -> dict[str, Any]:
        """Read a textual runtime snapshot at or before a requested tick."""
        if robot_id is not None and robot_id < 1:
            raise ValueError("robot_id must be positive")
        limit = max(1, min(limit, 200))
        requested_tick = tick if tick is not None and tick >= 0 else 2_147_483_647
        # Older states carry only ``robot_node_id``; newer writer batches also
        # carry ``robot_id``.  Derive one from the other so both shapes filter
        # and render consistently.
        robot_id_expr = "coalesce(s.robot_id,toInteger(last(split(s.robot_node_id,':'))))"
        sampled = self._query(f"""MATCH (s:AgentState {{run_id:$run_id}})
        WHERE s.tick <= $requested_tick AND ($robot_id IS NULL OR {robot_id_expr} = $robot_id)
        RETURN max(s.tick) AS sampled_tick""", run_id=run_id, requested_tick=requested_tick, robot_id=robot_id)
        sampled_tick = sampled[0].get("sampled_tick") if sampled else None
        if sampled_tick is None:
            return {"runId": run_id, "requestedTick": None if requested_tick == 2_147_483_647 else requested_tick,
                    "sampledTick": None, "robotId": robot_id, "states": []}
        rows = self._query(f"""MATCH (s:AgentState {{run_id:$run_id, tick:$sampled_tick}})
        WHERE $robot_id IS NULL OR {robot_id_expr} = $robot_id
        RETURN s {{ .stage,.battery,.x,.y,.task_id,.cargo_id }} AS state,
               {robot_id_expr} AS robot_id
        ORDER BY robot_id LIMIT $limit""", run_id=run_id, sampled_tick=sampled_tick, robot_id=robot_id, limit=limit)
        states = [{**row["state"], "robot_id": row.get("robot_id", row["state"].get("robot_id"))} for row in rows]
        return {"runId": run_id, "requestedTick": None if requested_tick == 2_147_483_647 else requested_tick,
                "sampledTick": sampled_tick, "robotId": robot_id, "states": states}
    def robot_timeline(self, run_id: str, robot_id: int, start_tick: int = 0, end_tick: int = 10_000, limit: int = 200) -> dict[str, Any]:
        limit = max(1, min(limit, 1000))
        node_id = f"{run_id}:robot:{robot_id}"
        rows = self._query("""MATCH (s:AgentState)-[:OF_ROBOT]->(:Robot {id:$id})
        WHERE s.tick >= $start AND s.tick <= $end RETURN s { .tick,.stage,.battery,.x,.y,.task_id } AS state ORDER BY s.tick LIMIT $limit""", id=node_id, start=start_tick, end=end_tick, limit=limit)
        events = self._query("""MATCH (e:RuntimeEvent)-[:ABOUT_ROBOT]->(:Robot {id:$id})
        WHERE e.tick >= $start AND e.tick <= $end RETURN e { .tick,.type,.text,.task_id } AS event ORDER BY e.tick LIMIT $limit""", id=node_id, start=start_tick, end=end_tick, limit=limit)
        return {"backend":"neo4j-warehouse","runId":run_id,"robotId":robot_id,"states":[r["state"] for r in rows],"events":[r["event"] for r in events]}
    def __init__(self, *, uri: str, user: str, password: str | None, database: str = "neo4j", dataset_id: str | None = None, driver: Any | None = None, driver_factory: Callable[..., Any] | None = None):
        self.uri, self.user, self.password, self.database, self.dataset_id = uri, user, password, database, dataset_id
        self.driver, self.driver_factory = driver, driver_factory

    @property
    def configured(self) -> bool:
        return bool(self.password) or self.driver is not None

    def close(self) -> None:
        if self.driver is not None:
            self.driver.close()
            self.driver = None

    def health(self) -> dict[str, Any]:
        if not self.configured:
            return {"status": "not_configured", "backend": "neo4j-warehouse", "uri": self.uri}
        try:
            self._get_driver().verify_connectivity()
        except Exception as exc:
            return {"status": "unavailable", "backend": "neo4j-warehouse", "uri": self.uri, "detail": str(exc)}
        return {"status": "ok", "backend": "neo4j-warehouse", "uri": self.uri, "database": self.database}

    def summary(self, dataset_id: str | None = None) -> dict[str, Any]:
        dataset_id = self._resolve_dataset_id(dataset_id)
        datasets = self._query("""
            MATCH (dataset:Dataset)
            WHERE $dataset_id IS NULL OR dataset.id = $dataset_id
            RETURN dataset.id AS id, dataset.version AS version, dataset.start AS start,
                   dataset.capacity AS capacity,
                   COUNT { (dataset)-[:INCLUDES_TASK]->(:WarehouseTask) } AS task_count,
                   COUNT { (dataset)-[:INCLUDES_GOODS]->(:Goods) } AS initial_goods_count
            ORDER BY start DESC
            """, dataset_id=dataset_id)
        warehouses = self._query("""
            MATCH (warehouse:Warehouse)
            RETURN warehouse.id AS warehouse_id,
                   COUNT { (warehouse)-[:HAS_ZONE]->(:Zone) } AS zone_count,
                   COUNT { (warehouse)-[:HAS_RACK]->(:Rack) } AS rack_count,
                   COUNT { (warehouse)-[:HAS_LOCATION]->(:Location {kind: 'slot'}) } AS slot_count,
                   COUNT { (warehouse)-[:HAS_LOCATION]->(:Location) } AS location_count
            ORDER BY warehouse.layout_version
            """)
        return {"backend": "neo4j-warehouse", "datasets": datasets, "warehouses": warehouses}

    def task_subgraph(self, task_id: str, dataset_id: str | None = None) -> dict[str, Any] | None:
        dataset_id = self._resolve_dataset_id(dataset_id)
        rows = self._query("""
            MATCH (dataset:Dataset)-[:INCLUDES_TASK]->(task:WarehouseTask {task_id: $task_id})
            WHERE $dataset_id IS NULL OR dataset.id = $dataset_id
            OPTIONAL MATCH (task)-[:CONCERNS]->(goods:Goods)
            OPTIONAL MATCH (task)-[:FROM]->(source:Location)
            OPTIONAL MATCH (task)-[:TO]->(target:Location)
            RETURN dataset.id AS dataset_id,
                   task { .id, .task_id, .task_type, .goods_id, .release_time, .due_time, .source_id, .target_id } AS task,
                   goods { .id, .goods_id } AS goods,
                   source { .id, .location_id, .kind, .rack_id, .level, .face, .bay, .x, .y } AS source,
                   target { .id, .location_id, .kind, .rack_id, .level, .face, .bay, .x, .y } AS target
            ORDER BY dataset.start DESC LIMIT 1
            """, task_id=task_id, dataset_id=dataset_id)
        if not rows:
            return None
        row = rows[0]
        task, goods, source, target = row["task"], row.get("goods"), row.get("source"), row.get("target")
        nodes = [{"id": task["id"], "label": task["task_id"], "kind": "task", "properties": task}]
        edges: list[dict[str, str]] = []
        for entity, relation, kind in ((goods, "CONCERNS", "goods"), (source, "FROM", "location"), (target, "TO", "location")):
            if entity and entity.get("id"):
                nodes.append({"id": entity["id"], "label": entity.get("goods_id") or entity.get("location_id"), "kind": kind, "properties": entity})
                edges.append({"source": task["id"], "target": entity["id"], "label": relation})
        return {"backend": "neo4j-warehouse", "datasetId": row["dataset_id"], "taskId": task_id, "nodes": nodes, "edges": edges}

    def robot_execution_subgraph(self, run_id: str, robot_id: int) -> dict[str, Any] | None:
        robot_node_id = f"{run_id}:robot:{robot_id}"
        robot_rows = self._query("""
            MATCH (run:Run {id: $run_id})<-[:IN_RUN]-(robot:Robot {id: $robot_node_id})
            RETURN run { .id, .backend, .robot_count, .max_active_robots } AS run,
                   robot { .id, .robot_id, .type, .stage, .battery, .x, .y, .task_id, .cargo_id, .updated_tick } AS robot
            """, run_id=run_id, robot_node_id=robot_node_id)
        if not robot_rows:
            return None
        states = self._query("""
            MATCH (state:AgentState)-[:OF_ROBOT]->(:Robot {id: $robot_node_id})
            RETURN state { .id, .tick, .stage, .battery, .x, .y, .task_id, .cargo_id } AS state
            ORDER BY state.tick DESC LIMIT 20
            """, robot_node_id=robot_node_id)
        executions = self._query("""
            MATCH (execution:Execution)-[:ASSIGNED_TO]->(:Robot {id: $robot_node_id})
            RETURN execution { .id, .task_id, .cargo_id, .status, .updated_tick } AS execution
            ORDER BY execution.updated_tick DESC LIMIT 10
            """, robot_node_id=robot_node_id)
        events = self._query("""
            MATCH (event:RuntimeEvent)-[:ABOUT_ROBOT]->(:Robot {id: $robot_node_id})
            RETURN event { .id, .type, .text, .tick, .time, .task_id, .cargo_id } AS event
            ORDER BY event.tick DESC LIMIT 20
            """, robot_node_id=robot_node_id)
        row, run, robot = robot_rows[0], robot_rows[0]["run"], robot_rows[0]["robot"]
        nodes = [{"id": run["id"], "label": run["id"], "kind": "run", "properties": run}, {"id": robot["id"], "label": f"HR-{robot_id:03d}", "kind": "robot", "properties": robot}]
        edges = [{"source": robot["id"], "target": run["id"], "label": "IN_RUN"}]
        for item in states:
            state = item["state"]; nodes.append({"id": state["id"], "label": f"tick {state['tick']}", "kind": "state", "properties": state}); edges.append({"source": state["id"], "target": robot["id"], "label": "OF_ROBOT"})
        for item in executions:
            execution = item["execution"]; nodes.append({"id": execution["id"], "label": execution["task_id"], "kind": "execution", "properties": execution}); edges.append({"source": execution["id"], "target": robot["id"], "label": "ASSIGNED_TO"})
        for item in events:
            event = item["event"]; nodes.append({"id": event["id"], "label": event["type"], "kind": "event", "properties": event}); edges.append({"source": event["id"], "target": robot["id"], "label": "ABOUT_ROBOT"})
        return {"backend": "neo4j-warehouse", "runId": run_id, "robotId": robot_id, "nodes": nodes, "edges": edges}

    def _get_driver(self) -> Any:
        if self.driver is not None:
            return self.driver
        if not self.password:
            raise WarehouseGraphUnavailable("WAREHOUSE_NEO4J_PASSWORD is not configured")
        if self.driver_factory is None:
            try:
                from neo4j import GraphDatabase
            except ImportError as exc:
                raise WarehouseGraphUnavailable("neo4j Python driver is not installed") from exc
            self.driver_factory = GraphDatabase.driver
        self.driver = self.driver_factory(self.uri, auth=(self.user, self.password))
        return self.driver

    def _resolve_dataset_id(self, dataset_id: str | None) -> str | None:
        return dataset_id or self.dataset_id

    def _query(self, query: str, **parameters: Any) -> list[dict[str, Any]]:
        if not self.configured:
            raise WarehouseGraphUnavailable("WAREHOUSE_NEO4J_PASSWORD is not configured")
        try:
            with self._get_driver().session(database=self.database) as session:
                return session.run(query, **parameters).data()
        except WarehouseGraphUnavailable:
            raise
        except Exception as exc:
            raise WarehouseGraphUnavailable(f"Neo4j warehouse query failed: {exc}") from exc
