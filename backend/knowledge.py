"""In-memory test knowledge graph derived from the authoritative simulation snapshot.

The public shape deliberately mirrors a future graph-database adapter: callers receive
small ``nodes``/``edges`` subgraphs, never the entire graph.  This implementation is
only test data and does not claim persistence or million-scale capacity.
"""

from __future__ import annotations

from copy import deepcopy
from typing import Any


class InMemoryKnowledgeStore:
    """Keep a bounded, queryable time series for the live demo."""

    def __init__(self, history_steps: int = 30):
        self.history_steps = history_steps
        self.run_id = ""
        self.tick = 0
        self.robots: dict[int, dict[str, Any]] = {}
        self.racks: dict[int, dict[str, Any]] = {}
        self.events: list[dict[str, Any]] = []
        self.states: dict[tuple[int, int], dict[str, Any]] = {}

    def refresh(self, snapshot: dict[str, Any]) -> None:
        """Record one snapshot without mutating the simulation-owned payload."""
        run_id = str(snapshot["runId"])
        if self.run_id and self.run_id != run_id:
            self.states.clear()
        self.run_id = run_id
        self.tick = int(snapshot["tick"])
        self.robots = {int(item["id"]): deepcopy(item) for item in snapshot["robots"]}
        self.racks = {int(item["id"]): deepcopy(item) for item in snapshot["racks"]}
        self.events = deepcopy(snapshot.get("events", []))[:100]
        for robot in self.robots.values():
            self.states[(int(robot["id"]), self.tick)] = {
                "id": f"state:{robot['id']}:{self.tick}",
                "robotId": int(robot["id"]),
                "tick": self.tick,
                "stage": robot["stage"],
                "battery": robot["battery"],
                "taskId": robot.get("taskId"),
            }
        first_retained_tick = self.tick - self.history_steps + 1
        self.states = {
            key: value for key, value in self.states.items() if key[1] >= first_retained_tick
        }

    def summary(self) -> dict[str, Any]:
        task_ids = {str(robot["taskId"]) for robot in self.robots.values() if robot.get("taskId")}
        zones = {str(rack["zone"]) for rack in self.racks.values()}
        node_count = 1 + len(self.robots) + len(self.racks) + len(zones) + len(task_ids) + len(self.events) + len(self.states)
        edge_count = (
            len(self.robots)  # Robot -> Run
            + len(self.racks)  # Rack -> Zone
            + len(task_ids)  # Task -> Robot
            + len(self.events)  # Event -> Robot/Run
            + len(self.states) * 2  # State -> Robot and State -> Step
        )
        return {
            "backend": "memory-test-data",
            "runId": self.run_id,
            "latestStep": self.tick,
            "nodeCount": node_count,
            "edgeCount": edge_count,
            "robotCount": len(self.robots),
            "eventCount": len(self.events),
            "stateCount": len(self.states),
            "retainedSteps": self.history_steps,
        }

    def robot_subgraph(self, robot_id: int) -> dict[str, Any] | None:
        robot = self.robots.get(robot_id)
        if robot is None:
            return None
        nodes: list[dict[str, Any]] = [
            {"id": f"run:{self.run_id}", "label": "Run", "title": self.run_id, "kind": "run"},
            {"id": f"robot:{robot_id}", "label": f"HR-{robot_id:03d}", "title": robot["stage"], "kind": "robot"},
        ]
        edges: list[dict[str, str]] = [
            {"source": f"robot:{robot_id}", "target": f"run:{self.run_id}", "label": "IN_RUN"}
        ]
        task_id = robot.get("taskId")
        if task_id:
            task_node = f"task:{task_id}"
            nodes.append({"id": task_node, "label": str(task_id), "title": "当前任务", "kind": "task"})
            edges.append({"source": f"robot:{robot_id}", "target": task_node, "label": "ASSIGNED"})
        rack_id = robot.get("rackId")
        if rack_id in self.racks:
            rack = self.racks[int(rack_id)]
            rack_node = f"rack:{rack_id}"
            zone_node = f"zone:{rack['zone']}"
            nodes.extend([
                {"id": rack_node, "label": f"货架 {rack_id}", "title": f"{rack['zone']} 区", "kind": "rack"},
                {"id": zone_node, "label": f"{rack['zone']} 区", "title": "仓储区域", "kind": "zone"},
            ])
            edges.extend([
                {"source": f"robot:{robot_id}", "target": rack_node, "label": "TARGETS"},
                {"source": rack_node, "target": zone_node, "label": "IN_ZONE"},
            ])
        history = sorted(
            (state for (state_robot_id, _), state in self.states.items() if state_robot_id == robot_id),
            key=lambda state: state["tick"], reverse=True,
        )[:5]
        for state in history:
            nodes.append({
                "id": state["id"], "label": f"Step {state['tick']}",
                "title": f"{state['stage']} · 电量 {state['battery']:.0f}%", "kind": "state",
            })
            edges.append({"source": state["id"], "target": f"robot:{robot_id}", "label": "OF_ROBOT"})
        robot_events = [event for event in self.events if event.get("robot_id") == robot_id][:4]
        for event in robot_events:
            event_node = f"event:{event['id']}"
            nodes.append({"id": event_node, "label": event.get("type", "event"), "title": event.get("text", ""), "kind": "event"})
            edges.append({"source": event_node, "target": f"robot:{robot_id}", "label": "ABOUT"})
        return {
            "robotId": robot_id,
            "tick": self.tick,
            "nodes": nodes,
            "edges": edges,
            "metadata": {"backend": "memory-test-data", "returnedNodes": len(nodes), "returnedEdges": len(edges)},
        }
