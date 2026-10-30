"""LaDe 风格分包；只使用已有 C 标签，不推断未记录的事实。"""
from __future__ import annotations

import math
from ..warehouse_graph_query import (Neo4jWarehouseGraph, ROBOT_CONTEXT_ANCHOR as ANCHOR,
                                     ROBOT_CONTEXT_QUERIES as QUERIES,
                                     RobotContextUnavailable as ContextUnavailable)

# 唯一约束与已有写者一致；复合索引支持运行隔离和时间范围。
SCHEMA = (
    "CREATE CONSTRAINT query_c_run_id IF NOT EXISTS FOR (n:Run) REQUIRE n.id IS UNIQUE",
    "CREATE INDEX query_c_step_run_tick IF NOT EXISTS FOR (n:Step) ON (n.run_id, n.tick)",
    "CREATE INDEX query_c_robot_run_id IF NOT EXISTS FOR (n:Robot) ON (n.run_id, n.robot_id)",
    "CREATE INDEX query_c_state_robot_tick IF NOT EXISTS FOR (n:AgentState) ON (n.run_id, n.robot_id, n.tick)",
    "CREATE INDEX query_c_event_robot_tick IF NOT EXISTS FOR (n:RuntimeEvent) ON (n.run_id, n.robot_id, n.tick)",
    "CREATE INDEX query_c_event_node_tick IF NOT EXISTS FOR (n:RuntimeEvent) ON (n.run_id, n.robot_node_id, n.tick)",
)
PACKS = {
    "task_decision": {
        "queries": ("step", "self_state", "recent_events", "nearby_robots"),
        "unavailable": ("pending_tasks", "task_deadlines", "task_requirements", "historical_task_ownership"),
    },
    "energy_safety": {
        "queries": ("self_state", "recent_states", "recent_events"),
        "unavailable": ("route_energy_cost", "reachable_chargers", "charger_occupancy", "safe_energy_margin"),
    },
    "request_help": {
        "queries": ("self_state", "recent_events", "help_candidates"),
        "unavailable": ("active_help_requests", "reserved_helpers", "handoff_feasibility"),
    },
    "provide_help": {
        "queries": ("self_state", "recent_events", "nearby_robots"),
        "unavailable": ("active_help_requests", "requester_priority", "payload_compatibility", "handoff_feasibility"),
    },
    "recovery": {
        "queries": ("step", "self_state", "recent_states", "recent_events"),
        "unavailable": ("failure_diagnosis", "recovery_plan", "historical_task_ownership"),
    },
}


def _integer(value, name, minimum=0):
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise ValueError(f"{name} must be an integer >= {minimum}")
    return value


def read_context(driver, run_id, robot_id, *, tick=None, state_window=400,
                 event_window=600, radius=15.0, helper_min_battery=60.0,
                 database="neo4j", require_complete=False):
    """读取完整五包。tick=None 固定一次 Run.latest_step；显式 tick 不读取未来完成步。

    最近窗口为 (t-window, t]，单位为写者 tick（不假定为秒）。
    最新采样可能早于 t；返回 observed_tick/age_ticks，禁止当作精确实时状态。
    """
    if not isinstance(run_id, str) or not run_id.strip():
        raise ValueError("run_id must be a nonempty string")
    _integer(robot_id, "robot_id")
    if tick is not None:
        _integer(tick, "tick")
    _integer(state_window, "state_window", 1)
    _integer(event_window, "event_window", 1)
    for name, value in (("radius", radius), ("helper_min_battery", helper_min_battery)):
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
            raise ValueError(f"{name} must be finite")
    if radius <= 0 or not 0 <= helper_min_battery <= 100:
        raise ValueError("radius must be positive; helper_min_battery must be in [0, 100]")

    graph = Neo4jWarehouseGraph(uri='', user='', password=None, database=database, driver=driver)
    t, rows, query_ms = graph.robot_context_rows(run_id, robot_id, tick=tick,
        state_window=state_window, event_window=event_window, require_complete=require_complete)
    own = rows["self_state"][0]["state"] if rows["self_state"] else None
    fleet = rows["fleet_states"]
    missing = ["self_state"] if own is None else []
    if not rows["step"]:
        missing.append("step")
    unavailable_robots = [r["robot_id"] for r in fleet if r["state"] is None]
    nearby, helpers = [], []
    if own and own.get("x") is not None and own.get("y") is not None:
        for row in fleet:
            state = row["state"]
            if row["robot_id"] == robot_id or state is None:
                continue
            if state.get("x") is None or state.get("y") is None:
                continue
            dx, dy = state["x"]-own["x"], state["y"]-own["y"]
            item = dict(robot_id=row["robot_id"], state=state, observed_tick=state["tick"],
                        age_ticks=t-state["tick"], distance=math.hypot(dx, dy))
            if abs(dx) < radius and abs(dy) < radius:
                nearby.append(item)
            if state.get("stage") == "idle" and state.get("battery") is not None and state["battery"] > helper_min_battery:
                helpers.append(item)
    else:
        missing.append("self_position")
    helpers.sort(key=lambda row: (row["distance"], row["robot_id"]))
    facts = dict(step=rows["step"][0]["step"] if rows["step"] else None,
                 self_state=own, recent_states=[r["state"] for r in rows["recent_states"]],
                 recent_events=[r["event"] for r in rows["recent_events"]],
                 nearby_robots=nearby, help_candidates=helpers)
    # Fleet facts are shared between nearby/helper packs; report the sum of
    # actual database queries used by each pack, without querying them twice.
    source_query = {'nearby_robots': 'fleet_states', 'help_candidates': 'fleet_states'}
    pack_ms = {name: query_ms['anchor'] + sum(query_ms[source_query.get(q, q)] for q in spec['queries'])
               for name, spec in PACKS.items()}
    pack_rows = {name: sum(len(facts[q]) if isinstance(facts[q], list) else int(facts[q] is not None)
                           for q in spec['queries']) for name, spec in PACKS.items()}
    return dict(run_id=run_id, robot_id=robot_id, tick=t,
                time_mode="latest_complete" if tick is None else "as_of",
                observed_tick=own["tick"] if own else None,
                age_ticks=t-own["tick"] if own else None,
                windows=dict(states_lower_exclusive=t-state_window, events_lower_exclusive=t-event_window,
                             upper_inclusive=t),
                missing=missing, robots_without_history=unavailable_robots,
                query_ms=query_ms, pack_ms=pack_ms, pack_rows=pack_rows,
                facts=facts, packs={name: dict(facts={q: facts[q] for q in spec["queries"]},
                    unavailable=list(spec["unavailable"])) for name, spec in PACKS.items()})
