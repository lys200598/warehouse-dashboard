"""LaDe 风格分包；只使用已有 C 标签，不推断未记录的事实。"""
from __future__ import annotations

import math

# 唯一约束与已有写者一致；复合索引支持运行隔离和时间范围。
SCHEMA = (
    "CREATE CONSTRAINT query_c_run_id IF NOT EXISTS FOR (n:Run) REQUIRE n.id IS UNIQUE",
    "CREATE INDEX query_c_step_run_tick IF NOT EXISTS FOR (n:Step) ON (n.run_id, n.tick)",
    "CREATE INDEX query_c_robot_run_id IF NOT EXISTS FOR (n:Robot) ON (n.run_id, n.robot_id)",
    "CREATE INDEX query_c_state_robot_tick IF NOT EXISTS FOR (n:AgentState) ON (n.run_id, n.robot_id, n.tick)",
    "CREATE INDEX query_c_event_robot_tick IF NOT EXISTS FOR (n:RuntimeEvent) ON (n.run_id, n.robot_id, n.tick)",
    "CREATE INDEX query_c_event_node_tick IF NOT EXISTS FOR (n:RuntimeEvent) ON (n.run_id, n.robot_node_id, n.tick)",
)
ANCHOR = """MATCH (run:Run {id: $run_id})
RETURN run.latest_step AS latest_step"""
QUERIES = {
    "step": """MATCH (st:Step {run_id: $run_id, tick: $t})
RETURN properties(st) AS step""",
    "self_state": """MATCH (s:AgentState {run_id: $run_id, robot_id: $robot_id})
WHERE s.tick <= $t
RETURN properties(s) AS state ORDER BY s.tick DESC, s.id LIMIT 1""",
    "recent_states": """MATCH (s:AgentState {run_id: $run_id, robot_id: $robot_id})
WHERE s.tick > $state_lower AND s.tick <= $t
RETURN properties(s) AS state ORDER BY s.tick DESC, s.id""",
    # 原型有 robot_id，正式写者有 robot_node_id。UNION 按事件去重且各走复合索引。
    "recent_events": """CALL () {
 WITH $run_id AS run_id, $robot_id AS robot_id, $event_lower AS lo, $t AS t
 MATCH (e:RuntimeEvent {run_id: run_id, robot_id: robot_id})
 WHERE e.tick > lo AND e.tick <= t
 RETURN e
 UNION
 WITH $run_id AS run_id, $robot_node_id AS robot_node_id, $event_lower AS lo, $t AS t
 MATCH (e:RuntimeEvent {run_id: run_id, robot_node_id: robot_node_id})
 WHERE e.tick > lo AND e.tick <= t
 RETURN e
}
RETURN properties(e) AS event ORDER BY e.tick DESC, e.id""",
    # Robot 只提供稳定身份；所有可变属性均来自 <= t 的 AgentState。
    "fleet_states": """MATCH (r:Robot {run_id: $run_id})
CALL (r) {
 OPTIONAL MATCH (s:AgentState {run_id: r.run_id, robot_id: r.robot_id})
 WHERE s.tick <= $t
 RETURN s ORDER BY s.tick DESC, s.id LIMIT 1
}
RETURN r.robot_id AS robot_id, properties(s) AS state ORDER BY robot_id""",
}
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


class ContextUnavailable(RuntimeError):
    """缺少运行或完成步时，禁止伪造最新上下文。"""


def _integer(value, name, minimum=0):
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise ValueError(f"{name} must be an integer >= {minimum}")
    return value


def read_context(driver, run_id, robot_id, *, tick=None, state_window=400,
                 event_window=600, radius=15.0, helper_min_battery=60.0,
                 database="neo4j"):
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

    def fetch(tx):
        anchor = tx.run(ANCHOR, run_id=run_id).data()
        if not anchor:
            raise ContextUnavailable(f"Run not found: {run_id}")
        t = tick if tick is not None else anchor[0]["latest_step"]
        if isinstance(t, bool) or not isinstance(t, int) or t < 0:
            raise ContextUnavailable("Run.latest_step is missing or invalid")
        p = dict(run_id=run_id, robot_id=robot_id, robot_node_id=f"{run_id}:robot:{robot_id}",
                 t=t, state_lower=t-state_window, event_lower=t-event_window)
        rows = {name: tx.run(query, **p).data() for name, query in QUERIES.items()}
        # 无 Step 的正式写者允许显式时刻查询；不能宣称该时刻已完整写入。
        if tick is None and len(rows["step"]) != 1:
            raise ContextUnavailable("latest_step must identify exactly one Step")
        if len(rows["step"]) > 1:
            raise ContextUnavailable("duplicate Step for run/tick")
        return t, rows

    # 一个读事务固定参数；Neo4j 非快照隔离，要求已完成历史不再回写。
    with driver.session(database=database, default_access_mode="READ") as session:
        t, rows = session.execute_read(fetch)
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
    return dict(run_id=run_id, robot_id=robot_id, tick=t,
                time_mode="latest_complete" if tick is None else "as_of",
                observed_tick=own["tick"] if own else None,
                age_ticks=t-own["tick"] if own else None,
                windows=dict(states_lower_exclusive=t-state_window, events_lower_exclusive=t-event_window,
                             upper_inclusive=t),
                missing=missing, robots_without_history=unavailable_robots,
                facts=facts, packs={name: dict(facts={q: facts[q] for q in spec["queries"]},
                    unavailable=list(spec["unavailable"])) for name, spec in PACKS.items()})
