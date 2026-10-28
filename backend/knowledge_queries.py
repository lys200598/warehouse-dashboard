"""Versioned, parameterized query catalog; no arbitrary Cypher execution."""
from __future__ import annotations

import json
import time

CATALOG_VERSION = "warehouse-v1"
CATALOG = {
    "context": "机器人在指定提交位置的状态、任务和近期事件",
    "robot_history": "单个机器人历史状态（有界分页）",
    "task": "指定任务在截止提交位置的状态",
    "slot": "指定格位在截止提交位置的占用和预约",
    "events": "按机器人/任务筛选的过程事件",
    "step": "已提交步骤摘要",
}


class WarehouseQueries:
    def __init__(self, mirror):
        self.mirror = mirror

    def execute(self, request):
        started = time.perf_counter()
        query_id = request.get("query_id", "context")
        if query_id not in CATALOG:
            raise ValueError("unknown query_id")
        run = request.get("run_id")
        if not isinstance(run, str) or not 1 <= len(run) <= 128:
            raise ValueError("run_id is required (1..128 characters)")
        limit = request.get("limit", 50)
        if type(limit) is not int or not 1 <= limit <= 200:
            raise ValueError("limit must be an integer in 1..200")
        robot = request.get("robot_id", 1)
        if type(robot) is not int or not 1 <= robot <= 500:
            raise ValueError("robot_id must be in 1..500")
        seq = request.get("as_of_seq")
        tick = request.get("as_of_tick")
        for name, value in (("as_of_seq", seq), ("as_of_tick", tick)):
            if value is not None and (type(value) is not int or value < 0):
                raise ValueError(name+" must be a nonnegative integer")
        if seq is not None and tick is not None:
            raise ValueError("choose as_of_seq OR as_of_tick")
        p = {"run": run, "robot": robot, "limit": limit,
             "task": request.get("task_id"), "slot": request.get("slot_id")}
        if query_id == "context":
            return self._context(p, seq, tick, started)
        if seq is not None:
            point = self.mirror.query("MATCH (s:WHStep {key:$key}) RETURN s.seq AS seq,s.tick AS tick", {"key":f"{run}:{seq}"})
        elif tick is not None:
            point = self.mirror.query("MATCH (s:WHStep {run_id:$run}) WHERE s.tick <= $tick RETURN s.seq AS seq,s.tick AS tick ORDER BY s.seq DESC LIMIT 1", {"run": run, "tick": tick})
        else:
            head = self.mirror.query("MATCH (r:WHRun {run_id:$run}) RETURN r.committed_seq AS seq", {"run": run})
            point = self.mirror.query("MATCH (s:WHStep {key:$key}) RETURN s.seq AS seq,s.tick AS tick",
                {"key":f"{run}:{head[0]['seq']}"}) if head else []
        if not point:
            raise ValueError("requested run/committed point does not exist")
        p["seq"] = point[0]["seq"]
        if query_id == "robot_history":
            rows = self.mirror.query("""MATCH (a:WHAgentState {run_id:$run,robot_id:$robot}) WHERE a.seq <= $seq
              RETURN a.seq AS seq,a.tick AS tick,a.sim_time AS sim_time,a.payload AS payload
              ORDER BY a.seq DESC LIMIT $limit""", p)
        elif query_id in {"task", "slot"}:
            label = "WHTaskState" if query_id == "task" else "WHSlotState"
            field = query_id+"_id"
            value = p[query_id]
            if not isinstance(value, str) or not 1 <= len(value) <= 128:
                raise ValueError(field+" is required")
            rows = self.mirror.query(f"""MATCH (a:{label} {{run_id:$run,{field}:$id}}) WHERE a.seq <= $seq
              RETURN a.seq AS seq,a.tick AS tick,a.payload AS payload ORDER BY a.seq DESC LIMIT 1""", {**p, "id": value})
        elif query_id == "events":
            rows = self._events(p, request.get("robot_id"))
        else:
            rows = self.mirror.query("MATCH (s:WHStep {key:$key}) RETURN s.seq AS seq,s.tick AS tick,s.sim_time AS sim_time,s.completed AS completed,s.running AS running,s.speed AS speed,s.flow_rate AS flow_rate", {"key":f"{run}:{p['seq']}"})
        for row in rows:
            if "payload" in row:
                row["data"] = json.loads(row.pop("payload"))
        result = {"catalog_version": CATALOG_VERSION, "query_id": query_id, "run_id": run,
                  "as_of_seq": p["seq"], "as_of_tick": point[0]["tick"], "rows": rows}
        result["client_ms"] = round((time.perf_counter()-started)*1000, 3)
        return result

    def _events(self, p, robot=None):
        # Filter before the bound; never report events later than the fixed context cutoff.
        rows = self.mirror.query("""MATCH (e:WHEvent {run_id:$run}) WHERE e.commit_seq <= $seq
          AND ($robot IS NULL OR e.robot_id=$robot) AND ($task IS NULL OR e.task_id=$task)
          RETURN e.payload AS payload ORDER BY e.event_seq DESC LIMIT $limit""", {**p, "robot": robot})
        return [json.loads(r["payload"]) for r in rows]

    def _context(self, p, seq, tick, started):
        if tick is not None:
            point=self.mirror.query('''MATCH (s:WHStep {run_id:$run}) WHERE s.tick <= $tick
                RETURN s.seq AS seq ORDER BY s.seq DESC LIMIT 1''',{'run':p['run'],'tick':tick})
            if not point:raise ValueError('requested committed point does not exist')
            seq,tick=point[0]['seq'],None
        if seq is None and tick is None:
            # Neo4j READ_COMMITTED is not a frozen statement snapshot. Pin an
            # already-observed immutable commit BEFORE opening the state-index
            # cursor; a mutable WHRun head may otherwise race a concurrent writer.
            point=self.mirror.query('MATCH (r:WHRun {run_id:$run}) RETURN r.committed_seq AS seq',{'run':p['run']})
            if not point:
                raise ValueError('requested run/committed point does not exist')
            seq=point[0]['seq']
        # One materialized read transaction, rather than recursive per-field requests.
        if seq is not None:
            cutoff = "MATCH (s:WHStep {key:$key}) WITH s.seq AS cutoff,s.tick AS cutoff_tick"
            p = {**p, "key":f"{p['run']}:{seq}"}
        elif tick is not None:
            cutoff = "MATCH (s:WHStep {run_id:$run}) WHERE s.tick <= $tick WITH s ORDER BY s.seq DESC LIMIT 1 WITH s.seq AS cutoff,s.tick AS cutoff_tick"
            p = {**p, "tick":tick}
        else:
            cutoff = "MATCH (r:WHRun {run_id:$run}) WITH r.committed_seq AS cutoff,r.tick AS cutoff_tick"
        query = cutoff + """
          // Every durable commit contains every robot. Seek its unique commit key
          // directly; a per-robot history range+sort is unnecessary at this point.
          MATCH (a:WHAgentState {key:$run+':'+toString(cutoff)+':'+toString($robot)})
          WITH a,cutoff,cutoff_tick
          CALL (a,cutoff) {
            MATCH (e:WHEvent {run_id:$run,robot_id:$robot})
            USING INDEX e:WHEvent(run_id,robot_id,commit_seq,event_seq)
            WHERE e.commit_seq <= cutoff AND e.event_seq IS NOT NULL
            WITH e ORDER BY e.commit_seq DESC,e.event_seq DESC LIMIT 20 RETURN collect(e.payload) AS event_payloads
          }
          CALL (a,cutoff) {
            OPTIONAL MATCH (t:WHTaskState {run_id:$run,task_id:a.task_id}) WHERE t.seq <= cutoff
            WITH t ORDER BY t.seq DESC LIMIT 1
            RETURN collect(t{.seq,.tick,.payload}) AS task_rows
          }
          RETURN cutoff,cutoff_tick,a.seq AS seq,a.tick AS tick,a.sim_time AS sim_time,
                 a.payload AS payload,event_payloads,task_rows
        """
        rows = self.mirror.query(query, p)
        if not rows:
            raise ValueError("requested run/robot/committed point does not exist")
        row = rows[0]
        tasks = [{"seq":t["seq"],"tick":t["tick"],"data":json.loads(t["payload"])} for t in row["task_rows"] if t and t.get("payload")]
        return {"catalog_version":CATALOG_VERSION,"query_id":"context","run_id":p["run"],
                "as_of_seq":row["cutoff"],"as_of_tick":row["cutoff_tick"],
                "rows":[{"seq":row["seq"],"tick":row["tick"],"sim_time":row["sim_time"],"data":json.loads(row["payload"])}],
                "events":[json.loads(e) for e in row["event_payloads"]],"task":tasks,
                "client_ms":round((time.perf_counter()-started)*1000,3)}
