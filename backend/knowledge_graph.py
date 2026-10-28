"""Warehouse-only labels: never read/delete/modify LaDe nodes."""
from __future__ import annotations

import json
import os
from .knowledge import KnowledgeError, digest


def properties(value):
    return {k: v for k, v in value.items() if v is None or isinstance(v, (str, int, float, bool))}


class Neo4jMirror:
    def __init__(self, driver, database="neo4j"):
        self.driver = driver
        self.database = database

    @classmethod
    def from_env(cls):
        from neo4j import GraphDatabase
        driver = GraphDatabase.driver(os.environ["NEO4J_URI"],
            auth=(os.environ.get("NEO4J_USER", "neo4j"), os.environ["NEO4J_PASSWORD"]),
            max_connection_pool_size=120, connection_timeout=10, connection_acquisition_timeout=30,
            max_transaction_retry_time=10)
        driver.verify_connectivity()
        mirror = cls(driver, os.environ.get("NEO4J_DATABASE", "neo4j"))
        mirror.ensure_schema()
        return mirror

    def ensure_schema(self):
        with self.driver.session(database=self.database) as session:
            for label in ("WHStep", "WHRobot", "WHAgentState", "WHTask", "WHTaskState", "WHSlot", "WHSlotState", "WHRack", "WHZone", "WHCargo", "WHEvent"):
                session.run(f"CREATE CONSTRAINT {label.lower()}_key IF NOT EXISTS FOR (n:{label}) REQUIRE n.key IS UNIQUE").consume()
            session.run("CREATE CONSTRAINT whrun_id IF NOT EXISTS FOR (n:WHRun) REQUIRE n.run_id IS UNIQUE").consume()
            for label, fields in (("WHAgentState", "run_id"), ("WHStep", "run_id"),
                                  ("WHStep", "run_id,tick,seq"), ("WHAgentState", "run_id,robot_id,seq"),
                                  ("WHAgentState", "run_id,seq"), ("WHEvent", "run_id,tick,event_seq"),
                                  ("WHEvent", "run_id,robot_id,commit_seq,event_seq"),
                                  ("WHTaskState", "run_id,task_id,seq"), ("WHSlotState", "run_id,slot_id,seq")):
                name = (label+"_"+fields.replace(",", "_")).lower()
                expression = ",".join("n."+p for p in fields.split(","))
                session.run(f"CREATE INDEX {name} IF NOT EXISTS FOR (n:{label}) ON ({expression})").consume()
            session.run("CALL db.awaitIndexes(120)").consume()

    def apply_many(self, batches):
        if not batches:
            return
        run = batches[0]["run_id"]
        if any(b["run_id"] != run for b in batches):
            raise KnowledgeError("mixed runs in graph transaction")
        with self.driver.session(database=self.database) as session:
            from neo4j import unit_of_work
            session.execute_write(unit_of_work(timeout=60)(self._apply), batches)

    def committed_point(self, run_id):
        rows = self.query("""MATCH (r:WHRun {run_id:$run})
          OPTIONAL MATCH (s:WHStep {key:$run+':'+toString(r.committed_seq)})
          RETURN r.committed_seq AS seq,s.checksum AS checksum""", {"run":run_id})
        return rows[0] if rows else None

    @staticmethod
    def _apply(tx, batches):
        run = batches[0]["run_id"]
        current = tx.run("MATCH (r:WHRun {run_id:$run}) RETURN r.committed_seq AS seq", run=run).single()
        head = int(current["seq"] or 0) if current else 0
        for batch in batches:
            if batch["seq"] <= head:
                previous = tx.run("MATCH (s:WHStep {key:$key}) RETURN s.checksum AS checksum", key=f"{run}:{batch['seq']}").single()
                if previous is None or previous["checksum"] != digest(batch):
                    raise KnowledgeError("conflicting graph replay")
        batches = [b for b in batches if b["seq"] > head]
        if not batches:
            return
        if [b["seq"] for b in batches] != list(range(head+1, head+1+len(batches))):
            raise KnowledgeError("graph commit sequence gap")
        first = batches[0]
        tx.run("""MERGE (r:WHRun {run_id:$run}) ON CREATE SET r.schema_version=1,
          r.config_json=$config,r.metadata_json=$metadata,r.created_at=datetime()""",
          run=run, config=json.dumps(first["config"], sort_keys=True), metadata=json.dumps(first["metadata"], sort_keys=True)).consume()
        steps, states, tasks, slots, racks, events = [], [], [], [], [], []
        for b in batches:
            seq, tick = b["seq"], b["tick"]
            step_key = f"{run}:{seq}"
            steps.append({"key": step_key, "run_id": run, "seq": seq, "tick": tick,
                          "sim_time": b["sim_time"], "completed": b["completed"], "checksum": digest(b),
                          **{k:b[k] for k in ("running","speed","flow_rate") if k in b}})
            for state in b["robots"]:
                row = properties(state)
                row.update(key=f"{step_key}:{state['robot_id']}", robot_key=f"{run}:{state['robot_id']}",
                           step_key=step_key, run_id=run, seq=seq, tick=tick, sim_time=b["sim_time"], payload=json.dumps(state, separators=(",", ":")))
                states.append(row)
            for label, items in (("task", b["tasks"]), ("slot", b["slots"])):
                target = tasks if label == "task" else slots
                for item in items:
                    row = properties(item)
                    item_id = item[label+"_id"]
                    row.update(key=f"{run}:{item_id}", version_key=f"{step_key}:{item_id}", run_id=run,
                               seq=seq, tick=tick, step_key=step_key, payload=json.dumps(item, separators=(",", ":")))
                    target.append(row)
            racks.extend({**r, "key": f"{run}:{r['id']}", "zone_key": f"{run}:{r['zone']}"} for r in b["racks"])
            for e in b["events"]:
                events.append({**properties(e), "key": e["event_id"], "run_id": run, "commit_seq": seq,
                               "step_key": step_key, "payload": json.dumps(e, ensure_ascii=False, separators=(",", ":"))})
        tx.run("""UNWIND $rows AS p MATCH (r:WHRun {run_id:p.run_id})
          MERGE (s:WHStep {key:p.key}) SET s += p MERGE (s)-[:IN_RUN]->(r)""", rows=steps).consume()
        tx.run("""UNWIND $rows AS p MATCH (r:WHRun {run_id:$run})
          MERGE (z:WHZone {key:p.zone_key}) SET z.zone=p.zone,z.run_id=$run MERGE (z)-[:IN_RUN]->(r)
          MERGE (a:WHRack {key:p.key}) SET a += p,a.run_id=$run MERGE (a)-[:IN_ZONE]->(z)""", run=run, rows=racks).consume()
        tx.run("""UNWIND $rows AS p MATCH (r:WHRun {run_id:p.run_id}), (s:WHStep {key:p.step_key})
          MERGE (a:WHRobot {key:p.robot_key}) SET a.run_id=p.run_id,a.robot_id=p.robot_id,a.robot_type_key=p.robot_type_key
          MERGE (a)-[:IN_RUN]->(r)
          MERGE (v:WHAgentState {key:p.key}) SET v += p
          MERGE (v)-[:OF_ROBOT]->(a) MERGE (v)-[:IN_STEP]->(s)""", rows=states).consume()
        for label, rows, field in (("Task", tasks, "task_id"), ("Slot", slots, "slot_id")):
            tx.run(f"""UNWIND $rows AS p WITH p ORDER BY p.seq
              MATCH (r:WHRun {{run_id:p.run_id}}), (s:WHStep {{key:p.step_key}})
              MERGE (a:WH{label} {{key:p.key}}) SET a += p MERGE (a)-[:IN_RUN]->(r)
              MERGE (v:WH{label}State {{key:p.version_key}}) SET v = p{{.*, key:p.version_key}}
              MERGE (v)-[:OF_{label.upper()}]->(a) MERGE (v)-[:IN_STEP]->(s)""", rows=rows).consume()
        tx.run("""UNWIND $rows AS p MATCH (a:WHSlot {key:p.key}), (r:WHRack {key:$run+':'+toString(p.rack_id)})
          MERGE (a)-[:IN_RACK]->(r)
          FOREACH (_ IN CASE WHEN p.cargo_id IS NULL THEN [] ELSE [1] END |
            MERGE (c:WHCargo {key:$run+':'+p.cargo_id}) SET c.run_id=$run,c.cargo_id=p.cargo_id)""", run=run, rows=slots).consume()
        tx.run("""UNWIND $rows AS p MATCH (t:WHTask {key:p.key}), (a:WHRobot {key:$run+':'+toString(p.robot_id)}),
          (s:WHSlot {key:$run+':'+p.slot_id})
          MERGE (t)-[:EXECUTED_BY]->(a) MERGE (t)-[:TARGETS]->(s)
          MERGE (c:WHCargo {key:$run+':'+p.cargo_id}) SET c.run_id=$run,c.cargo_id=p.cargo_id
          MERGE (t)-[:MOVES]->(c)""", run=run, rows=tasks).consume()
        tx.run("""UNWIND $rows AS p MATCH (r:WHRun {run_id:p.run_id}), (s:WHStep {key:p.step_key})
          MERGE (e:WHEvent {key:p.key}) SET e += p MERGE (e)-[:IN_RUN]->(r) MERGE (e)-[:IN_STEP]->(s)
          FOREACH (_ IN CASE WHEN p.robot_id IS NULL THEN [] ELSE [1] END |
            MERGE (a:WHRobot {key:p.run_id+':'+toString(p.robot_id)}) MERGE (e)-[:ABOUT_ROBOT]->(a))
          FOREACH (_ IN CASE WHEN p.task_id IS NULL THEN [] ELSE [1] END |
            MERGE (t:WHTask {key:p.run_id+':'+p.task_id}) MERGE (e)-[:ABOUT_TASK]->(t))""", rows=events).consume()
        tx.run("""MATCH (r:WHRun {run_id:$run}) SET r.committed_seq=$seq,r.tick=$tick,r.updated_at=datetime(),
               r.current_config_json=$config,r.current_metadata_json=$metadata""",
               run=run, seq=batches[-1]["seq"], tick=batches[-1]["tick"],
               config=json.dumps(batches[-1]['config'],sort_keys=True),
               metadata=json.dumps(batches[-1]['metadata'],sort_keys=True)).consume()

    def query(self, cypher, parameters, timeout=5):
        from neo4j import READ_ACCESS, Query
        # Each catalog read is one complete parameterized statement. Auto-commit
        # avoids BEGIN/COMMIT network round trips without splitting its cutoff.
        # Failures are returned to the caller, not hidden behind long retry waits.
        with self.driver.session(database=self.database, default_access_mode=READ_ACCESS) as session:
            result = session.run(Query(cypher,timeout=timeout), parameters)
            rows = [dict(r) for r in result]
            result.consume()
            return rows

    def close(self):
        self.driver.close()
