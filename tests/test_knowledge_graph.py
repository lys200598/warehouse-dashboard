import os
import unittest
from uuid import uuid4

from backend.knowledge import JournalKnowledgeStore
from backend.knowledge_graph import Neo4jMirror
from backend.knowledge_queries import WarehouseQueries
from backend.runtime import LocalRobotRuntime
from backend.simulation import SimulationConfig, SimulationCoordinator


@unittest.skipUnless(os.environ.get("WAREHOUSE_TEST_NEO4J") == "1", "opt-in real database integration")
class GraphIntegrationTest(unittest.TestCase):
    def setUp(self):
        self.mirror = Neo4jMirror.from_env()
        self.store = JournalKnowledgeStore(":memory:", self.mirror)
        self.sim = SimulationCoordinator(LocalRobotRuntime(), SimulationConfig(robot_count=3),
            knowledge_store=self.store, run_id="warehouse-integration-"+uuid4().hex[:12])
        self.queries = WarehouseQueries(self.mirror)
    def tearDown(self):
        self.sim.close()
        self.store.close()

    def test_history_context_and_run_isolation(self):
        initial = self.queries.execute({"run_id":self.sim.run_id,"robot_id":1,"as_of_tick":0})
        for _ in range(4):
            self.sim.step_once()
        past = self.queries.execute({"run_id":self.sim.run_id,"robot_id":1,"as_of_tick":0})
        self.assertEqual(initial["rows"], past["rows"])
        now = self.queries.execute({"run_id":self.sim.run_id,"robot_id":1})
        self.assertEqual(now["as_of_tick"], 4)
        self.assertTrue(all(e["tick"] <= 4 for e in now["events"]))
        history = self.queries.execute({"query_id":"robot_history","run_id":self.sim.run_id,"robot_id":1})
        self.assertEqual(len(history["rows"]), 5)
        with self.assertRaises(ValueError):
            self.queries.execute({"run_id":"warehouse-not-existing"})

    def test_entity_histories_and_idempotent_replay(self):
        task = next(iter(self.sim.tasks.values()))
        for kind, field, value in (("task","task_id",task["task_id"]),("slot","slot_id",task["slot_id"])):
            data = self.queries.execute({"query_id":kind,"run_id":self.sim.run_id,field:value})
            self.assertEqual(len(data["rows"]), 1)
        self.sim.step_once()
        batch = self.store.db.execute("SELECT batch FROM commits WHERE run_id=? ORDER BY seq DESC LIMIT 1",(self.sim.run_id,)).fetchone()[0]
        import zlib,json
        self.mirror.apply_many([json.loads(zlib.decompress(batch))])
        count = self.mirror.query("MATCH (a:WHAgentState {run_id:$run}) RETURN count(a) AS n",{"run":self.sim.run_id})[0]["n"]
        self.assertEqual(count,6)
        self.assertEqual(self.sim.knowledge_status()["lag"],0)

    def test_query_validation(self):
        for changes in ({"query_id":"arbitrary-cypher"},{"limit":1000000},{"as_of_seq":1,"as_of_tick":0},{"robot_id":True}):
            with self.assertRaises(ValueError):
                self.queries.execute({"run_id":self.sim.run_id,**changes})

    def test_latest_context_while_writer_advances(self):
        from concurrent.futures import ThreadPoolExecutor
        import threading
        gate=threading.Barrier(9)
        def writer():
            gate.wait(timeout=30)
            for _ in range(40):self.sim.step_once()
        def reader(robot):
            gate.wait(timeout=30)
            for _ in range(30):
                result=self.queries.execute({'run_id':self.sim.run_id,'robot_id':robot%3+1})
                self.assertEqual(result['as_of_seq'],result['rows'][0]['seq'])
                self.assertEqual(result['as_of_tick'],result['rows'][0]['tick'])
                self.assertTrue(all(e['tick']<=result['as_of_tick'] for e in result['events']))
        with ThreadPoolExecutor(max_workers=9) as pool:
            futures=[pool.submit(writer)]+[pool.submit(reader,i) for i in range(8)]
            for future in futures:future.result(timeout=60)

    def test_batched_task_versions_remain_distinct(self):
        self.store.flush_every = 25
        for _ in range(120):
            self.sim.step_once()
        self.store.flush(self.sim.run_id, batch_size=25)
        self.assertEqual(self.sim.knowledge_status()["lag"],0)
        counts = self.mirror.query("MATCH (v:WHTaskState {run_id:$run}) RETURN count(v) AS n,count(DISTINCT v.key) AS unique", {"run":self.sim.run_id})[0]
        self.assertGreater(counts["n"],3)
        self.assertEqual(counts["n"],counts["unique"])
