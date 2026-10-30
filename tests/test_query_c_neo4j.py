"""只对调用者明确指定的隔离 Neo4j 运行，不连接项目默认数据库。"""
import os
import unittest
import uuid

from backend.query_c import SCHEMA, ContextUnavailable, read_context


@unittest.skipUnless(os.environ.get("QUERY_C_TEST_URI"), "isolated Neo4j URI not supplied")
class Neo4jTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from neo4j import GraphDatabase
        uri = os.environ["QUERY_C_TEST_URI"]
        # 禁止误用本项目已知的共享实例。
        from urllib.parse import urlparse
        if urlparse(uri).port in (17692, 17695):
            raise RuntimeError("shared Neo4j ports are forbidden")
        password = os.environ.get("QUERY_C_TEST_PASSWORD")
        cls.driver = GraphDatabase.driver(uri, auth=("neo4j", password) if password else None)
        cls.run_id = "query-c-test-" + uuid.uuid4().hex
        with cls.driver.session() as s:
            for query in SCHEMA:
                s.run(query).consume()
            s.run("CALL db.awaitIndexes(60)").consume()
            s.run("CREATE (:Run {id:$run, latest_step:10})", run=cls.run_id).consume()
            s.run("UNWIND [4,10] AS t CREATE (:Step {run_id:$run,tick:t,id:$run+':step:'+toString(t),working:t})",
                  run=cls.run_id).consume()
            # 当前节点刻意写未来值，验证查询绝不会偷看它。
            s.run("""UNWIND [0,1,2,3,4] AS rid
CREATE (:Robot {id:$run+':robot:'+toString(rid),run_id:$run,robot_id:rid,
x:999,y:999,battery:1,stage:'future',updated_tick:99})""", run=cls.run_id).consume()
            states = [
                dict(robot_id=0,tick=0,x=0,y=0,battery=90,stage="idle"),
                dict(robot_id=0,tick=4,x=1,y=1,battery=80,stage="moving",task_id="OLD"),
                dict(robot_id=0,tick=8,x=2,y=2,battery=70,stage="moving",task_id="NOW"),
                dict(robot_id=0,tick=12,x=500,y=500,battery=5,stage="future",task_id="FUTURE"),
                dict(robot_id=1,tick=4,x=3,y=1,battery=90,stage="idle"),
                dict(robot_id=1,tick=8,x=100,y=100,battery=10,stage="moving"),
                dict(robot_id=2,tick=8,x=3,y=2,battery=95,stage="idle"),
                dict(robot_id=3,tick=4,x=4,y=1,battery=60,stage="idle"),
                dict(robot_id=4,tick=12,x=1,y=1,battery=99,stage="idle"),
            ]
            for i, state in enumerate(states):
                state.update(id=f"{cls.run_id}:state:{i}", run_id=cls.run_id)
            s.run("UNWIND $rows AS row CREATE (s:AgentState) SET s=row", rows=states).consume()
            # 一条原型事件，一条正式事件，一条双身份事件；恰在下界和未来的事件不返回。
            events = [
                dict(tick=0,type="excluded_lower",robot_id=0),
                dict(tick=1,type="prototype",robot_id=0,payload='{"detail":"kept"}'),
                dict(tick=4,type="runtime",robot_node_id=f"{cls.run_id}:robot:0",cargo_id="cargo"),
                dict(tick=4,type="both",robot_id=0,robot_node_id=f"{cls.run_id}:robot:0"),
                dict(tick=12,type="future",robot_id=0),
            ]
            for i, event in enumerate(events):
                event.update(id=f"{cls.run_id}:event:{i}",run_id=cls.run_id)
            s.run("UNWIND $rows AS row CREATE (e:RuntimeEvent) SET e=row", rows=events).consume()
            # 其他 run 同号机器人不能泄漏。
            s.run("CREATE (:AgentState {id:$id,run_id:$other,robot_id:0,tick:4,x:-999})",
                  id=cls.run_id+":foreign",other=cls.run_id+"-other").consume()

    @classmethod
    def tearDownClass(cls):
        with cls.driver.session() as s:
            s.run("MATCH (n) WHERE n.run_id IN [$run,$run+'-other'] OR (n:Run AND n.id=$run) DETACH DELETE n",
                  run=cls.run_id).consume()
        cls.driver.close()

    def test_historical_all_parts_are_bounded(self):
        c = read_context(self.driver,self.run_id,0,tick=4,state_window=4,event_window=4)
        f = c["facts"]
        self.assertEqual(f["step"]["tick"],4)
        self.assertEqual(f["self_state"]["task_id"],"OLD")
        self.assertEqual([s["tick"] for s in f["recent_states"]],[4])
        self.assertEqual({e["type"] for e in f["recent_events"]},{"prototype","runtime","both"})
        self.assertEqual(len(f["recent_events"]),3)
        self.assertTrue(any(e.get("payload") == '{"detail":"kept"}' for e in f["recent_events"]))
        self.assertEqual([r["robot_id"] for r in f["nearby_robots"]],[1,3])
        self.assertEqual([r["robot_id"] for r in f["help_candidates"]],[1])
        self.assertEqual(c["robots_without_history"],[2,4])
        self.assertEqual(c["age_ticks"],0)

    def test_latest_uses_history_and_reports_sampling_age(self):
        c = read_context(self.driver,self.run_id,0)
        self.assertEqual((c["tick"],c["observed_tick"],c["age_ticks"]),(10,8,2))
        self.assertEqual(c["facts"]["self_state"]["task_id"],"NOW")
        self.assertEqual([r["robot_id"] for r in c["facts"]["help_candidates"]],[2])
        self.assertNotIn("future",[e["type"] for e in c["facts"]["recent_events"]])

    def test_non_sample_tick_has_explicit_missing_step(self):
        c = read_context(self.driver,self.run_id,0,tick=7)
        self.assertEqual(c["observed_tick"],4)
        self.assertIn("step",c["missing"])
        self.assertEqual([r["robot_id"] for r in c["facts"]["help_candidates"]],[1])

    def test_missing_robot_and_run(self):
        c = read_context(self.driver,self.run_id,999,tick=4)
        self.assertIsNone(c["facts"]["self_state"])
        self.assertEqual(c["facts"]["help_candidates"],[])
        self.assertIn("self_state",c["missing"])
        with self.assertRaises(ContextUnavailable):
            read_context(self.driver,self.run_id+"-absent",0)

    def test_existing_runtime_writer_contract(self):
        from backend.warehouse_runtime_graph import Neo4jRuntimeGraphWriter
        run = self.run_id + "-runtime-writer"
        writer = Neo4jRuntimeGraphWriter(uri=os.environ["QUERY_C_TEST_URI"], user="neo4j",
                                        password=None, driver=self.driver)
        snapshot = dict(runId=run, backend="query-c-test", tick=4, generatedAtMs=0,
                        robotCount=1, maxActiveRobots=1,
                        robots=[dict(id=1,type={"key":"X3"},stage="moving",battery=81,
                                     x=3,y=4,taskId="task-before",cargoId="cargo-before")],
                        events=[dict(id="event-before",tick=4,type="picked",robot_id=1,
                                     task_id="task-before",cargo_id="cargo-before",text="picked",time=4)])
        try:
            writer.record_snapshot(snapshot)
            snapshot["tick"] = 8
            snapshot["robots"][0].update(taskId="task-future",x=999)
            snapshot["events"] = [dict(id="event-future",tick=8,type="future",robot_id=1)]
            writer.record_snapshot(snapshot)
            result = read_context(self.driver,run,1,tick=4)
            self.assertEqual(result["facts"]["self_state"]["task_id"],"task-before")
            self.assertEqual(result["facts"]["self_state"]["cargo_id"],"cargo-before")
            self.assertEqual(result["facts"]["self_state"]["x"],3)
            self.assertEqual([e["type"] for e in result["facts"]["recent_events"]],["picked"])
            self.assertIn("step",result["missing"])
            with self.assertRaises(ContextUnavailable):
                read_context(self.driver,run,1)
        finally:
            with self.driver.session() as s:
                s.run("MATCH (n) WHERE n.run_id=$run OR (n:Run AND n.id=$run) DETACH DELETE n",
                      run=run).consume()


if __name__ == "__main__":
    unittest.main()
