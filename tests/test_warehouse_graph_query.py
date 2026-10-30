import unittest

from backend.warehouse_graph_query import Neo4jWarehouseGraph, WarehouseGraphUnavailable


class FakeResult:
    def __init__(self, rows):
        self.rows = rows

    def data(self):
        return self.rows


class FakeSession:
    def __init__(self, responses):
        self.responses = responses
        self.queries = []

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False

    def run(self, query, **parameters):
        self.queries.append((query, parameters))
        return FakeResult(self.responses.pop(0))


class FakeDriver:
    def __init__(self, responses):
        self.session_instance = FakeSession(responses)
        self.closed = False

    def session(self, **_):
        return self.session_instance

    def verify_connectivity(self):
        return None

    def close(self):
        self.closed = True


class WarehouseGraphQueryTests(unittest.TestCase):
    def test_unconfigured_graph_does_not_attempt_a_connection(self):
        graph = Neo4jWarehouseGraph(uri="bolt://127.0.0.1:47687", user="neo4j", password=None)
        self.assertEqual(graph.health()["status"], "not_configured")
        with self.assertRaises(WarehouseGraphUnavailable):
            graph.summary()

    def test_summary_returns_bounded_dataset_and_layout_counts(self):
        driver = FakeDriver([[{"id": "dataset:day1", "task_count": 6912}], [{"warehouse_id": "warehouse:v1", "slot_count": 1728}]])
        graph = Neo4jWarehouseGraph(uri="bolt://test", user="neo4j", password=None, driver=driver)
        result = graph.summary("dataset:day1")
        self.assertEqual(result["datasets"][0]["task_count"], 6912)
        self.assertEqual(result["warehouses"][0]["slot_count"], 1728)
        self.assertEqual(driver.session_instance.queries[0][1]["dataset_id"], "dataset:day1")

    def test_task_subgraph_never_returns_an_unbounded_graph(self):
        driver = FakeDriver([[{
            "dataset_id": "dataset:day1",
            "task": {"id": "task:1", "task_id": "T-000001"},
            "goods": {"id": "goods:1", "goods_id": "G-1"},
            "source": {"id": "location:in", "location_id": "IN-1"},
            "target": {"id": "location:slot", "location_id": "R01-L1-C1-F1"},
        }]])
        graph = Neo4jWarehouseGraph(uri="bolt://test", user="neo4j", password=None, driver=driver)
        result = graph.task_subgraph("T-000001")
        self.assertEqual(len(result["nodes"]), 4)
        self.assertEqual(len(result["edges"]), 3)
        self.assertIn("LIMIT 1", driver.session_instance.queries[0][0])

    def test_query_preset_accepts_only_bounded_named_queries(self):
        driver = FakeDriver([[{"run_id": "run-a", "robots": 100, "states": 10}]])
        graph = Neo4jWarehouseGraph(uri="bolt://test", user="neo4j", password=None, driver=driver)
        result = graph.query_preset("run-progress", run_id="run-a")
        self.assertEqual(result["result"]["run_id"], "run-a")
        self.assertIn("backend_query_ms", result["timing"])
        with self.assertRaises(ValueError):
            graph.query_preset("MATCH (n) RETURN n")

    def test_replay_reads_by_run_and_tick_properties(self):
        driver = FakeDriver([[{"state": {"robot_id": 1, "tick": 8}}]])
        graph = Neo4jWarehouseGraph(uri="bolt://test", user="neo4j", password=None, driver=driver)
        self.assertEqual(graph.replay_states("run-a", 8), [{"robot_id": 1, "tick": 8}])
        query, parameters = driver.session_instance.queries[0]
        self.assertIn("run_id:$run_id, tick:$tick", query)
        self.assertEqual(parameters, {"run_id": "run-a", "tick": 8})

    def test_task_list_uses_configured_dataset_scope(self):
        driver = FakeDriver([[]])
        graph = Neo4jWarehouseGraph(uri="bolt://test", user="neo4j", password=None, dataset_id="dataset:day1", driver=driver)
        graph.task_list()
        self.assertEqual(driver.session_instance.queries[0][1]["dataset_id"], "dataset:day1")

    def test_task_list_deduplicates_executions_per_task(self):
        driver = FakeDriver([[]])
        graph = Neo4jWarehouseGraph(uri="bolt://test", user="neo4j", password=None, driver=driver)
        graph.task_list()
        query = driver.session_instance.queries[0][0]
        self.assertIn("head(collect(", query)
        self.assertIn("ORDER BY coalesce(e.updated_tick,-1) DESC", query)

    def test_latest_complete_step_reads_run_pointer_then_nearest_prior_states(self):
        driver = FakeDriver([
            [{"step": {"tick": 40, "complete": True}}],
            [{"robot_id": 1, "state": {"tick": 40, "stage": "idle"}}],
        ])
        graph = Neo4jWarehouseGraph(uri="bolt://test", user="neo4j", password=None, driver=driver)
        result = graph.latest_complete_step("run-a", include_states=True)
        self.assertEqual(result["step"]["tick"], 40)
        self.assertEqual(result["states"][0]["sampled_tick"], 40)
        self.assertIn("run.latest_step", driver.session_instance.queries[0][0])
        self.assertIn("state.tick <= cutoff", driver.session_instance.queries[1][0])

    def test_handoffs_filter_by_task_and_robot(self):
        driver = FakeDriver([[{"handoff": {"id": "run-a:handoff:e-1", "donor_id": 1, "receiver_id": 2}}]])
        graph = Neo4jWarehouseGraph(uri="bolt://test", user="neo4j", password=None, driver=driver)
        result = graph.handoffs("run-a", task_id="T-1", robot_id=1)
        self.assertEqual(result[0]["receiver_id"], 2)
        query, parameters = driver.session_instance.queries[0]
        self.assertIn("handoff.donor_id=$robot_id", query)
        self.assertEqual(parameters["task_id"], "T-1")

    def test_runtime_state_trace_derives_robot_id_from_node_id_for_old_states(self):
        driver = FakeDriver([
            [{"sampled_tick": 16}],
            [{"state": {"stage": "idle"}, "robot_id": 1}],
        ])
        graph = Neo4jWarehouseGraph(uri="bolt://test", user="neo4j", password=None, driver=driver)
        result = graph.runtime_state_trace("run-a", tick=17)
        self.assertEqual(result["states"][0]["robot_id"], 1)
        self.assertIn("toInteger(last(split(s.robot_node_id", driver.session_instance.queries[0][0])

    def test_robot_trace_returns_state_trajectory_execution_and_events(self):
        driver = FakeDriver([
            [{"state": {"tick": 8, "stage": "carrying", "x": 3, "y": 4, "task_id": "T-1"}}],
            [{"state": {"tick": 8}}, {"state": {"tick": 4}}],
            [{"execution": {"task_id": "T-1", "status": "carrying", "assigned_tick": 2, "picked_tick": 6}, "task": {"source_id": "IN-1", "target_id": "R-1"}}],
            [{"event": {"tick": 6, "type": "task_picked_up", "task_id": "T-1"}}],
        ])
        graph = Neo4jWarehouseGraph(uri="bolt://test", user="neo4j", password=None, driver=driver)
        result = graph.robot_trace_context("run-a", 1, tick=9, history_limit=20)
        self.assertEqual(result["latestState"]["tick"], 8)
        self.assertEqual(result["stateAgeTicks"], 1)
        self.assertEqual([state["tick"] for state in result["trajectory"]], [4, 8])
        self.assertEqual(result["executions"][0]["execution"]["task_id"], "T-1")
        self.assertEqual(result["events"][0]["type"], "task_picked_up")
        query, parameters = driver.session_instance.queries[0]
        self.assertIn("run_id:$run_id, robot_node_id:$robot_node_id", query)
        self.assertEqual(parameters["robot_node_id"], "run-a:robot:1")

    def test_robot_trace_treats_default_tick_as_latest(self):
        driver = FakeDriver([[], [], [], []])
        graph = Neo4jWarehouseGraph(uri="bolt://test", user="neo4j", password=None, driver=driver)
        result = graph.robot_trace_context("run-a", 1, tick=0)
        self.assertIsNone(result["requestedTick"])
        self.assertEqual(driver.session_instance.queries[0][1]["tick"], 2_147_483_647)

    def test_runtime_state_trace_reads_nearest_prior_sample(self):
        driver = FakeDriver([
            [{"sampled_tick": 16}],
            [{"state": {"robot_id": 1, "stage": "idle", "tick": 16}}],
        ])
        graph = Neo4jWarehouseGraph(uri="bolt://test", user="neo4j", password=None, driver=driver)
        result = graph.runtime_state_trace("run-a", tick=17, robot_id=1)
        self.assertEqual(result["requestedTick"], 17)
        self.assertEqual(result["sampledTick"], 16)
        self.assertEqual(result["states"][0]["robot_id"], 1)
        sampled_query, sampled_parameters = driver.session_instance.queries[0]
        self.assertIn("max(s.tick) AS sampled_tick", sampled_query)
        self.assertEqual(sampled_parameters["robot_id"], 1)

    def test_runtime_state_trace_reports_no_sample(self):
        driver = FakeDriver([[]])
        graph = Neo4jWarehouseGraph(uri="bolt://test", user="neo4j", password=None, driver=driver)
        result = graph.runtime_state_trace("run-a", tick=5)
        self.assertIsNone(result["sampledTick"])
        self.assertEqual(result["states"], [])


if __name__ == "__main__":
    unittest.main()
