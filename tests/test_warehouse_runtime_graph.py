import unittest
from types import SimpleNamespace

from backend.warehouse_runtime_graph import (
    Neo4jRuntimeGraphWriter,
    _LINK_EVENTS_TO_EXECUTIONS,
    _WRITE_EVENTS,
    _WRITE_EXECUTION_STATES,
    _WRITE_EXECUTIONS,
    _WRITE_AGENT_STATES,
    _WRITE_RUN_AND_ROBOTS,
    v5_runtime_snapshot,
)


class FakeResult:
    def consume(self):
        return None


class FakeSession:
    def __init__(self):
        self.calls = []

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False

    def run(self, query, **parameters):
        self.calls.append((query, parameters))
        return FakeResult()


class FakeDriver:
    def __init__(self):
        self.session_instance = FakeSession()
        self.closed = False

    def session(self, **_):
        return self.session_instance

    def close(self):
        self.closed = True


def snapshot():
    return {
        "runId": "warehouse-run-1", "backend": "local", "tick": 1, "generatedAtMs": 123,
        "maxActiveRobots": 100,
        "robots": [{"id": 1, "type": {"key": "A1"}, "stage": "toPick", "battery": 88.0, "x": 1.0, "y": 2.0, "taskId": "TK-000001", "cargoId": "BX-004001"}],
        "events": [{"id": "1:1:task_assigned:TK-000001", "tick": 1, "type": "task_assigned", "robot_id": 1, "task_id": "TK-000001", "cargo_id": "BX-004001", "rack_id": 1, "slot_id": "R01-L1-C1-D1", "text": "assigned", "time": "08:00:00"}],
    }


class RuntimeGraphWriterTests(unittest.TestCase):
    def test_actor_current_state_registers_robot_once_per_run(self):
        from backend.warehouse_runtime_graph import _WRITE_ROBOTS_ONLY, _UPDATE_ROBOTS_ONLY
        driver = FakeDriver()
        writer = Neo4jRuntimeGraphWriter(uri='bolt://test', user='neo4j', password=None, driver=driver)
        writer.schema_ready = True
        sample = snapshot()
        sample['events'] = []
        robot = {'robot_id': 1, 'battery': 88.0}
        for tick in range(1, 6):
            sample['tick'] = tick
            writer.record_robot_snapshot(sample)
            writer.record_robot_current_state(sample, robot)
        queries = [query for query, _ in driver.session_instance.calls]
        self.assertEqual(queries.count(_WRITE_ROBOTS_ONLY), 1)
        self.assertGreaterEqual(queries.count(_UPDATE_ROBOTS_ONLY), 5)
        sample['runId'] = 'another-run'
        writer.record_robot_snapshot(sample)
        writer.record_robot_current_state(sample, robot)
        self.assertEqual(sum(query == _WRITE_ROBOTS_ONLY for query, _ in driver.session_instance.calls), 2)

    def test_conditional_relationship_queries_avoid_foreach_case_syntax(self):
        for query in (_WRITE_RUN_AND_ROBOTS, _WRITE_EXECUTIONS, _WRITE_EVENTS):
            self.assertNotIn("FOREACH (_ IN CASE", query)
            self.assertNotIn("FOREACH (_ IN (CASE", query)

    def test_v5_adapter_uses_actual_v5_robot_state_and_task_event(self):
        sim = SimpleNamespace(
            run_id="v5-run", tick=12, elapsed=12.0,
            run_metadata={"backend": "warehouse-scheduler-v5"},
            robots={1: {"robot_id": 1, "robot_type_key": "X3", "stage": "charging", "battery": 57.5,
                        "x": 4.0, "y": 8.0, "task_id": "T-000001", "cargo_id": "G-1"}},
        )
        payload = v5_runtime_snapshot(sim, [{"event_seq": 8, "tick": 12, "sim_time": 12.0,
                                             "kind": "charge_started", "robot_id": 1,
                                             "task_id": "T-000001", "goods_id": "G-1",
                                             "location_id": "C-001"}])
        self.assertEqual(payload["runId"], "v5-run")
        self.assertEqual(payload["robots"][0]["stage"], "charging")
        self.assertEqual(payload["robots"][0]["taskId"], "T-000001")
        self.assertEqual(payload["events"][0]["id"], "v5:8")
        self.assertEqual(payload["events"][0]["slot_id"], "C-001")

    def test_snapshot_creates_run_robot_state_execution_and_event_in_batches(self):
        driver = FakeDriver()
        writer = Neo4jRuntimeGraphWriter(uri="bolt://test", user="neo4j", password=None, dataset_id="dataset:day1", driver=driver)
        writer.record_snapshot(snapshot())
        calls = driver.session_instance.calls
        self.assertGreaterEqual(len(calls), 20)  # schema plus explicit node/relationship batches
        run_call = next(parameters for query, parameters in calls if query == _WRITE_RUN_AND_ROBOTS)
        self.assertEqual(run_call["robots"][0]["id"], "warehouse-run-1:robot:1")
        self.assertEqual(run_call["run"]["dataset_id"], "dataset:day1")
        event_call = next(call for call in calls if call[0] == _WRITE_EVENTS)
        self.assertEqual(event_call[1]["events"][0]["id"], "warehouse-run-1:event:1:1:task_assigned:TK-000001")
        execution_call = next(call for call in calls if call[0] == _WRITE_EXECUTIONS)
        self.assertIn("dataset_id", execution_call[0])
        self.assertEqual(execution_call[1]["dataset_id"], "dataset:day1")
        history_call = next(call for call in calls if call[0] == _WRITE_EXECUTION_STATES)
        self.assertEqual(history_call[1]["states"][0]["status"], "assigned")
        execution_event_call = next(call for call in calls if call[0] == _LINK_EVENTS_TO_EXECUTIONS)
        self.assertIn("ABOUT_EXECUTION", execution_event_call[0])
        state_call = next(call for call in calls if call[0] == _WRITE_AGENT_STATES)
        self.assertEqual(state_call[1]["states"][0]["run_id"], "warehouse-run-1")
        self.assertEqual(state_call[1]["states"][0]["robot_id"], 1)

    def test_actor_snapshot_uses_fleet_total_for_run_robot_count(self):
        driver = FakeDriver()
        writer = Neo4jRuntimeGraphWriter(uri="bolt://test", user="neo4j", password=None, driver=driver)
        payload = snapshot()
        payload["robotCount"] = 100
        writer.record_snapshot(payload)
        run_call = next(call for call in driver.session_instance.calls if call[0] == _WRITE_RUN_AND_ROBOTS)
        self.assertEqual(run_call[1]["run"]["robot_count"], 100)

    def test_events_are_not_rewritten_when_the_same_snapshot_is_retried(self):
        driver = FakeDriver()
        writer = Neo4jRuntimeGraphWriter(uri="bolt://test", user="neo4j", password=None, driver=driver)
        payload = snapshot()
        writer.record_snapshot(payload)
        writer.record_snapshot(payload)
        event_calls = [parameters for query, parameters in driver.session_instance.calls if query == _WRITE_EVENTS]
        self.assertEqual(len(event_calls), 1)

    def test_event_on_a_non_sampled_tick_still_updates_its_execution(self):
        driver = FakeDriver()
        writer = Neo4jRuntimeGraphWriter(uri="bolt://test", user="neo4j", password=None, driver=driver)
        writer.record_snapshot(snapshot())
        payload = snapshot()
        payload["tick"] = 2
        payload["events"] = [{**payload["events"][0], "id": "2:1:task_completed:TK-000001", "tick": 2, "type": "task_completed"}]
        writer.record_snapshot(payload)
        execution_calls = [parameters for query, parameters in driver.session_instance.calls if query == _WRITE_EXECUTIONS]
        self.assertEqual(len(execution_calls), 2)
        self.assertEqual(execution_calls[-1]["executions"][0]["status"], "task_completed")


if __name__ == "__main__":
    unittest.main()
