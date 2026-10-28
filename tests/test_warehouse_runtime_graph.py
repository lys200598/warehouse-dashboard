import unittest

from backend.warehouse_runtime_graph import Neo4jRuntimeGraphWriter


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
    def test_snapshot_creates_run_robot_state_execution_and_event_in_batches(self):
        driver = FakeDriver()
        writer = Neo4jRuntimeGraphWriter(uri="bolt://test", user="neo4j", password=None, dataset_id="dataset:day1", driver=driver)
        writer.record_snapshot(snapshot())
        calls = driver.session_instance.calls
        self.assertEqual(len(calls), 13)  # 9 schema rules + run/robots + state + execution + event
        run_call = calls[9][1]
        self.assertEqual(run_call["robots"][0]["id"], "warehouse-run-1:robot:1")
        self.assertEqual(run_call["run"]["dataset_id"], "dataset:day1")
        event_call = calls[-1]
        self.assertEqual(event_call[1]["events"][0]["id"], "warehouse-run-1:event:1:1:task_assigned:TK-000001")
        execution_call = calls[-2]
        self.assertIn("EXECUTES", execution_call[0])
        self.assertIn("dataset_id", execution_call[0])
        self.assertEqual(execution_call[1]["dataset_id"], "dataset:day1")
        self.assertIn("ABOUT_EXECUTION", event_call[0])
        self.assertIn("completed_tick", event_call[0])
        self.assertEqual(calls[10][1]["states"][0]["run_id"], "warehouse-run-1")

    def test_events_are_not_rewritten_when_the_same_snapshot_is_retried(self):
        driver = FakeDriver()
        writer = Neo4jRuntimeGraphWriter(uri="bolt://test", user="neo4j", password=None, driver=driver)
        payload = snapshot()
        writer.record_snapshot(payload)
        writer.record_snapshot(payload)
        event_calls = [parameters for query, parameters in driver.session_instance.calls if "UNWIND $events" in query]
        self.assertEqual(len(event_calls), 1)

    def test_event_on_a_non_sampled_tick_still_updates_its_execution(self):
        driver = FakeDriver()
        writer = Neo4jRuntimeGraphWriter(uri="bolt://test", user="neo4j", password=None, driver=driver)
        writer.record_snapshot(snapshot())
        payload = snapshot()
        payload["tick"] = 2
        payload["events"] = [{**payload["events"][0], "id": "2:1:task_completed:TK-000001", "tick": 2, "type": "task_completed"}]
        writer.record_snapshot(payload)
        execution_calls = [parameters for query, parameters in driver.session_instance.calls if "UNWIND $executions" in query]
        self.assertEqual(len(execution_calls), 2)
        self.assertEqual(execution_calls[-1]["executions"][0]["status"], "task_completed")


if __name__ == "__main__":
    unittest.main()
