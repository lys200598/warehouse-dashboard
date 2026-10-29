import unittest

from backend.runtime import LocalRobotRuntime
from backend.simulation import SimulationConfig, SimulationCoordinator


class LiveDatasetSourceTests(unittest.TestCase):
    def test_realtime_coordinator_assigns_csv_task_id_and_outbound_semantics(self):
        records = [{
            "task_id": "T-000001", "task_type": "OUTBOUND", "cargo_id": "INITIAL-00001",
            "slot_id": "R01-L1-C1-F1", "source_id": "R01-L1-C1-F1", "target_id": "OUT-1",
            "priority": "normal", "release_offset": 0.0, "level": 1, "bay": 1, "depth_row": 1,
        }]
        runtime = LocalRobotRuntime()
        coordinator = SimulationCoordinator(
            runtime, SimulationConfig(robot_count=1, max_active_robots=1), knowledge_store=None,
            dataset_tasks=records,
            dataset_inventory=[{"goods_id": "INITIAL-00001", "location_id": "R01-L1-C1-F1"}],
        )
        try:
            coordinator.step_once()  # robot 1 clears its initial idle timer
            coordinator.step_once()  # next scheduling barrier dispatches CSV task
            snapshot = coordinator.snapshot()
            self.assertEqual(snapshot["runtime"]["taskSource"], "csv-dataset")
            self.assertEqual(snapshot["runtime"]["datasetTasksTotal"], 1)
            task = coordinator.tasks["T-000001"]
            self.assertEqual(task["task_type"], "OUTBOUND")
            self.assertEqual(task["cargo_id"], "INITIAL-00001")
        finally:
            coordinator.close()


if __name__ == "__main__":
    unittest.main()
