import csv
import tempfile
import unittest
from pathlib import Path

from backend.dataset import REQUIRED_COLUMNS, load_warehouse_tasks
from backend.runtime import LocalRobotRuntime
from backend.simulation import SimulationConfig, SimulationCoordinator


def dataset_rows():
    common = {
        "zone_id": "A", "warehouse": "WH-01", "robot_id": "HR-001", "rack_id": "1",
        "slot_id": "R01-L1-C1-D1", "task_type": "putaway", "priority": "high",
        "start_x": "1.55", "start_y": "18.9", "target_x": "8", "target_y": "4.3",
        "level": "1", "bay": "1", "depth_row": "1", "ds": "20261026",
    }
    return [
        {**common, "task_id": "WT-000000001", "cargo_id": "BX-000000001", "release_time": "2026-10-26 08:00:00"},
        {**common, "task_id": "WT-000000002", "cargo_id": "BX-000000002", "release_time": "2026-10-26 08:00:01"},
    ]


class DatasetTest(unittest.TestCase):
    def write_dataset(self, directory: str) -> Path:
        path = Path(directory) / "tasks.csv"
        with path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=sorted(REQUIRED_COLUMNS | {"zone_id", "warehouse", "assigned_time", "pickup_time", "completed_time", "ds"}))
            writer.writeheader()
            for row in dataset_rows():
                writer.writerow(row)
        return path

    def test_loader_orders_by_release_time_and_adds_offset(self):
        with tempfile.TemporaryDirectory() as directory:
            path = self.write_dataset(directory)
            records = load_warehouse_tasks(path)
        self.assertEqual([item["task_id"] for item in records], ["WT-000000001", "WT-000000002"])
        self.assertEqual(records[0]["release_offset"], 0.0)
        self.assertEqual(records[1]["release_offset"], 1.0)

    def test_simulation_releases_csv_task_instead_of_creating_random_task(self):
        with tempfile.TemporaryDirectory() as directory:
            path = self.write_dataset(directory)
            simulation = SimulationCoordinator(
                LocalRobotRuntime(),
                SimulationConfig(robot_count=4, max_active_robots=4, step_seconds=0.5, dataset_path=str(path)),
            )
            try:
                self.assertEqual(simulation.tasks, {})
                simulation.step_once()
                snapshot = simulation.step_once()
                self.assertTrue(snapshot["dataset"]["enabled"])
                self.assertEqual(snapshot["dataset"]["total"], 2)
                self.assertIn("WT-000000001", simulation.tasks)
                self.assertEqual(simulation.tasks["WT-000000001"]["priority"], "high")
                self.assertIn("source_task", simulation.tasks["WT-000000001"])
            finally:
                simulation.close()

    def test_loader_accepts_warehouse100_seven_field_tasks(self):
        path = Path(__file__).resolve().parents[1] / "data/generated/warehouse100-day1/tasks.csv"
        records = load_warehouse_tasks(path)
        self.assertEqual(len(records), 6912)
        self.assertEqual({record["task_type"] for record in records}, {"INBOUND", "OUTBOUND"})
        self.assertEqual(records[0]["release_offset"], 0.0)


if __name__ == "__main__":
    unittest.main()
