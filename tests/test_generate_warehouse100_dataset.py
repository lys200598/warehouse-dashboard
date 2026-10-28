import csv
import tempfile
import unittest
from datetime import date
from pathlib import Path

from scripts.generate_warehouse100_dataset import TASK_FIELDS, generate, load_locations, validate


class GenerateWarehouse100DatasetTest(unittest.TestCase):
    root = Path(__file__).resolve().parents[1]
    layout = root / "data/layouts/warehouse100-wide-v2.json"
    locations = root / "docs/dataset-contract/layout/task-locations.csv"

    def test_generates_balanced_design_compatible_day(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "day1"
            result = generate(output, self.layout, self.locations, date(2026, 10, 27), 7, 4, False)
            with (output / "tasks.csv").open(encoding="utf-8", newline="") as handle:
                tasks = list(csv.DictReader(handle))
            with (output / "initial_inventory.csv").open(encoding="utf-8", newline="") as handle:
                inventory = list(csv.DictReader(handle))
        self.assertEqual(tuple(tasks[0]), TASK_FIELDS)
        self.assertEqual(len(tasks), 6912)
        self.assertEqual(result["validation"]["inbound"], 3456)
        self.assertEqual(result["validation"]["outbound"], 3456)
        self.assertEqual(result["validation"]["planned_final_inventory"], 864)
        self.assertTrue(tasks[0]["release_time"].endswith("+08:00"))
        self.assertTrue(validate(tasks, inventory, load_locations(self.locations), 1728)["valid"])


if __name__ == "__main__":
    unittest.main()
