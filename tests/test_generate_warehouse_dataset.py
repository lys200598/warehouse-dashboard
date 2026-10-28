import csv
import tempfile
import unittest
from datetime import date
from pathlib import Path

from scripts.generate_warehouse_dataset import HEADER, estimate_graph_scale, iter_rows, write_dataset


class GenerateWarehouseDatasetTest(unittest.TestCase):
    def test_rows_are_deterministic_and_use_warehouse_fields(self):
        first = list(iter_rows(2, seed=7, day=date(2026, 10, 26), robot_count=100))
        second = list(iter_rows(2, seed=7, day=date(2026, 10, 26), robot_count=100))
        self.assertEqual(first, second)
        self.assertEqual(first[0]["task_id"], "WT-000000001")
        self.assertTrue(str(first[0]["robot_id"]).startswith("HR-"))
        self.assertIn(first[0]["zone_id"], {"A", "B", "C"})
        self.assertTrue(str(first[0]["slot_id"]).startswith("R"))
        self.assertLessEqual(first[0]["release_time"], first[0]["completed_time"])

    def test_write_dataset_has_expected_header_and_row_count(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "warehouse.csv"
            write_dataset(output, count=3, seed=1, day=date(2026, 10, 26), robot_count=4)
            with output.open(newline="", encoding="utf-8") as handle:
                rows = list(csv.DictReader(handle))
        self.assertEqual(tuple(rows[0]), HEADER)
        self.assertEqual(len(rows), 3)
        self.assertEqual(rows[-1]["task_id"], "WT-000000003")

    def test_scale_estimate_matches_task_plus_two_events_model(self):
        estimate = estimate_graph_scale(333_334)
        self.assertEqual(estimate["events"], 666_668)
        self.assertEqual(estimate["minimum_nodes"], 1_000_003)


if __name__ == "__main__":
    unittest.main()
