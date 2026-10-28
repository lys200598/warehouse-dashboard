from pathlib import Path
import unittest

from backend.warehouse_graph import LINK_LOCATION_SLOTS, SCHEMA_QUERIES, WRITE_LOCATIONS, load_graph_package, validation_report


ROOT = Path(__file__).resolve().parents[1]


class WarehouseGraphPackageTests(unittest.TestCase):
    def setUp(self):
        self.package = load_graph_package(
            ROOT / "data/examples/warehouse-v5",
            ROOT / "data/layouts/warehouse100-wide-v2.json",
            ROOT / "docs/dataset-contract/layout/task-locations.csv",
        )

    def test_generated_package_becomes_scoped_graph_records(self):
        summary = self.package.summary()
        self.assertEqual(summary["racks"], 36)
        self.assertEqual(summary["slots"], 1728)
        self.assertEqual(summary["initial_goods"], 864)
        self.assertEqual(summary["tasks"], 19)
        self.assertEqual(summary["inbound_tasks"], 9)
        self.assertEqual(summary["outbound_tasks"], 10)
        self.assertTrue(all(item["id"].startswith("warehouse:") for item in self.package.locations))
        self.assertTrue(all(item["id"].startswith("dataset:") for item in self.package.tasks))

    def test_schema_uses_unique_constraints_for_idempotent_imports(self):
        self.assertEqual(len(SCHEMA_QUERIES), 8)
        self.assertTrue(all(" IF NOT EXISTS " in query for query in SCHEMA_QUERIES))
        self.assertTrue(any("dataset_id, n.task_id" in query for query in SCHEMA_QUERIES))

    def test_location_slot_links_do_not_depend_on_foreach_case_syntax(self):
        self.assertNotIn("FOREACH", WRITE_LOCATIONS)
        self.assertIn("WHERE row.rack_node_id IS NOT NULL", LINK_LOCATION_SLOTS)

    def test_validation_report_exposes_dataset_and_import_estimate(self):
        report = validation_report(self.package)
        self.assertTrue(report["valid"])
        self.assertEqual(report["tasks"], 19)
        self.assertGreater(report["estimated_nodes"], report["tasks"])


if __name__ == "__main__":
    unittest.main()
