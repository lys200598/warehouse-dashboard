import csv
from datetime import datetime, timedelta
import json
from pathlib import Path
import tempfile
import unittest

from backend.day1.data import FIELDS, sha
from backend.scheduling_v3.data import MAX_DURATION_SECONDS, validate_dataset
from backend.scheduling_v3.scenarios import generate_scenario


class MultiDayContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.root = Path(__file__).resolve().parents[1]
        cls.scene = cls.root / "data/layouts/warehouse100-v1.json"

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="v3-data-", dir=self.root / "tests")
        assert Path(self.temp.name).resolve().is_relative_to(self.root)
        self.dataset = Path(self.temp.name) / "input"
        generate_scenario(self.scene, self.dataset, active_slots=12, duration_seconds=1800,
                          slot_interval_seconds=600, seed=17)

    def tearDown(self):
        self.temp.cleanup()

    def manifest(self):
        return json.loads((self.dataset / "manifest.json").read_text(encoding="utf-8"))

    def write_manifest(self, manifest):
        (self.dataset / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")

    def tasks(self):
        with (self.dataset / "tasks.csv").open(encoding="utf-8", newline="") as stream:
            return list(csv.DictReader(stream))

    def write_tasks(self, rows):
        with (self.dataset / "tasks.csv").open("w", encoding="utf-8", newline="") as stream:
            writer = csv.DictWriter(stream, fieldnames=FIELDS)
            writer.writeheader()
            writer.writerows(rows)
        manifest = self.manifest()
        manifest["files"]["tasks.csv"] = sha(self.dataset / "tasks.csv")
        self.write_manifest(manifest)

    def test_manifest_counts_and_capacity_are_not_trusted(self):
        original = self.manifest()
        for key, value in [("tasks", original["tasks"] + 1), ("inbound", -1),
                           ("initial_inventory", True), ("capacity", 1729),
                           ("duration_seconds", MAX_DURATION_SECONDS + 1)]:
            with self.subTest(key=key):
                self.write_manifest({**original, key: value})
                with self.assertRaises(ValueError):
                    validate_dataset(self.scene, self.dataset)
        self.write_manifest(original)

    def test_raw_timestamp_precision_timezone_and_range_are_checked_before_int(self):
        original_rows, manifest = self.tasks(), self.manifest()
        changes = [
            ("release_time", original_rows[0]["release_time"].replace("+08:00", "")),
            ("release_time", original_rows[0]["release_time"].replace("+08:00", "+09:00")),
            ("release_time", (datetime.fromisoformat(original_rows[0]["release_time"]) + timedelta(microseconds=500000)).isoformat()),
            ("due_time", original_rows[0]["release_time"]),
            ("release_time", (datetime.fromisoformat(manifest["start"]) + timedelta(seconds=manifest["duration_seconds"])).isoformat()),
        ]
        for field, value in changes:
            with self.subTest(field=field, value=value):
                rows = [dict(row) for row in original_rows]
                rows[0][field] = value
                self.write_tasks(rows)
                with self.assertRaises(ValueError):
                    validate_dataset(self.scene, self.dataset)

    def test_start_must_have_timezone_and_integer_seconds(self):
        original = self.manifest()
        for start in [original["start"].replace("+08:00", ""),
                      (datetime.fromisoformat(original["start"]) + timedelta(microseconds=1)).isoformat()]:
            self.write_manifest({**original, "start": start})
            with self.assertRaises(ValueError):
                validate_dataset(self.scene, self.dataset)

    def test_duplicate_task_id_wrong_goods_and_reused_inbound_goods_rejected(self):
        original = self.tasks()
        duplicate = [dict(row) for row in original]
        duplicate[1]["task_id"] = duplicate[0]["task_id"]
        self.write_tasks(duplicate)
        with self.assertRaisesRegex(ValueError, "duplicate task"):
            validate_dataset(self.scene, self.dataset)
        bad_out = [dict(row) for row in original]
        next(row for row in bad_out if row["task_type"] == "OUTBOUND")["goods_id"] = "MISSING-CARGO"
        self.write_tasks(bad_out)
        with self.assertRaisesRegex(ValueError, "outbound reference"):
            validate_dataset(self.scene, self.dataset)
        with (self.dataset / "initial_inventory.csv").open(encoding="utf-8", newline="") as stream:
            initial_goods = next(csv.DictReader(stream))["goods_id"]
        reused = [dict(row) for row in original]
        next(row for row in reused if row["task_type"] == "INBOUND")["goods_id"] = initial_goods
        self.write_tasks(reused)
        with self.assertRaisesRegex(ValueError, "inbound reference"):
            validate_dataset(self.scene, self.dataset)

    def test_cross_day_dependency_violation_is_detected(self):
        other = Path(self.temp.name) / "two-days"
        generate_scenario(self.scene, other, active_slots=24, duration_seconds=172800,
                          slot_interval_seconds=21600, seed=2710)
        self.dataset = other
        rows = self.tasks()
        start = datetime.fromisoformat(self.manifest()["start"])
        incoming = {row["goods_id"]: row for row in rows if row["task_type"] == "INBOUND"}
        outbound = next(row for row in rows if row["task_type"] == "OUTBOUND" and row["goods_id"] in incoming and
            datetime.fromisoformat(incoming[row["goods_id"]]["release_time"]) < start + timedelta(days=1) <=
            datetime.fromisoformat(row["release_time"]))
        outbound["goods_id"] = "WRONG-DAY2-CARGO"
        self.write_tasks(rows)
        with self.assertRaisesRegex(ValueError, "outbound reference dependency"):
            validate_dataset(self.scene, self.dataset)

    def test_input_file_manifest_cannot_reference_other_paths(self):
        manifest = self.manifest()
        manifest["files"]["../not-an-input"] = "0" * 64
        self.write_manifest(manifest)
        with self.assertRaisesRegex(ValueError, "input files must be exactly"):
            validate_dataset(self.scene, self.dataset)


if __name__ == "__main__":
    unittest.main()
