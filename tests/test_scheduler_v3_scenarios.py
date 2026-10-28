import collections
import csv
import json
from pathlib import Path
import tempfile
import unittest

from backend.day1.data import FIELDS, read_dataset, validate
from backend.scheduling_v3.scenarios import SCENARIOS, generate_scenario
from backend.scheduling_v3.data import MAX_DURATION_SECONDS, validate_dataset


class ScenarioTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.scene = Path(__file__).resolve().parents[1] / "data/layouts/warehouse100-v1.json"

    def setUp(self):
        # Managed Windows sandboxes may expose a TMP path with non-inheriting
        # write ACLs. Keep disposable test data inside the writable workspace.
        self.temp = tempfile.TemporaryDirectory(prefix="v3-scenarios-", dir=Path(__file__).resolve().parent)
        self.root = Path(self.temp.name)
        self.assertTrue(self.root.resolve().is_relative_to(Path(__file__).resolve().parents[1]))

    def tearDown(self):
        self.temp.cleanup()

    def generate(self, name="sample", **kwargs):
        out = self.root / name
        manifest, report = generate_scenario(self.scene, out, active_slots=40,
            duration_seconds=3600, slot_interval_seconds=600, **kwargs)
        return out, manifest, report

    def test_all_scenarios_are_compatible_and_preserve_capacity_and_dependencies(self):
        for scenario in SCENARIOS:
            with self.subTest(scenario=scenario):
                out, manifest, report = self.generate(scenario, scenario=scenario)
                self.assertTrue(report["valid"])
                self.assertEqual(manifest["initial_inventory"], 864)
                self.assertEqual(manifest["capacity"], 1728)
                self.assertEqual(manifest["active_slots"], 40)
                self.assertGreater(manifest["tasks"], 0)
                self.assertLessEqual(report["planned_max_inventory"], 1728)
                self.assertGreaterEqual(report["planned_min_inventory"], 0)
                self.assertFalse(report["runtime_feasibility_claim"])
                self.assertTrue(validate(self.scene, out)["valid"])
                with (out / "tasks.csv").open(encoding="utf-8", newline="") as stream:
                    self.assertEqual(csv.DictReader(stream).fieldnames, FIELDS)
                _, tasks, _ = read_dataset(self.scene, out)
                by_slot = collections.defaultdict(list)
                for task in tasks:
                    self.assertTrue(0 <= task["release_second"] < 3600)
                    self.assertGreater(task["due_second"], task["release_second"])
                    slot = task["source_id"] if task["task_type"] == "OUTBOUND" else task["target_id"]
                    by_slot[slot].append(task)
                for rows in by_slot.values():
                    for previous, current in zip(rows, rows[1:]):
                        self.assertGreater(current["release_second"], previous["release_second"])
                        self.assertNotEqual(previous["task_type"], current["task_type"])
                        if current["task_type"] == "OUTBOUND":
                            self.assertEqual(previous["goods_id"], current["goods_id"])

    def test_reproducible_files_same_seed_and_different_seed_changes_input(self):
        a, ma, _ = self.generate("a", scenario="continuous", seed=7)
        b, mb, _ = self.generate("b", scenario="continuous", seed=7)
        _, mc, _ = self.generate("c", scenario="continuous", seed=8)
        self.assertEqual(ma, mb)
        self.assertEqual((a / "tasks.csv").read_bytes(), (b / "tasks.csv").read_bytes())
        self.assertNotEqual(ma["files"]["tasks.csv"], mc["files"]["tasks.csv"])

    def test_peak_time_windows_are_explicit_not_random_due_dates(self):
        out, _, _ = self.generate(scenario="peak")
        _, tasks, _ = read_dataset(self.scene, out)
        for task in tasks:
            t = task["release_second"] / 3600
            self.assertTrue(any(start - 1e-9 <= t <= start + .15 + 1e-9 for start in [.05, .40, .75]))

    def test_hotspot_is_local_and_has_four_times_slot_turnover(self):
        out, manifest, _ = self.generate(scenario="hotspot")
        _, tasks, _ = read_dataset(self.scene, out)
        counts = collections.Counter(task["source_id"] if task["task_type"] == "OUTBOUND" else task["target_id"] for task in tasks)
        hot = set(manifest["hotspot_slot_ids"])
        self.assertEqual(len(hot), 8)
        self.assertEqual({counts[key] for key in hot}, {24})
        self.assertEqual({value for key, value in counts.items() if key not in hot}, {6})

    def test_continuous_interarrivals_vary_but_always_increase(self):
        out, _, _ = self.generate(scenario="continuous")
        _, tasks, _ = read_dataset(self.scene, out)
        groups = collections.defaultdict(list)
        for task in tasks:
            slot = task["source_id"] if task["task_type"] == "OUTBOUND" else task["target_id"]
            groups[slot].append(task["release_second"])
        gaps = {b - a for times in groups.values() for a, b in zip(times, times[1:])}
        self.assertGreater(len(gaps), 3)
        self.assertGreater(min(gaps), 0)

    def test_output_not_overwritten_and_hash_corruption_rejected(self):
        out, _, _ = self.generate()
        before = (out / "tasks.csv").read_bytes()
        with self.assertRaises(FileExistsError):
            generate_scenario(self.scene, out)
        self.assertEqual((out / "tasks.csv").read_bytes(), before)
        (out / "tasks.csv").write_bytes(before + b"\n")
        with self.assertRaisesRegex(ValueError, "dataset hash mismatch"):
            read_dataset(self.scene, out)

    def test_invalid_or_excessive_scope_fails_before_creating_output(self):
        for i, kwargs in enumerate([{"scenario": "business-orders"}, {"duration_seconds": MAX_DURATION_SECONDS + 1},
                {"duration_seconds": 0}, {"slot_interval_seconds": 0}, {"active_slots": 1729},
                {"seed": None},
                {"duration_seconds": 86400, "slot_interval_seconds": 60, "max_tasks": 20}]):
            out = self.root / str(i)
            with self.assertRaises(ValueError):
                generate_scenario(self.scene, out, **kwargs)
            self.assertFalse(out.exists())

    def test_one_day_ordinary_dataset_uses_unchanged_day1_validator(self):
        out = self.root / "day"
        manifest, report = generate_scenario(self.scene, out, duration_seconds=86400,
            slot_interval_seconds=21600, profile="conservative-sensitivity-v1")
        self.assertEqual(manifest["tasks"], 6912)
        self.assertEqual(report["planned_final_inventory"], 864)
        self.assertEqual(manifest["profile"], "conservative-sensitivity-v1")
        json.dumps(manifest, allow_nan=False)

    def test_two_days_fixed_seed_and_cross_day_cargo_dependencies(self):
        directories = [self.root / name for name in ("two-days-a", "two-days-b")]
        outputs = [generate_scenario(self.scene, path, duration_seconds=2 * 86400,
                    active_slots=24, slot_interval_seconds=21600, seed=2710) for path in directories]
        self.assertEqual(outputs[0], outputs[1])
        manifest, report = outputs[0]
        self.assertEqual(manifest["tasks"], 24 * 8)
        self.assertEqual(report["daily_tasks"], [24 * 4, 24 * 4])
        self.assertEqual(len(report["hourly_tasks"]), 48)
        self.assertTrue(validate_dataset(self.scene, directories[0])["valid"])
        _, tasks, _ = read_dataset(self.scene, directories[0])
        inbound = {task["goods_id"]: task for task in tasks if task["task_type"] == "INBOUND"}
        cross_day = [task for task in tasks if task["task_type"] == "OUTBOUND" and
                     task["goods_id"] in inbound and
                     inbound[task["goods_id"]]["release_second"] < 86400 <= task["release_second"]]
        self.assertGreater(len(cross_day), 0)
        self.assertEqual((directories[0] / "tasks.csv").read_bytes(), (directories[1] / "tasks.csv").read_bytes())


if __name__ == "__main__":
    unittest.main()
