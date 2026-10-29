import json
import sqlite3
import tempfile
import unittest
import zlib
from pathlib import Path

from backend.checkpoint import write_checkpoint
from backend.replay_service import RunCatalog, frame_at, scene_for, status_for


class ReplayServiceTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.run = self.root / "run-a"
        self.run.mkdir()
        write_checkpoint(self.run / "latest.json", {
            "runId": "run-a", "elapsed": 20, "running": False,
            "knowledge": {"backend": "journal", "durable_seq": 2, "mirrored_seq": 2, "lag": 0},
        })
        write_checkpoint(self.run / "checkpoint.json", {
            "run_metadata": {"scene": {"spec": {"width": 10, "depth": 10}, "racks": []}},
        })
        with sqlite3.connect(self.run / "frames.sqlite") as db:
            db.execute("CREATE TABLE frames (second INTEGER PRIMARY KEY, payload BLOB NOT NULL)")
            for second in (10, 20):
                payload = json.dumps({"runId": "run-a", "elapsed": second}).encode()
                db.execute("INSERT INTO frames VALUES (?, ?)", (second, zlib.compress(payload)))
        (self.run / "results.json").write_text(
            json.dumps({"run_id": "run-a", "status": "complete", "wall_seconds": 2,
                        "tasks_total": 5, "released": 5, "unfinished": 0}),
            encoding="utf-8",
        )

    def tearDown(self):
        self.temp.cleanup()

    def test_catalog_discovers_a_recorded_run(self):
        catalog = RunCatalog([self.root])
        runs = catalog.refresh()
        self.assertEqual(len(runs), 1)
        self.assertEqual(runs[0]["id"], "run-a")
        self.assertEqual(runs[0]["run_id"], "run-a")
        self.assertEqual(runs[0]["status"], "complete")
        self.assertEqual(runs[0]["frame_count"], 2)
        self.assertEqual(runs[0]["latest_second"], 20)
        self.assertEqual(catalog.resolve("run-a"), self.run)

    def test_active_run_is_marked_current_and_sorted_first(self):
        live = self.root / "run-live"
        live.mkdir()
        write_checkpoint(live / "latest.json", {
            "runId": "run-live", "elapsed": 5, "running": True,
            "knowledge": {"backend": "journal", "durable_seq": 1, "mirrored_seq": 1, "lag": 0},
        })
        write_checkpoint(live / "checkpoint.json", {
            "run_metadata": {"scene": {"spec": {"width": 10, "depth": 10}, "racks": []}},
        })
        with sqlite3.connect(live / "frames.sqlite") as db:
            db.execute("CREATE TABLE frames (second INTEGER PRIMARY KEY, payload BLOB NOT NULL)")
            db.execute("INSERT INTO frames VALUES (?, ?)", (5, zlib.compress(json.dumps({"elapsed": 5}).encode())))
        runs = RunCatalog([self.root]).refresh()
        self.assertEqual(runs[0]["id"], "run-live")
        self.assertTrue(runs[0]["current"])
        self.assertTrue(any(not run["current"] for run in runs[1:]))

    def test_unknown_run_is_rejected(self):
        catalog = RunCatalog([self.root])
        catalog.refresh()
        with self.assertRaises(KeyError):
            catalog.resolve("missing")

    def test_frame_lookup_uses_nearest_prior_sample_and_clamps(self):
        self.assertEqual(frame_at(self.run, 15)["elapsed"], 10)
        self.assertEqual(frame_at(self.run, 999)["elapsed"], 20)

    def test_status_and_scene_are_read_from_the_recorded_checkpoint(self):
        self.assertEqual(status_for(self.run)["run_id"], "run-a")
        self.assertEqual(status_for(self.run)["latest_second"], 20)
        self.assertEqual(scene_for(self.run)["spec"]["width"], 10)


if __name__ == "__main__":
    unittest.main()
