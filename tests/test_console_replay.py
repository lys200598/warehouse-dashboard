import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from backend.checkpoint import read_checkpoint
from backend.console_replay import ConsoleFrameRecorder, console_frame, console_scene
from backend.replay_service import RunCatalog, frame_at, scene_for


def snapshot(elapsed=0.0):
    return {
        "runId": "console-run-a",
        "tick": int(elapsed * 4),
        "elapsed": elapsed,
        "running": True,
        "completed": 3,
        "metrics": {"occupiedSlots": 2, "inProgressTasks": 1},
        "knowledge": {"backend": "journal", "durable_seq": 5, "mirrored_seq": 5, "lag": 0},
        "scene": {
            "inbound": {"x": 0.6, "y": 18.55, "width": 30.8, "depth": 2.7},
            "stations": [{"id": 1, "x": 1.55, "y": 18.9}],
        },
        "racks": [{
            "id": 1, "x": 8.0, "y": 4.3, "width": 2.8, "depth": 1.15,
            "slots": [
                {"slotId": "R01-L1-C1-D1", "level": 1, "bay": 0, "depthRow": 0, "occupied": True},
                {"slotId": "R01-L2-C2-D2", "level": 2, "bay": 1, "depthRow": 1, "occupied": False},
            ],
        }],
        "robots": [{
            "id": 1, "x": 1.55, "y": 0.9, "stage": "idle", "battery": 90.0,
            "carrying": False, "taskId": None, "cargoId": "G-1",
        }, {
            "id": 2, "x": 3.03, "y": 0.9, "stage": "toPick", "battery": 80.0,
            "carrying": False, "taskId": "T-1", "cargoId": None,
        }],
    }


class ConsoleReplayTests(unittest.TestCase):
    def test_scene_and_frame_adapt_console_snapshot(self):
        scene = console_scene(snapshot())
        self.assertGreater(scene["spec"]["width"], 0)
        self.assertEqual(scene["capacity"], 2)
        self.assertTrue(any(station["kind"] == "outbound" for station in scene["stations"]))
        frame = console_frame(snapshot(5.0))
        self.assertEqual(frame["runId"], "console-run-a")
        self.assertEqual(frame["robots"][0]["robot_id"], 1)
        self.assertEqual(frame["counts"]["moving"], 1)
        self.assertEqual(frame["counts"]["standby"], 1)
        self.assertEqual(frame["metrics"]["completed"], 3)
        self.assertEqual(frame["inside"], 2)

    def test_recorder_writes_replay_run_and_catalog_marks_it_current(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            recorder = ConsoleFrameRecorder(root, "console-run-a", interval_seconds=5)
            recorder.record(snapshot(0.0))
            recorder.record(snapshot(5.0))
            self.assertTrue((recorder.directory / "latest.json").is_file())
            self.assertTrue((recorder.directory / "checkpoint.json").is_file())
            with sqlite3.connect(recorder.directory / "frames.sqlite") as db:
                self.assertEqual(db.execute("SELECT COUNT(*) FROM frames").fetchone()[0], 2)
            self.assertEqual(read_checkpoint(recorder.directory / "latest.json")["elapsed"], 5.0)
            self.assertEqual(scene_for(recorder.directory)["capacity"], 2)
            self.assertEqual(frame_at(recorder.directory, 3)["elapsed"], 0.0)
            runs = RunCatalog([root]).refresh()
            self.assertEqual(runs[0]["id"], "console-console-run-a")
            self.assertTrue(runs[0]["current"])
            recorder.close()
            self.assertFalse(read_checkpoint(recorder.directory / "latest.json")["running"])


if __name__ == "__main__":
    unittest.main()
