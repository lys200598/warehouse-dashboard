import json
import tempfile
import unittest
from pathlib import Path

from backend.server import dataset_id_for_task_file


class DatasetIdResolutionTests(unittest.TestCase):
    def test_reads_dataset_identity_from_adjacent_manifest(self):
        with tempfile.TemporaryDirectory() as directory:
            package = Path(directory)
            (package / "manifest.json").write_text(json.dumps({"version": "warehouse-v2", "start": "2026-10-27T08:00:00+08:00"}), encoding="utf-8")
            self.assertEqual(dataset_id_for_task_file(package / "tasks.csv"), "dataset:warehouse-v2:2026-10-27")

    def test_returns_none_without_generated_manifest(self):
        self.assertIsNone(dataset_id_for_task_file(Path("/tmp/no-such-package/tasks.csv")))


if __name__ == "__main__":
    unittest.main()
