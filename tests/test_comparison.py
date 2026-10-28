import json
from pathlib import Path
import tempfile
import unittest

from backend.comparison.experiment import prepare, source_identity
from backend.comparison.metrics import distribution, task_timeline, compare_runs, input_and_checkpoint_tasks
from backend.checkpoint import write_checkpoint
from backend.scheduling_v3.scenarios import generate_scenario


ROOT = Path(__file__).parents[1]


class MetricsTests(unittest.TestCase):
    def test_empty_distribution_is_not_zero_latency(self):
        self.assertIsNone(distribution([])['mean'])

    def test_backlog_exact_area_and_window(self):
        tasks = [dict(release_second=0, status='completed', completed_at=10),
                 dict(release_second=5, status='assigned')]
        value = task_timeline(tasks, 20, 15)
        self.assertEqual(value['peak_released_unfinished'], 2)
        self.assertEqual(value['mean_released_unfinished'], 1.15)
        self.assertEqual(value['released_unfinished_at_release_window_or_observed_end'], 1)

    def test_timeout_does_not_invent_future_release(self):
        tasks = [dict(release_second=5, status='assigned'),
                 dict(release_second=50, status='scheduled')]
        value = task_timeline(tasks, 10, 100)
        self.assertFalse(value['release_window_observed'])
        self.assertEqual(value['release_window_evaluation_second'], 10)
        self.assertEqual(value['released_unfinished_at_observed_end'], 1)

    def test_same_timestamp_grouped(self):
        tasks = [dict(release_second=0, status='completed', completed_at=10),
                 dict(release_second=10, status='assigned')]
        self.assertEqual(task_timeline(tasks, 10, 10)['peak_released_unfinished'], 1)

    def test_release_at_cutoff_not_yet_executed(self):
        tasks = [dict(release_second=10, status='scheduled')]
        self.assertEqual(task_timeline(tasks, 10, 10)['released_unfinished_at_observed_end'], 0)

    def test_config_mismatch_not_controlled(self):
        def run(speed):
            return dict(input_identity={'tasks_sha256': 'a', 'initial_inventory_sha256': 'b'},
                        config={'speed': speed}, status='complete', all_input_completed=True,
                        wall_seconds=1, completed=1, late_completed=0, released_unfinished=0,
                        resource_metrics={})
        self.assertFalse(compare_runs(run(.8), run(1))['controlled_except_layout_resources'])

    def test_success_never_general_bottleneck_claim(self):
        run = dict(input_identity={'tasks_sha256': 'a', 'initial_inventory_sha256': 'b'},
                   config={}, status='complete', all_input_completed=True, wall_seconds=1,
                   completed=1, late_completed=0, released_unfinished=0, resource_metrics={})
        self.assertTrue(compare_runs(run, run)['pair_complete'])
        self.assertFalse(compare_runs(run, run)['bottleneck_eliminated_generally'])

    def test_source_identity_contains_both_adapters(self):
        files = source_identity()['files']
        self.assertIn('scheduling_v3/sim.py', files)
        self.assertIn('wide/sim.py', files)

    def test_prepare_preserves_bytes_and_checkpoint_overlay_retains_unreleased(self):
        temporary_root = ROOT / 'data/local-check'
        temporary_root.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(dir=temporary_root) as folder:
            folder = Path(folder)
            source = folder / 'source'
            generate_scenario(ROOT / 'data/layouts/warehouse100-v1.json', source,
                              duration_seconds=60, active_slots=4, slot_interval_seconds=60)
            output = folder / 'pair'
            plan = prepare(old_scene=ROOT / 'data/layouts/warehouse100-v1.json',
                           wide_scene=ROOT / 'data/layouts/warehouse100-wide-v2.json',
                           source_dataset=source, output=output, run_prefix='unit-pair')
            self.assertEqual((output / 'input-old/tasks.csv').read_bytes(),
                             (output / 'input-wide/tasks.csv').read_bytes())
            self.assertEqual(plan['source_validation']['tasks'], 4)
            run = output / 'run-old'
            run.mkdir()
            write_checkpoint(run / 'checkpoint.json', {'run_id': 'unit', 'elapsed': 0, 'tasks': {}})
            _, tasks, _ = input_and_checkpoint_tasks(run)
            self.assertEqual(len(tasks), 4)
            self.assertTrue(all(task['status'] == 'scheduled' for task in tasks))
            with self.assertRaises(FileExistsError):
                prepare(old_scene=ROOT / 'data/layouts/warehouse100-v1.json',
                        wide_scene=ROOT / 'data/layouts/warehouse100-wide-v2.json',
                        source_dataset=source, output=output, run_prefix='unit-pair')


if __name__ == '__main__':
    unittest.main()
