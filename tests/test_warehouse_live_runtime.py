from contextlib import closing
import importlib.util
import json
import math
from pathlib import Path
import sqlite3
import tempfile
import time
import unittest


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location('owned_live_runtime', ROOT / 'ops/warehouse_live_runtime.py')
MOD = importlib.util.module_from_spec(SPEC);SPEC.loader.exec_module(MOD)
TEMP = ROOT / 'data/local-check'
TEMP.mkdir(parents=True, exist_ok=True)
REGISTRY = {'sample19': {'scene': str(ROOT / 'data/layouts/warehouse100-wide-v2.json'),
                          'dataset': str(ROOT / 'data/examples/warehouse-v5'),
                          'config': {'charging_policy': 'critical-only'}}}


class ValidationTests(unittest.TestCase):
    def test_wall_speed_is_bounded_finite_and_not_bool(self):
        for invalid in (True, False, 0, -.1, .01, 121, math.nan, math.inf, '2'):
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                MOD.positive_speed(invalid)
        for value in (.1, .5, 1, 20, 60, 120):
            self.assertEqual(MOD.positive_speed(value), float(value))

    def test_run_id_is_single_safe_directory(self):
        for invalid in ('', '..', '../old', 'a/b', 'a\\b', 'x:y', '.hidden'):
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                MOD.run_name(invalid)
        self.assertEqual(MOD.run_name('wh-live-100'), 'wh-live-100')

    def test_required_journal_mode_rejected_before_start(self):
        with self.assertRaisesRegex(ValueError, 'needs Neo4j'):
            MOD.WarehouseLiveRuntime(source_root=ROOT, output_root=TEMP / 'must-not-create',
                datasets=REGISTRY, dataset_id='sample19', knowledge_mode='required')

    def test_disturbance_registration_rejects_invalid_selector_and_values(self):
        good = {'at': 250, 'kind': 'soc', 'robot': 'carrying_moving', 'soc': 24, 'selection_timeout': 600}
        self.assertEqual(MOD.validate_disturbances([good]), [good])
        for changed in ({'robot': 'carried'}, {'robot': True}, {'soc': math.nan}, {'soc': True},
                        {'at': -1}, {'selection_timeout': -1}, {'kind': 'unknown'}):
            with self.subTest(changed=changed), self.assertRaises(ValueError):
                MOD.validate_disturbances([{**good, **changed}])


class LiveStateTests(unittest.TestCase):
    def test_actual_state_control_new_run_and_owned_resume(self):
        with tempfile.TemporaryDirectory(dir=TEMP) as folder:
            options = dict(source_root=ROOT, output_root=folder, datasets=REGISTRY,
                           dataset_id='sample19', run_id='live-test-first')
            runtime = MOD.WarehouseLiveRuntime(**options)
            try:
                initial = runtime.latest()
                self.assertEqual(initial['status'], 'ready')
                self.assertEqual(initial['sourceKind'], 'live_simulation_memory')
                self.assertEqual(len(initial['robots']), 100)
                self.assertEqual(initial['knowledge']['durable_seq'], 1)
                self.assertFalse(initial['running'])
                instance_id = initial['runtimeInstanceId']
                first = runtime.command('step', expected_run_id=initial['runId'])
                self.assertEqual(first['elapsed'], 1)
                self.assertEqual(first['status'], 'paused')
                self.assertEqual(first['knowledge']['committed_sim_time'], 1)
                self.assertEqual(first['knowledge']['durable_seq'], 2)
                with closing(sqlite3.connect(Path(folder) / 'live-test-first/journal.sqlite')) as db:
                    self.assertEqual(db.execute('SELECT seq FROM heads').fetchone()[0], 2)
                with self.assertRaisesRegex(ValueError, 'stale run'):
                    runtime.command('step', expected_run_id='old-run')
                for action in ('reset', 'set_flow_rate', 'inject_task', 'config'):
                    with self.subTest(action=action), self.assertRaises(ValueError):
                        runtime.command(action, 100)
                for speed in (True, math.nan, 0, 121):
                    with self.assertRaises(ValueError):
                        runtime.command('speed', speed)
                changed = runtime.command('speed', 120)
                self.assertEqual(changed['speed'], 120)
                from backend.checkpoint import read_checkpoint
                cp = read_checkpoint(Path(folder) / 'live-test-first/checkpoint.json')
                self.assertEqual(cp['config']['speed'], .8)
                started = runtime.command('start')
                self.assertTrue(started['running'])
                with self.assertRaisesRegex(ValueError, 'pause'):
                    runtime.command('new_run', {'dataset_id': 'sample19'})
                deadline = time.monotonic() + 5
                while runtime.latest()['elapsed'] <= 1 and time.monotonic() < deadline:
                    time.sleep(.01)
                paused = runtime.command('pause')
                self.assertGreater(paused['elapsed'], 1)
                time.sleep(.03)
                self.assertEqual(runtime.latest()['elapsed'], paused['elapsed'])
                old_checkpoint = Path(folder) / 'live-test-first/checkpoint.json'
                checksum = MOD.file_sha(old_checkpoint)
                with self.assertRaisesRegex(ValueError, 'unknown'):
                    runtime.command('new_run', {'dataset_id': 'unregistered'})
                self.assertEqual(runtime.latest()['runId'], 'live-test-first')
                second = runtime.command('new_run', {'dataset_id': 'sample19', 'run_id': 'live-test-second'})
                self.assertEqual(second['runId'], 'live-test-second')
                self.assertEqual(second['elapsed'], 0)
                self.assertEqual(second['runtimeInstanceId'], instance_id)
                self.assertGreater(second['liveRevision'], paused['liveRevision'])
                self.assertEqual(MOD.file_sha(old_checkpoint), checksum)
                runtime.command('step')
            finally:
                runtime.shutdown()
            restored = MOD.WarehouseLiveRuntime(source_root=ROOT, output_root=folder,
                datasets=REGISTRY, dataset_id='sample19', resume_run_id='live-test-second')
            try:
                self.assertEqual(restored.latest()['elapsed'], 1)
                self.assertEqual(restored.latest()['status'], 'paused')
                self.assertNotEqual(restored.latest()['runtimeInstanceId'], instance_id)
                self.assertEqual(restored.command('step')['elapsed'], 2)
            finally:
                restored.shutdown()

    def test_failed_step_is_frozen_and_requires_explicit_new_run(self):
        class FailingRuntime(MOD.WarehouseLiveRuntime):
            def _advance_one(self, force_persist=False):
                self._sim.robots[1]['x'] += 1000
                raise ValueError('diagnostic partial step failure')
        with tempfile.TemporaryDirectory(dir=TEMP) as folder:
            runtime = FailingRuntime(source_root=ROOT, output_root=folder, datasets=REGISTRY,
                                     dataset_id='sample19', run_id='live-failed')
            before = runtime.latest()
            try:
                with self.assertRaisesRegex(RuntimeError, 'partial step'):
                    runtime.command('step')
                after = runtime.latest()
                self.assertEqual(after['status'], 'failed')
                self.assertEqual(after['robots'], before['robots'])
                self.assertEqual(after['elapsed'], 0)
                self.assertTrue(list((Path(folder) / 'live-failed').glob('failure-*.json')))
                with self.assertRaises(ValueError):
                    runtime.command('resume')
                with self.assertRaises(ValueError):
                    runtime.command('speed', 2)
                new = runtime.command('new_run', {'dataset_id': 'sample19'})
                self.assertEqual(new['status'], 'ready')
                self.assertNotEqual(new['runId'], 'live-failed')
            finally:
                runtime.shutdown()
            with self.assertRaisesRegex(ValueError, 'reconciliation'):
                MOD.WarehouseLiveRuntime(source_root=ROOT, output_root=folder, datasets=REGISTRY,
                    dataset_id='sample19', resume_run_id='live-failed')


if __name__ == '__main__':
    unittest.main()
