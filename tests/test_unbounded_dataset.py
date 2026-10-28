"""Unbounded fixed-dataset duration, bounded resources, unchanged integrity."""
import csv
from datetime import datetime, timedelta
import hashlib
import importlib.util
import json
from pathlib import Path
import tempfile
import types
import unittest
from zipfile import ZipFile

from backend.day1.data import FIELDS, sha
from backend.wide.data import (MAX_DURATION_SECONDS, MAX_DENSE_REPORT_BUCKETS,
                               load_scene, validate_dataset)
from backend.wide.scenarios import generate_scenario


REPO = Path(__file__).resolve().parents[1]
WORKSPACE = REPO.parent
SCENE = REPO/'data/layouts/warehouse100-wide-v2.json'
CONTRACT = REPO/'docs/dataset-contract'
SOURCE_ZIP = WORKSPACE/'research/warehouse-finalization-20261027/warehouse-scheduler-v5.0.0-source.zip'
PUBLISHED = WORKSPACE/'publication/warehouse-v5-20261028/docs/dataset-contract'
# Canonical report digests verified against the ORIGINAL frozen validator.
# These keep clone/remote tests self-contained when the old release ZIP is not
# present; existing local archives add direct complete-dictionary comparison.
SHORT_REPORT_SHA = {
    'sample-input': '2ce11fd8a5de9d00582177a850836b6258bab8f58a765cc6d590d814b3baf95b',
    'continuous-2d': '08e65ec4dff0bf3618a9cd12bbfd8853b20d74ab594e4399468557b1993fa454',
    'hotspot-2h': '0ac168a29610b880379ae1726ada595c268f743ecddf2c39962413319a48776d',
    'peak-2h': 'ee33469e70f5762a9fe2e77fa8c9c12ee4a7e13640acd8f3b5ca99031fae688c',
    'generated-1': '718b8a299f8c5167f2b16038213f8954f29c0b5aca0221866010369e60d6b0cf',
    'generated-2': '76a77974e4d5e2ab366b510b21d1cebbae77cd2d637c5c290e6275646d83ce3a',
    'generated-7': 'a772e058b34154f2be212381a0519a654f9716dae667a1d2b06e8ce73e91f4a8'}


def report_sha(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':'),
        ensure_ascii=False, allow_nan=False).encode('utf-8')).hexdigest()


def _load_independent(path):
    spec = importlib.util.spec_from_file_location('unbounded_standalone_contract', path)
    value = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(value)
    return value


def _old_data_validator():
    if not SOURCE_ZIP.is_file():
        return None
    with ZipFile(SOURCE_ZIP) as archive:
        source = archive.read('backend/wide/data.py')
    module = types.ModuleType('backend.wide._frozen_validator_test_only')
    module.__package__ = 'backend.wide'
    exec(compile(source, '<original-frozen-wide-data>', 'exec'), module.__dict__)
    return module.validate_dataset


class UnboundedDatasetTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.scene, cls.slots = load_scene(SCENE)
        cls.keys = list(cls.slots)
        cls.independent = _load_independent(CONTRACT/'validate_package.py')
        cls.ind_scene, cls.ind_slots, cls.stations, cls.layout = cls.independent.scene_contract(CONTRACT)
        old = _old_data_validator()
        cls.old_validate = staticmethod(old) if old is not None else None

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='unbounded-data-', dir=REPO/'tests')
        self.root = Path(self.temp.name)

    def tearDown(self):
        self.temp.cleanup()

    def write_dataset(self, *, duration=30*86400, tasks=None, initial=None, name='dataset'):
        folder = self.root/name
        folder.mkdir()
        start = datetime.fromisoformat('2026-10-28T00:00:00+08:00')
        if tasks is None:
            tasks = [('INBOUND', 'G-ONE', 0, 'IN-1', self.keys[0]),
                     ('OUTBOUND', 'G-ONE', 10*86400, self.keys[0], 'OUT-1')]
        materialized = []
        for i, (kind, goods, second, source, target) in enumerate(tasks, 1):
            materialized.append(dict(task_id=f'T-{i:03d}', task_type=kind, goods_id=goods,
                release_time=(start+timedelta(seconds=second)).isoformat(),
                due_time=(start+timedelta(seconds=second+60)).isoformat(),
                source_id=source, target_id=target))
        initial = initial or []
        for name, values, fields in [('tasks.csv', materialized, FIELDS),
                ('initial_inventory.csv', initial, ['goods_id', 'location_id'])]:
            with (folder/name).open('w', encoding='utf-8', newline='') as stream:
                writer = csv.DictWriter(stream, fieldnames=fields)
                writer.writeheader(); writer.writerows(values)
        manifest = dict(version='test-fixed-unbounded-v1', start=start.isoformat(),
            duration_seconds=duration, scene_sha256=sha(SCENE), capacity=1728,
            tasks=len(materialized), inbound=sum(t['task_type']=='INBOUND' for t in materialized),
            outbound=sum(t['task_type']=='OUTBOUND' for t in materialized), initial_inventory=len(initial),
            files={name: sha(folder/name) for name in ('tasks.csv', 'initial_inventory.csv')})
        (folder/'manifest.json').write_text(json.dumps(manifest), encoding='utf-8')
        return folder

    def standalone(self, folder, **kwargs):
        return self.independent.validate_dataset(folder, self.ind_scene, self.ind_slots,
            self.stations, self.layout['scene_sha256'], **kwargs)

    def alter_manifest(self, folder, **changes):
        path = folder/'manifest.json'
        manifest = json.loads(path.read_text(encoding='utf-8'))
        manifest.update(changes)
        path.write_text(json.dumps(manifest), encoding='utf-8')
        return manifest

    def rewrite_tasks(self, folder, mutate):
        path = folder/'tasks.csv'
        with path.open(encoding='utf-8', newline='') as stream:
            values = list(csv.DictReader(stream))
        mutate(values)
        with path.open('w', encoding='utf-8', newline='') as stream:
            writer = csv.DictWriter(stream, fieldnames=FIELDS)
            writer.writeheader(); writer.writerows(values)
        manifest = json.loads((folder/'manifest.json').read_text(encoding='utf-8'))
        manifest['files']['tasks.csv'] = sha(path)
        self.alter_manifest(folder, files=manifest['files'])

    def test_compatibility_export_is_unbounded(self):
        self.assertIsNone(MAX_DURATION_SECONDS)
        self.assertEqual(MAX_DENSE_REPORT_BUCKETS, 4096)

    def test_eight_and_thirty_day_generation_remain_fixed_and_reproducible(self):
        for days in (8, 30):
            with self.subTest(days=days):
                kwargs = dict(duration_seconds=days*86400, active_slots=4,
                              slot_interval_seconds=86400, max_tasks=200, seed=42)
                a, ra = generate_scenario(SCENE, self.root/f'{days}-a', **kwargs)
                b, rb = generate_scenario(SCENE, self.root/f'{days}-b', **kwargs)
                self.assertEqual(a, b)
                self.assertEqual(ra, rb)
                self.assertEqual(a['tasks'], days*4)
                self.assertEqual(len(ra['daily_tasks']), days)
                self.assertEqual(sum(ra['daily_tasks']), days*4)
                self.assertTrue(ra['hashes_verified'])
                self.assertFalse(ra['runtime_feasibility_claim'])
                self.assertTrue(self.standalone(self.root/f'{days}-a')['reference_valid'])

    def test_all_four_profiles_accept_eight_days_without_changing_capacity(self):
        for profile in ('uniform', 'peak', 'hotspot', 'continuous'):
            with self.subTest(profile=profile):
                manifest, report = generate_scenario(SCENE, self.root/profile,
                    scenario=profile, duration_seconds=8*86400, active_slots=2,
                    slot_interval_seconds=86400, seed=43, max_tasks=300)
                self.assertEqual(manifest['capacity'], 1728)
                self.assertEqual(manifest['initial_inventory'], 864)
                self.assertTrue(report['valid'])

    def test_extremely_long_sparse_manifest_never_allocates_duration_arrays(self):
        duration = 10**400
        folder = self.write_dataset(duration=duration)
        report = validate_dataset(SCENE, folder)
        independent = self.standalone(folder)
        for field, seconds in [('hourly_tasks', 3600), ('daily_tasks', 86400)]:
            histogram = report[field]
            self.assertEqual(histogram['format'], 'sparse-v1')
            self.assertEqual(histogram['bucket_count'], (duration+seconds-1)//seconds)
            self.assertEqual(histogram['bucket_seconds'], seconds)
            self.assertTrue(histogram['zero_buckets_omitted'])
            self.assertEqual(histogram['nonzero'], [{'bucket': 0, 'tasks': 1},
                {'bucket': 10*86400//seconds, 'tasks': 1}])
        self.assertEqual(report['daily_tasks'], independent['daily_tasks'])
        self.assertLess(len(json.dumps(report)), 5000)
        self.assertEqual(report['planned_final_inventory'], 0)

    def test_long_empty_workload_uses_empty_sparse_counts(self):
        folder = self.write_dataset(duration=10**100, tasks=[])
        report = validate_dataset(SCENE, folder)
        self.assertEqual(report['tasks'], 0)
        self.assertEqual(report['hourly_tasks']['nonzero'], [])
        self.assertEqual(report['daily_tasks']['nonzero'], [])

    def test_invalid_duration_types_and_nonpositive_values_still_rejected(self):
        folder = self.write_dataset(tasks=[])
        for duration in (0, -1, True, False, 1.0, 30.5, '2592000', None):
            with self.subTest(duration=duration):
                self.alter_manifest(folder, duration_seconds=duration)
                with self.assertRaises(ValueError):
                    validate_dataset(SCENE, folder)
                with self.assertRaises(ValueError):
                    self.standalone(folder)
                with self.assertRaises(ValueError):
                    generate_scenario(SCENE, self.root/f'bad-{str(duration)}', duration_seconds=duration)

    def test_actual_dates_and_release_due_range_still_strict(self):
        for label, mutation in [
                ('no-zone', lambda r: r[0].update(release_time='2026-10-28T00:00:00')),
                ('bad-date', lambda r: r[0].update(release_time='2026-02-30T00:00:00+08:00')),
                ('fraction', lambda r: r[0].update(release_time='2026-10-28T00:00:00.1+08:00')),
                ('offset', lambda r: r[0].update(release_time='2026-10-28T00:00:00+07:00')),
                ('due', lambda r: r[0].update(due_time=r[0]['release_time']))]:
            with self.subTest(label=label):
                folder = self.write_dataset(name=label)
                self.rewrite_tasks(folder, mutation)
                with self.assertRaises(ValueError): validate_dataset(SCENE, folder)
                with self.assertRaises(ValueError): self.standalone(folder)

    def test_generator_datetime_overflow_is_clear_and_creates_no_partial_output(self):
        for profile in ('uniform', 'peak', 'hotspot', 'continuous'):
            with self.subTest(profile=profile):
                output = self.root/profile
                with self.assertRaisesRegex(ValueError, 'ISO8601 datetime range'):
                    generate_scenario(SCENE, output, scenario=profile, duration_seconds=10**400,
                        slot_interval_seconds=10**400, active_slots=1, max_tasks=4)
                self.assertFalse(output.exists())

    def test_max_tasks_is_configurable_resource_budget_not_calendar_gate(self):
        output = self.root/'long'
        with self.assertRaisesRegex(ValueError, 'task budget'):
            generate_scenario(SCENE, output, duration_seconds=30*86400,
                active_slots=4, slot_interval_seconds=86400, max_tasks=119)
        self.assertFalse(output.exists())
        manifest, report = generate_scenario(SCENE, output, duration_seconds=30*86400,
            active_slots=4, slot_interval_seconds=86400, max_tasks=120)
        self.assertEqual(manifest['tasks'], 120)
        self.assertTrue(report['valid'])
        self.assertTrue(validate_dataset(SCENE, output, max_tasks=120)['valid'])
        self.assertTrue(self.standalone(output, max_tasks=120)['reference_valid'])
        for validate in (lambda: validate_dataset(SCENE, output, max_tasks=119),
                         lambda: self.standalone(output, max_tasks=119)):
            with self.assertRaisesRegex(ValueError, 'max_tasks'): validate()
        for limit in (False, 0, -1, 1.0, '120'):
            with self.subTest(limit=limit):
                with self.assertRaises(ValueError): validate_dataset(SCENE, output, max_tasks=limit)
                with self.assertRaises(ValueError): self.standalone(output, max_tasks=limit)

    def test_budget_checks_actual_rows_even_if_manifest_underreports(self):
        folder = self.write_dataset()
        self.alter_manifest(folder, tasks=1, inbound=1, outbound=0)
        with self.assertRaisesRegex(ValueError, 'max_tasks'): validate_dataset(SCENE, folder, max_tasks=1)
        with self.assertRaisesRegex(ValueError, 'resource budget'): self.standalone(folder, max_tasks=1)

    def test_sha_and_manifest_capacity_remain_required(self):
        folder = self.write_dataset()
        with (folder/'tasks.csv').open('a', encoding='utf-8') as stream: stream.write('\n')
        with self.assertRaisesRegex(ValueError, 'hash mismatch'): validate_dataset(SCENE, folder)
        with self.assertRaisesRegex(ValueError, 'SHA mismatch'): self.standalone(folder)
        manifest = json.loads((folder/'manifest.json').read_text())
        manifest['files']['tasks.csv'] = sha(folder/'tasks.csv')
        self.alter_manifest(folder, files=manifest['files'], capacity=1729)
        with self.assertRaisesRegex(ValueError, 'capacity'): validate_dataset(SCENE, folder)
        with self.assertRaisesRegex(ValueError, 'capacity'): self.standalone(folder)

    def test_duplicate_task_goods_initial_goods_and_occupied_slot_still_rejected(self):
        bad = {
            'reused-goods': ([('INBOUND', 'G1', 0, 'IN-1', self.keys[0]),
                ('OUTBOUND', 'G1', 1, self.keys[0], 'OUT-1'),
                ('INBOUND', 'G1', 2, 'IN-1', self.keys[0])], []),
            'occupied-slot': ([('INBOUND', 'G1', 0, 'IN-1', self.keys[0]),
                ('INBOUND', 'G2', 1, 'IN-1', self.keys[0])], []),
            'initial-duplicate-goods': ([], [{'goods_id': 'SAME', 'location_id': self.keys[0]},
                {'goods_id': 'SAME', 'location_id': self.keys[1]}])}
        for name, (tasks, initial) in bad.items():
            with self.subTest(name=name):
                folder = self.write_dataset(name=name, tasks=tasks, initial=initial)
                with self.assertRaises(ValueError): validate_dataset(SCENE, folder)
                with self.assertRaises(ValueError): self.standalone(folder)
        folder = self.write_dataset(name='duplicate-task')
        self.rewrite_tasks(folder, lambda r: r[1].update(task_id=r[0]['task_id']))
        with self.assertRaisesRegex(ValueError, 'duplicate task'): validate_dataset(SCENE, folder)
        with self.assertRaisesRegex(ValueError, 'duplicate task'): self.standalone(folder)

    def test_published_short_datasets_keep_identical_old_validation_results_and_bytes(self):
        before = {str(p.relative_to(PUBLISHED)): sha(p) for p in PUBLISHED.rglob('*') if p.is_file()} if PUBLISHED.is_dir() else None
        datasets = [CONTRACT/'sample-input']+sorted(p.parent for p in (CONTRACT/'supplement-inputs').glob('*/manifest.json'))
        for dataset in datasets:
            with self.subTest(dataset=dataset.name):
                report = validate_dataset(SCENE, dataset)
                self.assertEqual(report_sha(report), SHORT_REPORT_SHA[dataset.name])
                if self.old_validate is not None:
                    self.assertEqual(report, self.old_validate(SCENE, dataset))
        if before is not None:
            after = {str(p.relative_to(PUBLISHED)): sha(p) for p in PUBLISHED.rglob('*') if p.is_file()}
            self.assertEqual(before, after)

    def test_new_one_two_seven_day_inputs_keep_old_report_exactly(self):
        for days in (1, 2, 7):
            with self.subTest(days=days):
                folder = self.root/str(days)
                _, report = generate_scenario(SCENE, folder, duration_seconds=days*86400,
                    active_slots=4, slot_interval_seconds=86400, seed=8)
                # Generator adds scenario metadata after the pure validator.
                report = validate_dataset(SCENE, folder)
                self.assertEqual(report_sha(report), SHORT_REPORT_SHA[f'generated-{days}'])
                if self.old_validate is not None:
                    self.assertEqual(report, self.old_validate(SCENE, folder))
                self.assertEqual(len(report['daily_tasks']), days)

    def test_local_independent_contract_has_no_duration_cap_and_snapshot_is_unchanged(self):
        contract = json.loads((CONTRACT/'contract/data-contract.json').read_text(encoding='utf-8'))
        self.assertIsNone(contract['maximum_duration_seconds'])
        if PUBLISHED.is_dir():
            frozen = json.loads((PUBLISHED/'contract/data-contract.json').read_text(encoding='utf-8'))
            self.assertEqual(frozen['maximum_duration_seconds'], 7*86400)


if __name__ == '__main__':
    unittest.main()
