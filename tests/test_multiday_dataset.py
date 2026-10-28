from collections import Counter
import csv
from datetime import datetime
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from backend.wide.data import FIELDS, validate_dataset
from backend.wide.multiday import (DAY, PROFILES, day_release_seconds, generate_multiday)


ROOT = Path(__file__).resolve().parents[1]
SCENE = ROOT/'data/layouts/warehouse100-wide-v2.json'


class FixtureRoadReference:
    """Declared deterministic unit-test road substitute, never production input."""
    def __init__(self, scene, slots, speed):
        self.deadhead_seconds = 600

    def task_seconds(self, slot, station):
        return 20+int(station.split('-')[-1])


class MultidayDatasetTests(unittest.TestCase):
    def setUp(self):
        (ROOT/'data/local-check').mkdir(parents=True, exist_ok=True)
        self.temp = tempfile.TemporaryDirectory(dir=ROOT/'data/local-check')
        self.folder = Path(self.temp.name)
        self.road = patch('backend.wide.multiday.RoadReference', FixtureRoadReference)
        self.road.start()

    def tearDown(self):
        self.road.stop()
        self.temp.cleanup()

    def generate(self, name='data', **kwargs):
        options = {'days': 7, 'tasks_per_day': 96, 'active_slots': 64, 'seed': 20261028}
        options.update(kwargs)
        result = generate_multiday(SCENE, self.folder/name, **options)
        return self.folder/name, result

    def read(self, path, name):
        return json.loads((path/name).read_text(encoding='utf-8'))

    def rows(self, path, name='tasks.csv'):
        with (path/name).open(encoding='utf-8', newline='') as stream:
            return list(csv.DictReader(stream))

    def test_seven_day_types_counts_hashes_and_independent_validation(self):
        folder, (manifest, report) = self.generate()
        self.assertEqual(manifest['day_profiles'], list(PROFILES))
        self.assertEqual(manifest['tasks'], 96*6+120)
        self.assertEqual(manifest['initial_inventory'], 864)
        self.assertTrue(report['valid'])
        self.assertEqual(report['duration_seconds'], 7*DAY)
        independent = validate_dataset(SCENE, folder)
        self.assertEqual(independent['tasks'], manifest['tasks'])
        for name, expected in {**manifest['files'], **manifest['reports']}.items():
            self.assertEqual(hashlib.sha256((folder/name).read_bytes()).hexdigest(), expected)

    def test_repeat_seed_produces_identical_files(self):
        a, _ = self.generate('first')
        b, _ = self.generate('second')
        self.assertEqual({p.name:p.read_bytes() for p in a.iterdir()},
                         {p.name:p.read_bytes() for p in b.iterdir()})

    def test_different_seed_changes_inputs(self):
        a, _ = self.generate('first')
        b, _ = self.generate('second', seed=20261029)
        self.assertNotEqual((a/'tasks.csv').read_bytes(), (b/'tasks.csv').read_bytes())

    def test_one_global_goods_ledger_and_task_ids_across_midnight(self):
        folder, (manifest, _) = self.generate()
        initial = self.rows(folder, 'initial_inventory.csv')
        stock = {row['location_id']: row['goods_id'] for row in initial}
        ever_seen = set(stock.values())
        task_ids = set()
        for task in self.rows(folder):
            self.assertNotIn(task['task_id'], task_ids)
            task_ids.add(task['task_id'])
            if task['task_type'] == 'INBOUND':
                self.assertNotIn(task['target_id'], stock)
                self.assertNotIn(task['goods_id'], ever_seen)
                stock[task['target_id']] = task['goods_id']
                ever_seen.add(task['goods_id'])
            else:
                self.assertEqual(stock.pop(task['source_id']), task['goods_id'])
            self.assertLessEqual(len(stock), 1728)
        reference = self.read(folder, 'reference-inventory.json')
        self.assertEqual(reference['inventory_after_reference_drain']['inside'], len(stock))
        self.assertEqual(len(task_ids), manifest['tasks'])

    def test_daily_inventory_continuity_and_physical_capacity(self):
        folder, _ = self.generate()
        days = self.read(folder, 'profiles-report.json')['days']
        for index, day in enumerate(days):
            opening, closing = day['opening_reference_inventory'], day['closing_reference_inventory']
            self.assertEqual(opening['inside']+day['reference_received_during_day']-
                             day['reference_shipped_during_day'], closing['inside'])
            self.assertTrue(day['reference_inventory_conserved'])
            if index:
                self.assertEqual(days[index-1]['closing_reference_inventory'], opening)
        inventory = self.read(folder, 'reference-inventory.json')
        self.assertGreaterEqual(inventory['minimum_inside'], 0)
        self.assertLessEqual(inventory['maximum_inside'], 1728)
        for value in inventory['hourly_inventory']:
            self.assertEqual(value['inside'], value['stored']+value['inbound_pending']+value['outbound_pending'])
            self.assertEqual(1728, value['stored']+value['inbound_pending']+value['outbound_pending']+value['empty'])

    def test_slot_turnaround_and_due_formula_are_input_rules(self):
        folder, (manifest, _) = self.generate()
        last = {}
        for task in self.rows(folder):
            release, due = datetime.fromisoformat(task['release_time']), datetime.fromisoformat(task['due_time'])
            key = task['target_id'] if task['task_type'] == 'INBOUND' else task['source_id']
            station = task['source_id'] if task['task_type'] == 'INBOUND' else task['target_id']
            self.assertEqual((due-release).total_seconds(), 600+20+int(station.split('-')[-1])+22+900)
            if key in last:
                self.assertGreaterEqual((release-last[key]).total_seconds(), 1800)
            last[key] = release
        self.assertEqual(manifest['generator_parameters']['deadline_slack_seconds'], 900)

    def test_seven_fields_and_algorithm_separation(self):
        folder, (manifest, _) = self.generate()
        with (folder/'tasks.csv').open(encoding='utf-8', newline='') as stream:
            self.assertEqual(next(csv.reader(stream)), FIELDS)
        rows = self.rows(folder)
        self.assertTrue(all(set(row) == set(FIELDS) for row in rows))
        self.assertIn('SOC', manifest['assumptions']['algorithm_separation'])
        self.assertNotIn('robot_id', rows[0])

    def test_batch_same_second_and_per_port_envelope(self):
        folder, _ = self.generate(tasks_per_day=288)
        batches = Counter()
        inbound = Counter()
        start = datetime.fromisoformat('2026-10-28T00:00:00+08:00')
        for task in self.rows(folder):
            release = int((datetime.fromisoformat(task['release_time'])-start).total_seconds())
            if release//DAY == 3:
                batches[release] += 1
                if task['task_type'] == 'INBOUND':
                    inbound[release, task['source_id']] += 1
        self.assertTrue(batches)
        self.assertLessEqual(max(batches.values()), 48)
        self.assertGreater(max(batches.values()), 1)
        self.assertLessEqual(max(inbound.values()), 6)

    def test_temporal_profiles_isolate_peak_and_recovery(self):
        import random
        peak = day_release_seconds('fixed_peak', 2400, random.Random(8))
        fraction = sum(second//3600 in {8,9,14,15,20,21} for second in peak)/len(peak)
        self.assertAlmostEqual(fraction, .8, delta=.01)
        mixed = day_release_seconds('mixed_recovery', 2400, random.Random(8))
        self.assertAlmostEqual(sum(6*3600 <= second < 12*3600 for second in mixed)/2400, .7, delta=.01)
        self.assertAlmostEqual(sum(12*3600 <= second < 18*3600 for second in mixed)/2400, .05, delta=.01)

    def test_spatial_hotspots_are_actual_map_regions_and_can_move(self):
        folder, _ = self.generate(tasks_per_day=192)
        report = self.read(folder, 'profiles-report.json')
        a, b = report['spatial_definitions']['hotspot_a'], report['spatial_definitions']['hotspot_b']
        self.assertNotEqual(a['centre'], b['centre'])
        self.assertTrue(set(a['slot_ids']).isdisjoint(b['slot_ids']))
        for index in (2, 4):
            stats = report['days'][index]['hotspot_statistics']
            self.assertGreater(stats['inside_current_hotspot']/stats['evaluated_tasks'], .5)

    def test_tidal_day_has_out_then_in_tendency(self):
        # Start at the common 50% reference stock. A preceding scenario may
        # already leave a tiny fixture at its low target, in which case the
        # correct adaptive policy preserves it rather than forcing fake OUTs.
        folder, _ = self.generate(days=1, profiles=['tidal'], tasks_per_day=288)
        day = self.read(folder, 'profiles-report.json')['days'][0]
        self.assertGreater(sum(day['hourly_outbound'][:8]), sum(day['hourly_inbound'][:8]))
        self.assertGreater(sum(day['hourly_inbound'][8:16]), sum(day['hourly_outbound'][8:16]))

    def test_eight_and_ten_days_no_seven_day_ceiling(self):
        for days in (8, 10):
            with self.subTest(days=days):
                folder, (manifest, validation) = self.generate('long-'+str(days), days=days, tasks_per_day=12)
                self.assertEqual(manifest['duration_seconds'], days*DAY)
                self.assertEqual(len(manifest['day_profiles']), days)
                self.assertTrue(validation['valid'])
                self.assertEqual(len(self.read(folder, 'profiles-report.json')['days']), days)

    def test_configurable_profile_cycle(self):
        folder, (manifest, _) = self.generate(days=3, profiles=['uniform', 'fixed_peak'])
        self.assertEqual(manifest['day_profiles'], ['uniform', 'fixed_peak', 'uniform'])

    def test_existing_output_is_preserved(self):
        folder, _ = self.generate()
        before = (folder/'manifest.json').read_bytes()
        with self.assertRaises(FileExistsError):
            self.generate()
        self.assertEqual(before, (folder/'manifest.json').read_bytes())

    def test_explicit_job_budget_rejects_before_roadmap(self):
        with patch('backend.wide.multiday.RoadReference') as road:
            with self.assertRaisesRegex(ValueError, 'max_tasks'):
                self.generate(days=10000000000, tasks_per_day=1, max_tasks=100)
            road.assert_not_called()

    def test_unachievable_turnover_is_reported_without_changing_release_profile(self):
        with self.assertRaisesRegex(ValueError, 'turnover capacity exhausted'):
            self.generate(active_slots=1, days=1, tasks_per_day=100, minimum_slot_gap_seconds=3600)
        self.assertFalse((self.folder/'data').exists())

    def test_invalid_arguments(self):
        for values in ({'days': True}, {'days': 0}, {'tasks_per_day': -1}, {'seed': False},
                       {'profiles': ['unknown']}, {'mixed_load_factor': float('nan')},
                       {'batch_size': 49}, {'active_slots': 1729}, {'date': '2026-99-99'}):
            with self.subTest(values=values), self.assertRaises(ValueError):
                self.generate(**values)


if __name__ == '__main__':
    unittest.main()
