import csv
from copy import deepcopy
import hashlib
import json
from pathlib import Path
import tempfile
import unittest

from backend.wide.data import load_scene, migrate_dataset, validate_dataset
from backend.wide.layout_candidates import BASE, build_candidate, write_candidate


class ExpansionLayoutTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.repo = Path(__file__).resolve().parents[1]
        cls.temp = tempfile.TemporaryDirectory(prefix='expansion-layout-', dir=cls.repo/'tests')
        cls.root = Path(cls.temp.name)
        cls.paths = {}
        cls.reports = {}
        cls.source_sha = hashlib.sha256(BASE.read_bytes()).hexdigest()
        for name in ('medium', 'large'):
            output = cls.root/name
            cls.reports[name] = write_candidate(name, output, audit=True)
            cls.paths[name] = output/'layout.json'

    @classmethod
    def tearDownClass(cls):
        cls.temp.cleanup()

    def test_declared_capacity_matches_explicit_physical_racks(self):
        for name, count, size in [('medium', 48, (120., 90.)), ('large', 60, (150., 100.))]:
            with self.subTest(candidate=name):
                scene, slots = load_scene(self.paths[name])
                self.assertEqual((scene['spec']['width'], scene['spec']['depth']), size)
                self.assertEqual(len(scene['racks']), count)
                self.assertEqual(scene['capacity'], count*48)
                self.assertEqual(len(slots), count*48)
                self.assertEqual(scene['spec']['robotRadius'], .55)
                self.assertEqual(scene['spec']['robotBodyWidth'], .7)

    def test_every_old_task_endpoint_and_bay_id_is_preserved(self):
        old, old_slots = load_scene(BASE)
        for path in self.paths.values():
            scene, slots = load_scene(path)
            self.assertTrue(set(old_slots) <= set(slots))
            self.assertEqual({p['id'] for p in old['stations']}, {p['id'] for p in scene['stations']})
            self.assertEqual({p['id'] for p in old['docks']}, {p['id'] for p in scene['docks']})
            self.assertEqual({p['id'] for p in old['homes']}, {p['id'] for p in scene['homes']})
            self.assertNotEqual(slots['R01-L1-C1-F1'], old_slots['R01-L1-C1-F1'])
            self.assertNotEqual(scene['spec']['version'], old['spec']['version'])

    def test_canonical_bays_and_home_aliases_remain_capacity_one(self):
        for path in self.paths.values():
            scene, _ = load_scene(path)
            self.assertEqual(scene['physical_bay_count'], 100)
            self.assertEqual(len(set(scene['location_aliases'].values())), 100)
            by_id = {d['id']: d for d in scene['docks']}
            for h in scene['homes']:
                d = by_id[h['alias_of']]
                self.assertEqual(h['point'], d['point'])
                self.assertEqual(h['gate'], d['gate'])
                self.assertEqual(d['capacity'], 1)
                self.assertFalse(d['staticCollision'])
                self.assertTrue(d['robotCollisionWhenOccupied'])

    def test_actual_roadmap_edges_and_task_connectors_pass_geometry_audit(self):
        for name, path in self.paths.items():
            with self.subTest(candidate=name):
                report = json.loads((path.parent/'geometry-audit.json').read_text())
                self.assertTrue(report['initial_roadmap_usable'])
                self.assertTrue(report['topology']['connected'])
                self.assertEqual(report['terminals']['unreachable_ids'], [])
                self.assertEqual(report['terminals']['unsafe_connectors'], [])
                self.assertEqual(report['unsafe_public_edges'], [])
                self.assertTrue(self.reports[name]['geometric_audit_passed'])
                self.assertFalse(self.reports[name]['throughput_accepted'])
                self.assertFalse(self.reports[name]['default_layout_changed'])

    def test_position_table_matches_all_exact_loader_approach_coordinates(self):
        for path in self.paths.values():
            scene, slots = load_scene(path)
            with (path.parent/'task-locations.csv').open(newline='') as f:
                rows = list(csv.DictReader(f))
            self.assertEqual(len(rows), len(slots)+16)
            self.assertEqual(len({r['location_id'] for r in rows}), len(rows))
            for row in rows:
                if row['kind'] != 'slot':
                    continue
                slot = slots[row['location_id']]
                self.assertEqual((float(row['x']), float(row['y'])), (slot['x'], slot['y']))
                self.assertEqual(int(row['face']), slot['face']+1)
                self.assertEqual(row['rack_id'], slot['rack_id'])

    def test_capacity_generalization_rejects_malformed_declarations(self):
        change_list = [lambda s: s.update(capacity=True), lambda s: s.update(capacity=2304.),
                       lambda s: s.update(capacity=2303), lambda s: s['spec'].update(slotsPerRack=47),
                       lambda s: s['racks'][0].update(capacity=47), lambda s: s['racks'][0].update(id='R02'),
                       lambda s: s.update(racks=[])]
        for index, change in enumerate(change_list):
            value = build_candidate('medium')
            change(value)
            path = self.root/f'invalid-{index}.json'
            path.write_text(json.dumps(value))
            with self.subTest(change=index), self.assertRaises(ValueError):
                load_scene(path)

    def test_body_safety_and_boundaries_are_not_relaxed(self):
        for index, change in enumerate([lambda s: s['spec'].update(robotRadius=.1),
                                        lambda s: s['racks'][0].update(x=-1),
                                        lambda s: s['docks'][0].update(capacity=2)]):
            scene = build_candidate('large');change(scene)
            path = self.root/f'unsafe-{index}.json';path.write_text(json.dumps(scene))
            with self.assertRaises(ValueError):
                load_scene(path)

    def test_old_sample_csv_can_be_migrated_without_deadline_or_id_changes(self):
        original = self.repo/'data/examples/warehouse-v5'
        for name, scene in self.paths.items():
            output = self.root/f'migrate-{name}'
            manifest, report = migrate_dataset(BASE, scene, original, output)
            self.assertTrue(report['valid'])
            self.assertEqual(manifest['migration']['due_time_changes'], 0)
            self.assertFalse(manifest['migration']['uses_measured_completion_times'])
            for item in ('tasks.csv', 'initial_inventory.csv'):
                self.assertEqual((original/item).read_bytes(), (output/item).read_bytes())
            self.assertTrue(validate_dataset(scene, output)['valid'])

    def test_generation_deterministic_and_existing_output_not_overwritten(self):
        self.assertEqual(build_candidate('medium'), build_candidate('medium'))
        for name in self.paths:
            with self.assertRaises(FileExistsError):
                write_candidate(name, self.paths[name].parent)
        self.assertEqual(hashlib.sha256(BASE.read_bytes()).hexdigest(), self.source_sha)
        self.assertEqual(self.source_sha, 'ce99154d80fc643610dcb6d27384ca3724f95dc9121b8d2934212d56ba9eda24')

    def test_explicit_rack_count_supports_area_only_ablation_without_renumbering(self):
        full = build_candidate('medium')
        area_only = build_candidate('medium', rack_count=36)
        self.assertEqual(area_only['racks'], full['racks'][:36])
        self.assertEqual(area_only['capacity'], 1728)
        self.assertNotEqual(area_only['spec']['version'], full['spec']['version'])
        for count in (True, 35, 49, 36.):
            with self.assertRaises(ValueError):
                build_candidate('medium', rack_count=count)

    def make_high_inventory_sim(self, suffix, *, initial_count=1800, full_cycle=False):
        from backend.scheduling_v5.sim import CandidateSimulation, Config
        from backend.day1.data import FIELDS, sha
        scene_path = self.paths['large']
        _, slots = load_scene(scene_path)
        keys = list(slots)
        directory = self.root/suffix
        directory.mkdir()
        with (directory/'initial_inventory.csv').open('w', newline='') as stream:
            writer = csv.DictWriter(stream, fieldnames=['goods_id', 'location_id'])
            writer.writeheader()
            writer.writerows({'goods_id': f'CAP-INITIAL-{i}', 'location_id': key} for i, key in enumerate(keys[:initial_count]))
        task = {'task_id': 'CAP-IN-1', 'task_type': 'INBOUND', 'goods_id': 'CAP-NEW-1',
                'source_id': 'IN-1', 'target_id': keys[0] if full_cycle else keys[initial_count],
                'release_time': '2026-10-28T00:00:00+08:00', 'due_time': '2026-10-28T00:30:00+08:00'}
        tasks = [task]
        if full_cycle:
            task['release_time'] = '2026-10-28T00:00:01+08:00'
            tasks.insert(0, {**task, 'task_id': 'CAP-OUT-1', 'task_type': 'OUTBOUND',
                             'goods_id': 'CAP-INITIAL-0', 'source_id': keys[0], 'target_id': 'OUT-1',
                             'release_time': '2026-10-28T00:00:00+08:00'})
        with (directory/'tasks.csv').open('w', newline='') as stream:
            writer = csv.DictWriter(stream, fieldnames=FIELDS);writer.writeheader();writer.writerows(tasks)
        manifest = {'version': 'capacity-runtime-fixture-v1', 'scenario': 'one-inbound-high-inventory',
                    'start': '2026-10-28T00:00:00+08:00', 'duration_seconds': 3600,
                    'scene_sha256': sha(scene_path), 'capacity': len(slots), 'initial_inventory': initial_count,
                    'tasks': len(tasks), 'inbound': 1, 'outbound': int(full_cycle),
                    'files': {n: sha(directory/n) for n in ['tasks.csv', 'initial_inventory.csv']}}
        (directory/'manifest.json').write_text(json.dumps(manifest))
        self.assertTrue(validate_dataset(scene_path, directory)['valid'])
        return CandidateSimulation(scene_path, directory, 'capacity-'+suffix,
                                   Config(charging_policy='critical-only'))

    def test_v5_can_admit_and_advance_above_old_1728_inventory_limit(self):
        sim = self.make_high_inventory_sim('high-inventory')
        self.assertEqual(sim.inside, 1800)
        self.assertEqual(len(sim.slots), 2880)
        sim.step()
        self.assertEqual(sim.inside, 1801)
        self.assertEqual(sim.metrics['received'], 1)
        self.assertEqual(sim.tasks['CAP-IN-1']['received_at'], 0)
        sim.assert_inventory()

    def test_v5_rejects_inbound_at_real_capacity_and_asserts_physical_overflow(self):
        sim = self.make_high_inventory_sim('boundary')
        # Isolate admission's capacity guard even when its target is free.
        # No claim that this synthetic counter change is a consistent inventory.
        sim.inside = len(sim.slots)
        sim._release()
        self.assertEqual(sim.tasks['CAP-IN-1']['status'], 'waiting')
        self.assertEqual(sim.metrics['received'], 0)
        self.assertEqual(sim.inside, 2880)
        # The inherited physical invariant must still reject overflow.
        sim.inside = len(sim.slots)+1
        with self.assertRaisesRegex(AssertionError, 'capacity invariant'):
            sim.step()

    def test_full_2880_inventory_releases_capacity_on_shipping_not_on_pickup(self):
        sim = self.make_high_inventory_sim('full-cycle', initial_count=2880, full_cycle=True)
        for _ in range(600):
            sim.step()
            if sim.tasks['CAP-OUT-1']['status'] == 'carrying':
                break
        else:
            self.fail('outbound task not picked in bounded real V5 fixture')
        self.assertEqual(sim.inside, 2880)
        self.assertFalse(sim.slots[sim.tasks['CAP-OUT-1']['source_id']]['occupied'])
        self.assertEqual(sim.tasks['CAP-IN-1']['status'], 'waiting')
        self.assertEqual(sim.metrics['received'], 0)
        sim.assert_inventory()
        for _ in range(600):
            sim.step()
            if sim.metrics['received'] == 1:
                break
        else:
            self.fail('new inbound not admitted after physical shipment')
        self.assertEqual(sim.metrics['shipped'], 1)
        self.assertEqual(sim.inside, 2880)
        self.assertEqual(sim.goods['CAP-INITIAL-0'], 'SHIPPED')
        sim.assert_inventory()


if __name__ == '__main__':
    unittest.main()
