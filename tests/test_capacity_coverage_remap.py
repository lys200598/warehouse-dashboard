import csv
from copy import deepcopy
import importlib.util
import json
from pathlib import Path
import shutil
import tempfile
import unittest

from backend.day1.data import sha
from backend.wide.data import load_scene, validate_dataset
from backend.wide.layout_candidates import BASE, write_candidate
from backend.wide.scenarios import generate_scenario

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location('coverage_remap_test_tool', ROOT/'ops/remap_capacity_coverage.py')
tool = importlib.util.module_from_spec(SPEC); SPEC.loader.exec_module(tool)


class CapacityCoverageRemapTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp = tempfile.TemporaryDirectory(prefix='capacity-coverage-', dir=ROOT/'tests')
        cls.root = Path(cls.temp.name)
        cls.dataset = cls.root/'source'
        generate_scenario(BASE, cls.dataset, active_slots=128, duration_seconds=3600,
                          slot_interval_seconds=3600, seed=20261028)
        cls.scenes = {}
        for name in ('medium', 'large'):
            output = cls.root/name
            write_candidate(name, output)
            cls.scenes[name] = output/'layout.json'
        cls.outputs = {}
        cls.reports = {}
        for name, scene in cls.scenes.items():
            output = cls.root/f'remap-{name}'
            cls.reports[name] = tool.remap(BASE, cls.dataset, scene, output, seed=91)
            cls.outputs[name] = output

    @classmethod
    def tearDownClass(cls):
        cls.temp.cleanup()

    def test_full_workload_covers_new_racks_with_actual_tasks_not_only_initial_stock(self):
        for name, count in [('medium', 48), ('large', 60)]:
            report = self.reports[name]
            self.assertTrue(report['valid'])
            self.assertEqual(report['coverage']['task_source_slots'], 128)
            self.assertEqual(report['coverage']['task_rack_count'], count)
            self.assertTrue(report['coverage']['task_rack_coverage_complete'])
            self.assertEqual(report['coverage']['uncovered_task_racks'], [])
            self.assertFalse(report['csv_byte_identical_control'])
            self.assertFalse(report['runtime_throughput_claim'])

    def test_mapping_is_injective_and_preserves_levels_faces_for_all_input_references(self):
        _, old = load_scene(BASE)
        tasks, initial = tool.csv_rows(self.dataset/'tasks.csv'), tool.csv_rows(self.dataset/'initial_inventory.csv')
        used = {r['location_id'] for r in initial} | {r[k] for r in tasks for k in ('source_id', 'target_id') if r[k] in old}
        for name, output in self.outputs.items():
            _, new = load_scene(self.scenes[name])
            mapping = json.loads((output/'slot-map.json').read_text())['mapping']
            self.assertEqual(set(mapping), used)
            self.assertEqual(len(mapping), len(set(mapping.values())))
            for a, b in mapping.items():
                self.assertEqual((old[a]['level'], old[a]['face']), (new[b]['level'], new[b]['face']))

    def test_all_task_identity_type_time_and_counts_retained_only_storage_endpoints_change(self):
        before = tool.csv_rows(self.dataset/'tasks.csv')
        old_initial = tool.csv_rows(self.dataset/'initial_inventory.csv')
        for output in self.outputs.values():
            after = tool.csv_rows(output/'tasks.csv')
            self.assertEqual(len(after), len(before))
            for a, b in zip(before, after):
                for key in a.keys()-{'source_id', 'target_id'}:
                    self.assertEqual(a[key], b[key])
                for key in ('source_id', 'target_id'):
                    if a[key].startswith(('IN-', 'OUT-')):
                        self.assertEqual(a[key], b[key])
            new_initial = tool.csv_rows(output/'initial_inventory.csv')
            self.assertEqual(len(new_initial), len(old_initial))
            self.assertEqual([r['goods_id'] for r in new_initial], [r['goods_id'] for r in old_initial])
            self.assertEqual(self.reports[output.name.removeprefix('remap-')]['deadline_changes'], 0)

    def test_manifest_matches_target_and_complete_mapping_provenance(self):
        for name, output in self.outputs.items():
            manifest = json.loads((output/'manifest.json').read_text())
            self.assertEqual(manifest['capacity'], 2304 if name == 'medium' else 2880)
            self.assertEqual(manifest['scene_sha256'], sha(self.scenes[name]))
            proof = manifest['coverage_remap']
            self.assertEqual(proof['mapping_sha256'], sha(output/'slot-map.json'))
            self.assertEqual(proof['source_sha256']['tasks.csv'], sha(self.dataset/'tasks.csv'))
            self.assertEqual(proof['changed_fields'], ['source_id', 'target_id', 'location_id'])
            self.assertEqual(proof['deadline_changes'], 0)
            self.assertFalse(proof['uses_measured_completion_times'])
            self.assertTrue(validate_dataset(self.scenes[name], output)['valid'])

    def test_identical_seed_is_deterministic_different_seed_changes_mapping(self):
        same, different = self.root/'repeat', self.root/'different'
        tool.remap(BASE, self.dataset, self.scenes['large'], same, seed=91)
        tool.remap(BASE, self.dataset, self.scenes['large'], different, seed=92)
        for name in ('tasks.csv', 'initial_inventory.csv', 'slot-map.json', 'manifest.json'):
            self.assertEqual((same/name).read_bytes(), (self.outputs['large']/name).read_bytes())
        self.assertNotEqual((same/'slot-map.json').read_bytes(), (different/'slot-map.json').read_bytes())

    def test_source_and_old_scene_immutable_and_existing_output_refused(self):
        before = {n: sha(self.dataset/n) for n in ('tasks.csv', 'initial_inventory.csv', 'manifest.json')}
        original_scene = sha(BASE)
        with self.assertRaises(FileExistsError):
            tool.remap(BASE, self.dataset, self.scenes['medium'], self.outputs['medium'])
        with self.assertRaises(ValueError):
            tool.remap(BASE, self.dataset, self.scenes['medium'], self.dataset/'nested-output')
        self.assertEqual(before, {n: sha(self.dataset/n) for n in before})
        self.assertEqual(original_scene, sha(BASE))

    def test_small_task_set_does_not_claim_inventory_coverage_as_task_coverage(self):
        result = tool.remap(BASE, ROOT/'data/examples/warehouse-v5', self.scenes['large'], self.root/'small')
        self.assertTrue(result['coverage']['all_referenced_rack_coverage_complete'])
        self.assertFalse(result['coverage']['task_rack_coverage_complete'])
        self.assertLess(result['coverage']['task_rack_count'], 60)
        self.assertEqual(result['coverage']['task_rack_count'], result['coverage']['maximum_task_racks_possible'])

    def test_insufficient_signature_capacity_and_invalid_seed_rejected(self):
        old = {'a': {'level': 3, 'face': 1, 'bay': 1, 'rack_id': 'R01'},
               'b': {'level': 3, 'face': 1, 'bay': 2, 'rack_id': 'R02'}}
        new = {'c': {'level': 3, 'face': 1, 'bay': 1, 'rack_id': 'R01'}}
        with self.assertRaises(ValueError):
            tool.mapping_for_coverage(old, new, set(old), set(old), 7)
        with self.assertRaises(ValueError):
            tool.mapping_for_coverage(old, new, set(old), set(old), True)

    def test_invalid_source_hash_fails_before_creating_target_output(self):
        bad = self.root/'bad-source';shutil.copytree(self.dataset, bad)
        with (bad/'tasks.csv').open('a') as stream:stream.write('\n')
        output = self.root/'invalid-output'
        with self.assertRaises(ValueError):
            tool.remap(BASE, bad, self.scenes['medium'], output)
        self.assertFalse(output.exists())


if __name__ == '__main__':
    unittest.main()
