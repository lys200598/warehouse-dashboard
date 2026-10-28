from copy import deepcopy
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

from backend.wide import layout_candidates as factory
from backend.wide.data import load_scene

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location('balanced_layout_test_tool', ROOT/'ops/generate_balanced_layout.py')
tool = importlib.util.module_from_spec(SPEC); SPEC.loader.exec_module(tool)


class BalancedLayoutTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp = tempfile.TemporaryDirectory(prefix='balanced-layout-', dir=ROOT/'tests')
        cls.output = Path(cls.temp.name)/'balanced'
        cls.registry_before = deepcopy(factory.CANDIDATES)
        cls.result = tool.generate(cls.output)

    @classmethod
    def tearDownClass(cls):
        cls.temp.cleanup()

    def test_final_dimensions_are_100_by_90_with_40_racks_1920_slots(self):
        scene, slots = load_scene(self.output/'layout.json')
        self.assertEqual((scene['spec']['width'], scene['spec']['depth']), (100., 90.))
        self.assertEqual((len(scene['racks']), len(slots), scene['capacity']), (40, 1920, 1920))
        self.assertEqual(scene['spec']['ordinaryAisleWidth'], 5.)
        self.assertEqual(scene['spec']['mainAisleWidth'], 8.)

    def test_bad_84_and_exact_88_boundary_diagnostics_are_retained(self):
        evidence = self.result['boundary_preflight']
        attempts = evidence['attempts']
        self.assertEqual([r['requested_depth_m'] for r in attempts], [84., 88., 90.])
        self.assertFalse(attempts[0]['accepted_by_existing_factory'])
        self.assertFalse(attempts[1]['accepted_by_existing_factory'])
        self.assertTrue(attempts[2]['accepted_by_existing_factory'])
        self.assertLess(attempts[0]['actual_factory_top_margin_m'], 14.)
        self.assertLess(attempts[1]['actual_factory_top_margin_m'], 14.)
        self.assertGreater(attempts[2]['actual_factory_top_margin_m'], 14.)
        self.assertFalse(evidence['core_thresholds_changed'])
        self.assertFalse(evidence['floating_representation_adjustment_used'])

    def test_all_original_task_ids_and_canonical_bays_remain(self):
        old, old_slots = load_scene(factory.BASE)
        scene, slots = load_scene(self.output/'layout.json')
        self.assertTrue(set(old_slots) <= set(slots))
        self.assertEqual({p['id'] for p in old['stations']}, {p['id'] for p in scene['stations']})
        self.assertEqual(scene['physical_bay_count'], 100)
        self.assertEqual(len(set(scene['location_aliases'].values())), 100)
        self.assertEqual(scene['spec']['robotRadius'], .55)

    def test_real_geometry_audit_passes_without_throughput_claim(self):
        report = json.loads((self.output/'geometry-audit.json').read_text())
        self.assertTrue(report['initial_roadmap_usable'])
        self.assertEqual(report['unsafe_public_edges'], [])
        self.assertEqual(report['terminals']['unsafe_connectors'], [])
        self.assertEqual(report['terminals']['unreachable_ids'], [])
        self.assertFalse(self.result['manifest']['throughput_accepted'])
        self.assertFalse(self.result['manifest']['default_layout_changed'])

    def test_registry_and_frozen_source_identity_unchanged(self):
        self.assertEqual(factory.CANDIDATES, self.registry_before)
        evidence = self.result['boundary_preflight']
        self.assertTrue(evidence['source_files_unchanged'])
        self.assertEqual(tool.sha(factory.__file__), evidence['source_identity']['core_factory_sha256'])
        self.assertEqual(tool.sha(factory.BASE), evidence['source_identity']['base_scene_sha256'])
        self.assertEqual(tool.sha(factory.BASE), 'ce99154d80fc643610dcb6d27384ca3724f95dc9121b8d2934212d56ba9eda24')

    def test_repeated_output_refused_without_overwriting_evidence(self):
        original = (self.output/'layout.json').read_bytes()
        with self.assertRaises(FileExistsError):
            tool.generate(self.output)
        self.assertEqual((self.output/'layout.json').read_bytes(), original)
        self.assertEqual(factory.CANDIDATES, self.registry_before)


if __name__ == '__main__':
    unittest.main()
