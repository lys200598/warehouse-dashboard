import hashlib
import json
from pathlib import Path
import unittest

from backend.warehouse.composition import physical_classes
from backend.wide.data import validate_dataset


ROOT = Path(__file__).resolve().parents[1]
SCENE = ROOT / 'data/layouts/expansion-candidates/medium/layout.json'
DATASET = ROOT / 'data/examples/wheeled-medium-sample19'
CONFIG = ROOT / 'config/wheeled-medium-100.json'


class MediumWheeledTest(unittest.TestCase):
    def test_medium_layout_and_dataset_have_one_2304_slot_identity(self):
        scene = json.loads(SCENE.read_text(encoding='utf-8'))
        manifest = json.loads((SCENE.parent / 'manifest.json').read_text(encoding='utf-8'))
        dataset = json.loads((DATASET / 'manifest.json').read_text(encoding='utf-8'))
        digest = hashlib.sha256(SCENE.read_bytes()).hexdigest()
        self.assertEqual(scene['spec']['width'], 120.0)
        self.assertEqual(scene['spec']['depth'], 90.0)
        self.assertEqual(len(scene['racks']), 48)
        self.assertEqual(sum(rack['capacity'] for rack in scene['racks']), 2304)
        self.assertEqual(scene['spec']['levels'] * scene['spec']['faces'] * scene['spec']['baysPerFace'], 48)
        self.assertEqual(manifest['scene_sha256'], digest)
        self.assertEqual(dataset['scene_sha256'], digest)
        self.assertEqual(dataset['capacity'], 2304)

        report = validate_dataset(str(SCENE), str(DATASET))
        self.assertTrue(report['valid'])
        self.assertEqual(report['tasks'], 19)
        self.assertEqual(report['capacity'], 2304)

    def test_wheeled_composition_uses_medium_config_and_finite_batteries(self):
        Simulation, Config = physical_classes('wheeled')
        config = Config(**json.loads(CONFIG.read_text(encoding='utf-8')))
        self.assertEqual(config.speed, 1.5)
        self.assertEqual(config.replenishment_policy, 'swap-first')
        self.assertEqual(config.swap_stations, 12)
        self.assertEqual(config.max_active_robots, 100)

        sim = Simulation(str(SCENE), str(DATASET), 'unit-medium-wheeled', config,
                         knowledge_mode='rules')
        self.assertEqual(len(sim.scene['racks']), 48)
        self.assertEqual(len(sim.slots), 2304)
        self.assertEqual(len(sim.robots), 100)
        service = sim.frame()['batteryService']
        self.assertEqual(service['packCounts'], {
            'ready': 48, 'reserved': 0, 'waiting': 0, 'charging': 0})
        sim.step()
        self.assertEqual(sim.elapsed, 1)

    def test_unknown_mobility_is_rejected(self):
        with self.assertRaises(ValueError):
            physical_classes('flying')


if __name__ == '__main__':
    unittest.main()
