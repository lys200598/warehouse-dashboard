import json
from pathlib import Path
import tempfile
import unittest

from backend.scheduling_v3.sim import Config, Simulation
from backend.scheduling_v3.scenarios import generate_scenario


class RuntimeTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp = tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parents[1] / 'data/local-check')
        cls.scene = Path(__file__).resolve().parents[1] / 'data/layouts/warehouse100-v1.json'
        cls.dataset = Path(cls.temp.name) / 'dataset'
        generate_scenario(cls.scene, cls.dataset, active_slots=12, slot_interval_seconds=3600, duration_seconds=1800)

    @classmethod
    def tearDownClass(cls):
        cls.temp.cleanup()

    def make(self):
        return Simulation(self.scene, self.dataset, 'runtime-tests')

    def test_real_low_energy_handoff_delivers_once_and_keeps_donor(self):
        sim = self.make()
        donor = None
        for _ in range(1000):
            sim.step()
            donor = next((i for i, r in sim.robots.items() if r['carrying']), None)
            if donor:
                break
        self.assertIsNotNone(donor)
        task_id = sim.robots[donor]['task_id']
        goods_id = sim.robots[donor]['cargo_id']
        sim.robots[donor]['battery'] = 24.
        self.assertTrue(sim.request_assistance(donor))
        self.assertEqual(sim.goods[goods_id], f'ROBOT-{donor}')
        positions = dict(sim.robots[donor])
        for _ in range(700):
            sim.step()
        self.assertEqual(sim.metrics['handoffs'], 1)
        self.assertEqual(sim.tasks[task_id]['status'], 'completed')
        self.assertEqual(sim.tasks[task_id]['handoffs'], 1)
        self.assertIn(donor, sim.robots)
        events = [e for e in sim.pending_events if e.get('task_id') == task_id]
        self.assertEqual(sum(e['kind'] == 'cargo_handoff' for e in events), 1)
        self.assertEqual(sum(e['kind'] in {'placed', 'shipped'} for e in events), 1)
        self.assertFalse(sim.robots[donor]['carrying'])
        self.assertTrue(sim.robots[donor]['stage'] in {'to_charge', 'charging', 'returning', 'idle', 'traffic_wait'})
        self.assertGreaterEqual(sim.metrics['min_separation_m'], 1.1 - 1e-6)
        self.assertGreaterEqual(sim.metrics['min_soc'], 10.)
        sim.assert_inventory()


if __name__ == '__main__':
    unittest.main()
