"""Independent persistent-body review: no disappearance or implicit repair."""
from collections import defaultdict
from copy import deepcopy
from pathlib import Path
import tempfile
import unittest

from backend.checkpoint import read_checkpoint, write_checkpoint
from backend.day1.nav import hold
from backend.scheduling_v3.obstacles import MotionTable
from backend.scheduling_v3.scenarios import generate_scenario
from backend.scheduling_v3.sim import Simulation
from backend.scheduling_v3.transactions import try_replace_many


def permanent(point, start=0):
    return {**hold(point, 'fault_wait', start, 10**12 - start), 'permanent_obstacle': True}


class BudgetGrid(defaultdict):
    """Fail quickly if a sentinel query accidentally iterates huge time ranges."""
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.reads = 0

    def get(self, key, default=None):
        self.reads += 1
        if self.reads > 1000:
            raise AssertionError('persistent candidate enumerated time buckets')
        return super().get(key, default)


class PersistentTableReviewTests(unittest.TestCase):
    def test_long_candidate_uses_spatial_index_and_failure_keeps_fixed_and_old_plans(self):
        table = MotionTable()
        point = {'x': 2., 'y': 0.}
        table.reserve([hold(point, 'placing', 100, 10)], owner=2)
        table.reserve([permanent({'x': 20., 'y': 20.})], owner=99)
        table.timegrid = BudgetGrid(dict, table.timegrid)
        before = deepcopy(table.__dict__)
        result = try_replace_many(table, {1: [permanent(point)]}, now=0)
        self.assertFalse(result)
        self.assertEqual(result.conflicts, (2,))
        self.assertEqual(table.__dict__, before)

    def test_unrelated_success_and_prune_keep_permanent_body(self):
        table = MotionTable()
        point = {'x': 20., 'y': 20.}
        table.reserve([hold(point, 'handoff_give', 0, 10), permanent(point, 10)], owner=7)
        self.assertTrue(try_replace_many(table, {8: [hold({'x': 30., 'y': 30.}, 'placing', 0, 20)]}, now=0))
        for at in [15, 1000, 604800]:
            table.prune(at)
            self.assertIn(7, table.fixed)
            self.assertFalse(table.clear_motion(point, point, at, at + 1, 8))
        clone = deepcopy(table)
        clone._erase(7)
        self.assertNotIn(7, clone.fixed)
        self.assertIn(7, table.fixed)

    def test_generator_reservation_preserves_ordinary_prefix_and_fixed_suffix(self):
        table = MotionTable()
        point = {'x': 20., 'y': 20.}
        table.reserve((part for part in [hold(point, 'handoff_give', 0, 10), permanent(point, 10)]), owner=7)
        self.assertFalse(table.clear_motion(point, point, 5, 6, 8))
        self.assertFalse(table.clear_motion(point, point, 100000, 100001, 8))
        self.assertTrue(table.owners[7])


class PersistentRuntimeReviewTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.base = Path(__file__).resolve().parents[1]
        parent = cls.base.parent / 'research/scheduler-v3-adversarial'
        parent.mkdir(parents=True, exist_ok=True)
        cls.temp = tempfile.TemporaryDirectory(dir=parent)
        cls.scene = cls.base / 'data/layouts/warehouse100-v1.json'
        cls.dataset = Path(cls.temp.name) / 'input'
        generate_scenario(cls.scene, cls.dataset, active_slots=64,
                          slot_interval_seconds=3600, duration_seconds=1800)

    @classmethod
    def tearDownClass(cls):
        cls.temp.cleanup()

    def carrier(self, suffix):
        sim = Simulation(self.scene, self.dataset, 'persistent-review-' + suffix)
        for _ in range(900):
            sim.step()
            donor = next((i for i, robot in sim.robots.items()
                          if robot['carrying'] and robot['stage'] == 'to_target'), None)
            if donor is not None:
                return sim, donor, sim.robots[donor]['task_id']
        self.fail('no to-target carrier in first 900 simulated seconds')

    def test_durable_persistent_body_and_power_isolation_survive_until_explicit_clear(self):
        sim, donor, task_id = self.carrier('restore-clear')
        point = sim._point(sim.robots[donor])
        self.assertTrue(sim.inject_stop(donor, None))
        self.assertTrue(sim.request_assistance(donor, reason='persistent_drive_fault'))
        for _ in range(700):
            sim.step()
            self.assertEqual(sim._point(sim.robots[donor]), point)
        self.assertEqual(sim.tasks[task_id]['status'], 'completed')
        self.assertFalse(sim.robots[donor]['carrying'])
        battery = sim.robots[donor]['battery']
        path = Path(self.temp.name) / 'persistent-checkpoint.json'
        write_checkpoint(path, sim.checkpoint())
        restored = Simulation(self.scene, self.dataset, sim.run_id, sim.config,
                              checkpoint=read_checkpoint(path))
        for _ in range(100):
            sim.step()
            restored.step()
            self.assertEqual(restored._point(restored.robots[donor]), point)
            self.assertEqual(restored.robots[donor]['battery'], battery)
        self.assertEqual(sim.robots, restored.robots)
        self.assertEqual(sim.jobs, restored.jobs)
        self.assertEqual(sim.goods, restored.goods)
        self.assertIn(donor, restored.reservations.fixed)
        self.assertFalse(restored.reservations.clear_motion(point, point, restored.elapsed,
                                                           restored.elapsed + 60, -1))
        self.assertTrue(restored.clear_fault(donor))
        self.assertNotIn(donor, restored.faults)
        self.assertNotIn(donor, restored.reservations.fixed)
        distance = restored.robots[donor]['distance']
        for _ in range(500):
            restored.step()
            if restored.robots[donor]['distance'] > distance:
                break
        self.assertGreater(restored.robots[donor]['distance'], distance)
        restored.assert_inventory()

    def test_persistent_handoff_receiver_delay_does_not_add_to_none_until(self):
        sim, donor, task_id = self.carrier('paired-delay')
        point = sim._point(sim.robots[donor])
        self.assertTrue(sim.inject_stop(donor, None))
        self.assertTrue(sim.request_assistance(donor))
        receiver = sim.assists[donor]['receiver']
        self.assertTrue(sim.inject_delay(receiver, 60))
        self.assertIsNone(sim.faults[donor]['until'])
        for _ in range(700):
            sim.step()
            self.assertEqual(sim._point(sim.robots[donor]), point)
        self.assertEqual(sim.tasks[task_id]['status'], 'completed')
        self.assertIn(donor, sim.reservations.fixed)
        self.assertEqual(sim.metrics['handoffs'], 1)
        sim.assert_inventory()

    def test_fault_clear_cannot_resume_carried_task_without_energy_reserve(self):
        sim, donor, _ = self.carrier('low-energy-clear')
        self.assertTrue(sim.inject_stop(donor, None))
        sim.robots[donor]['battery'] = sim.config.emergency_reserve_soc + .001
        jobs, goods, point = deepcopy(sim.jobs), deepcopy(sim.goods), sim._point(sim.robots[donor])
        self.assertFalse(sim.clear_fault(donor))
        self.assertIn(donor, sim.faults)
        self.assertIn(donor, sim.reservations.fixed)
        self.assertEqual(sim.jobs, jobs)
        self.assertEqual(sim.goods, goods)
        self.assertEqual(sim._point(sim.robots[donor]), point)


if __name__ == '__main__':
    unittest.main()
