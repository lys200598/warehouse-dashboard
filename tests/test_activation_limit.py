from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from backend.wide.sim import Config, Simulation
from backend.scheduling_v3.sim import Simulation as V3Simulation
from backend.wheeled.sim import WheeledConfig, WheeledSimulation

ROOT = Path(__file__).resolve().parents[1]


class ActivationLimitTests(unittest.TestCase):
    def bare(self, limit=2):
        sim = WheeledSimulation.__new__(WheeledSimulation)
        sim.config = SimpleNamespace(max_active_robots=limit)
        sim.jobs = {1: {'task_id': 'old'}, 2: {'task_id': None, 'dock_id': 'C-A02'}}
        sim.elapsed = 10
        return sim

    def test_config_defaults_and_validation(self):
        self.assertEqual(Config().max_active_robots, 100)
        self.assertEqual(WheeledConfig(max_active_robots=50).max_active_robots, 50)
        for value in (0, -1, 101, True, 50.0, '50', None):
            with self.subTest(value=value), self.assertRaises(ValueError):
                WheeledConfig(max_active_robots=value)

    def test_return_and_charge_jobs_hold_slots_existing_owner_can_continue(self):
        sim = self.bare()
        self.assertTrue(sim.activation_available({'robot_id': 1}))
        self.assertTrue(sim.activation_available({'robot_id': 2}))
        self.assertFalse(sim.activation_available({'robot_id': 3}))
        del sim.jobs[2]
        self.assertTrue(sim.activation_available({'robot_id': 3}))

    def test_task_and_swap_are_rejected_before_any_planning_or_pack_mutation(self):
        sim = self.bare()
        self.assertFalse(sim.schedule_task({'robot_id': 3}, {}))
        self.assertFalse(sim.schedule_charge({'robot_id': 3}))
        self.assertEqual(sim._last_block['reason'], 'activation_limit')
        self.assertEqual(len(sim.jobs), 2)

    def test_direct_commit_and_multi_owner_recovery_cannot_add_a_51st_owner(self):
        sim = self.bare()
        with self.assertRaisesRegex(AssertionError, 'activation limit'):
            sim._commit({'robot_id': 3}, [], {})
        self.assertFalse(sim._coordinated_replan({3: ([], {})}))
        self.assertFalse(sim._coordinated_replan({}, preferred=[3]))

    def test_helper_needs_a_free_slot(self):
        sim = self.bare()
        sim.task_capability_reason = lambda *args, **kw: None
        with patch.object(V3Simulation, '_assistance_helpers', return_value=[{'robot_id': 3}]):
            self.assertEqual(sim._assistance_helpers({'robot_id': 1}, {}), [])
            del sim.jobs[2]
            self.assertEqual(sim._assistance_helpers({'robot_id': 1}, {}), [{'robot_id': 3}])

    def test_actual_v5_dispatch_holds_cap_without_route_retry_penalties(self):
        sim = WheeledSimulation(ROOT/'data/layouts/warehouse100-wide-v2.json',
            ROOT/'data/examples/warehouse-v5', 'activation-unit',
            WheeledConfig(max_active_robots=2, charging_policy='none'), policy='port-guard')
        # Bring real existing input to one release boundary, preserving task
        # IDs, stock, routes, energy checks and all 100 obstacle bodies.
        for task in sim.tasks.values():
            task['release_second'] = 0
        peak = 0
        for _ in range(30):
            sim.step()
            peak = max(peak, len(sim.jobs))
            self.assertLessEqual(len(sim.jobs), 2)
        self.assertEqual(peak, 2)
        self.assertEqual(sim.metrics['matching_retries'], 0)
        self.assertNotIn('activation_limit', [x.get('reason') for x in sim.blocked.values()])
        self.assertEqual(sim.frame()['activation']['limit'], 2)
        self.assertEqual(len(sim.frame()['robots']), 100)
        self.assertEqual(len(sim.robots), 100)


if __name__ == '__main__':
    unittest.main()
