from copy import deepcopy
import os
from pathlib import Path
import unittest

from backend.scheduling_v5.sim import CandidateSimulation, Config


ROOT = Path(os.environ.get('WAREHOUSE_FIXTURE_ROOT', Path(__file__).resolve().parents[1]))


class PortReliefRestoreTests(unittest.TestCase):
    def make(self, **kwargs):
        return CandidateSimulation(ROOT/'data/layouts/warehouse100-wide-v2.json',
            ROOT/'data/examples/warehouse-v5','port-relief-restore',
            Config(charging_policy='critical-only',**kwargs),
            policy='port-guard',planner='original-sipp')

    def test_policy_switch_refuses_old_checkpoint(self):
        state=self.make().checkpoint()
        for settings in ({'inbound_admission_policy':'deadline'}, {'port_relief_policy':'deadline'}):
            with self.subTest(settings=settings),self.assertRaisesRegex(ValueError,'identity'):
                self.make(**settings).restore(state)

    def test_same_policy_restore_preserves_physics_and_task_deadlines(self):
        settings={'inbound_admission_policy':'deadline','port_relief_policy':'deadline'}
        sim=self.make(**settings)
        for _ in range(5):sim.step()
        state=sim.checkpoint();original=deepcopy(sim.tasks)
        restored=self.make(**settings);restored.restore(state)
        self.assertEqual(restored.tasks,original)
        self.assertEqual(restored.robots,sim.robots)
        for _ in range(5):sim.step();restored.step()
        self.assertEqual(restored.tasks,sim.tasks)
        self.assertEqual(restored.robots,sim.robots)
        self.assertEqual(restored.goods,sim.goods)


if __name__=='__main__':unittest.main()
