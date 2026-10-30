"""Checkpoint restoration reconstructs robot processes and preserves continuation."""
from copy import deepcopy
import json
import os
import unittest

from backend.robot_actor.local import RobotProcessSplit
from backend.wheeled.sim import WheeledSimulation
from tests import test_dispatch_golden as golden


class Split(RobotProcessSplit, WheeledSimulation):
    pass


def physical_checkpoint(sim):
    state = deepcopy(sim.checkpoint())
    for robot in state['robots']:
        robot.pop('worker_pid', None)
    for key in ('planning_seconds', 'dispatch_wall_seconds', 'recovery_wall_seconds'):
        state['metrics'].pop(key, None)
    return state


class RobotRecoveryTests(unittest.TestCase):
    def test_same_instance_rewind_discards_future_boundary_bindings(self):
        _, sim = golden.record_relay(sim_class=Split, max_ticks=270, with_sim=True)
        saved = deepcopy(sim.checkpoint())
        try:
            for _ in range(40):
                sim.step()
            expected = physical_checkpoint(sim)
            sim.restore(saved)
            self.assertTrue(all(task['status'] == 'scheduled' for key, task in sim.tasks.items()
                                if key not in saved['tasks']))
            for _ in range(40):
                sim.step()
            self.assertEqual(physical_checkpoint(sim), expected)
        finally:
            sim.close()

    def test_resume_through_handoff_rebuilds_pool_and_matches_uninterrupted(self):
        # The saved boundary is after measured-low-SOC and before handoff at 282.
        _, uninterrupted = golden.record_relay(sim_class=Split, max_ticks=270, with_sim=True)
        state = deepcopy(uninterrupted.checkpoint())
        config = type(uninterrupted.config)(**state['config'])
        resumed = Split(golden.SCENE, golden.DATASET, uninterrupted.run_id, config)
        resumed._robot_pool()  # prove restore closes an existing pool too
        old_pool = resumed._pool
        reset_calls = []
        resumed._reset_after_restore = lambda: reset_calls.append(resumed.tick)
        resumed.restore(state)
        self.assertIsNone(resumed._pool)
        self.assertEqual(old_pool.processes, {})
        self.assertEqual(reset_calls, [270])
        for _ in range(130):
            uninterrupted.step()
            resumed.step()
        self.assertEqual(physical_checkpoint(resumed), physical_checkpoint(uninterrupted))
        self.assertEqual(resumed.metrics['handoffs'], 1)
        resumed.close()
        uninterrupted.close()

    @unittest.skipUnless(os.environ.get('WAREHOUSE_RAY_RECOVERY_TEST') == '1' and
                         os.environ.get('WAREHOUSE_RAY_TEST_ADDRESS'),
                         'set WAREHOUSE_RAY_RECOVERY_TEST=1 and WAREHOUSE_RAY_TEST_ADDRESS')
    def test_ray_actor_rebuild_matches_the_same_continuation(self):
        from backend.robot_actor.ray_actor import RayRobotProcessSplit
        class RaySplit(RayRobotProcessSplit, WheeledSimulation):
            ray_address = os.environ['WAREHOUSE_RAY_TEST_ADDRESS']
            ray_actor_cpus = .02
        _, sim = golden.record_relay(sim_class=RaySplit, max_ticks=270, with_sim=True)
        saved = deepcopy(sim.checkpoint())
        try:
            for _ in range(40):
                sim.step()
            expected = physical_checkpoint(sim)
            pool = sim._pool
            sim.restore(saved)
            self.assertIsNone(sim._pool)
            self.assertEqual(pool.actors, {})
            for _ in range(40):
                sim.step()
            self.assertEqual(physical_checkpoint(sim), expected)
            self.assertEqual(sim.metrics['handoffs'], 1)
        finally:
            sim.close()


if __name__ == '__main__':
    unittest.main()
