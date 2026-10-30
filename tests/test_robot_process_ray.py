"""Ray actor robot processes reproduce the sequential engine exactly.

Needs a Ray cluster: set WAREHOUSE_RAY_TEST_ADDRESS (for example 127.0.0.1:27380,
the test head reserved for the frontend/dispatch side). Skipped otherwise; the
same logic is covered without Ray by tests/test_robot_process_split.py.
"""
import json
import os
import time
import unittest

from tests import test_dispatch_golden as golden

ADDRESS = os.environ.get('WAREHOUSE_RAY_TEST_ADDRESS')


@unittest.skipUnless(ADDRESS, 'set WAREHOUSE_RAY_TEST_ADDRESS to run Ray actor tests')
class RayRobotProcessTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from backend.robot_actor.ray_actor import RayRobotProcessSplit
        from backend.wheeled.sim import WheeledSimulation

        class Engine(RayRobotProcessSplit, WheeledSimulation):
            ray_address = ADDRESS
            ray_actor_cpus = 0.02  # as in the manual: 100 actors x 0.02 CPU
        cls.Engine = Engine

    def assert_same(self, actual, expected):
        for index, (got, want) in enumerate(zip(actual['assignments'], expected['assignments'])):
            self.assertEqual(got, want, f'assignment #{index} is the first difference')
        self.assertEqual(len(actual['assignments']), len(expected['assignments']))
        self.assertEqual(actual['completed_at'], expected['completed_at'])
        self.assertEqual(actual['ticks'], expected['ticks'])
        self.assertEqual(actual.get('handoffs'), expected.get('handoffs'))

    def test_ordinary_baseline_on_ray_actors(self):
        expected = json.loads(golden.FIXTURE.read_text(encoding='utf-8'))
        self.assert_same(golden.record(sim_class=self.Engine), expected)

    def test_relay_baseline_on_ray_actors(self):
        expected = json.loads(golden.RELAY_FIXTURE.read_text(encoding='utf-8'))
        actual, sim = golden.record_relay(sim_class=self.Engine, with_sim=True)
        sim.close_robot_pool()
        self.assert_same(actual, expected)

    def test_redo_on_ray_actors(self):
        sequential = json.loads(golden.REDO_FIXTURE.read_text(encoding='utf-8'))
        began = time.perf_counter()
        actual, sim = golden.record_relay(sim_class=self.Engine, report_at=300, with_sim=True)
        seconds = time.perf_counter() - began
        sim.close_robot_pool()
        self.assert_same(actual, sequential)
        self.assertGreaterEqual(sim.split_stats['redo'], 1)
        print(f'\n[ray] {actual["ticks"]} ticks in {seconds:.1f}s ({1000 * seconds / actual["ticks"]:.1f} ms/tick), '
              f'redo={sim.split_stats["redo"]}')


if __name__ == '__main__':
    unittest.main()
