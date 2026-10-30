"""The split engine (robot processes, local version) reproduces the sequential engine exactly."""
import json
import unittest

from backend.robot_actor.local import RobotProcessSplit
from backend.wheeled.sim import WheeledSimulation
from tests import test_dispatch_golden as golden


class SplitWheeledSimulation(RobotProcessSplit, WheeledSimulation):
    pass


class RobotProcessSplitTests(unittest.TestCase):
    def assert_same(self, actual, expected):
        for index, (got, want) in enumerate(zip(actual['assignments'], expected['assignments'])):
            self.assertEqual(got, want, f'assignment #{index} is the first difference')
        self.assertEqual(len(actual['assignments']), len(expected['assignments']))
        self.assertEqual(actual['completed_at'], expected['completed_at'])
        self.assertEqual(actual['ticks'], expected['ticks'])
        self.assertEqual(actual.get('handoffs'), expected.get('handoffs'))

    def test_ordinary_baseline_is_reproduced(self):
        expected = json.loads(golden.FIXTURE.read_text(encoding='utf-8'))
        self.assert_same(golden.record(sim_class=SplitWheeledSimulation), expected)

    def test_relay_baseline_is_reproduced(self):
        expected = json.loads(golden.RELAY_FIXTURE.read_text(encoding='utf-8'))
        self.assert_same(golden.record_relay(sim_class=SplitWheeledSimulation), expected)

    def test_handoff_that_changes_a_later_robot_redoes_its_second(self):
        # At t=300 robot 9 reports low battery and hands its goods to robot 6 at
        # t=366: the donor comes after the receiver in the sequential order, so the
        # receiver's marker changes a robot that has not advanced yet -> redo.
        sequential = json.loads(golden.REDO_FIXTURE.read_text(encoding='utf-8'))
        split, sim = golden.record_relay(sim_class=SplitWheeledSimulation, report_at=300, with_sim=True)
        self.assertTrue(sequential['handoffs'], 'scenario must contain a handoff')
        self.assertTrue(any(h['donor_id'] > h['receiver_id'] for h in sequential['handoffs']),
                        'scenario must hand off from a higher to a lower robot number')
        self.assert_same(split, sequential)
        self.assertGreaterEqual(sim.split_stats['redo'], 1)


if __name__ == '__main__':
    unittest.main()
