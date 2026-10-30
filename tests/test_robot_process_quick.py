"""Fast check (about 10 s) while editing the split: first 400 ticks of the relay
scenario (handoff at t=282) on the local split engine against the stored baseline.
Run the full baselines, Ray and live checks when a step is finished."""
import json
import unittest

from backend.robot_actor.local import RobotProcessSplit
from backend.wheeled.sim import WheeledSimulation
from tests import test_dispatch_golden as golden

TICKS = 400


class Split(RobotProcessSplit, WheeledSimulation):
    pass


class QuickSplitTests(unittest.TestCase):
    def test_first_400_ticks_of_the_relay_match_the_baseline(self):
        expected = json.loads(golden.RELAY_FIXTURE.read_text(encoding='utf-8'))
        actual = golden.record_relay(sim_class=Split, max_ticks=TICKS)
        self.assertEqual(actual['handoffs'], [h for h in expected['handoffs'] if h['tick'] <= TICKS])
        self.assertEqual(actual['assignments'], [a for a in expected['assignments'] if a['tick'] <= TICKS])


if __name__ == '__main__':
    unittest.main()
