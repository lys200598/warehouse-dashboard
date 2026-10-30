"""Robot-process query input boundaries and five-pack telemetry without services."""
import os
import unittest

from backend.query_c import ContextUnavailable, PACKS, RobotContextClient, context_distributions
from backend.query_c.catalog import ANCHOR, QUERIES
from tests.test_query_c_catalog import FakeDriver, Result


class CompleteDriver(FakeDriver):
    def __init__(self, latest=10, complete=True, step=True):
        super().__init__(latest, step)
        self.complete = complete
        self.closed = False

    def run(self, query, **params):
        if query == QUERIES['step']:
            self.calls.append((query, params))
            return Result([{'step': {'tick': params['t'], 'complete': self.complete}}] if self.step else [])
        return super().run(query, **params)

    def close(self):
        self.closed = True


class RobotContextTests(unittest.TestCase):
    def test_five_pack_metrics_pin_latest_and_preserve_query_windows(self):
        driver = CompleteDriver()
        client = RobotContextClient(driver=driver, state_window=4, event_window=6)
        result = client.query('run', 1)
        self.assertEqual((result['tick'], result['robot_id'], result['pid']), (10, 1, os.getpid()))
        self.assertEqual(set(result['pack_ms']), set(PACKS))
        self.assertEqual(set(result['pack_rows']), set(PACKS))
        self.assertGreaterEqual(result['total_ms'], 0)
        self.assertEqual(len(driver.calls), 6)
        for query, params in driver.calls[1:]:
            self.assertEqual((params['t'], params['state_lower'], params['event_lower']), (10, 6, 4))
        self.assertFalse(any('SET ' in query or 'MERGE ' in query for query, _ in driver.calls))
        client.close()
        self.assertTrue(driver.closed)

    def test_actor_client_never_accepts_future_missing_or_incomplete_steps(self):
        for driver, tick in ((CompleteDriver(), 11), (CompleteDriver(None), 4),
                             (CompleteDriver(step=False), 4), (CompleteDriver(complete=False), 4)):
            with self.subTest(tick=tick, latest=driver.latest), self.assertRaises(ContextUnavailable):
                RobotContextClient(driver=driver).query('run', 1, tick=tick)

    def test_actor_client_accepts_complete_historical_zero(self):
        result = RobotContextClient(driver=CompleteDriver()).query('run', 1, tick=0)
        self.assertEqual(result['tick'], 0)

    def test_actor_client_missing_latest_is_error(self):
        for latest in (None, True, -1):
            with self.subTest(latest=latest), self.assertRaises(ContextUnavailable):
                RobotContextClient(driver=CompleteDriver(latest)).query('run', 1)

    def test_distributions_include_each_robot_pack_and_count_failures(self):
        def sample(robot, total):
            return dict(robot_id=robot, pid=robot+100, total_ms=total,
                        pack_ms={name: total/2 for name in PACKS},
                        pack_rows={name: robot for name in PACKS})
        stats = context_distributions([sample(1, 100), sample(1, 600), sample(2, 50),
                                       {'robot_id': 2, 'error': 'missing complete Step'}])
        self.assertEqual((stats['requests'], stats['errors'], stats['robots'], stats['processes']), (3, 1, 2, 2))
        self.assertEqual(stats['total_ms']['over_500'], 1)
        self.assertEqual(stats['per_robot']['1']['total_ms']['mean'], 350)
        self.assertEqual(stats['per_robot']['1']['packs']['recovery']['query_ms']['mean'], 175)
        self.assertEqual(stats['per_robot']['2']['packs']['task_decision']['rows']['mean'], 2)


if __name__ == '__main__':
    unittest.main()
