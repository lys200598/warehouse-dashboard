import importlib.util
from pathlib import Path
import unittest


SPEC = importlib.util.spec_from_file_location('hundred_replay',
    Path(__file__).resolve().parents[1] / 'ops/replay_hundred_frame.py')
MOD = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MOD)


class ReplayChecks(unittest.TestCase):
    def test_crossing_catches_endpoint_false_safety(self):
        self.assertEqual(MOD.swept_distance({'x': -1, 'y': 0}, {'x': 1, 'y': 0},
            {'x': 0, 'y': -1}, {'x': 0, 'y': 1}), 0)

    def test_full_all_pairs(self):
        robots = {i: {'x': i * 2., 'y': 0} for i in range(1, 101)}
        result = MOD.audit_motion(robots, robots)
        self.assertEqual(result['checked_pairs'], 4950)
        self.assertEqual(result['min_separation_m'], 2.)

    def test_stationary_robot_still_collides(self):
        robots = {i: {'x': i * 2., 'y': 0} for i in range(1, 101)}
        end = {**robots, 1: {'x': 4., 'y': 0}}
        with self.assertRaisesRegex(AssertionError, 'unsafe swept'):
            MOD.audit_motion(robots, end)

    def test_business_definition_excludes_queue_return_charge(self):
        stages = ['to_source', 'to_target', 'picking', 'placing', 'to_handoff',
                  'handoff_receive', 'traffic_wait', 'returning', 'charging', 'idle']
        self.assertEqual(MOD.active_business([{'stage': s} for s in stages]), 6)

    def test_all_robot_fields_are_exact(self):
        frame = {name: None for name in MOD.FRAME_FIELDS}
        frame.update(elapsed=10, robots=[{'robot_id': i, 'cargo_id': None, 'battery': 50.}
                                        for i in range(1, 101)])
        other = {**frame, 'robots': [dict(r) for r in frame['robots']]}
        self.assertEqual(MOD.compare_sample(frame, other), MOD.compare_sample(frame, frame))
        other['robots'][22]['battery'] += 1e-10
        with self.assertRaisesRegex(AssertionError, 'robots'):
            MOD.compare_sample(frame, other)

    def test_missing_robot_fails(self):
        frame = {name: None for name in MOD.FRAME_FIELDS}
        frame['robots'] = [{'robot_id': i} for i in range(1, 100)]
        with self.assertRaisesRegex(AssertionError, '100 distinct'):
            MOD.project_frame(frame)


if __name__ == '__main__':
    unittest.main()
