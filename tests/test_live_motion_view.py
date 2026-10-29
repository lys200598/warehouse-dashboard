from copy import deepcopy
import math
from pathlib import Path
import tempfile
import unittest

from ops.live_motion_view import LiveMotionView


def part(a, b, start, end, phase='to_source'):
    return {'a': {'x': a[0], 'y': a[1]}, 'b': {'x': b[0], 'y': b[1]},
            'start': start, 'end': end, 'phase': phase}


def job(parts, start=0, task='T'):
    return {'start': start, 'parts': parts, 'task_id': task, 'phase_index': 0}


def robot(x, y, stage='to_source'):
    return {'robot_id': 6, 'x': x, 'y': y, 'stage': stage, 'task_id': 'T'}


class LiveMotionViewTests(unittest.TestCase):
    def view(self, payload, plan, now, **kwargs):
        out = deepcopy(payload)
        self.builder.annotate([out], {6: plan} if plan else {}, now, **kwargs)
        return out

    def setUp(self):
        self.builder = LiveMotionView()

    def test_actual_hr6_first_diagonal_step_has_prior_second_without_previous_job(self):
        # Actual V5 first mission: tick190 idle -> tick191 to_source. The old
        # frontend skipped interpolation because both x and y changed.
        actual = robot(9.647619047619047, 3.8571428571428568)
        p = job([part((9.75, 2.4), (7.6, 33.), 0, 21)], start=190, task='V3-T-0000807')
        v = self.view(actual, p, 191)['motion']
        self.assertEqual(v['tick'], 191)
        self.assertEqual(v['executed']['from_tick'], 190)
        self.assertEqual(v['executed']['to_tick'], 191)
        self.assertEqual(v['executed']['points'][0], {'x': 9.75, 'y': 2.4, 't': 190})
        self.assertEqual(v['executed']['points'][-1], {'x': actual['x'], 'y': actual['y'], 't': 191})
        self.assertEqual(v['path'], [{'x': actual['x'], 'y': actual['y']}, {'x': 7.6, 'y': 33.}])
        self.assertAlmostEqual(v['heading'], math.atan2(actual['y']-2.4, actual['x']-9.75))

    def test_turn_keeps_corner_and_absolute_time_not_diagonal_chord(self):
        p = job([part((0, 0), (1, 0), 0, .5), part((1, 0), (1, 1), .5, 1),
                 part((1, 1), (2, 1), 1, 2)])
        v = self.view(robot(1, 1), p, 1)['motion']
        self.assertEqual(v['executed']['points'], [{'x': 0., 'y': 0., 't': 0},
            {'x': 1., 'y': 0., 't': .5}, {'x': 1, 'y': 1, 't': 1}])
        self.assertAlmostEqual(v['heading'], math.pi/2)
        self.assertEqual(v['path'], [{'x': 1, 'y': 1}, {'x': 2, 'y': 1}])

    def test_wait_has_timed_duplicate_positions_and_no_invented_heading(self):
        p = job([part((2, 3), (2, 3), 0, 10, 'traffic_wait'), part((2, 3), (4, 3), 10, 12)])
        v = self.view(robot(2, 3, 'traffic_wait'), p, 5)['motion']
        self.assertEqual(v['executed']['points'], [{'x': 2., 'y': 3., 't': 4}, {'x': 2, 'y': 3, 't': 5}])
        self.assertIsNone(v['heading'])
        self.assertEqual(v['path'], [{'x': 2, 'y': 3}, {'x': 4, 'y': 3}])

    def test_future_path_compresses_collinear_only_and_retains_return(self):
        p = job([part((0, 0), (1, 0), 0, 1), part((1, 0), (2, 0), 1, 2),
            part((2, 0), (2, 2), 2, 4), part((2, 2), (2, 2), 4, 10, 'placing'),
            part((2, 2), (0, 2), 10, 12, 'returning'), part((0, 2), (0, 0), 12, 14, 'returning')])
        self.assertEqual(self.view(robot(0, 0), p, 0)['motion']['path'],
            [{'x': 0, 'y': 0}, {'x': 2, 'y': 0}, {'x': 2, 'y': 2}, {'x': 0, 'y': 2}, {'x': 0, 'y': 0}])

    def test_backtrack_is_not_removed_by_collinear_compression(self):
        p = job([part((0, 0), (2, 0), 0, 2), part((2, 0), (1, 0), 2, 3)])
        self.assertEqual(self.view(robot(0, 0), p, 0)['motion']['path'],
                         [{'x': 0, 'y': 0}, {'x': 2, 'y': 0}, {'x': 1, 'y': 0}])

    def test_new_job_without_prior_second_does_not_invent_executed_motion(self):
        p = job([part((3, 4), (5, 4), 0, 2)], start=10)
        v = self.view(robot(3, 4), p, 10)['motion']
        self.assertIsNone(v['executed'])
        self.assertEqual(v['path'], [{'x': 3, 'y': 4}, {'x': 5, 'y': 4}])

    def test_no_job_clears_old_plan_after_completion(self):
        self.view(robot(1, 0), job([part((0, 0), (2, 0), 0, 2)]), 1)
        v = self.view(robot(2, 0, 'idle'), None, 2)['motion']
        self.assertIsNone(v['plan_id'])
        self.assertIsNone(v['executed'])
        self.assertIsNone(v['heading'])
        self.assertEqual(v['path'], [{'x': 2, 'y': 0}])
        self.assertEqual(self.builder._cache, {})

    def test_actual_endpoint_mismatch_prevents_execution_and_future_shortcut(self):
        p = job([part((0, 0), (2, 0), 0, 2)])
        v = self.view(robot(8, 9), p, 1)['motion']
        self.assertIsNone(v['executed'])
        self.assertIsNone(v['heading'])
        self.assertEqual(v['path'], [{'x': 8, 'y': 9}])

    def test_gap_or_geometric_discontinuity_does_not_draw_between_segments(self):
        for p in (job([part((0, 0), (1, 0), 0, 1), part((1, 0), (2, 0), 2, 3)]),
                  job([part((0, 0), (1, 0), 0, 1), part((8, 0), (9, 0), 1, 2)])):
            v = self.view(robot(1, 0), p, 1)['motion']
            self.assertIsNone(v['executed'])
            self.assertIsNone(v['plan_id'])
            self.assertEqual(v['path'], [{'x': 1, 'y': 0}])

    def test_plan_identity_stable_with_cursor_but_changes_on_task_or_origin(self):
        p = job([part((0, 0), (3, 0), 0, 3)])
        first = self.view(robot(1, 0), p, 1)['motion']['plan_id']
        p['phase_index'] = 1
        self.assertEqual(first, self.view(robot(2, 0), p, 2)['motion']['plan_id'])
        p['task_id'] = 'T-new'
        second = self.view(robot(2, 0), p, 2)['motion']['plan_id']
        self.assertNotEqual(first, second)
        p['parts'][0]['a']['x'] = -1
        self.assertNotEqual(second, self.view(robot(2, 0), p, 2)['motion']['plan_id'])
        p['start'] = 10
        self.assertNotEqual(second, self.view(robot(2, 0), p, 12)['motion']['plan_id'])

    def test_work_uses_actual_boundaries_slot_and_action_clock(self):
        p = job([part((1, 2), (1, 2), 0, 4, 'placing')], start=190)
        tasks = {'T': {'task_id': 'T', 'task_type': 'INBOUND', 'goods_id': 'BOX-1', 'status': 'carrying',
                      'source_id': 'IN-6', 'target_id': 'R03-L2-C2-F1', 'slot_id': 'R03-L2-C2-F1'}}
        w = self.view(robot(1, 2, 'placing'), p, 192, tasks=tasks, slots={'R03-L2-C2-F1': {}})['work']
        self.assertEqual(w['source_id'], 'IN-6')
        self.assertEqual(w['slot_id'], 'R03-L2-C2-F1')
        self.assertEqual(w['goods_id'], 'BOX-1')
        self.assertEqual((w['action_start'], w['action_end']), (190, 194))
        self.assertEqual((w['action_elapsed'], w['action_duration'], w['phase_progress']), (2, 4, 50))

    def test_moving_progress_uses_all_same_phase_lengths_not_return(self):
        p = job([part((0, 0), (1, 0), 0, 1), part((1, 0), (1, 0), 1, 2, 'traffic_wait'),
                 part((1, 0), (1, 3), 2, 4), part((1, 3), (0, 0), 4, 8, 'returning')])
        tasks = {'T': {'task_id': 'T', 'task_type': 'OUTBOUND', 'status': 'assigned', 'source_id': 'R01', 'target_id': 'OUT-3'}}
        w = self.view(robot(1, 1.5), p, 3, tasks=tasks, slots={'R01': {}})['work']
        self.assertEqual(w['phase_progress'], 62.5)
        self.assertIsNone(w['action_duration'])

    def test_permanent_fault_obstacle_does_not_advertise_fake_action_duration(self):
        q = part((2, 3), (2, 3), 0, 10**12, 'fault_wait')
        q['permanent_obstacle'] = True
        tasks = {'T': {'task_id': 'T', 'status': 'carrying'}}
        v = self.view(robot(2, 3, 'fault_wait'), job([q]), 5, tasks=tasks)
        self.assertEqual(v['motion']['path'], [{'x': 2, 'y': 3}])
        self.assertIsNone(v['work']['action_duration'])
        self.assertIsNone(v['work']['phase_progress'])

    def test_ui_annotation_does_not_mutate_source_robots_jobs_or_tasks(self):
        r = robot(1, 0)
        p = job([part((0, 0), (2, 0), 0, 2)])
        original = deepcopy((r, p))
        v = self.view(r, p, 1)
        v['motion']['path'][0]['x'] = 100
        self.assertEqual((r, p), original)
        self.assertEqual(self.view(r, p, 1)['motion']['path'][0]['x'], 1)

    def test_cache_is_bounded_to_current_snapshot_owners(self):
        self.view(robot(1, 0), job([part((0, 0), (2, 0), 0, 2)]), 1)
        self.builder.annotate([], {}, 2)
        self.assertEqual(self.builder._cache, {})


class LiveMotionPublicationTests(unittest.TestCase):
    def test_runtime_exposes_ui_metadata_without_inserting_it_into_robot_state(self):
        from ops.warehouse_live_runtime import WarehouseLiveRuntime
        root = Path(__file__).resolve().parents[1]
        temp = root/'data/local-check/live-motion'
        temp.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(dir=temp) as folder:
            runtime = WarehouseLiveRuntime(source_root=root, output_root=folder,
                datasets={'sample': {'scene': str(root/'data/layouts/warehouse100-wide-v2.json'),
                    'dataset': str(root/'data/examples/warehouse-v5'), 'config': {'charging_policy': 'none'}}},
                dataset_id='sample', mobility='wheeled', knowledge_mode='rules', knowledge_backend='journal')
            try:
                snap = runtime.latest()
                self.assertEqual(len(snap['robots']), 100)
                self.assertEqual(snap['robots'][0]['motion']['tick'], snap['tick'])
                self.assertIn('work', snap['robots'][0])
                self.assertNotIn('motion', runtime._sim.robots[1])
                self.assertNotIn('work', runtime._sim.robots[1])
            finally:
                runtime.shutdown()


if __name__ == '__main__':
    unittest.main()
