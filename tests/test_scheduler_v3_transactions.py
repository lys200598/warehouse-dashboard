import copy
import json
import math
import random
import unittest

from backend.scheduling_v2.motion import MotionTable
from backend.day1.nav import position
from backend.traffic import swept_distance
from backend.scheduling_v3.progress import ProgressMonitor
from backend.scheduling_v3.transactions import stationary_guard, try_replace_many


def move(a, b, start=0, end=4):
    return {'a': {'x': a[0], 'y': a[1]}, 'b': {'x': b[0], 'y': b[1]},
            'start': start, 'end': end, 'phase': 'moving'}


class ReservationTransactionTests(unittest.TestCase):
    def test_multi_owner_replacement_is_continuous_and_removes_old_future(self):
        table = MotionTable()
        table.reserve([stationary_guard({'x': 8, 'y': 8}, 0, 10)], owner=1)
        table.reserve([stationary_guard({'x': 10, 'y': 8}, 0, 10)], owner=2)
        result = try_replace_many(table, {
            1: [move((0, 0), (4, 0)), stationary_guard({'x': 4, 'y': 0}, 4, 10)],
            2: [move((0, 3), (4, 3)), stationary_guard({'x': 4, 'y': 3}, 4, 10)]},
            now=0, current_positions={1: {'x': 0, 'y': 0}, 2: {'x': 0, 'y': 3}})
        self.assertTrue(result)
        self.assertEqual(result.reason, 'committed')
        self.assertTrue(table.clear_motion({'x': 8, 'y': 8}, {'x': 8, 'y': 8}, 2, 3, 9))
        self.assertFalse(table.clear_motion({'x': 4, 'y': 0}, {'x': 4, 'y': 0}, 8, 9, 9))

    def test_crossing_group_is_rejected_even_when_endpoints_are_separated(self):
        table = MotionTable()
        before = copy.deepcopy(table.__dict__)
        result = try_replace_many(table, {1: [move((0, 0), (4, 0))], 2: [move((2, -2), (2, 2))]}, now=0)
        self.assertFalse(result)
        self.assertEqual(result.reason, 'continuous_collision')
        self.assertEqual(result.conflicts, (1,))
        self.assertEqual(result.details['owner'], 2)
        self.assertEqual(table.__dict__, before)

    def test_unchanged_robot_conflict_keeps_entire_live_table_and_counters(self):
        table = MotionTable()
        table.reserve([stationary_guard({'x': 20, 'y': 20}, 0, 10)], owner=1)
        table.reserve([stationary_guard({'x': 2, 'y': 0}, 0, 10)], owner=99)
        before = copy.deepcopy(table.__dict__)
        result = try_replace_many(table, {1: [move((0, 0), (4, 0))]}, now=0)
        self.assertFalse(result)
        self.assertEqual(result.conflicts, (99,))
        self.assertEqual(table.__dict__, before)

    def test_dry_run_success_does_not_commit(self):
        table = MotionTable()
        table.reserve([stationary_guard({'x': 20, 'y': 20}, 0, 10)], owner=1)
        before = copy.deepcopy(table.__dict__)
        result = try_replace_many(table, {1: [move((0, 0), (4, 0))]}, validate_only=True)
        self.assertTrue(result)
        self.assertEqual(result.reason, 'validated')
        self.assertEqual(table.__dict__, before)

    def test_candidate_input_does_not_alias_committed_geometry(self):
        table = MotionTable()
        plan = [move((0, 0), (4, 0))]
        self.assertTrue(try_replace_many(table, {1: plan}))
        plan[0]['a']['x'] = 200
        self.assertEqual(table.owners[1][0][1]['a']['x'], 0)

    def test_stationary_guard_is_a_real_other_robot(self):
        table = MotionTable()
        before = copy.deepcopy(table.__dict__)
        result = try_replace_many(table, {1: [move((0, 0), (4, 0))]},
            stationary_guards={99: {'point': {'x': 2, 'y': 0}, 'start': 0, 'end': 10}})
        self.assertFalse(result)
        self.assertEqual(table.__dict__, before)
        result = try_replace_many(table, {1: [move((0, 0), (4, 0))]},
            stationary_guards={99: {'point': {'x': 2, 'y': 3}, 'start': 0, 'end': 10}})
        self.assertTrue(result)
        self.assertIn(99, table.owners)
        self.assertFalse(table.clear_motion({'x': 2, 'y': 3}, {'x': 2, 'y': 3}, 6, 7, 1))

    def test_guard_is_not_allowed_to_move(self):
        result = try_replace_many(MotionTable(), {}, stationary_guards={1: move((0, 0), (1, 0))})
        self.assertFalse(result)
        self.assertEqual(result.reason, 'guard_is_not_stationary')

    def test_invalid_candidate_rejection_does_not_remove_previous_plan(self):
        invalid = [
            ([], 'empty_plan_would_remove_body'),
            ([move((0, 0), (4, 0), 0, 0)], 'invalid_part_interval'),
            ([move((0, 0), (math.nan, 0))], 'invalid_point'),
            ([move((0, 0), (4, 0)), move((4, 0), (6, 0), 5, 7)], 'time_discontinuity'),
            ([move((0, 0), (4, 0)), move((5, 0), (6, 0), 4, 7)], 'position_discontinuity'),
            ([move((0, 0), (4, 0), 1, 4)], 'plan_does_not_start_now'),
        ]
        for parts, reason in invalid:
            with self.subTest(reason=reason):
                table = MotionTable()
                table.reserve([stationary_guard({'x': 0, 'y': 0}, 0, 10)], owner=1)
                before = copy.deepcopy(table.__dict__)
                result = try_replace_many(table, {1: parts}, now=0)
                self.assertFalse(result)
                self.assertEqual(result.reason, reason)
                self.assertEqual(table.__dict__, before)

    def test_actual_pose_is_required_for_each_owner_and_checked(self):
        for positions, expected in [({}, 'missing_current_position'),
                                    ({1: {'x': 1, 'y': 0}}, 'start_position_mismatch')]:
            result = try_replace_many(MotionTable(), {1: [move((0, 0), (4, 0))]}, current_positions=positions)
            self.assertFalse(result)
            self.assertEqual(result.reason, expected)

    def test_group_can_retime_together_but_not_into_an_unmodelled_idle_body(self):
        table = MotionTable()
        # Planned crossing: robot 2 only reaches the intersection after robot 1.
        table.reserve([move((0, 0), (4, 0), 0, 4)], owner=1)
        table.reserve([stationary_guard({'x': 2, 'y': -2}, 0, 5), move((2, -2), (2, 2), 5, 9)], owner=2)
        before = copy.deepcopy(table.__dict__)
        one = [stationary_guard({'x': 0, 'y': 0}, 0, 5), move((0, 0), (4, 0), 5, 9)]
        failed = try_replace_many(table, {1: one}, now=0)
        self.assertFalse(failed)
        self.assertEqual(failed.conflicts, (2,))
        self.assertEqual(table.__dict__, before)
        both = {1: one,
                2: [stationary_guard({'x': 2, 'y': -2}, 0, 10), move((2, -2), (2, 2), 10, 14)]}
        self.assertTrue(try_replace_many(table, both, now=0))

    def test_random_transactions_match_independent_all_pair_geometry_oracle(self):
        rng = random.Random(271027)
        for _ in range(160):
            table = MotionTable()
            for owner in [1, 2]:
                table.reserve([stationary_guard({'x': 20 + owner * 3, 'y': 20}, 0, 10)], owner=owner)
            fixed = stationary_guard({'x': 0, 'y': 0}, 0, 10)
            table.reserve([fixed], owner=99)
            candidates = {owner: [move((rng.uniform(-5, 5), rng.uniform(-5, 5)),
                                      (rng.uniform(-5, 5), rng.uniform(-5, 5)), 0, 10)] for owner in [1, 2]}
            pairs = [(candidates[1][0], candidates[2][0]),
                     (candidates[1][0], fixed), (candidates[2][0], fixed)]
            expected = all(swept_distance(position(a, 0), position(a, 10),
                                          position(b, 0), position(b, 10)) >= 1.1 - 1e-7 for a, b in pairs)
            before = copy.deepcopy(table.__dict__)
            result = try_replace_many(table, candidates, now=0)
            self.assertEqual(bool(result), expected)
            if not result:
                self.assertEqual(table.__dict__, before)


class ProgressTests(unittest.TestCase):
    def test_ring_is_only_escalated_after_persistent_no_progress(self):
        monitor = ProgressMonitor(stall_seconds=10, dependency_ttl=4)
        for now in [0, 3, 6, 9, 12]:
            for owner, blocker in [(1, 2), (2, 3), (3, 1)]:
                monitor.record_block(owner, [blocker], now)
            report = monitor.snapshot(now)
            self.assertEqual(report['cycle_candidates'], [[1, 2, 3]])
            self.assertEqual(report['persistent_cycles'], [[1, 2, 3]] if now >= 10 else [])

    def test_expected_release_prevents_false_deadlock_and_cannot_slide_forever(self):
        monitor = ProgressMonitor(stall_seconds=5, dependency_ttl=20, release_grace=2)
        for owner, blocker in [(1, 2), (2, 1)]:
            monitor.record_block(owner, [blocker], 0, expected_release=10)
            monitor.record_block(owner, [blocker], 8, expected_release=1000)
        self.assertEqual(monitor.snapshot(10)['persistent_cycles'], [])
        self.assertEqual(monitor.snapshot(12)['persistent_cycles'], [])
        self.assertEqual(monitor.snapshot(13)['persistent_cycles'], [[1, 2]])

    def test_stale_dependencies_do_not_form_live_cycles(self):
        monitor = ProgressMonitor(stall_seconds=5, dependency_ttl=3)
        monitor.record_block(1, [2], 0)
        monitor.record_block(2, [1], 0)
        self.assertEqual(monitor.snapshot(8)['cycle_candidates'], [])
        monitor.record_block(1, [2], 8)
        monitor.record_block(2, [1], 8)
        self.assertEqual(monitor.snapshot(8)['persistent_cycles'], [])

    def test_movement_clears_dependency_but_repeated_pose_does_not(self):
        monitor = ProgressMonitor(stall_seconds=5, dependency_ttl=20)
        monitor.record_progress(1, 0, {'x': 0, 'y': 0}, token=[0, None])
        monitor.record_block(1, [2], 0)
        monitor.record_progress(1, 6, {'x': 0, 'y': 0}, token=[0, None])
        self.assertEqual(monitor.snapshot(6)['stalled_owners'], [1])
        monitor.record_progress(1, 7, {'x': .1, 'y': 0}, token=[0, None])
        self.assertEqual(monitor.snapshot(7)['dependencies'], [])

    def test_stage_oscillation_is_not_progress_and_task_age_includes_unassigned(self):
        monitor = ProgressMonitor(stall_seconds=5, dependency_ttl=20)
        robot = {'robot_id': 1, 'x': 0, 'y': 0, 'completed': 0, 'cargo_id': None, 'stage': 'idle'}
        monitor.observe(0, [robot])
        monitor.record_block(1, [2], 0)
        robot['stage'] = 'mission_queue'
        report = monitor.observe(8, [robot], {
            'A': {'task_id': 'A', 'status': 'waiting', 'release_second': 1, 'due_second': 5},
            'B': {'task_id': 'B', 'status': 'completed', 'release_second': 0},
            'C': {'task_id': 'C', 'status': 'scheduled', 'release_second': 20},
        })
        self.assertEqual(report['stalled_owners'], [1])
        self.assertEqual(report['max_task_age_seconds'], 7)
        self.assertEqual(report['overdue_tasks'], 1)
        self.assertEqual(len(report['unfinished_tasks']), 1)

    def test_checkpoint_roundtrip_preserves_owner_types_and_diagnostics(self):
        monitor = ProgressMonitor(stall_seconds=5, dependency_ttl=20)
        monitor.record_progress(1, 0, {'x': 0, 'y': 0}, token=(0, None))
        monitor.record_progress(2, 0, {'x': 2, 'y': 0}, token=(0, None))
        monitor.record_block(1, [2], 0)
        monitor.record_block(2, [1], 0)
        state = json.loads(json.dumps(monitor.export_state()))
        other = ProgressMonitor.from_state(state)
        self.assertEqual(other.snapshot(8), monitor.snapshot(8))
        # JSON converts tuple markers to lists: normalized restoration remains usable.
        other.observe(9, [{'robot_id': 1, 'x': 0, 'y': 0, 'completed': 0, 'cargo_id': None}])
        self.assertEqual(other.robots[1]['last_progress_at'], 0)


if __name__ == '__main__':
    unittest.main()
