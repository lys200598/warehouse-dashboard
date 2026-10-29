"""Opt-in model capabilities are checked by real matching and action paths."""
from collections import Counter
from copy import deepcopy
from pathlib import Path
import unittest
from unittest.mock import patch

from backend.wide.capabilities import (fleet_assignment, profile, slot_requirement,
                                       access_reason, spare_cost)
from backend.scheduling_v5.sim import CandidateSimulation, Config


ROOT = Path(__file__).resolve().parents[1]
SCENE = ROOT/'data/layouts/warehouse100-wide-v2.json'
DATASET = ROOT/'data/examples/warehouse-v5'


class ProfileTests(unittest.TestCase):
    def test_default_stays_all_x3_and_mixed_reuses_original_number_groups(self):
        self.assertEqual(set(fleet_assignment().values()), {'X3'})
        mixed = fleet_assignment('mixed')
        self.assertEqual(Counter(mixed.values()), {'A1': 40, 'R2': 40, 'X3': 20})
        self.assertEqual((mixed[1], mixed[4], mixed[8]), ('A1', 'R2', 'X3'))

    def test_custom_counts_are_exact_deterministic_and_spread(self):
        value = fleet_assignment('mixed', {'A1': 20, 'R2': 30, 'X3': 50})
        self.assertEqual(Counter(value.values()), {'A1': 20, 'R2': 30, 'X3': 50})
        self.assertEqual(value, fleet_assignment('mixed', {'X3': 50, 'R2': 30, 'A1': 20}))
        self.assertEqual(set(value[i] for i in range(1, 11)), {'A1', 'R2', 'X3'})

    def test_invalid_or_implicit_custom_mix_fails_closed(self):
        for mode, counts in [('bad', None), ('all-x3', {'X3': 100}),
                             ('mixed', {'X3': 99}), ('mixed', {'other': 100}),
                             ('mixed', {'A1': True, 'X3': 99}), ('mixed', {'A1': -1, 'X3': 101})]:
            with self.assertRaises(ValueError):
                fleet_assignment(mode, counts)
        with self.assertRaises(ValueError):
            Config(capability_spare_cost_seconds=float('nan'))

    def test_horizontal_extra_extension_and_vertical_height_are_independent(self):
        scene = {'spec': {'levels': 3, 'rackHeight': 1.8}, 'racks': [{'id': 'R01'}]}
        low = slot_requirement({'level': 1, 'rack_id': 'R01'}, scene)
        self.assertIsNone(access_reason({'robot_type_key': 'A1'}, [low]))
        self.assertAlmostEqual(low['height_m'], .175)
        extended = {**low, 'extra_extension_m': .3}
        self.assertEqual(access_reason({'robot_type_key': 'A1'}, [extended]), 'capability_extension')
        self.assertIsNone(access_reason({'robot_type_key': 'R2'}, [extended]))
        tall = {**low, 'height_m': 1.2}
        self.assertEqual(access_reason({'robot_type_key': 'R2'}, [tall]), 'capability_height')
        self.assertEqual(profile('A1')['extension'], .18)
        self.assertEqual(spare_cost({'robot_type_key': 'X3'}, [low], 15), 30)


class MixedRuntimeTests(unittest.TestCase):
    def make(self, **kwargs):
        return CandidateSimulation(SCENE, DATASET, 'mixed-test',
            Config(fleet_mode='mixed', charging_policy='critical-only'), **kwargs)

    def test_inbound_target_and_outbound_source_both_gate_direct_admission(self):
        sim = self.make()
        short = next(r for r in sim.robots.values() if r['robot_type_key'] == 'A1')
        medium = next(r for r in sim.robots.values() if r['robot_type_key'] == 'R2')
        long = next(r for r in sim.robots.values() if r['robot_type_key'] == 'X3')
        out = next(t for t in sim.tasks.values() if t['task_type'] == 'OUTBOUND' and '-L3-' in t['source_id'])
        inbound = dict(out, task_type='INBOUND', source_id='IN-1', target_id=out['source_id'])
        for task in [out, inbound]:
            before = deepcopy(sim.robots)
            self.assertFalse(sim.schedule_task(short, task))
            self.assertEqual(sim._last_block['reason'], 'capability_level')
            self.assertIsNotNone(sim.task_capability_reason(medium, task))
            self.assertIsNone(sim.task_capability_reason(long, task))
            self.assertEqual(sim.robots, before)
        # A collected high-shelf OUT can be relayed by A1 to a ground boundary.
        self.assertIsNone(sim.task_capability_reason(short, out, remaining=True))

    def test_matcher_masks_unreachable_pairs_before_priority_and_cardinality(self):
        sim = self.make()
        short = sim.robots[1]
        long = sim.robots[8]
        task = dict(next(iter(sim.tasks.values())), task_type='INBOUND', source_id='IN-1',
                    target_id='R01-L3-C1-F1', ready_at=0, due_second=10000)
        result = sim._select_assignment([[0, 100, 1e8]], [task], [[100, 100]], [short, long])
        self.assertIn((0, 1), result)
        # Retried columns may be a subset: the matcher must use that exact list.
        result = sim._select_assignment([[100, 1e8]], [task], [[100]], [long])
        self.assertIn((0, 0), result)

    def test_soft_low_shelf_preference_does_not_prevent_long_arm_only_work(self):
        sim = self.make()
        short, long = sim.robots[1], sim.robots[8]
        task = dict(next(iter(sim.tasks.values())), task_type='INBOUND', source_id='IN-1',
                    target_id='R01-L1-C1-F1', ready_at=0, due_second=10000)
        self.assertIn((0, 0), sim._select_assignment([[0, 0, 1e8]], [task], [[100, 100]], [short, long]))
        self.assertIn((0, 0), sim._select_assignment([[0, 1e8]], [task], [[100]], [long]))

    def test_assistance_helpers_and_last_action_reject_incapable_receiver(self):
        sim = self.make()
        donor = sim.robots[8]
        task = dict(next(iter(sim.tasks.values())), task_type='INBOUND', source_id='IN-1', target_id='R01-L3-C1-F1')
        helpers = sim._assistance_helpers(donor, task)
        self.assertTrue(helpers)
        self.assertEqual({r['robot_type_key'] for r in helpers}, {'X3'})
        sim.tasks[task['task_id']] = task
        before = deepcopy(sim.goods)
        with self.assertRaisesRegex(AssertionError, 'capability rejected before handoff_received'):
            sim.marker(sim.robots[1], {'task_id': task['task_id']}, 'handoff_received')
        self.assertEqual(sim.goods, before)

    def test_fault_recovery_checks_unpicked_source_but_allows_completed_pickup(self):
        sim = self.make()
        short = sim.robots[1]
        task = deepcopy(next(t for t in sim.tasks.values() if t['task_type'] == 'OUTBOUND' and '-L3-' in t['source_id']))
        task.update(status='assigned', robot_id=1)
        with patch('backend.scheduling_v3.sim.Simulation._task_parts', return_value=[]) as parent:
            self.assertIsNone(sim._task_parts(short, task))
            parent.assert_not_called()
            short['carrying'] = True
            task['status'] = 'carrying'
            self.assertEqual(sim._task_parts(short, task), [])
            parent.assert_called_once()
        # Supplied synchronized-recovery plans cannot bypass the same gate.
        high_in = dict(task, task_type='INBOUND', source_id='IN-1', target_id='R01-L3-C1-F1')
        sim.tasks[high_in['task_id']] = high_in
        with patch('backend.scheduling_v3.sim.Simulation._coordinated_replan', return_value=True) as parent:
            self.assertFalse(sim._coordinated_replan({1: ([], {'task_id': high_in['task_id'], 'assist_donor': 8})}))
            parent.assert_not_called()

    def test_real_high_shelf_inbound_handoff_uses_a_capable_receiver(self):
        sim = self.make()
        donor = None
        for _ in range(1600):
            sim.step()
            donor = next((i for i, r in sim.robots.items() if r['carrying'] and
                sim.tasks[r['task_id']]['task_type'] == 'INBOUND' and '-L3-' in sim.tasks[r['task_id']]['target_id']), None)
            if donor is not None:
                break
        self.assertIsNotNone(donor)
        self.assertTrue(sim.report_soc(donor, 24.))
        receiver = sim.assists[donor]['receiver']
        self.assertEqual(sim.robots[receiver]['robot_type_key'], 'X3')
        # Checkpoint the actual, still-pending two-party transfer.
        restored = self.make(checkpoint=sim.checkpoint())
        for _ in range(500):
            sim.step(); restored.step()
            if sim.metrics['handoffs']:
                break
        self.assertEqual(sim.metrics['handoffs'], 1)
        self.assertEqual(restored.metrics['handoffs'], 1)
        self.assertEqual(sim.robots, restored.robots)
        self.assertEqual(sim.goods, restored.goods)
        sim.assert_inventory()

    def test_checkpoint_preserves_types_and_rejects_profile_tampering(self):
        sim = self.make()
        for _ in range(5):
            sim.step()
        state = sim.checkpoint()
        restored = self.make(checkpoint=state)
        self.assertEqual([r['robot_type_key'] for r in sim.robots.values()],
                         [r['robot_type_key'] for r in restored.robots.values()])
        for _ in range(5):
            sim.step(); restored.step()
        self.assertEqual(sim.robots, restored.robots)
        bad = deepcopy(state)
        bad['robots'][0]['state']['robot_type_key'] = 'X3'
        with self.assertRaisesRegex(ValueError, 'configured fleet identity'):
            self.make(checkpoint=bad)

    def test_real_19_task_run_completes_and_every_assignment_is_capable(self):
        sim = self.make()
        assigned = Counter()
        seen = 0
        for _ in range(3200):
            sim.step()
            for event in sim.pending_events[seen:]:
                if event['kind'] == 'task_assigned':
                    robot = sim.robots[event['robot_id']]
                    task = sim.tasks[event['task_id']]
                    self.assertIsNone(sim.task_capability_reason(robot, task))
                    assigned[robot['robot_type_key']] += 1
            seen = len(sim.pending_events)
            if sim.metrics['completed'] == 19:
                break
        self.assertEqual(sim.metrics['completed'], 19)
        self.assertEqual(sim.metrics['late'], 0)
        self.assertEqual(set(assigned), {'A1', 'R2', 'X3'})
        self.assertEqual(sim.metrics['unsafe_events'], 0)
        sim.assert_inventory()


if __name__ == '__main__':
    unittest.main()
