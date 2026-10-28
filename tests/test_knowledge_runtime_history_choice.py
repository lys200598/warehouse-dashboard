"""Independent history-sensitive choices on the actual small physical engine.

The provider supplies a contract-complete, deliberately synthetic prior
transfer for this unit test. No Neo4j/provenance/performance claim is made.
The running selection, SIPP, energy admission, reservation transaction and
physical transfer use the real implementation unless a test explicitly
injects a rejected transaction to verify fail-closed propagation.
"""
from copy import deepcopy
from pathlib import Path
import unittest
from unittest.mock import patch

from backend.knowledge import digest
from backend.knowledge_runtime.policy import DecisionRejected
from backend.knowledge_runtime.sim import KnowledgeSimulation
from backend.real_scale.writer import Config
from backend.scheduling_v3.sim import Simulation as PhysicalSimulation
from backend.scheduling_v3.layout import handover_candidates


ROOT = Path(__file__).resolve().parents[1]
SCENE = ROOT/'data/layouts/warehouse100-wide-v2.json'
DATASET = ROOT/'data/examples/warehouse-v5'


class PriorTransferProvider:
    """Coherent synthetic history; live current custody stays authoritative."""
    def __init__(self, former_donor=None, *, stale_task_owner=False):
        self.former_donor = former_donor
        self.stale_task_owner = stale_task_owner
        self.calls = []

    def context(self, sim, task):
        current = task['robot_id']
        prior = self.former_donor if self.former_donor is not None else current
        common = {'run_id': sim.run_id, 'seq': 7, 'graph_committed_seq': 7,
                  'has_more': False, 'next_after_event_seq': None}
        first_kind = 'received' if task['task_type'] == 'INBOUND' else 'initial_stock'
        goods = [dict(event_seq=1, seq=1, sim_time=1, kind=first_kind,
                      location_id=task['source_id'], event_key=sim.run_id+':fixture:1'),
                 dict(event_seq=2, seq=2, sim_time=2, kind='picked',
                      location_id=f'ROBOT-{prior}', event_key=sim.run_id+':fixture:2')]
        cooperation = []
        if self.former_donor is not None:
            goods.append(dict(event_seq=3, seq=3, sim_time=3, kind='cargo_handoff',
                              location_id=f'ROBOT-{current}', event_key=sim.run_id+':fixture:3'))
            event = dict(event_seq=3, event_id=sim.run_id+':fixture:3', sim_time=3,
                kind='cargo_handoff', task_id=task['task_id'], goods_id=task['goods_id'],
                robot_id=current, donor_id=prior, donor_robot_id=prior,
                receiver_id=current, receiver_robot_id=current, location_id=f'ROBOT-{current}')
            cooperation.append({'seq': 3, 'event_seq': 3, 'event': event})
        # A second knowledge read at the real transfer boundary observes the
        # reservation just produced by the actual coordinator in this test.
        for event in sim.pending_events:
            if event.get('task_id') == task['task_id'] and event.get('kind') == 'handoff_reserved':
                cooperation.append({'seq': 7, 'event_seq': event['event_seq'], 'event': deepcopy(event)})
        value = {'run_id': sim.run_id, 'seq': 7, 'graph_committed_seq': 7,
            'batch_checksum': digest({'fixture': 'prior-transfer', 'run': sim.run_id, 'task': task['task_id']}),
            'task_id': task['task_id'], 'goods_id': task['goods_id'], 'focus_task': deepcopy(task),
            'request_cursors': {'goods_after': 0, 'cooperation_after': 0},
            'goods_history': {**common, 'goods_id': task['goods_id'], 'rows': goods,
                'current_as_of': {key: goods[-1][key] for key in ('event_seq', 'location_id', 'sim_time')}},
            'cooperation': {**common, 'task_id': task['task_id'], 'rows': cooperation}}
        if self.stale_task_owner:
            value['focus_task']['robot_id'] = prior
        self.calls.append(deepcopy(value))
        return value


class HistoryChangesActualHelperChoiceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        base = KnowledgeSimulation(SCENE, DATASET, 'history-choice-physical-unit',
            Config(charging_policy='none'), knowledge_mode='required', commit_seconds=5)
        for _ in range(1800):
            base.step()
            carriers = [owner for owner, robot in base.robots.items() if robot['carrying'] and owner not in base.assists]
            if carriers:
                cls.owner = carriers[0]
                break
        else:
            raise AssertionError('small real input must produce an actual carrier')
        task = base.tasks[base.robots[cls.owner]['task_id']]
        cls.original_ids = [robot['robot_id'] for robot in PhysicalSimulation._assistance_helpers(
            base, base.robots[cls.owner], task)]
        if len(cls.original_ids) < 3:
            raise AssertionError('fixture needs independently available alternative helpers')
        cls.state = base.checkpoint()

    def make(self, *, former=None, stale_task_owner=False):
        sim = KnowledgeSimulation(SCENE, DATASET, self.state['run_id'],
            Config(charging_policy='none'), checkpoint=deepcopy(self.state),
            knowledge_mode='required', commit_seconds=5)
        sim.history_provider = PriorTransferProvider(former, stale_task_owner=stale_task_owner)
        return sim

    def task(self, sim):
        return sim.tasks[sim.robots[self.owner]['task_id']]

    def physical(self, sim):
        return deepcopy((sim.elapsed, sim.robots, sim.tasks, sim.jobs, sim.goods, sim.assists))

    def test_prior_donor_history_changes_real_plan_and_real_transfer(self):
        nearest, next_nearest = self.original_ids[:2]
        control = self.make()
        self.assertTrue(control.request_assistance(self.owner))
        self.assertEqual(control.assists[self.owner]['receiver'], nearest)
        self.assertEqual(control.knowledge_stats['helper_order_changes'], 0)

        informed = self.make(former=nearest)
        task = self.task(informed)
        original_energy = informed._energy_admission
        original_transaction = informed._coordinated_replan
        with patch.object(informed, '_energy_admission', wraps=original_energy) as energy, \
             patch.object(informed, '_coordinated_replan', wraps=original_transaction) as transaction:
            self.assertTrue(informed.request_assistance(self.owner))
        self.assertEqual(informed.assists[self.owner]['receiver'], next_nearest)
        self.assertEqual(informed.knowledge_stats['helper_order_changes'], 1)
        self.assertTrue(any(call.args[0]['robot_id'] == next_nearest for call in energy.call_args_list))
        self.assertGreater(transaction.call_count, 0)
        self.assertEqual(task['robot_id'], self.owner)
        self.assertEqual(informed.goods[task['goods_id']], f'ROBOT-{self.owner}')

        # Execute the actual reserved movement and handoff, not just a sort or
        # projected choice. The provider is consulted again before transfer.
        target_second = informed.assists[self.owner]['handoff_at']
        self.assertLess(target_second-informed.elapsed, 900)
        while informed.elapsed < target_second:
            informed.step()
        self.assertEqual(informed.metrics['handoffs'], 1)
        self.assertEqual(task['robot_id'], next_nearest)
        self.assertEqual(informed.goods[task['goods_id']], f'ROBOT-{next_nearest}')
        self.assertEqual(len(informed.history_provider.calls), 2)
        self.assertEqual(informed.knowledge_stats['approved_transfers'], 1)
        self.assertGreaterEqual(informed.metrics['min_separation_m'], 1.1-1e-7)
        informed.assert_inventory()

    def test_actual_helper_seam_preserves_distance_order_inside_history_groups(self):
        nearest = self.original_ids[0]
        sim = self.make(former=nearest)
        task = self.task(sim)
        decision = sim._decision(self.owner, task)
        self.assertEqual(decision['historical_donor_robot_ids'], [nearest])
        sim._planning_decision = decision
        try:
            reordered = [robot['robot_id'] for robot in sim._assistance_helpers(sim.robots[self.owner], task)]
        finally:
            sim._planning_decision = None
        self.assertEqual(reordered, self.original_ids[1:]+[nearest])
        self.assertEqual(set(reordered), set(self.original_ids))

    def test_history_preference_still_obeys_real_energy_admission(self):
        nearest, preferred = self.original_ids[:2]
        sim = self.make(former=nearest)
        # Keep the candidate eligible by the 35% helper cutoff, but make the
        # actual mission+return+reserve calculation reject its tiny margin.
        # This is an explicit in-memory scenario configuration for a gate test.
        sim.config.task_reserve_soc = 34.9
        sim.robots[preferred]['battery'] = sim.config.charge_request_soc+.0001
        observed = []
        original = sim._energy_admission

        def record(robot, parts):
            result = original(robot, parts)
            observed.append((robot['robot_id'], result.admitted, result.reason))
            return result

        with patch.object(sim, '_energy_admission', side_effect=record):
            self.assertTrue(sim.request_assistance(self.owner))
        self.assertTrue(any(owner == preferred and not accepted for owner, accepted, _ in observed))
        receiver = sim.assists[self.owner]['receiver']
        self.assertNotEqual(receiver, preferred)
        self.assertTrue(any(owner == receiver and accepted for owner, accepted, _ in observed))
        self.assertEqual(sim.goods[self.task(sim)['goods_id']], f'ROBOT-{self.owner}')
        sim.assert_inventory()

    def test_history_does_not_bypass_rejected_reservation_transaction(self):
        nearest = self.original_ids[0]
        sim = self.make(former=nearest)
        before = self.physical(sim)
        candidates = handover_candidates(sim.roadmap, sim._point(sim.robots[self.owner]), limit=8)
        self.assertTrue(candidates)
        # Bound the negative probe to one genuine handover site. Route/energy
        # work remains real, but inject transaction refusal to verify that an
        # authorized/history-prioritized proposal cannot force a commit.
        with patch('backend.scheduling_v3.sim.handover_candidates', return_value=candidates[:1]), \
             patch.object(sim, '_coordinated_replan', return_value=False) as reject:
            self.assertFalse(sim.request_assistance(self.owner))
        self.assertGreater(reject.call_count, 0)
        self.assertEqual(before, self.physical(sim))
        self.assertEqual(sim.knowledge_stats['approved_transfers'], 0)
        self.assertFalse(sim.assists)

    def test_past_owner_cannot_override_live_current_custody(self):
        sim = self.make(former=self.original_ids[0], stale_task_owner=True)
        before = self.physical(sim)
        with patch.object(sim, '_assistance_helpers', wraps=sim._assistance_helpers) as helpers:
            with self.assertRaisesRegex(DecisionRejected, 'shared task owner differs'):
                sim.request_assistance(self.owner)
        self.assertEqual(helpers.call_count, 0)
        self.assertEqual(before, self.physical(sim))
        self.assertEqual(sim.knowledge_stats['approved_plans'], 0)
        self.assertEqual(sim.knowledge_stats['rejected'], 1)


if __name__ == '__main__':
    unittest.main()
