from copy import deepcopy
import gzip
import hashlib
import json
from pathlib import Path
import tempfile
import unittest

from backend.knowledge import JournalKnowledgeStore, digest
from backend.query_v2.demand import (build_plan, iter_plan, execute_plan_sample, _state_reasons, _policy, ALERT_KINDS,
    handoff_shadow_demo, validate_robot_context, validate_cooperation_context)
from backend.query_v2.history_decision import advise_from_history
from backend.real_scale.oracle import RealOracle
from tests.test_real_scale_oracle import checkpoint, events


ROOT = Path(__file__).resolve().parents[1]


class OracleService:
    """Test oracle-backed API substitute; never called in remote measurements."""
    def __init__(self, oracle):
        self.oracle, self.calls = oracle, []

    def robot_context(self, run, seq, ids):
        self.calls.append(('robot', seq, ids))
        return {'run_id': run, 'seq': seq, 'graph_committed_seq': self.oracle.prefix,
            'batch_checksum': self.oracle.batch_checksum(seq), 'robots': [
                {'robot_id': robot, 'robot': self.oracle.state(seq, robot),
                 'robot_sha256': digest(self.oracle.state(seq, robot)),
                 'task': self.oracle.task(seq, self.oracle.state(seq, robot).get('task_id'))}
                for robot in sorted(ids)]}

    def cooperation_context(self, run, seq, task, goods):
        self.calls.append(('cooperation', seq, task))
        return {'run_id': run, 'seq': seq, 'graph_committed_seq': self.oracle.prefix,
            'batch_checksum': self.oracle.batch_checksum(seq), 'task_id': task, 'goods_id': goods,
            'request_cursors': {'goods_after': 0, 'cooperation_after': 0},
            'focus_task': self.oracle.task(seq, task), 'goods_history': self.oracle.goods_history(seq, goods),
            'cooperation': self.oracle.cooperation(seq, task)}


class QueryDemandTests(unittest.TestCase):
    def setUp(self):
        (ROOT/'data/local-check').mkdir(parents=True, exist_ok=True)
        self.temp = tempfile.TemporaryDirectory(dir=ROOT/'data/local-check')
        self.folder = Path(self.temp.name)
        source = self.folder/'journal.sqlite'
        writer = JournalKnowledgeStore(source)
        try:
            for seq in range(1, 6):
                cp = checkpoint(seq)
                cp['config']['assistance_soc'] = 25.
                writer.append(cp, events(seq))
        finally:
            writer.close()
        self.oracle = RealOracle(source)
        self.store = OracleService(self.oracle)

    def tearDown(self):
        self.oracle.close()
        self.temp.cleanup()

    def pair(self, seq):
        return (self.store.cooperation_context(self.oracle.run_id, seq, 'T1', 'G1'),
                self.store.robot_context(self.oracle.run_id, seq, [1, 2]))

    def alert_oracle(self, kind, **extra):
        source = self.folder/('alert-'+kind+'.sqlite')
        writer = JournalKnowledgeStore(source)
        try:
            for seq in range(1, 6):
                cp = checkpoint(seq)
                cp['config']['assistance_soc'] = 25.
                emitted = events(seq)
                if seq == 5:
                    cp['event_sequence'] = 6
                    emitted = [{'event_id': cp['run_id']+':event:6', 'event_seq': 6,
                                'tick': 20, 'sim_time': 20, 'kind': kind, **extra}]
                writer.append(cp, emitted)
        finally:
            writer.close()
        return RealOracle(source)

    def test_plan_event_trigger_batching_no_normal_tick_polling_and_source_unchanged(self):
        before = hashlib.sha256(self.oracle.index_path.read_bytes()).hexdigest()
        plan = list(iter_plan(self.oracle))
        self.assertEqual(len(plan), 6)
        bootstrap = plan[0]
        self.assertEqual(bootstrap['kind'], 'robot_context')
        self.assertEqual(bootstrap['robot_ids'], list(range(1, 101)))
        low = next(row for row in plan if row['seq'] == 2 and row['kind'] == 'robot_context')
        self.assertIn('low_soc_while_carrying', low['reasons_by_robot']['1'])
        self.assertNotIn(5, {row['seq'] for row in plan})
        self.assertEqual(hashlib.sha256(self.oracle.index_path.read_bytes()).hexdigest(), before)

    def test_plan_summary_is_deterministic_explicit_model_not_speedup(self):
        first, second = self.folder/'first', self.folder/'second'
        first.mkdir(); second.mkdir()
        a, b = build_plan(self.oracle, first), build_plan(self.oracle, second)
        self.assertEqual(a, b)
        self.assertEqual(a['hypothetical_polling_requests'], 500)
        self.assertEqual(a['plan_requests'], 6)
        self.assertEqual(a['robot_snapshots_requested'], 104)
        self.assertTrue(a['not_measured_performance_speedup'])
        self.assertFalse(a['all_plan_requests_executed'])

    def test_actual_warning_kinds_with_robots_refresh_even_without_state_change(self):
        cases = [('persistent_wait_cycle', {'affected_robots': [3, 7]}, [3, 7]),
                 ('traffic_recovery_rejected', {'robot_id': 4, 'affected_robot': 9}, [4, 9]),
                 ('fault_injection_unresolved', {'robot_id': 5}, [5]),
                 ('energy_recovery_unresolved', {'robot_id': 6}, [6])]
        for kind, extra, expected in cases:
            with self.subTest(kind=kind), self.alert_oracle(kind, **extra) as oracle:
                rows = [row for row in iter_plan(oracle) if row['seq'] == 5]
                self.assertEqual(len(rows), 1)
                self.assertEqual(rows[0]['kind'], 'robot_context')
                self.assertEqual(rows[0]['robot_ids'], expected)
                self.assertTrue(all('event:'+kind in row for row in rows[0]['reasons_by_robot'].values()))

    def test_task_only_progress_warning_queries_real_task_not_empty_robot_batch(self):
        with self.alert_oracle('task_progress_warning', task_id='T1') as oracle:
            rows = [row for row in iter_plan(oracle) if row['seq'] == 5]
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]['kind'], 'cooperation_context')
            self.assertEqual((rows[0]['task_id'], rows[0]['goods_id']), ('T1', 'G1'))
            self.assertEqual(rows[0]['trigger_event_seqs'], [6])
            self.assertIn('event:task_progress_warning', rows[0]['reasons'])

    def test_unroutable_warning_rejected_instead_of_fabricating_empty_query(self):
        with self.alert_oracle('energy_recovery_unresolved') as oracle:
            with self.assertRaisesRegex(ValueError, 'unroutable alert'):
                list(iter_plan(oracle))

    def test_policy_explicitly_routes_input_messages_and_all_five_alerts(self):
        policy = _policy(self.oracle.identity)
        self.assertEqual(policy['version'], 'warehouse-event-driven-shadow-demand-v2')
        self.assertEqual(set(policy['alert_refresh_triggers']), set(ALERT_KINDS))
        self.assertEqual(len(ALERT_KINDS), 5)
        self.assertEqual(set(policy['input_events_not_redundantly_queried']), {'initial_stock', 'received'})
        plan = list(iter_plan(self.oracle))
        self.assertFalse(any('event:initial_stock' in reasons for row in plan if row['kind'] == 'robot_context'
                             for reasons in row['reasons_by_robot'].values()))

    def test_future_alert_does_not_change_earlier_plan_or_source(self):
        with self.alert_oracle('task_progress_warning', task_id='T1') as oracle:
            before = hashlib.sha256(oracle.index_path.read_bytes()).hexdigest()
            baseline = [row for row in iter_plan(self.oracle) if row['seq'] < 5]
            augmented = [row for row in iter_plan(oracle) if row['seq'] < 5]
            self.assertEqual(baseline, augmented)
            self.assertEqual(before, hashlib.sha256(oracle.index_path.read_bytes()).hexdigest())

    def test_before_handoff_retains_current_owner_no_future_knowledge(self):
        context, robots = self.pair(2)
        value = advise_from_history(context, robots)
        self.assertEqual(value['current_holder_robot_id'], 1)
        self.assertEqual(value['observed_handoff_count'], 0)
        self.assertNotIn(4, value['historical_event_seqs_used'])
        self.assertEqual(value['actions'][0]['action'], 'continue_current_task')

    def test_at_handoff_history_explains_donor_stop_receiver_continuation(self):
        context, robots = self.pair(3)
        value = advise_from_history(context, robots)
        self.assertEqual(value['current_holder_robot_id'], 2)
        self.assertEqual(value['observed_handoff_count'], 1)
        self.assertEqual(value['actions'][0]['action'], 'do_not_repeat_delivery_after_transfer')
        self.assertEqual(value['actions'][1]['action'], 'continue_current_task')
        self.assertEqual(value['actions'][0]['history_evidence_event_seqs'], [4])
        self.assertTrue(value['real_action_requires_current_head_and_atomic_custody_recheck'])

    def test_completed_task_does_not_follow_historical_receiver_again(self):
        context, robots = self.pair(4)
        value = advise_from_history(context, robots)
        self.assertEqual(value['current_cargo_location'], 'SHIPPED')
        self.assertIsNone(value['current_holder_robot_id'])
        self.assertTrue(all(row['action'] == 'do_not_repeat_completed_task' for row in value['actions']))

    def test_future_event_rejected(self):
        context, robots = self.pair(3)
        context['cooperation']['rows'][-1]['seq'] = 4
        with self.assertRaisesRegex(ValueError, 'future'):
            advise_from_history(context, robots)

    def test_old_owner_cannot_override_current_custody(self):
        context, robots = self.pair(3)
        context['focus_task']['robot_id'] = 1
        with self.assertRaisesRegex(ValueError, 'holder disagree'):
            advise_from_history(context, robots)

    def test_duplicate_current_carrier_rejected(self):
        context, robots = self.pair(3)
        robots['robots'][0]['robot'].update(cargo_id='G1', carrying=True, task_id='T1')
        with self.assertRaisesRegex(ValueError, 'duplicate'):
            advise_from_history(context, robots)

    def test_incomplete_history_rejected(self):
        context, robots = self.pair(3)
        context['goods_history']['has_more'] = True
        with self.assertRaisesRegex(ValueError, 'complete first-page'):
            advise_from_history(context, robots)

    def test_last_page_without_first_page_is_not_complete_history(self):
        for field, value in [('goods_after', 1), ('cooperation_after', 2), ('goods_after', False)]:
            context, robots = self.pair(3)
            context['request_cursors'][field] = value
            with self.subTest(field=field, value=value), self.assertRaisesRegex(ValueError, 'first-page'):
                advise_from_history(context, robots)

    def test_handoff_donor_must_match_prior_custody_history(self):
        context, robots = self.pair(3)
        context['cooperation']['rows'][-1]['event']['donor_robot_id'] = 3
        with self.assertRaisesRegex(ValueError, 'contradicts cargo'):
            advise_from_history(context, robots)

    def test_actual_source_fault_and_handoff_stage_transitions_trigger_refresh(self):
        before = {'task': 'T1', 'cargo': 'G1', 'carrying': True, 'stage': 'to_target', 'low': False}
        for stage in ('fault_wait', 'recovery_wait', 'traffic_wait', 'handoff_give', 'handoff_receive'):
            with self.subTest(stage=stage):
                self.assertIn('control_stage_changed', _state_reasons(before, 'T1', 'G1', stage, True, False))
                entered = {**before, 'stage': stage}
                self.assertIn('control_stage_changed', _state_reasons(entered, 'T1', 'G1', 'to_target', True, False))
        self.assertFalse(_state_reasons(before, 'T1', 'G1', 'to_target', True, False))

    def test_stopped_current_carrier_without_transfer_must_not_be_told_to_drive(self):
        context, robots = self.pair(2)
        robots['robots'][0]['robot']['stage'] = 'fault_wait'
        value = advise_from_history(context, robots)
        self.assertEqual(value['actions'][0]['action'], 'request_assistance_while_retaining_custody')

    def test_mixed_watermark_or_checksum_rejected(self):
        for key, value, message in [('graph_committed_seq', 2, 'watermark'),
                                    ('batch_checksum', 'wrong', 'checksum')]:
            context, robots = self.pair(3)
            robots[key] = value
            with self.subTest(key=key), self.assertRaisesRegex(ValueError, message):
                advise_from_history(context, robots)

    def test_advancing_heads_do_not_invalidate_same_immutable_cutoff(self):
        context, robots = self.pair(3)
        robots['graph_committed_seq'] = 6
        value = advise_from_history(context, robots)
        self.assertEqual(value['observed_graph_heads'], [5, 6])
        self.assertEqual(value['graph_committed_seq'], 5)
        self.assertEqual(value['robot_graph_committed_seq'], 6)
        self.assertEqual(value['current_holder_robot_id'], 2)

    def test_numeric_alias_boolean_cannot_be_event_or_robot(self):
        context, robots = self.pair(3)
        context['cooperation']['rows'][-1]['event']['event_seq'] = True
        with self.assertRaisesRegex(ValueError, 'identity'):
            advise_from_history(context, robots)
        context, robots = self.pair(3)
        context['cooperation']['rows'][-1]['event']['donor_id'] = True
        with self.assertRaisesRegex(ValueError, 'donor'):
            advise_from_history(context, robots)
        value = self.store.robot_context(self.oracle.run_id, 3, [1])
        value['robots'][0]['robot_id'] = True
        with self.assertRaisesRegex(ValueError, 'identities'):
            validate_robot_context(value, self.oracle, {'seq': 3, 'robot_ids': [1]})

    def test_invalid_source_threshold_not_invented(self):
        original = self.oracle._identity['config'].pop('assistance_soc')
        try:
            with self.assertRaisesRegex(ValueError, 'do not invent'):
                list(iter_plan(self.oracle))
        finally:
            self.oracle._identity['config']['assistance_soc'] = original

    def test_full_api_source_validators_reject_payload_tampering(self):
        context, robots = self.pair(3)
        validate_robot_context(robots, self.oracle, {'seq': 3, 'robot_ids': [1, 2]})
        validate_cooperation_context(context, self.oracle, {'seq': 3, 'task_id': 'T1', 'goods_id': 'G1'})
        context['focus_task']['robot_id'] = True
        with self.assertRaisesRegex(ValueError, 'checksum/task'):
            validate_cooperation_context(context, self.oracle, {'seq': 3, 'task_id': 'T1', 'goods_id': 'G1'})
        robots['robots'][0]['robot']['battery'] += 1
        with self.assertRaisesRegex(ValueError, 'payload'):
            validate_robot_context(robots, self.oracle, {'seq': 3, 'robot_ids': [1, 2]})

    def test_bounded_sample_checks_every_selected_payload_and_full_range(self):
        output = self.folder/'sample'; output.mkdir()
        build_plan(self.oracle, output)
        summary = execute_plan_sample(self.store, self.oracle, output/'demand-plan.jsonl', output, 3)
        self.assertEqual(summary['executed_requests'], 3)
        self.assertEqual(summary['failures'], 0)
        self.assertFalse(summary['all_plan_requests_executed'])
        with gzip.open(output/'business-sample-payloads.jsonl.gz', 'rt', encoding='utf-8') as stream:
            values = [json.loads(row) for row in stream]
        self.assertEqual(len(values), 3)
        self.assertEqual(values[0]['case']['seq'], 1)
        self.assertEqual(values[-1]['case']['seq'], 4)
        with self.assertRaisesRegex(ValueError, 'bounded'):
            execute_plan_sample(self.store, self.oracle, output/'demand-plan.jsonl', output, 1001)

    def test_real_handoff_before_at_after_shadow_demo_complete(self):
        output = self.folder/'demo'; output.mkdir()
        summary = handoff_shadow_demo(self.store, self.oracle, output)
        self.assertEqual(summary['source_handoffs'], 1)
        self.assertEqual(summary['decision_points'], 3)
        self.assertEqual(summary['failures'], 0)
        self.assertTrue(summary['no_claim_of_improved_completion_or_live_closed_loop'])
        rows = json.loads((output/'handoff-shadow-decisions.json').read_text(encoding='utf-8'))
        self.assertEqual([row['phase'] for row in rows], ['before', 'at', 'after'])
        self.assertTrue(all(row['oracle_checks'].startswith('complete') for row in rows))


if __name__ == '__main__':
    unittest.main()
