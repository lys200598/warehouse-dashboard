import unittest
import json
from pathlib import Path
import tempfile
from unittest.mock import Mock, patch

from ops.kg_joint_acceptance import judge, overlap_peak, stats, observed_advances, watch_writer, summarize_round


class CJointVerdictTests(unittest.TestCase):
    def rounds(self):
        return [dict(round=i, before={'node_count': 1_100_000}, unique_worker_pids=list(range(100)),
            client_observed_peak=100, writes_during_round=True, errors=[], count=100,
            mean_ms=100, over_500ms=2) for i in range(1, 4)]

    def test_original_metric_is_average_per_round(self):
        self.assertTrue(judge(self.rounds(), 100)['pass'])
        rows = self.rounds()
        rows[2]['mean_ms'] = 501
        self.assertTrue(judge(rows, 100)['qualified'])
        self.assertFalse(judge(rows, 100)['pass'])

    def test_scale_concurrency_writes_and_three_rounds_are_required(self):
        changes = [('before', {'node_count': 999_999}), ('unique_worker_pids', list(range(99))),
                   ('client_observed_peak', 99), ('writes_during_round', False), ('errors', ['error'])]
        for key, value in changes:
            with self.subTest(key=key):
                rows = self.rounds()
                rows[0][key] = value
                self.assertFalse(judge(rows, 100)['qualified'])
        self.assertFalse(judge(self.rounds()[:2], 100)['qualified'])
        self.assertFalse(judge(self.rounds(), 99)['qualified'])

    def test_tail_statistics_and_observed_query_overlap(self):
        self.assertEqual(stats([10, 500, 501])['over_500ms'], 1)
        self.assertEqual(overlap_peak([(0, 4), (1, 5), (2, 3)]), 3)
        self.assertEqual(overlap_peak([(0, 1), (1, 2)]), 1)

    def test_commit_evidence_requires_two_reads_inside_measured_window(self):
        trace = [dict(started_wall_ms=9, observed_wall_ms=11, latest_step=1),
                 dict(started_wall_ms=12, observed_wall_ms=13, latest_step=2),
                 dict(started_wall_ms=15, observed_wall_ms=16, latest_step=4),
                 dict(started_wall_ms=19, observed_wall_ms=21, latest_step=5)]
        self.assertEqual(observed_advances(trace, (10, 20)), 2)

    def test_writer_observer_preserves_interval_timestamp_contract(self):
        from threading import Event
        stop, trace = Event(), []
        def point(*args):
            stop.set()
            return {'observed_at_ms': 20, 'latest_step': 4}
        with patch('ops.kg_joint_acceptance.observe', side_effect=point), \
             patch('ops.kg_joint_acceptance.time.time_ns', return_value=19_000_000):
            watch_writer(None, 'neo4j', 'run', stop, trace)
        self.assertEqual(trace[0]['observed_wall_ms'], 20)
        self.assertEqual(observed_advances(trace, (0, 30)), 0)

    def test_direct_commit_proof_does_not_require_production_timestamp_overlap(self):
        before = {'latest_step': 1}
        after = {'latest_step': 2, 'write_steps_in_interval': 0, 'observed_commit_advances': 1}
        records = [{'pid': 1, 'samples': [dict(total_ms=10, start_ns=1, end_ns=2,
                    pack_ms={}, pack_rows={})]}]
        self.assertTrue(summarize_round(1, before, after, records, 100)['writes_during_round'])

    def test_preparation_failure_still_writes_unqualified_report(self):
        from ops.kg_joint_acceptance import main
        with tempfile.TemporaryDirectory() as folder:
            output = Path(folder) / 'result'
            with patch('ops.kg_joint_acceptance.driver_for', return_value=Mock()), \
                 patch('ops.kg_joint_acceptance.ensure_context_schema'), \
                 patch('ops.kg_joint_acceptance.add_prefix', side_effect=RuntimeError('heap space')), \
                 patch('ops.kg_joint_acceptance.remove_prefix', return_value=0):
                code = main(['--run-id', 'wz-test-failure', '--prefix-nodes', '1000', '--output', str(output)])
            self.assertEqual(code, 2)
            report = json.loads((output / 'acceptance.json').read_text())
            self.assertFalse(report['verdict']['qualified'])
            self.assertEqual(report['failure']['stage'], 'history-prefix')
            self.assertEqual(report['history_prefix_cleanup_remaining'], 0)

    def test_small_graph_is_rejected_before_starting_reader_processes(self):
        from ops.kg_joint_acceptance import main
        with tempfile.TemporaryDirectory() as folder:
            output = Path(folder) / 'result'
            with patch('ops.kg_joint_acceptance.driver_for', return_value=Mock()), \
                 patch('ops.kg_joint_acceptance.ensure_context_schema'), \
                 patch('ops.kg_joint_acceptance.add_prefix'), \
                 patch('ops.kg_joint_acceptance.observe', return_value={'node_count': 90000}):
                code = main(['--run-id', 'wz-test-small', '--output', str(output)])
            report = json.loads((output / 'acceptance.json').read_text())
            self.assertEqual(code, 2)
            self.assertEqual(report['rounds'], [])
            self.assertIn('reader processes were not started', report['failure']['error'])
