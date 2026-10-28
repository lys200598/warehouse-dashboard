import gzip
import json
from pathlib import Path
import tempfile
import time
import unittest

from backend.query_v2.benchmark import (analyze_legacy, build_plan, execute_phase,
                                      interval_peak, summarize_rows)
from backend.real_scale.store import RealStore
from tests.test_real_scale_query import FakeMirror, FakeOracle


TEMP = Path(__file__).resolve().parents[1]/'data/local-check'


class TestOracle(FakeOracle):
    index_path = Path('test-only-immutable-index')

    def close(self):
        pass


class SlowStore(RealStore):
    def __init__(self):
        super().__init__(FakeMirror(TestOracle()))

    def full_query(self, *args, **kwargs):
        time.sleep(.03)
        return super().full_query(*args, **kwargs)


class ErrorStore(SlowStore):
    def full_query(self, *args, **kwargs):
        if args[2] % 2 == 0:
            raise RuntimeError('retained request failure')
        return super().full_query(*args, **kwargs)


class CorruptStore(SlowStore):
    def full_query(self, *args, **kwargs):
        result = super().full_query(*args, **kwargs)
        result['robot']['carrying'] = 0  # Python 0 == False must not pass.
        return result


def oracle_factory(path):
    return TestOracle()


def slow_store():
    return SlowStore()


def error_store():
    return ErrorStore()


def corrupt_store():
    return CorruptStore()


def broken_store():
    raise RuntimeError('retained initialization failure')


class BarrierStore(SlowStore):
    def full_query(self, *args, **kwargs):
        time.sleep(.2)
        return super().full_query(*args, **kwargs)


def barrier_store():
    return BarrierStore()


def options(folder, *, validation='deferred', clients=4, samples=2, processes=1):
    phase = build_plan(TestOracle(), clients=clients, samples=samples, rounds=1)['phases'][0]
    return {'folder': str(Path(folder)/'phase'), 'phase': phase, 'clients': clients,
            'worker_processes': processes, 'implementation': 'baseline',
            'oracle_index': str(TestOracle.index_path), 'validation': validation,
            'timeout': 5, 'pool_size': 120, 'deadline': time.perf_counter()+40,
            'pending_budget_per_worker': 16*1024*1024,
            'reserve_bytes_per_result': 128*1024}


class QueryV2BenchmarkTests(unittest.TestCase):
    def setUp(self):
        TEMP.mkdir(parents=True, exist_ok=True)

    def test_same_case_plan_has_no_implementation_or_validation_influence(self):
        a = build_plan(TestOracle(), clients=100, samples=3, rounds=3)
        b = build_plan(TestOracle(), clients=100, samples=3, rounds=3)
        self.assertEqual(a, b)
        self.assertEqual(len(a['phases'][0]['cases']), 300)
        self.assertNotIn('implementation', a)
        for phase in a['phases']:
            for client in range(1, 101):
                self.assertEqual({r['sample'] for r in phase['cases'] if r['client_id'] == client}, {0, 1, 2})

    def test_deferred_validation_is_after_all_query_groups_and_every_response_saved(self):
        with tempfile.TemporaryDirectory(dir=TEMP) as folder:
            opts = options(folder)
            rows, report = execute_phase(opts, store_factory=slow_store, oracle_factory=oracle_factory)
            self.assertEqual(len(rows), 8)
            self.assertTrue(report['all_correct'])
            self.assertEqual(report['peak_active_service_requests'], 4)
            self.assertTrue(report['deferred_validator_started_after_all_groups'])
            self.assertTrue(report['all_clients_ready_before_release'])
            self.assertTrue(all(r['service_start_ns'] < r['service_end_ns'] <= r['end_ns']
                                <= report['global_query_finished_ns'] <= r['validation_start_ns']
                                <= r['validation_end_ns'] <= r['fingerprint_start_ns']
                                <= r['fingerprint_end_ns'] for r in rows))
            self.assertTrue(all(r['result_sha256'] and r['result_json_bytes'] > 0 for r in rows))
            path = Path(opts['folder'])/'worker-0-responses.jsonl.gz'
            with gzip.open(path, 'rt', encoding='utf-8') as stream:
                complete = [json.loads(line) for line in stream]
            self.assertEqual(len(complete), len(rows))
            for response in complete:
                self.assertEqual(set(response['result']), {
                    'run_id', 'seq', 'robot_id', 'graph_committed_seq', 'config', 'metadata',
                    'real_identity', 'batch_checksum', 'robot', 'robot_sha256', 'task',
                    'focus_task_id', 'focus_task', 'history', 'query_vector', 'similar',
                    'goods_history', 'cooperation', 'query_mode', 'retrieval_path', 'bolt_statements'})

    def test_inline_retains_separate_real_service_and_validation_timers(self):
        with tempfile.TemporaryDirectory(dir=TEMP) as folder:
            rows, report = execute_phase(options(folder, validation='inline'),
                                        store_factory=slow_store, oracle_factory=oracle_factory)
            self.assertTrue(report['all_correct'])
            self.assertFalse(report['deferred_validator_started_after_all_groups'])
            self.assertTrue(all(r['service_end_ns'] <= r['validation_start_ns']
                                <= r['fingerprint_end_ns'] <= r['end_ns'] for r in rows))
            self.assertTrue(all(r['full_ms'] >= r['service_ms'] for r in rows))
            self.assertGreater(report['full']['mean_ms'], report['service']['mean_ms'])

    def test_service_and_validation_failures_are_retained_not_dropped(self):
        for factory, key in [(error_store, 'service_error'), (corrupt_store, 'validation_error')]:
            with self.subTest(factory=factory.__name__), tempfile.TemporaryDirectory(dir=TEMP) as folder:
                rows, report = execute_phase(options(folder), store_factory=factory, oracle_factory=oracle_factory)
                self.assertEqual(len(rows), 8)
                self.assertFalse(report['all_correct'])
                self.assertTrue(report['completed_requested_workload'])
                self.assertGreater(report['failures'], 0)
                self.assertTrue(any(key in r for r in rows))

    def test_worker_failure_keeps_error_evidence(self):
        with tempfile.TemporaryDirectory(dir=TEMP) as folder:
            rows, report = execute_phase(options(folder), store_factory=broken_store, oracle_factory=oracle_factory)
            self.assertEqual(rows, [])
            self.assertFalse(report['all_correct'])
            self.assertFalse(report['all_clients_ready_before_release'])
            self.assertTrue(any('retained initialization failure' in e['error'] for e in report['process_errors']))

    def test_reservation_budget_failure_does_not_qualify_or_drop_errors(self):
        with tempfile.TemporaryDirectory(dir=TEMP) as folder:
            opts = options(folder)
            opts['pending_budget_per_worker'] = 10
            rows, report = execute_phase(opts, store_factory=slow_store, oracle_factory=oracle_factory)
            self.assertEqual(rows, [])
            self.assertFalse(report['completed_requested_workload'])
            self.assertTrue(any(e['error'] == 'pending_memory_reservation_budget' for e in report['process_errors']))

    def test_actual_response_budget_failure_is_not_reported_as_success(self):
        with tempfile.TemporaryDirectory(dir=TEMP) as folder:
            opts = options(folder)
            opts['reserve_bytes_per_result'] = 1
            rows, report = execute_phase(opts, store_factory=slow_store, oracle_factory=oracle_factory)
            self.assertEqual(len(rows), 8)
            self.assertTrue(all(r['ok'] for r in rows))
            self.assertFalse(report['all_correct'])
            self.assertTrue(any('actual response bytes exceed reserved' in e['error'] for e in report['process_errors']))

    def test_overlap_never_uses_full_validation_intervals_to_fake_service_concurrency(self):
        rows = [dict(client_id=i+1, sample=0, service_start_ns=10+i*10,
                     service_end_ns=20+i*10, start_ns=1, end_ns=2000,
                     service_ms=.00001, full_ms=.001999, service_thread_cpu_ms=0., ok=True)
                for i in range(100)]
        result = summarize_rows(rows, clients=100, expected_requests=100,
                                phase_start_ns=1, phase_end_ns=2000)
        self.assertEqual(result['peak_active_service_requests'], 1)
        self.assertEqual(result['peak_active_full_inline_requests'], 100)
        self.assertEqual(interval_peak(rows), 1)
        with self.assertRaises(ValueError):
            interval_peak([dict(service_start_ns=True, service_end_ns=20)])

    def test_duplicate_or_missing_sample_is_not_complete(self):
        row = dict(client_id=1, sample=0, service_start_ns=10, service_end_ns=20,
                   start_ns=10, end_ns=20, service_ms=.00001, full_ms=.00001,
                   service_thread_cpu_ms=0., ok=True)
        result = summarize_rows([row, row], clients=1, expected_requests=2,
                                phase_start_ns=1, phase_end_ns=20)
        self.assertFalse(result['sample_count_complete'])
        self.assertFalse(result['all_correct'])

    def test_fast_ms_with_slow_raw_interval_is_rejected(self):
        row = dict(client_id=1, sample=0, service_start_ns=1000000000,
            service_end_ns=2000000000, start_ns=1000000000, end_ns=3000000000,
            service_ms=1., full_ms=2000., service_thread_cpu_ms=0., ok=True)
        with self.assertRaisesRegex(ValueError, 'independent absolute interval'):
            summarize_rows([row], clients=1, expected_requests=1,
                           phase_start_ns=1, phase_end_ns=4000000000)

    def test_stability_sample_cap_is_not_accepted_as_requested_duration(self):
        with tempfile.TemporaryDirectory(dir=TEMP) as folder:
            opts = options(folder, clients=1, samples=1)
            opts['phase']['stability_seconds'] = 2.
            rows, report = execute_phase(opts, store_factory=slow_store, oracle_factory=oracle_factory)
            self.assertEqual(len(rows), 1)
            self.assertFalse(report['duration_complete'])
            self.assertFalse(report['all_correct'])

    def test_legacy_analysis_is_explicitly_not_deferred_service_measurement(self):
        with tempfile.TemporaryDirectory(dir=TEMP) as folder:
            root = Path(folder)
            source = root/'old.jsonl'
            source.write_text(json.dumps(dict(round=1, ok=True, ms=100,
                fetch_materialize_ms=20, oracle_ms=70, fingerprint_ms=10))+'\n', encoding='utf-8')
            result = analyze_legacy(source, root/'analysis')
            self.assertIn('not a deferred service benchmark', result['scope'])
            self.assertEqual(result['rounds']['1']['phases']['oracle_ms']['mean_ms'], 70)

    def test_eight_process_hierarchical_ready_and_global_deferred_finish(self):
        with tempfile.TemporaryDirectory(dir=TEMP) as folder:
            opts = options(folder, clients=100, samples=1, processes=8)
            rows, report = execute_phase(opts, store_factory=barrier_store, oracle_factory=oracle_factory)
            self.assertEqual(len(rows), 100)
            self.assertTrue(report['all_correct'])
            self.assertEqual(report['worker_group_sizes'], [13, 13, 13, 13, 12, 12, 12, 12])
            self.assertEqual(report['total_clients_ready_observed'], 100)
            self.assertTrue(report['all_clients_ready_before_release'])
            self.assertEqual(len({r['worker_pid'] for r in rows}), 8)
            self.assertEqual(report['peak_active_service_requests'], 100)
            self.assertTrue(all(r['validation_start_ns'] >= report['global_query_finished_ns']
                                >= max(r['service_end_ns'] for r in rows) for r in rows))


if __name__ == '__main__':
    unittest.main()
