from pathlib import Path
import tempfile
import time
import unittest

from backend.real_scale.benchmark import make_case
from backend.real_scale.loadgen import overlap, parallel_round
from backend.real_scale.store import RealStore
from tests.test_real_scale_query import FakeMirror, FakeOracle


TEMP = Path(__file__).resolve().parents[1]/'data/local-check'


class ProcessOracle(FakeOracle):
    index_path = Path('test-only-no-real-database')

    def close(self):
        pass


class SleepingStore(RealStore):
    def __init__(self):
        super().__init__(FakeMirror(ProcessOracle()))

    def full_query(self, *args, **kwargs):
        time.sleep(.08)
        return super().full_query(*args, **kwargs)


class OneErrorStore(SleepingStore):
    def full_query(self, *args, **kwargs):
        if args[2] == 7:
            raise ValueError('retained test request failure')
        return super().full_query(*args, **kwargs)


def oracle_factory(path):
    return ProcessOracle()


def sleeping_store():
    return SleepingStore()


def one_error_store():
    return OneErrorStore()


def broken_store():
    raise RuntimeError('worker initialization failed')


class ParallelLoadgenTests(unittest.TestCase):
    def run_round(self, directory, factory=sleeping_store, stability=0., processes=4):
        return parallel_round(SleepingStore(), ProcessOracle(), clients=100, samples=1000 if stability else 1,
            round_id=1, mode='ordered', deadline=time.perf_counter()+40, writer_samples=[],
            stability_seconds=stability, output_dir=directory,
            worker_processes=processes,
            _store_factory=factory, _oracle_factory=oracle_factory)

    def test_global_barrier_hundred_real_intervals_and_unchanged_deterministic_cases(self):
        with tempfile.TemporaryDirectory(dir=TEMP) as temp:
            rows, report = self.run_round(temp)
            self.assertEqual(len(rows), 100)
            self.assertEqual(report['failures'], 0)
            self.assertEqual(report['peak_active_full_api_requests'], 100)
            self.assertEqual(report['global_start_barrier_parties'], 4)
            self.assertEqual(report['total_clients'], 100)
            self.assertEqual(report['total_clients_ready_observed'], 100)
            self.assertTrue(report['all_clients_ready_before_release'])
            self.assertEqual(report['barrier_implementation'], 'hierarchical_4_groups')
            self.assertEqual(len({r['worker_pid'] for r in rows}), 4)
            self.assertEqual({r['client_id'] for r in rows}, set(range(1, 101)))
            self.assertTrue(report['completed_requested_sample_count'])
            self.assertFalse(report['writer_commits_observed_inside_span'])
            self.assertTrue(all(len(w['client_ids'])==25 and not w['error'] and
                                'loadgen.py' in w['reader_sha256'] for w in report['worker_identities']))
            for row in rows:
                expected = make_case(ProcessOracle(), row['client_id'], 0, 1, 100, ProcessOracle().handoff_points())
                self.assertEqual({k: row[k] for k in expected}, expected)
                self.assertLessEqual(row['case_ready_ns'], report['barrier_release_ns'])
                self.assertLessEqual(report['barrier_release_ns'], row['start_ns'])
            self.assertEqual(len(list(Path(temp).glob('worker-round-1/process-*.jsonl'))), 4)

    def test_failed_request_and_worker_initialization_are_never_silently_dropped(self):
        with tempfile.TemporaryDirectory(dir=TEMP) as temp:
            rows, report = self.run_round(temp, one_error_store)
            self.assertEqual(len(rows), 100)
            self.assertEqual(report['failures'], 1)
            self.assertIn('retained test request failure', next(r for r in rows if not r['ok'])['error'])
        with tempfile.TemporaryDirectory(dir=TEMP) as temp:
            rows, report = self.run_round(temp, broken_store)
            self.assertEqual(rows, [])
            self.assertFalse(report['completed_requested_sample_count'])
            self.assertTrue(report['process_errors'])
            self.assertEqual(report['peak_active_full_api_requests'], 0)

    def test_stability_duration_begins_at_global_release_not_process_spawn(self):
        with tempfile.TemporaryDirectory(dir=TEMP) as temp:
            rows, report = self.run_round(temp, stability=.25)
            self.assertTrue(rows)
            self.assertGreaterEqual(report['actual_request_span_seconds'], .25)
            self.assertTrue(report['completed_requested_sample_count'])

    def test_interval_sweep_does_not_add_independent_process_peaks(self):
        self.assertEqual(overlap([dict(start_ns=1, end_ns=10), dict(start_ns=10, end_ns=20)]), 1)
        self.assertEqual(overlap([dict(start_ns=1, end_ns=11), dict(start_ns=10, end_ns=20)]), 2)
        with self.assertRaises(ValueError):
            overlap([dict(start_ns=5, end_ns=4)])

    def test_eight_balanced_workers_still_have_hundred_global_clients_and_one_barrier(self):
        with tempfile.TemporaryDirectory(dir=TEMP) as temp:
            rows, report = self.run_round(temp, processes=8)
            self.assertEqual(report['failures'], 0)
            self.assertEqual(report['worker_processes'], 8)
            self.assertEqual(report['worker_group_sizes'], [13, 13, 13, 13, 12, 12, 12, 12])
            self.assertEqual(report['global_start_barrier_parties'], 8)
            self.assertEqual(report['total_clients'], 100)
            self.assertEqual(report['total_clients_ready_observed'], 100)
            self.assertTrue(report['all_clients_ready_before_release'])
            self.assertTrue(all(len(w['client_ready_ns'])==len(w['client_ids']) for w in report['worker_identities']))
            self.assertEqual(len({r['worker_pid'] for r in rows}), 8)
            self.assertEqual({r['client_id'] for r in rows}, set(range(1, 101)))
            self.assertEqual(report['peak_active_full_api_requests'], 100)
            self.assertEqual(len(rows), 100)


if __name__ == '__main__':
    unittest.main()
