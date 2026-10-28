import argparse
from copy import deepcopy
from pathlib import Path
import tempfile
import unittest

from backend.real_scale.benchmark import distribution, make_case, one_round, run, business_coverage
from backend.real_scale.store import RealStore
from tests.test_real_scale_query import FakeMirror, FakeOracle

TEMP = Path(__file__).resolve().parents[1]/'data/local-check'


class BenchmarkContractTests(unittest.TestCase):
    def test_failed_and_slow_samples_are_not_dropped_and_quantiles_are_nearest_rank(self):
        result = distribution([dict(ms=100., ok=True), dict(ms=600., ok=False)], 2)
        self.assertEqual(result['mean_ms'], 350)
        self.assertEqual(result['failures'], 1)
        self.assertEqual(result['p99_ms'], 600)
        self.assertEqual(result['over_500ms'], 1)
        self.assertIsNone(distribution([], 0)['mean_ms'])

    def test_hundred_client_plan_covers_hundred_distinct_agents_and_handoff_boundaries(self):
        oracle = FakeOracle()
        points = oracle.handoff_points()
        cases = [make_case(oracle, i, 0, 1, 100, points) for i in range(1, 101)]
        self.assertEqual({c['robot_id'] for c in cases}, set(range(1, 101)))
        self.assertEqual(cases[0]['boundary'], 'handoff_before')
        self.assertEqual(make_case(oracle, 1, 1, 1, 100, points)['boundary'], 'handoff_at')
        self.assertEqual(make_case(oracle, 1, 2, 1, 100, points)['boundary'], 'handoff_after')
        self.assertEqual(make_case(oracle, 2, 0, 1, 100, points)['focus_task_id'], 'T-1')
        self.assertTrue(all(c['focus_task_id'] and c['goods_id'] for c in cases))

    def test_small_real_api_double_round_exercises_every_component(self):
        import time
        oracle = FakeOracle(); store = RealStore(FakeMirror(oracle))
        rows, report = one_round(store, oracle, clients=2, samples=3, round_id=1,
            mode='ordered', deadline=time.perf_counter()+10, writer_samples=[])
        self.assertEqual(report['requests'], 6)
        self.assertEqual(report['failures'], 0)
        self.assertTrue(all(r['focus_task_present'] and r['goods_present'] for r in rows))
        self.assertFalse(report['writer_commits_observed_inside_span'])

    def test_complete_report_writes_raw_and_does_not_claim_million_or_writer_from_mock(self):
        with tempfile.TemporaryDirectory(dir=TEMP) as temporary:
            args = argparse.Namespace(output_dir=Path(temporary)/'run', min_states=1000,
                mode='ordered', clients=2, rounds=3, samples=2, observe_writer=False,
                max_wall_seconds=10, stability_seconds=0, recall_samples=1)
            result = run(args, RealStore(FakeMirror()), FakeOracle())
            self.assertTrue(result['qualification']['all_correct'])
            self.assertFalse(result['qualification']['same_run_actual_million_states'])
            self.assertFalse(result['qualification']['writer_progress_each_round'])
            self.assertFalse(result['qualification']['hundred_clients_configured'])
            self.assertFalse(result['qualification']['edge_device'])
            self.assertEqual(len((args.output_dir/'requests.jsonl').read_text().splitlines()), 12)
            self.assertTrue(result['ann_recall_samples'][0]['ok'])

    def test_wrong_index_schema_and_incomplete_prefix_fail_before_measurements(self):
        class BadStore(RealStore):
            def counts(self, *args):
                value = super().counts(*args)
                value['vector_indexes'][0]['properties'] = ['embedding']
                return value
        with tempfile.TemporaryDirectory(dir=TEMP) as temporary:
            args = argparse.Namespace(output_dir=Path(temporary)/'bad', min_states=1000,
                mode='ordered', clients=2, rounds=3, samples=2, observe_writer=False,
                max_wall_seconds=10, stability_seconds=0, recall_samples=0)
            with self.assertRaisesRegex(ValueError, 'index'):
                run(args, BadStore(FakeMirror()), FakeOracle())
            self.assertFalse((args.output_dir/'requests.jsonl').exists())

    def test_profile_is_diagnostic_and_keeps_complete_api_oracle(self):
        from backend.real_scale.profile import run as profile
        with tempfile.TemporaryDirectory(dir=TEMP) as temporary:
            args = argparse.Namespace(output_dir=Path(temporary)/'profile', clients=2, samples=2,
                profile_client=1, mode='ordered', max_wall_seconds=10, server_profile=False)
            result = profile(args, RealStore(FakeMirror()), FakeOracle())
            self.assertTrue(result['diagnostic_only'])
            self.assertEqual(result['requests'], 4)
            self.assertEqual(result['failures'], 0)
            self.assertNotIn('qualification', result)
            self.assertTrue((args.output_dir/'worker.pstats').exists())
            self.assertIn('fetch_materialize_ms', result['phase_mean_ms'])

    def test_real_similarity_enum_is_case_insensitive_but_not_coerced(self):
        class EnumStore(RealStore):
            def counts(self, *args):
                value = super().counts(*args)
                value['vector_indexes'][0]['options']['indexConfig']['vector.similarity_function'] = similarity
                return value
        for similarity in ['cosine', 'COSINE', 'euclidean', None, 32, True, ['cosine']]:
            with self.subTest(similarity=similarity), tempfile.TemporaryDirectory(dir=TEMP) as temporary:
                args = argparse.Namespace(output_dir=Path(temporary)/'enum', min_states=1000,
                    mode='ordered', clients=1, rounds=1, samples=1, observe_writer=False,
                    max_wall_seconds=10, stability_seconds=0, recall_samples=0)
                if similarity in ('cosine', 'COSINE'):
                    self.assertTrue(run(args, EnumStore(FakeMirror()), FakeOracle())['qualification']['all_correct'])
                else:
                    with self.assertRaisesRegex(ValueError, 'index'):
                        run(args, EnumStore(FakeMirror()), FakeOracle())
                    self.assertFalse((args.output_dir/'requests.jsonl').exists())

    def coverage_rows(self):
        return [dict(ok=True, robot_id=1, seq=seq, boundary=kind, handoff_event_seq=20,
            focus_task_id='T-1', goods_id='G-1', current_task_present=True, focus_task_present=True,
            goods_present=True, goods_rows=2, cargo_handoff_event_seqs=[] if seq<5 else [20])
            for kind, seq in [('handoff_before', 4), ('handoff_at', 5), ('handoff_after', 6)]]

    def test_coverage_requires_actual_handoff_chain_and_all_successful_boundaries(self):
        rows = self.coverage_rows()
        self.assertTrue(business_coverage(FakeOracle(), rows)['business_coverage_sufficient'])
        changes = [lambda r: r.pop(),
                   lambda r: r[1].update(ok=False),
                   lambda r: r[1].update(focus_task_id='T-2'),
                   lambda r: r[1].update(goods_id='G-2'),
                   lambda r: r[1].update(seq=7),
                   lambda r: r[1].update(robot_id=99),
                   lambda r: [v.update(cargo_handoff_event_seqs=[]) for v in r],
                   lambda r: [v.update(goods_present=False) for v in r],
                   lambda r: [v.update(current_task_present=False) for v in r]]
        for edit in changes:
            changed = deepcopy(rows); edit(changed)
            self.assertFalse(business_coverage(FakeOracle(), changed)['business_coverage_sufficient'])

    def test_no_handoff_is_not_applicable_not_collaboration_success(self):
        class NoHandoff(FakeOracle):
            def handoff_points(self, limit=100):
                return []
        value = business_coverage(NoHandoff(), self.coverage_rows())
        self.assertFalse(value['cooperation_applicable'])
        self.assertIsNone(value['business_coverage_sufficient'])

    def test_cli_allows_longer_soak_margin_but_keeps_hundred_client_process_gate(self):
        from contextlib import redirect_stderr
        import io
        from unittest.mock import patch
        from backend.real_scale.benchmark import main
        class ReachedOracle(Exception):
            pass
        base = ['--oracle-index', 'unused.sqlite', '--output-dir', 'unused-output']
        with patch('backend.real_scale.oracle.RealOracle.open', side_effect=ReachedOracle), self.assertRaises(ReachedOracle):
            main([*base, '--stability-seconds', '130'])
        for flags in [['--stability-seconds', '181'], ['--worker-processes', '4', '--clients', '25']]:
            with patch('backend.real_scale.oracle.RealOracle.open') as opened, redirect_stderr(io.StringIO()):
                with self.assertRaises(SystemExit):
                    main([*base, *flags])
                opened.assert_not_called()


if __name__ == '__main__':
    unittest.main()
