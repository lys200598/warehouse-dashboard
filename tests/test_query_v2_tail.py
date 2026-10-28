from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
import json
import math
import threading
import unittest

from backend.query_v2.service import build_full_query
from backend.query_v2.service_fragments import FragmentQueryService
from backend.query_v2.service_tail import TailQueryService, build_tail_query
from backend.real_scale.benchmark import validate_result
from tests.test_query_v2_service import HeaderMirror


class TailMirror(HeaderMirror):
    initial_count = expanded_count = 5
    wrong_path = None
    incomplete = False
    cold_barrier = None

    def query(self, query, params, timeout=5):
        if 'AS tail_retrieval_path' not in query:
            return super().query(query, params, timeout)
        # A test double for the SQL branch outputs, not performance evidence.
        rows = super().query(build_full_query(), params, timeout)
        if not rows:
            return rows
        self.queries[-1] = (query, deepcopy(params), timeout)
        row = rows[0]
        initial = self.initial_count
        if initial>=5:
            path, expanded = 'ann100', 0
        elif self.expanded_count>=5:
            path, expanded = 'ann1000_after_underfilled100', 5
        else:
            path, expanded = 'exact_after_underfilled1000', self.expanded_count
        row.update(tail_retrieval_path=self.wrong_path or path,
            tail_ann100_eligible=initial, tail_ann1000_eligible=expanded)
        if path=='exact_after_underfilled1000':
            row['similar'] = row['similar'][:self.expanded_count]
        if self.incomplete:
            row['similar'] = row['similar'][:4]
        if self.cold_barrier is not None:
            self.cold_barrier.wait(timeout=10)
        return rows


class QueryV2TailTests(unittest.TestCase):
    def setUp(self):
        self.mirror = TailMirror()
        self.store = TailQueryService(self.mirror)
        self.args = ('actual-test', 7, 1, 'G-1', 'T-1')
        self.case = dict(seq=7, robot_id=1, goods_id='G-1', focus_task_id='T-1')

    def test_normal_full_contract_equal_to_fragments(self):
        expected = FragmentQueryService(HeaderMirror()).full_query(*self.args)
        for _ in range(2):
            got = self.store.full_query(*self.args)
            self.assertEqual(got, expected)
            validate_result(got, self.mirror.oracle, self.case)
            self.assertTrue(self.store.latest_metrics['ann_fused'])
            self.assertEqual(self.store.latest_metrics['ann100_eligible'], 5)
            self.assertEqual(self.store.latest_metrics['ann1000_eligible'], 0)

    def test_fallback_keeps_paths_and_actual_single_bolt_statement(self):
        for initial, expanded, expected in [(3, 5, 'ann1000_after_underfilled100'),
                (1, 4, 'exact_after_underfilled1000')]:
            self.mirror.initial_count, self.mirror.expanded_count = initial, expanded
            result = self.store.full_query(*self.args)
            validate_result(result, self.mirror.oracle, self.case)
            self.assertEqual(result['retrieval_path'], expected)
            expected_bolt = 2 if expanded<5 else 1
            self.assertEqual(result['bolt_statements'], expected_bolt)
            self.assertEqual(self.store.latest_metrics['bolt_statements'], expected_bolt)
            self.assertEqual(self.store.latest_metrics['ann100_eligible'], initial)

    def test_incomplete_or_wrong_fused_branch_results_are_never_silently_accepted(self):
        self.mirror.incomplete = True
        with self.assertRaisesRegex(ValueError, 'incomplete'):
            self.store.full_query(*self.args)
        self.mirror.incomplete, self.mirror.wrong_path = False, 'invented'
        with self.assertRaisesRegex(ValueError, 'unknown fused'):
            self.store.full_query(*self.args)

    def test_flags_allow_exact_original_fallback_without_modifying_old_code(self):
        mirror = HeaderMirror()
        mirror.empty_initial = mirror.empty_expanded = True
        store = TailQueryService(mirror, fuse_vectors=False, singleflight_headers=False)
        result = store.full_query(*self.args)
        self.assertEqual(result['bolt_statements'], 3)
        self.assertEqual(result['retrieval_path'], 'exact_after_underfilled1000')
        self.assertFalse(store.latest_metrics['ann_fused'])
        for value in [0, 1, None, 'true']:
            with self.assertRaises(ValueError):
                TailQueryService(mirror, fuse_vectors=value)

    def test_query_keeps_candidate_counts_cutoffs_and_materializes_vector_once(self):
        query = build_tail_query()
        self.assertIn('queryNodes($index,$ann_count,tail_query_vector)', query)
        self.assertIn('queryNodes($index,1000,tail_query_vector)', query)
        self.assertEqual(query.count('node.run_id=$run AND node.seq<=$seq'), 2)
        self.assertIn('WITH *,p.real_embedding AS tail_query_vector', query)
        self.assertNotIn('vector.similarity.cosine', query)  # Measured faster as a separate parameter query.
        self.assertIn('WHERE size(initial_similar)<5', query)
        self.assertIn('WHEN size(expanded_similar)>=5', query)
        for word in ['CREATE ', 'MERGE ', 'DELETE ', ' SET ']:
            self.assertNotIn(word, query)
        self.assertIn('metadata_json', query)
        self.assertIn('history', query)

    def test_cold_concurrent_identical_wire_headers_only_decode_once(self):
        self.mirror.cold_barrier = threading.Barrier(12)
        def fetch(robot):
            value = self.store.full_query('actual-test', 7, robot, f'G-{robot}', f'T-{robot}')
            return value, self.store.latest_metrics
        with ThreadPoolExecutor(max_workers=12) as executor:
            values = list(executor.map(fetch, range(1, 13)))
        self.assertEqual(self.mirror.headers_sent, 12)  # No hidden preload.
        self.assertEqual(sum(m['header_parse_reused'] for _,m in values), 11)
        self.assertEqual(sum(m['fragment_cache_hit'] for _,m in values), 11)
        self.assertTrue(all(not m['metadata_cache_hit'] for _,m in values))
        self.assertTrue(all(m['metadata_wire_chars']>0 for _,m in values))
        values[0][0]['metadata'].clear()
        self.assertTrue(values[1][0]['metadata'])

    def test_changed_raw_strings_cannot_be_hidden_by_same_version_cache(self):
        # Simulate a cold request that began before the original header was
        # cached, then received a DIFFERENT wire string at the same version.
        self.store.full_query(*self.args)
        params = dict(run='actual-test', seq=7, robot=1, goods='G-1', focus_task='T-1')
        row = HeaderMirror().query(build_full_query(), params)[0]
        row['metadata_json'] = json.dumps({'different': True})
        self.store._begin()
        metadata, config, marker = self.store._header('actual-test', row, None)
        self.assertEqual(metadata, {'different': True})
        self.assertFalse(self.store.latest_metrics['header_parse_reused'])
        row['metadata_json'] = '{"bad": NaN}'
        with self.assertRaises(ValueError):
            self.store._header('actual-test', row, None)

    def test_cache_disable_clear_and_lru_are_preserved(self):
        store = TailQueryService(self.mirror, header_cache_size=0)
        store.full_query(*self.args)
        store.full_query(*self.args)
        self.assertEqual(len(store._raw_headers), 0)
        self.assertFalse(store.latest_metrics['header_parse_reused'])
        self.store.full_query(*self.args)
        self.assertEqual(len(self.store._raw_headers), 1)
        self.store.clear_header_cache()
        self.assertEqual(len(self.store._raw_headers), 0)
        self.assertEqual(len(self.store._headers), 0)
        self.assertEqual(len(self.store._fragment_headers), 0)
        bounded = TailQueryService(self.mirror, header_cache_size=1)
        bounded.full_query(*self.args)
        bounded.full_query('other-run', *self.args[1:])
        self.assertEqual(len(bounded._raw_headers), 1)

    def test_nonfinite_dynamic_data_and_bad_source_vector_are_rejected(self):
        self.store.full_query(*self.args)
        self.mirror.returned_header = {'robot_payload': json.dumps({'bad': math.inf})}
        with self.assertRaises(ValueError):
            self.store.full_query(*self.args)
        self.mirror.returned_header = None
        self.mirror.bad_vector = True
        with self.assertRaises(ValueError):
            self.store.full_query(*self.args)

    def test_new_version_and_new_run_never_share_wire_header_cache(self):
        self.store.full_query(*self.args)
        self.mirror.created_at = 'new-version'
        result = self.store.full_query(*self.args)
        self.assertFalse(self.store.latest_metrics['header_parse_reused'])
        self.assertEqual(len(self.store._raw_headers), 1)
        other = self.store.full_query('other-run', *self.args[1:])
        self.assertEqual(other['metadata'], {'run': 'other-run'})
        self.assertNotEqual(result['metadata'], other['metadata'])

    def test_business_apis_are_not_rewritten_or_vector_queried(self):
        robots = self.store.robot_context('actual-test', 7, [2, 1])
        self.assertEqual([x['robot_id'] for x in robots['robots']], [1, 2])
        self.assertFalse(self.store.latest_metrics['ann_fused'])
        context = self.store.cooperation_context('actual-test', 7, 'T-1', 'G-1')
        self.assertEqual(context['request_cursors'], {'goods_after': 0, 'cooperation_after': 0})
        self.assertFalse(self.store.latest_metrics['ann_fused'])


if __name__ == '__main__':
    unittest.main()
