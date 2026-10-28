from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
import json
import math
import unittest

from backend.knowledge import digest
from backend.query_v2.service import QueryService
from backend.query_v2.service_fragments import FragmentQueryService
from backend.real_scale.benchmark import validate_result
from tests.test_query_v2_service import HeaderMirror


class QueryV2FragmentsTests(unittest.TestCase):
    def setUp(self):
        self.mirror = HeaderMirror()
        self.store = FragmentQueryService(self.mirror)
        self.args = ('actual-test', 7, 1, 'G-1', 'T-1')

    def test_cold_and_warm_complete_results_equal_existing_full_service(self):
        expected = QueryService(HeaderMirror()).full_query(*self.args)
        for _ in range(3):
            actual = self.store.full_query(*self.args)
            self.assertEqual(actual, expected)
            self.assertEqual(digest(actual), digest(expected))
            validate_result(actual, self.mirror.oracle, dict(seq=7, robot_id=1,
                goods_id='G-1', focus_task_id='T-1'))
        self.assertTrue(self.store.latest_metrics['fragment_cache_hit'])
        self.assertTrue(self.store.latest_metrics['metadata_cache_hit'])
        self.assertEqual(self.store.latest_metrics['metadata_wire_chars'], 0)

    def test_unicode_quotes_slashes_braces_and_newlines_are_json_data_not_structure(self):
        metadata = {'text': '中文🙂 \" } , \"injected\": true, { \\ \n \t',
                    'nested': [{'empty': {}}, [], False, 0, 1.25, None]}
        config = {'text': '"metadata": {"wrong": true}', 'numbers': [1e-15, -0.0, True]}
        self.mirror.returned_header = {'metadata_json': json.dumps(metadata, ensure_ascii=False),
                                      'config_json': json.dumps(config, ensure_ascii=False)}
        for _ in range(2):
            result = self.store.full_query(*self.args)
            self.assertEqual(result['metadata'], metadata)
            self.assertEqual(result['config'], config)
            self.assertNotIn('injected', result)
            self.assertEqual(result, json.loads(json.dumps(result, allow_nan=False)))

    def test_complete_final_json_decode_retains_bool_int_float_types(self):
        self.mirror.returned_header = {'metadata_json': json.dumps(
            {'false': False, 'zero': 0, 'zero_float': 0.0, 'true': True, 'one': 1})}
        first = self.store.full_query(*self.args)
        second = self.store.full_query(*self.args)
        for result in [first, second]:
            self.assertIs(type(result['metadata']['false']), bool)
            self.assertIs(type(result['metadata']['zero']), int)
            self.assertIs(type(result['metadata']['zero_float']), float)
            self.assertIs(type(result['metadata']['true']), bool)
            self.assertIs(type(result['metadata']['one']), int)

    def test_response_mutation_cannot_modify_fragments_or_other_responses(self):
        first = self.store.full_query(*self.args)
        first['metadata'].clear()
        first['config']['robot_count'] = 2
        first['robot']['battery'] = 0
        first['similar'][0]['real_embedding'][0] = 9999
        again = self.store.full_query(*self.args)
        self.assertEqual(again['config']['robot_count'], 100)
        self.assertEqual(again['robot']['battery'], 75.)
        self.assertTrue(again['metadata'])
        self.assertNotEqual(again['similar'][0]['real_embedding'][0], 9999)

    def test_new_creation_version_or_source_marker_invalidates_static_fragment(self):
        self.store.full_query(*self.args)
        self.mirror.created_at = '2026-10-28T10:00:00Z'
        self.mirror.returned_header = {'metadata_json': json.dumps({'changed': 'creation'})}
        result = self.store.full_query(*self.args)
        self.assertEqual(result['metadata'], {'changed': 'creation'})
        self.assertFalse(self.store.latest_metrics['fragment_cache_hit'])
        self.assertEqual(len(self.store._fragment_headers), 1)
        self.mirror.returned_header = {'real_vector_kind': 'new-version',
                                      'metadata_json': json.dumps({'changed': 'marker'})}
        result = self.store.full_query(*self.args)
        self.assertEqual(result['metadata'], {'changed': 'marker'})
        self.assertFalse(self.store.latest_metrics['fragment_cache_hit'])
        self.assertEqual(len(self.store._fragment_headers), 1)

    def test_same_created_at_different_runs_never_share_fragments(self):
        a = self.store.full_query(*self.args)
        b = self.store.full_query('other-run', *self.args[1:])
        again = self.store.full_query(*self.args)
        self.assertEqual(again['metadata'], a['metadata'])
        self.assertEqual(b['metadata'], {'run': 'other-run'})
        self.assertEqual(len(self.store._fragment_headers), 2)

    def test_disabling_header_cache_disables_fragments_and_matches_full_service(self):
        store = FragmentQueryService(self.mirror, header_cache_size=0)
        first, second = store.full_query(*self.args), store.full_query(*self.args)
        self.assertEqual(first, second)
        self.assertFalse(store.latest_metrics['fragment_encoding_enabled'])
        self.assertFalse(store.latest_metrics['metadata_cache_hit'])
        self.assertEqual(self.mirror.headers_sent, 2)
        self.assertEqual(len(store._fragment_headers), 0)

    def test_missing_immutable_version_disables_fragments(self):
        self.mirror.created_at = None
        self.store.full_query(*self.args)
        self.store.full_query(*self.args)
        self.assertFalse(self.store.latest_metrics['fragment_encoding_enabled'])
        self.assertEqual(len(self.store._fragment_headers), 0)

    def test_clear_removes_both_header_and_fragment_caches(self):
        self.store.full_query(*self.args)
        self.store.full_query(*self.args)
        self.assertTrue(self.store.latest_metrics['fragment_cache_hit'])
        self.store.clear_header_cache()
        self.assertEqual(len(self.store._headers), 0)
        self.assertEqual(len(self.store._fragment_headers), 0)
        self.store.full_query(*self.args)
        self.assertFalse(self.store.latest_metrics['fragment_cache_hit'])
        self.assertFalse(self.store.latest_metrics['metadata_cache_hit'])

    def test_lru_has_the_same_bounded_capacity_and_no_dynamic_response_cache(self):
        store = FragmentQueryService(self.mirror, header_cache_size=1)
        store.full_query(*self.args)
        store.full_query('other-run', *self.args[1:])
        self.assertEqual(len(store._fragment_headers), 1)
        self.mirror.head_seq = 20
        result = store.full_query(*self.args)
        self.assertEqual(result['graph_committed_seq'], 20)
        self.assertEqual(result['cooperation']['graph_committed_seq'], 20)
        self.assertEqual(len(store._fragment_headers), 1)
        self.assertFalse(store.latest_metrics['metadata_cache_hit'])

    def test_cold_nonfinite_metadata_or_config_rejected_and_cannot_poison_warm_cache(self):
        for field in ['metadata_json', 'config_json']:
            for value in [math.nan, math.inf, -math.inf]:
                with self.subTest(field=field, value=value):
                    mirror = HeaderMirror()
                    mirror.returned_header = {field: json.dumps({'bad': [value]})}
                    store = FragmentQueryService(mirror)
                    for _ in range(2):
                        with self.assertRaises(ValueError):
                            store.full_query(*self.args)
                    self.assertEqual(len(store._fragment_headers), 0)

    def test_nonfinite_dynamic_values_rejected_even_when_static_fragments_are_warm(self):
        self.store.full_query(*self.args)
        for value in [math.nan, math.inf, -math.inf]:
            with self.subTest(value=value):
                self.mirror.returned_header = {'robot_payload': json.dumps({'bad': [value]})}
                with self.assertRaises(ValueError):
                    self.store.full_query(*self.args)

    def test_ann_fallback_pagination_and_full_fields_are_unchanged(self):
        self.mirror.empty_initial = self.mirror.empty_expanded = True
        result = self.store.full_query(*self.args, goods_limit=1, cooperation_limit=1,
            goods_after=0, cooperation_after=0)
        self.assertEqual(result['bolt_statements'], 3)
        self.assertEqual(result['retrieval_path'], 'exact_after_underfilled1000')
        self.assertEqual(result['goods_history']['rows'][0]['location_id'], 'S1')
        self.assertEqual(result['cooperation']['rows'][0]['event']['kind'], 'cargo_handoff')
        validate_result(result, self.mirror.oracle, dict(seq=7, robot_id=1,
            goods_id='G-1', focus_task_id='T-1'))

    def test_business_apis_still_use_original_full_json_path_and_report_no_fragment_hit(self):
        self.store.full_query(*self.args)
        robots = self.store.robot_context('actual-test', 7, [2, 1])
        self.assertEqual([r['robot_id'] for r in robots['robots']], [1, 2])
        self.assertFalse(self.store.latest_metrics['fragment_encoding_enabled'])
        context = self.store.cooperation_context('actual-test', 7, 'T-1', 'G-1')
        self.assertEqual(context['request_cursors'], {'goods_after': 0, 'cooperation_after': 0})
        self.assertFalse(self.store.latest_metrics['fragment_encoding_enabled'])

    def test_100_concurrent_callers_return_independent_correct_full_results(self):
        def query(robot):
            args = ('actual-test', 7, robot, f'G-{robot}', f'T-{robot}')
            first = self.store.full_query(*args)
            second = self.store.full_query(*args)
            validate_result(second, self.mirror.oracle, dict(seq=7, robot_id=robot,
                goods_id=f'G-{robot}', focus_task_id=f'T-{robot}'))
            first['metadata']['domain'] = str(robot)
            self.assertEqual(second['metadata']['domain'], 'warehouse-wide-inbound-outbound')
            return second, self.store.latest_metrics
        with ThreadPoolExecutor(max_workers=16) as executor:
            rows = list(executor.map(query, range(1, 101)))
        self.assertEqual({r['robot_id'] for r, _ in rows}, set(range(1, 101)))
        self.assertTrue(any(metrics['fragment_cache_hit'] for _, metrics in rows))
        self.assertEqual(len(self.store._fragment_headers), 1)


if __name__ == '__main__':
    unittest.main()
