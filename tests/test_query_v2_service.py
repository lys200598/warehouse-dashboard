from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
import json
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from backend.real_scale.benchmark import validate_result
from backend.real_scale.store import RealStore
from backend.query_v2.service import (QueryService, IDENTITY_FIELDS,
    build_full_query, ROBOT_CONTEXT_QUERY, COOPERATION_CONTEXT_QUERY)
from tests.test_real_scale_query import FakeMirror, FakeOracle


class HeaderMirror(FakeMirror):
    def __init__(self):
        super().__init__()
        self.created_at = '2026-10-28T00:00:00.000000001Z'
        self.header_versions, self.headers_sent = {}, 0
        self.head_seq = 12
        self.no_rows = False
        self.returned_rows = None
        self.returned_header = None

    def query(self, query, params, timeout=5):
        if self.no_rows:
            self.queries.append((query, deepcopy(params), timeout))
            return []
        if query == ROBOT_CONTEXT_QUERY:
            self.queries.append((query, deepcopy(params), timeout))
            if self.returned_rows is not None:
                return deepcopy(self.returned_rows)
            return [dict(head=self.head_seq, batch_checksum=self.oracle.batch_checksum(params['seq']),
                robot_id=r, robot_payload=json.dumps(self.oracle.state(params['seq'], r)),
                robot_sha256='sha', task_payload=json.dumps(self.oracle.task(params['seq'], f'T-{r}')))
                for r in params['robots']]
        if query == COOPERATION_CONTEXT_QUERY:
            self.queries.append((query, deepcopy(params), timeout))
            if self.returned_rows is not None:
                return deepcopy(self.returned_rows)
            seq = params['seq']
            goods = self.oracle.goods_history(seq, params['goods'])
            coop = self.oracle.cooperation(seq, params['focus_task'])
            now = goods['current_as_of']
            return [dict(head=self.head_seq, batch_checksum=self.oracle.batch_checksum(seq),
                focus_task_payload=json.dumps(self.oracle.task(seq, params['focus_task'])),
                goods_location=now['location_id'], goods_event_seq=now['event_seq'],
                goods_sim_time=now['sim_time'], goods_rows=[r for r in goods['rows']
                    if r['event_seq']>params['goods_after']][:params['goods_limit']],
                coop_rows=[dict(event_seq=r['event_seq'], seq=r['seq'], payload=json.dumps(r['event']))
                    for r in coop['rows'] if r['event_seq']>params['coop_after']][:params['coop_limit']])]
        rows = super().query(query, params, timeout)
        if 'AS robot_payload' in query:
            for row in rows:
                row['head'] = self.head_seq
                if 'run_created_at' not in query:
                    continue
                run = params['run']
                row['run_created_at'] = self.header_versions.get(run, self.created_at)
                if run != self.oracle.run_id:
                    row['real_source_run'] = run
                    row['metadata_json'] = json.dumps({'run': run})
                if self.returned_header:
                    row.update(deepcopy(self.returned_header))
                known = params.get('known_created_at') == row['run_created_at'] and all(
                    params.get('known_'+f)==row[f] for f in IDENTITY_FIELDS)
                if known:
                    row['metadata_json'] = row['config_json'] = None
                else:
                    self.headers_sent += 1
        return rows


class QueryV2ServiceTests(unittest.TestCase):
    def setUp(self):
        self.mirror = HeaderMirror()
        self.service = QueryService(self.mirror)
        self.args = ('actual-test', 7, 1, 'G-1', 'T-1')

    def test_full_output_identical_to_frozen_ordered_contract_cold_and_warm(self):
        expected = RealStore(self.mirror).full_query(*self.args, mode='ordered')
        for _ in range(2):
            got = self.service.full_query(*self.args)
            self.assertEqual(got, expected)
            validate_result(got, self.mirror.oracle, dict(seq=7, robot_id=1,
                goods_id='G-1', focus_task_id='T-1'))
        self.assertTrue(self.service.latest_metrics['metadata_cache_hit'])
        self.assertEqual(self.service.latest_metrics['metadata_wire_chars'], 0)

    def test_warm_header_not_transmitted_and_dynamic_head_never_cached(self):
        first = self.service.full_query(*self.args)
        self.mirror.head_seq = 15
        second = self.service.full_query(*self.args)
        self.assertEqual(first['graph_committed_seq'], 12)
        self.assertEqual(second['graph_committed_seq'], 15)
        self.assertEqual(second['goods_history']['graph_committed_seq'], 15)
        self.assertEqual(self.mirror.headers_sent, 1)
        self.assertEqual(len(self.mirror.queries), 2)

    def test_returned_mutations_do_not_change_cached_header_or_another_result(self):
        a = self.service.full_query(*self.args)
        a['metadata']['domain'] = 'corrupt'
        a['config']['robot_count'] = 1
        a['real_identity']['real_source_kind'] = 'wrong'
        b = self.service.full_query(*self.args)
        self.assertEqual(b['metadata']['domain'], 'warehouse-wide-inbound-outbound')
        self.assertEqual(b['config']['robot_count'], 100)
        self.assertEqual(b['real_identity']['real_source_kind'], 'actual_runtime_committed_state')

    def test_same_run_new_created_version_refreshes_header(self):
        self.service.full_query(*self.args)
        self.mirror.created_at = '2026-10-28T00:00:01Z'
        self.service.full_query(*self.args)
        self.assertEqual(self.mirror.headers_sent, 2)
        self.assertFalse(self.service.latest_metrics['metadata_cache_hit'])

    def test_each_source_identity_marker_is_part_of_exact_cache_version(self):
        first = self.service.full_query(*self.args)
        for field, value in [('real_scale_version', 'new'), ('real_source_kind', 'new'),
                ('real_source_run', 'other'), ('real_dimensions', 16), ('real_vector_kind', 'new')]:
            with self.subTest(field=field):
                self.service.clear_header_cache()
                self.mirror.returned_header = None
                self.service.full_query(*self.args)
                self.mirror.returned_header = {field: value}
                result = self.service.full_query(*self.args)
                self.assertFalse(self.service.latest_metrics['metadata_cache_hit'])
                self.assertEqual(result['real_identity'][field], value)

    def test_cache_is_scoped_by_run_id_not_only_identical_created_at(self):
        self.service.full_query(*self.args)
        other = self.service.full_query('another-run', *self.args[1:])
        self.assertEqual(other['metadata'], {'run': 'another-run'})
        again = self.service.full_query(*self.args)
        self.assertEqual(again['metadata']['domain'], 'warehouse-wide-inbound-outbound')

    def test_missing_created_at_disables_cache_and_lru_is_bounded(self):
        self.mirror.created_at = None
        self.service.full_query(*self.args)
        self.service.full_query(*self.args)
        self.assertFalse(self.service.latest_metrics['metadata_cache_hit'])
        self.assertEqual(self.service._headers, {})
        self.mirror.created_at = 'version'
        service = QueryService(self.mirror, header_cache_size=1)
        service.full_query(*self.args)
        service.full_query('other-run', *self.args[1:])
        self.assertEqual(list(service._headers), ['other-run'])

    def test_cache_can_be_disabled_without_changing_full_result(self):
        service = QueryService(self.mirror, header_cache_size=0)
        a, b = service.full_query(*self.args), service.full_query(*self.args)
        self.assertEqual(a, b)
        self.assertFalse(service.latest_metrics['metadata_cache_hit'])
        self.assertEqual(self.mirror.headers_sent, 2)

    def test_ann_fallback_and_vector_validation_are_preserved(self):
        self.mirror.empty_initial = self.mirror.empty_expanded = True
        result = self.service.full_query(*self.args)
        self.assertEqual(result['bolt_statements'], 3)
        self.assertEqual(result['retrieval_path'], 'exact_after_underfilled1000')
        self.assertEqual(self.service.latest_metrics['bolt_statements'], 3)
        self.mirror.bad_vector = True
        with self.assertRaisesRegex(ValueError, 'query vector'):
            self.service.full_query(*self.args)

    def test_paging_parameters_and_full_json_roundtrip_are_preserved(self):
        value = self.service.full_query(*self.args, goods_limit=3,
            cooperation_limit=7, goods_after=4, cooperation_after=8)
        params = self.mirror.queries[-1][1]
        self.assertEqual((params['goods_limit'], params['coop_limit'], params['goods_after'],
            params['coop_after']), (4, 8, 4, 8))
        self.assertEqual(value, json.loads(json.dumps(value, allow_nan=False)))
        metrics = self.service.latest_metrics
        self.assertGreater(metrics['returned_json_chars'], 0)
        for field in ['query_bolt_ms', 'decode_assemble_ms', 'json_roundtrip_ms']:
            self.assertGreaterEqual(metrics[field], 0)

    def test_cold_and_warm_100_callers_do_not_share_mutable_results(self):
        def query(robot):
            value = self.service.full_query('actual-test', 7, robot, f'G-{robot}', f'T-{robot}')
            value['config']['robot_count'] = robot
            return value, self.service.latest_metrics
        with ThreadPoolExecutor(max_workers=16) as executor:
            values = list(executor.map(query, range(1, 101)))
        self.assertEqual({v['robot_id'] for v, _ in values}, set(range(1, 101)))
        self.assertEqual(self.service.full_query(*self.args)['config']['robot_count'], 100)
        self.assertTrue(any(metrics['metadata_cache_hit'] for _, metrics in values))

    def test_invalid_full_arguments_do_not_reach_database(self):
        for index, wrong in [(0, ''), (0, 'bad\nrun'), (1, True), (1, 0), (2, False),
                (2, 101), (3, None), (4, 'bad/task')]:
            args = list(self.args); args[index] = wrong
            with self.assertRaises(ValueError):
                self.service.full_query(*args)
        for kwargs in [{'mode': 'unknown'}, {'goods_limit': True}, {'cooperation_limit': 1001},
                {'goods_after': -1}, {'cooperation_after': False}]:
            with self.assertRaises(ValueError):
                self.service.full_query(*self.args, **kwargs)
        self.assertEqual(self.mirror.queries, [])

    def test_missing_point_and_missing_cache_entry_fail_closed(self):
        self.mirror.no_rows = True
        with self.assertRaisesRegex(ValueError, 'point absent'):
            self.service.full_query(*self.args)
        row = dict(real_scale_version='v', real_source_kind='s', real_source_run='r',
            real_dimensions=32, real_vector_kind='k', run_created_at='version',
            metadata_json=None, config_json=None)
        self.service._begin()
        with self.assertRaisesRegex(ValueError, 'without exact cached'):
            self.service._header('run', row, None)

    def test_robot_context_is_one_fixed_cutoff_statement_and_complete_batch(self):
        value = self.service.robot_context('actual-test', 7, list(range(100, 0, -1)))
        self.assertEqual([r['robot_id'] for r in value['robots']], list(range(1, 101)))
        self.assertEqual(value['robots'][0]['robot'], self.mirror.oracle.state(7, 1))
        self.assertEqual(value['robots'][0]['task'], self.mirror.oracle.task(7, 'T-1'))
        self.assertEqual(value['graph_committed_seq'], 12)
        self.assertEqual(len(self.mirror.queries), 1)
        query, params, _ = self.mirror.queries[0]
        self.assertIn('p.seq=$seq', query)
        self.assertIn('ts.seq<=$seq', query)
        self.assertNotIn('db.index.vector', query)
        self.assertEqual(params['seq'], 7)

    def test_robot_context_rejects_invalid_ids_partial_duplicate_and_mixed_watermark(self):
        for ids in [[], [True], [0], [101], [1, 1], set([1]), '1']:
            with self.assertRaises(ValueError):
                self.service.robot_context('actual-test', 7, ids)
        value = self.service.robot_context('actual-test', 7, [1, 2])
        valid = [dict(head=12, batch_checksum='batch', robot_id=r['robot_id'],
            robot_payload=json.dumps(r['robot']), robot_sha256='sha',
            task_payload=json.dumps(r['task'])) for r in value['robots']]
        for rows in [valid[:1], [valid[0], valid[0]],
                     [valid[0], {**valid[1], 'head': 13}]]:
            self.mirror.returned_rows = rows
            with self.assertRaises(ValueError):
                self.service.robot_context('actual-test', 7, [1, 2])

    def test_cooperation_context_returns_same_history_as_full_contract(self):
        full = self.service.full_query(*self.args)
        lean = self.service.cooperation_context('actual-test', 7, 'T-1', 'G-1')
        for field in ['run_id', 'seq', 'graph_committed_seq', 'batch_checksum',
                      'focus_task', 'goods_history', 'cooperation']:
            self.assertEqual(lean[field], full[field])
        self.assertNotIn('metadata', lean)
        self.assertNotIn('similar', lean)
        self.assertEqual(lean['request_cursors'], {'goods_after': 0, 'cooperation_after': 0})
        self.assertEqual(self.service.latest_metrics['bolt_statements'], 1)

    def test_cooperation_context_keeps_temporal_boundaries_and_cursors(self):
        before = self.service.cooperation_context('actual-test', 4, 'T-1', 'G-1')
        at = self.service.cooperation_context('actual-test', 5, 'T-1', 'G-1')
        after_cursor = self.service.cooperation_context('actual-test', 7, 'T-1', 'G-1',
            goods_after=1, cooperation_after=20)
        self.assertEqual(before['cooperation']['rows'], [])
        self.assertEqual(at['cooperation']['rows'][0]['event']['kind'], 'cargo_handoff')
        self.assertEqual(after_cursor['cooperation']['rows'], [])
        self.assertEqual(after_cursor['goods_history']['rows'], [])
        self.assertIsNotNone(after_cursor['goods_history']['current_as_of'])
        self.assertEqual(after_cursor['request_cursors'], {'goods_after': 1, 'cooperation_after': 20})

    def test_cooperation_context_rejects_mismatched_business_identity(self):
        with self.assertRaisesRegex(ValueError, 'mismatched task/goods'):
            self.service.cooperation_context('actual-test', 7, 'T-1', 'G-2')
        with self.assertRaises(ValueError):
            self.service.cooperation_context('actual-test', True, 'T-1', 'G-1')

    def test_read_only_from_env_uses_no_schema_and_closes_on_setup_error(self):
        driver = SimpleNamespace(verify_connectivity=lambda: None, close=lambda: None)
        received = {}
        def connect(uri, **kwargs):
            received.update(uri=uri, **kwargs)
            return driver
        module = SimpleNamespace(GraphDatabase=SimpleNamespace(driver=connect))
        with patch.dict('sys.modules', {'neo4j': module}), patch.dict('os.environ', {
                'NEO4J_URI': 'bolt://test:9999', 'NEO4J_PASSWORD': 'test-only'}), \
                patch('backend.knowledge_graph.Neo4jMirror.ensure_schema') as schema:
            service = QueryService.from_env(pool_size=25)
        schema.assert_not_called()
        self.assertEqual(received['max_connection_pool_size'], 25)
        self.assertEqual(received['max_transaction_retry_time'], 0)
        with patch.dict('sys.modules', {'neo4j': module}), patch.dict('os.environ', {
                'NEO4J_URI': 'bolt://test:9999', 'NEO4J_PASSWORD': 'test-only'}), \
                patch.object(driver, 'close') as close:
            with self.assertRaises(ValueError):
                QueryService.from_env(header_cache_size=-1)
            close.assert_called_once()

    def test_build_query_has_no_write_and_retains_original_full_work(self):
        query = build_full_query('ordered')
        for field in ['metadata_json', 'config_json', 'run_created_at', 'history', 'goods_rows',
                      'coop_rows', 'similar', 'queryNodes', 'batch_checksum']:
            self.assertIn(field, query)
        for field in IDENTITY_FIELDS:
            self.assertIn('r.'+field+'=$known_'+field, query)
        for bad in ['CREATE ', 'MERGE ', 'DELETE ', ' SET ']:
            self.assertNotIn(bad, query)


if __name__ == '__main__':
    unittest.main()
