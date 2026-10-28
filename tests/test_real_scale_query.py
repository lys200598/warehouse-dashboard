from copy import deepcopy
import json
import math
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from backend.knowledge import digest
from backend.real_scale import real_vector
from backend.real_scale.store import RealStore, build_query, VECTOR_INDEX
from backend.real_scale.benchmark import validate_result, normalized_score


class FakeOracle:
    run_id = 'actual-test'
    prefix = 10
    min_task_seq = 2
    identity = {'run_id': run_id, 'prefix': prefix, 'prefix_sha256': 'source-prefix',
                'metadata': {'domain': 'warehouse-wide-inbound-outbound'}, 'config': {'robot_count': 100}}

    def state(self, seq, robot):
        if not 1 <= seq <= self.prefix or not 1 <= robot <= 100:
            raise ValueError('outside independent source')
        return {'robot_id': robot, 'x': float(robot), 'y': float(seq), 'battery': 75.,
                'task_id': f'T-{robot}', 'stage': 'to_source', 'carrying': False, 'cargo_id': None}

    def task(self, seq, task_id):
        return None if task_id is None else {'task_id': task_id, 'goods_id': 'G-'+task_id.split('-')[-1],
            'status': 'assigned', 'due_second': 1000, 'source_id': 'S1', 'target_id': 'OUT-1'}

    def batch_checksum(self, seq):
        return digest(['batch', seq])

    def vector(self, seq, robot):
        return real_vector(self.state(seq, robot), seq, robot)

    def check_source_identity(self, metadata, config):
        if digest(metadata) != digest(self.identity['metadata']) or digest(config) != digest(self.identity['config']):
            raise ValueError('immutable source metadata/config mismatch')

    def history(self, seq, robot, limit=5):
        return [{'key': f'{self.run_id}:{s}:{robot}', 'seq': s, 'robot_id': robot,
                 'payload': json.dumps(self.state(s, robot)),
                 'real_payload_sha256': digest(self.state(s, robot))}
                for s in range(seq, max(0, seq-limit), -1)]

    def focus(self, seq, robot=None):
        robot = robot or 1
        return {'task_id': f'T-{robot}', 'goods_id': f'G-{robot}'}

    def handoff_points(self, limit=100):
        return [dict(seq=5, before_seq=4, event_seq=20, task_id='T-1', goods_id='G-1',
                     donor_robot_id=1, receiver_robot_id=2)][:limit]

    def goods_history(self, seq, goods_id, after_event_seq=0, limit=200):
        rows = [dict(event_seq=1, seq=1, sim_time=0, kind='initial_stock', location_id='S1',
                     event_key=self.run_id+':event:1')]
        rows = [r for r in rows if r['event_seq']>after_event_seq]
        more = len(rows)>limit
        rows = rows[:limit]
        return {'run_id': self.run_id, 'goods_id': goods_id, 'seq': seq,
                'graph_committed_seq': self.prefix, 'current_as_of':
                {'location_id': 'S1', 'event_seq': 1, 'sim_time': 0}, 'rows': rows,
                'has_more': more, 'next_after_event_seq': rows[-1]['event_seq'] if more else None}

    def cooperation(self, seq, task_id=None, limit=100, after_event_seq=0):
        rows = []
        if seq>=5 and task_id=='T-1' and after_event_seq<20:
            rows = [dict(event_seq=20, seq=5, event=dict(kind='cargo_handoff', task_id='T-1',
                goods_id='G-1', donor_robot_id=1, receiver_robot_id=2, sim_time=20))]
        more = len(rows)>limit
        rows = rows[:limit]
        return {'run_id': self.run_id, 'task_id': task_id, 'seq': seq,
                'graph_committed_seq': self.prefix, 'rows': rows,
                'has_more': more, 'next_after_event_seq': rows[-1]['event_seq'] if more else None}


class FakeMirror:
    def __init__(self, oracle=None):
        self.oracle = oracle or FakeOracle()
        self.queries = []
        self.empty_initial = self.empty_expanded = False
        self.bad_vector = False
        self.database = 'neo4j'

    def similar(self, seq, robot, query):
        ids = [1+(robot-1+i)%100 for i in range(5)]
        rows = [dict(key=f'{self.oracle.run_id}:{seq}:{i}', run_id=self.oracle.run_id, seq=seq, robot_id=i,
            real_embedding=self.oracle.vector(seq, i), real_payload_sha256=digest(self.oracle.state(seq, i)),
            score=normalized_score(query, self.oracle.vector(seq, i))) for i in ids]
        return sorted(rows, key=lambda r: r['score'], reverse=True)

    def query(self, query, params, timeout=5):
        self.queries.append((query, deepcopy(params), timeout))
        o = self.oracle
        if 'AS robot_payload' in query:
            seq, robot = params['seq'], params['robot']
            self.last_robot = robot
            goods = o.goods_history(seq, params['goods'])
            cooperation = o.cooperation(seq, params['focus_task'])
            vector = o.vector(seq, robot)
            current = goods['current_as_of']
            return [dict(head=o.prefix+2, metadata_json=json.dumps(o.identity['metadata']),
                config_json=json.dumps(o.identity['config']), real_scale_version='warehouse-real-scale-v1',
                real_source_kind='actual_runtime_committed_state', real_source_run=o.run_id,
                real_dimensions=32, real_vector_kind='32d_structured_physical_time_identity_features_not_semantic',
                batch_checksum=o.batch_checksum(seq), robot_payload=json.dumps(o.state(seq, robot)),
                robot_sha256=digest(o.state(seq, robot)), query_vector=vector[:2] if self.bad_vector else vector,
                robot_task_payload=json.dumps(o.task(seq, o.state(seq, robot)['task_id'])),
                focus_task_payload=json.dumps(o.task(seq, params['focus_task'])), history=o.history(seq, robot),
                goods_location=current['location_id'], goods_event_seq=current['event_seq'],
                goods_sim_time=current['sim_time'], goods_rows=goods['rows'],
                coop_rows=[dict(event_seq=r['event_seq'], seq=r['seq'], payload=json.dumps(r['event']))
                           for r in cooperation['rows']],
                similar=[] if self.empty_initial else self.similar(seq, robot, vector))]
        if 'db.index.vector.queryNodes' in query or 'vector.similarity.cosine' in query:
            if 'queryNodes' in query and self.empty_expanded:
                return []
            robot = self.last_robot
            return self.similar(params['seq'], robot, params['vector'])
        if 'payload_hashes' in query:
            return [dict(states=params['seq']*100, vectors=params['seq']*100,
                payload_hashes=params['seq']*100, robots=100, first_seq=1, last_seq=params['seq'],
                min_dimensions=32, max_dimensions=32)]
        if 'derived_links' in query:
            return [dict(derived_links=0, archive_states=0)]
        if query.startswith('SHOW INDEXES'):
            return [dict(name=VECTOR_INDEX, state='ONLINE', type='VECTOR', labelsOrTypes=['WHRealState'],
                properties=['real_embedding'], options={'indexConfig': {'vector.dimensions': 32,
                'vector.similarity_function': 'cosine'}})]
        if 'dbms.components' in query:
            return [dict(name='test-double', versions=['test'], edition='test')]
        if 'real_last_seq' in query:
            return [dict(seq=o.prefix+2, tick=100, real_last_seq=o.prefix+2, real_last_batch_sha256='head')]
        raise AssertionError('unexpected query: '+query[:80])

    def close(self):
        pass


class QueryContractTests(unittest.TestCase):
    def setUp(self):
        self.oracle = FakeOracle()
        self.mirror = FakeMirror(self.oracle)
        self.store = RealStore(self.mirror)
        self.case = dict(seq=7, robot_id=1, goods_id='G-1', focus_task_id='T-1')

    def fetch(self, mode='baseline'):
        c = self.case
        return self.store.full_query(self.oracle.run_id, c['seq'], c['robot_id'], c['goods_id'], c['focus_task_id'], mode=mode)

    def test_baseline_and_ordered_return_same_complete_business_fields(self):
        a, b = self.fetch(), self.fetch('ordered')
        validate_result(a, self.oracle, self.case)
        validate_result(b, self.oracle, self.case)
        a.pop('query_mode'); b.pop('query_mode')
        self.assertEqual(a, b)
        self.assertTrue(a['task'] and a['focus_task'] and a['goods_history']['rows'] and a['cooperation']['rows'])

    def test_both_queries_keep_vector_graph_temporal_cutoff_and_parameterized_ids(self):
        for mode in ['baseline', 'ordered']:
            query = build_query(mode)
            for part in ['db.index.vector.queryNodes', 'real_embedding', 'WHRealState',
                         'node.run_id=$run', 'node.seq<=$seq', 'goods_rows', 'coop_rows',
                         'metadata_json', 'config_json', 'batch_checksum', 'robot_task_payload',
                         'history', 'focus_task_payload']:
                self.assertIn(part, query)
            self.assertNotIn('WHScaleArchive', query)
        self.assertIn('USING INDEX h:WHAgentState(run_id,robot_id,seq)', build_query('ordered'))
        self.assertIn('<-[:OF_ROBOT]', build_query('baseline'))

    def test_ordered_includes_all_fixed_composite_prefixes_without_changing_baseline(self):
        query = build_query('ordered')
        for order in ['ts.run_id DESC,ts.task_id DESC,ts.seq DESC',
                      'h.run_id DESC,h.robot_id DESC,h.seq DESC',
                      'ft.run_id DESC,ft.task_id DESC,ft.seq DESC',
                      'g.run_id DESC,g.goods_id DESC,g.event_seq DESC',
                      'g.run_id,g.goods_id,g.event_seq LIMIT',
                      'e.run_id,e.task_id,e.event_seq LIMIT']:
            self.assertIn('ORDER BY '+order, query)
            self.assertNotIn('ORDER BY '+order, build_query('baseline'))
        # The relationship baseline must remain byte-identical to the v3 pilot.
        import hashlib
        self.assertEqual(hashlib.sha256(build_query('baseline').encode()).hexdigest(),
                         '43bdd1fc2acfca7dc5541ce68eb2b05e55a3f0bc930a3202834b094c8f0a3127')

    def test_underfilled_ann_expands_then_exact_and_records_real_path(self):
        self.mirror.empty_initial = self.mirror.empty_expanded = True
        result = self.fetch('ordered')
        self.assertEqual(result['bolt_statements'], 3)
        self.assertEqual(result['retrieval_path'], 'exact_after_underfilled1000')
        validate_result(result, self.oracle, self.case)

    def test_source_vector_is_checked_before_fallback(self):
        self.mirror.bad_vector = True
        with self.assertRaisesRegex(ValueError, 'query vector'):
            self.fetch()
        self.assertEqual(len(self.mirror.queries), 1)

    def test_invalid_requests_never_reach_database(self):
        base = [self.oracle.run_id, 7, 1, 'G-1', 'T-1']
        for index, value in [(0, 'bad\nrun'), (1, True), (1, 0), (2, 101), (3, None), (4, '')]:
            args = list(base); args[index] = value
            with self.assertRaises(ValueError):
                self.store.full_query(*args)
        with self.assertRaises(ValueError):
            self.store.full_query(*base, mode='raw-cypher')
        self.assertEqual(self.mirror.queries, [])

    def test_read_only_connection_never_bootstraps_schema(self):
        driver = SimpleNamespace(verified=False)
        def verify():
            driver.verified = True
        driver.verify_connectivity = verify
        received = {}
        def connect(uri, **parameters):
            received.update(uri=uri, **parameters)
            return driver
        module = SimpleNamespace(GraphDatabase=SimpleNamespace(driver=connect))
        with patch.dict('sys.modules', {'neo4j': module}), patch.dict('os.environ', {
                'NEO4J_URI': 'bolt://127.0.0.1:9999', 'NEO4J_PASSWORD': 'test-only',
                'NEO4J_DATABASE': 'real-test'}), patch('backend.knowledge_graph.Neo4jMirror.ensure_schema') as schema:
            store = RealStore.from_env()
        schema.assert_not_called()
        self.assertTrue(driver.verified)
        self.assertEqual(store.mirror.database, 'real-test')
        self.assertEqual(received['max_connection_pool_size'], 120)
        self.assertEqual(received['max_transaction_retry_time'], 0)

    def test_oracle_rejects_each_component_corruption(self):
        original = self.fetch('ordered')
        mutations = [
            lambda r: r['task'].update(due_second=99999),
            lambda r: r['focus_task'].update(goods_id='wrong'),
            lambda r: r['robot'].update(carrying=True),
            lambda r: r['history'][0].update(payload='{}'),
            lambda r: r['query_vector'].__setitem__(0, float('nan')),
            lambda r: r['similar'][0].update(seq=8),
            lambda r: r['similar'][0].update(real_payload_sha256='wrong'),
            lambda r: r['goods_history']['current_as_of'].update(location_id='ROBOT-99'),
            lambda r: r['cooperation']['rows'][0]['event'].update(receiver_robot_id=88),
            lambda r: r['metadata'].update(domain='archive'),
            lambda r: r.update(batch_checksum='wrong'),
            lambda r: r['real_identity'].update(real_source_kind='resampled'),
        ]
        for change in mutations:
            changed = deepcopy(original); change(changed)
            with self.assertRaises((ValueError, KeyError)):
                validate_result(changed, self.oracle, self.case)

    def test_validation_reuses_each_ann_source_state_without_identity_copies(self):
        result = self.fetch('ordered')
        expected_history = self.oracle.history(self.case['seq'], self.case['robot_id'])
        original_state = self.oracle.state
        with patch.object(self.oracle, 'state', wraps=original_state) as state, \
                patch.object(self.oracle, 'history', return_value=expected_history), \
                patch.object(self.oracle, 'vector', side_effect=AssertionError('duplicate state parse')), \
                patch.object(self.oracle, 'check_source_identity', wraps=self.oracle.check_source_identity) as identity_check:
            validate_result(result, self.oracle, self.case)
        self.assertEqual(state.call_count, 6)  # One point plus each of five independently verified ANN states.
        identity_check.assert_called_once_with(result['metadata'], result['config'])

    def test_type_approximate_payload_tampering_cannot_reuse_stored_source_hash(self):
        original = self.fetch('ordered')
        def bad_history(result):
            payload = json.loads(result['history'][0]['payload'])
            payload['carrying'] = 0
            result['history'][0]['payload'] = json.dumps(payload)
        mutations = [lambda r: r['robot'].update(carrying=0), bad_history,
                     lambda r: r['task'].update(due_second=1000.0),
                     lambda r: r['focus_task'].update(due_second=1000.0),
                     lambda r: r['goods_history'].update(has_more=0),
                     lambda r: r['cooperation']['rows'][0]['event'].update(donor_robot_id=True)]
        for change in mutations:
            value = deepcopy(original)
            change(value)
            with self.assertRaises(ValueError):
                validate_result(value, self.oracle, self.case)

    def test_normalized_goods_time_integer_and_sqlite_real_are_equal_but_boolean_is_not(self):
        result = self.fetch('ordered')
        result['goods_history']['current_as_of']['sim_time'] = 0.0
        result['goods_history']['rows'][0]['sim_time'] = 0.0
        validate_result(result, self.oracle, self.case)
        result['goods_history']['current_as_of']['sim_time'] = False
        with self.assertRaisesRegex(ValueError, 'goods_history'):
            validate_result(result, self.oracle, self.case)

    def test_wrong_vector_index_is_visible_in_count_contract(self):
        counts = self.store.counts(self.oracle.run_id, self.oracle.prefix)
        self.assertEqual(counts['states'], 1000)
        self.assertEqual(counts['vector_indexes'][0]['properties'], ['real_embedding'])

    def test_full_api_validates_against_real_streaming_sqlite_handoff_oracle(self):
        from backend.real_scale.oracle import RealOracle
        from tests.test_real_scale_oracle import make_journal
        root = Path(__file__).resolve().parents[1]/'data/local-check'
        with tempfile.TemporaryDirectory(dir=root) as temporary:
            source = Path(temporary)/'journal.sqlite'
            make_journal(source)
            with RealOracle(source, 4) as oracle:
                store = RealStore(FakeMirror(oracle))
                for seq, robot in [(2, 1), (3, 2), (4, 99)]:
                    focus = oracle.focus(seq, robot)
                    case = dict(seq=seq, robot_id=robot, focus_task_id=focus['task_id'], goods_id=focus['goods_id'])
                    for mode in ['baseline', 'ordered']:
                        value = store.full_query(oracle.run_id, seq, robot, focus['goods_id'], focus['task_id'], mode=mode)
                        validate_result(value, oracle, case)
                    if seq==3:
                        self.assertEqual(value['goods_history']['current_as_of']['location_id'], 'ROBOT-2')
                        self.assertTrue(any(e['event']['kind']=='cargo_handoff' for e in value['cooperation']['rows']))


if __name__ == '__main__':
    unittest.main()
