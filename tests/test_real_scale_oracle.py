from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
import hashlib
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch
import zlib

from backend.knowledge import JournalKnowledgeStore, canonical, digest
from backend.real_scale import VERSION, SOURCE_KIND, VECTOR_KIND, real_vector
from backend.real_scale.oracle import RealOracle, _observe_event


ROOT = Path(__file__).resolve().parents[1]
SCENE = json.loads((ROOT/'data/layouts/warehouse100-wide-v2.json').read_text(encoding='utf-8'))
SLOT = 'R01-L1-C1-F1'
RUN = 'actual-oracle-test'


def checkpoint(seq):
    location = {1: SLOT, 2: 'ROBOT-1', 3: 'ROBOT-2', 4: 'SHIPPED'}.get(seq, 'SHIPPED')
    holder = 1 if seq == 2 else 2 if seq == 3 else None
    robots = []
    for owner, home in enumerate(SCENE['homes'], 1):
        state = {'robot_id': owner, 'robot_type_key': 'X3', 'x': home['point']['x'], 'y': home['point']['y'],
                 'battery': 24. if owner == 1 and seq in (2, 3) else 80.,
                 'carrying': owner == holder, 'cargo_id': 'G1' if owner == holder else None,
                 'task_id': 'T1' if owner == holder else None, 'stage': 'to_target' if owner == holder else 'idle',
                 'dock_id': None, 'home_id': home['id']}
        robots.append({'state': state, 'worker_pid': 12345})
    task = {'task_id': 'T1', 'goods_id': 'G1', 'task_type': 'OUTBOUND', 'source_id': SLOT,
            'target_id': 'OUT-1', 'release_second': 0, 'due_second': 200,
            'status': 'completed' if seq >= 4 else 'carrying' if seq >= 2 else 'waiting',
            'robot_id': holder, 'completed_at': 15 if seq >= 4 else None}
    slot = {'slot_id': SLOT, 'rack_id': 'R01', 'x': 20., 'y': 20., 'occupied': seq == 1,
            'cargo_id': 'G1' if seq == 1 else None, 'reservedBy': None}
    return {'run_id': RUN, 'knowledge_seq': seq, 'tick': (seq-1)*5, 'elapsed': (seq-1)*5,
            'completed': int(seq >= 4), 'running': seq < 4, 'speed': 1.,
            'config': {'robot_count': 100, 'chargers': 100, 'flow_rate': 0},
            'run_metadata': {'domain': 'warehouse-wide-inbound-outbound', 'scene': SCENE,
                'scene_sha256': 'a'*64, 'tasks_sha256': 'b'*64, 'initial_inventory_sha256': 'c'*64,
                'manifest_sha256': 'd'*64, 'execution_code_sha256': 'e'*64,
                'real_scale': {'version': VERSION, 'source_kind': SOURCE_KIND,
                               'commit_seconds': 5, 'vector_kind': VECTOR_KIND}},
            'robots': robots, 'tasks': {'T1': task},
            'racks': [{'id': 'R01', 'zone': 'A', 'slots': [slot]}],
            'goods': {'G1': location}, 'inside': int(seq < 4),
            'event_sequence': min(5, {1: 1, 2: 3, 3: 4}.get(seq, 5))}


def events(seq):
    def make(number, kind, **extra):
        return {'event_id': f'{RUN}:event:{number}', 'event_seq': number, 'tick': (seq-1)*5,
                'sim_time': (seq-1)*5, 'kind': kind, 'robot_id': None, 'task_id': 'T1', **extra}
    if seq == 1:
        return [make(1, 'initial_stock', goods_id='G1', location_id=SLOT)]
    if seq == 2:
        return [make(2, 'picked', robot_id=1, goods_id='G1', location_id='ROBOT-1'),
                make(3, 'battery_measurement_corrected', robot_id=1, reason='low_battery', soc=24)]
    if seq == 3:
        return [make(4, 'cargo_handoff', robot_id=2, goods_id='G1', location_id='ROBOT-2',
                     donor_robot_id=1, receiver_robot_id=2, reason='low_battery')]
    if seq == 4:
        return [make(5, 'shipped', robot_id=2, goods_id='G1', location_id='SHIPPED')]
    return []


def make_journal(path, through=4):
    with_store = JournalKnowledgeStore(path)
    try:
        for seq in range(1, through+1):
            with_store.append(checkpoint(seq), events(seq))
    finally:
        with_store.close()


def mutate_batch(path, seq, edit, rehash=True):
    with sqlite3.connect(path) as db:
        blob = db.execute('SELECT batch FROM commits WHERE seq=?', (seq,)).fetchone()[0]
        value = json.loads(zlib.decompress(blob)); edit(value)
        if rehash:
            db.execute('UPDATE commits SET checksum=?,batch=? WHERE seq=?', (digest(value), zlib.compress(canonical(value)), seq))
        else:
            db.execute('UPDATE commits SET batch=? WHERE seq=?', (zlib.compress(canonical(value)), seq))


class RealOracleTests(unittest.TestCase):
    def setUp(self):
        (ROOT/'data/local-check').mkdir(parents=True, exist_ok=True)
        self.temp = tempfile.TemporaryDirectory(dir=ROOT/'data/local-check')
        self.folder = Path(self.temp.name)
        self.source = self.folder/'journal.sqlite'
        make_journal(self.source)

    def tearDown(self):
        self.temp.cleanup()

    def test_full_source_independent_history_task_custody_and_handoff(self):
        before = hashlib.sha256(self.source.read_bytes()).hexdigest()
        with RealOracle(self.source, 4) as oracle:
            self.assertEqual(oracle.prefix, 4)
            self.assertEqual(oracle.identity['states'], 400)
            self.assertEqual(oracle.state(2, 1)['cargo_id'], 'G1')
            self.assertEqual(oracle.task(2, 'T1')['robot_id'], 1)
            self.assertEqual(oracle.task(3, 'T1')['robot_id'], 2)
            self.assertEqual([r['seq'] for r in oracle.history(4, 2)], [4, 3, 2, 1])
            self.assertEqual(oracle.goods_history(2, 'G1')['current_as_of']['location_id'], 'ROBOT-1')
            self.assertEqual(oracle.goods_history(3, 'G1')['current_as_of']['location_id'], 'ROBOT-2')
            self.assertEqual(oracle.goods_history(4, 'G1')['current_as_of']['location_id'], 'SHIPPED')
            self.assertEqual(oracle.handoff_points()[0]['before_seq'], 2)
            self.assertEqual(len(oracle.cooperation(3, 'T1')['rows']), 2)
            self.assertEqual(oracle.focus(2, 1), {'task_id': 'T1', 'goods_id': 'G1'})
            self.assertEqual(oracle.vector(2, 1), real_vector(oracle.state(2, 1), 2, 1))
        self.assertEqual(before, hashlib.sha256(self.source.read_bytes()).hexdigest())

    def test_fixed_prefix_does_not_leak_later_handoff_or_final_task(self):
        with RealOracle(self.source, 2) as oracle:
            self.assertEqual(oracle.identity['source_head_at_snapshot'], 4)
            self.assertEqual(oracle.task(2, 'T1')['status'], 'carrying')
            self.assertEqual(oracle.goods_history(2, 'G1')['current_as_of']['location_id'], 'ROBOT-1')
            self.assertEqual(oracle.handoff_points(), [])
            with self.assertRaises(ValueError): oracle.state(3, 1)
            store = JournalKnowledgeStore(self.source)
            store.append(checkpoint(5), []); store.close()
            self.assertEqual(oracle.prefix, 2)
            self.assertEqual(oracle.state(2, 1)['cargo_id'], 'G1')

    def test_source_identity_full_equality_without_deepcopy(self):
        with RealOracle(self.source) as oracle:
            identity = oracle.identity
            with patch('backend.real_scale.oracle.deepcopy', side_effect=AssertionError('unexpected copy')):
                self.assertIsNone(oracle.check_source_identity(identity['metadata'], identity['config']))
            bad_metadata = deepcopy(identity['metadata'])
            bad_metadata['scene']['homes'][0]['point']['x'] += 1
            with self.assertRaisesRegex(ValueError, '^immutable source metadata/config mismatch$'):
                oracle.check_source_identity(bad_metadata, identity['config'])
            bad_config = {**identity['config'], 'chargers': 99}
            with self.assertRaisesRegex(ValueError, '^immutable source metadata/config mismatch$'):
                oracle.check_source_identity(identity['metadata'], bad_config)
            with self.assertRaisesRegex(ValueError, 'metadata/config mismatch'):
                oracle.check_source_identity({**identity['metadata'], 'unexpected': 1}, identity['config'])

    def test_source_identity_public_copy_stays_protected(self):
        with RealOracle(self.source) as oracle:
            first, second = oracle.identity, oracle.identity
            first['metadata']['scene']['homes'][0]['point']['x'] += 7
            first['config']['robot_count'] = 99
            self.assertEqual(oracle.identity, second)
            self.assertIsNone(oracle.check_source_identity(second['metadata'], second['config']))
            with self.assertRaisesRegex(ValueError, 'metadata/config mismatch'):
                oracle.check_source_identity(first['metadata'], first['config'])

    def test_source_identity_canonical_rejects_python_numeric_aliases(self):
        typed = self.folder/'typed-journal.sqlite'
        store = JournalKnowledgeStore(typed)
        try:
            for seq in range(1, 5):
                cp = checkpoint(seq)
                cp['run_metadata']['typed_probe'] = {'zero': 0, 'one': 1, 'float_one': 1.0}
                cp['config']['flow_rate'] = 1
                store.append(cp, events(seq))
        finally:
            store.close()
        with RealOracle(typed) as oracle:
            identity = oracle.identity
            self.assertIsNone(oracle.check_source_identity(identity['metadata'], identity['config']))
            for key, alias in [('zero', False), ('one', True), ('one', 1.0), ('float_one', 1)]:
                bad = deepcopy(identity['metadata'])
                bad['typed_probe'][key] = alias
                self.assertEqual(bad, identity['metadata'])  # Python eq would incorrectly accept.
                with self.subTest(key=key, alias=alias), self.assertRaisesRegex(ValueError, 'metadata/config mismatch'):
                    oracle.check_source_identity(bad, identity['config'])
            bad_config = {**identity['config'], 'flow_rate': True}
            self.assertEqual(bad_config, identity['config'])
            with self.assertRaisesRegex(ValueError, 'metadata/config mismatch'):
                oracle.check_source_identity(identity['metadata'], bad_config)

    def test_source_identity_canonical_rejects_nonfinite_actual_fields(self):
        with RealOracle(self.source) as oracle:
            identity = oracle.identity
            bad = deepcopy(identity['metadata'])
            bad['scene']['homes'][0]['point']['x'] = float('nan')
            with self.assertRaisesRegex(ValueError, 'metadata/config mismatch'):
                oracle.check_source_identity(bad, identity['config'])
            with self.assertRaisesRegex(ValueError, 'metadata/config mismatch'):
                oracle.check_source_identity(identity['metadata'], {**identity['config'], 'flow_rate': float('nan')})
            self.assertEqual(oracle.identity, identity)

    def test_pagination_and_cursors_keep_committed_cutoff(self):
        with RealOracle(self.source) as oracle:
            first = oracle.goods_history(4, 'G1', limit=2)
            self.assertTrue(first['has_more'])
            second = oracle.goods_history(4, 'G1', after_event_seq=first['next_after_event_seq'], limit=2)
            self.assertEqual([r['kind'] for r in first['rows']+second['rows']], ['initial_stock', 'picked', 'cargo_handoff', 'shipped'])
            self.assertFalse(second['has_more'])
            self.assertEqual(oracle.goods_history(2, 'unknown')['rows'], [])

    def test_persistent_index_can_be_reopened_and_shared_by_threads(self):
        index = self.folder/'oracle.sqlite'
        with RealOracle(self.source, 3, index_path=index) as built:
            expected = built.identity
        with RealOracle.open(index) as oracle:
            self.assertEqual(oracle.identity, expected)
            with ThreadPoolExecutor(max_workers=8) as pool:
                states = list(pool.map(lambda robot: oracle.state(3, robot), range(1, 101)))
            self.assertEqual([s['robot_id'] for s in states], list(range(1, 101)))
        with self.assertRaises(FileExistsError): RealOracle(self.source, 3, index_path=index)

    def test_checksum_and_valid_hash_custody_corruption_are_rejected(self):
        mutate_batch(self.source, 2, lambda b: b['robots'][0].update(x=999), rehash=False)
        with self.assertRaisesRegex(ValueError, 'SHA'): RealOracle(self.source, 2)
        other = self.folder/'other.sqlite'; make_journal(other)
        mutate_batch(other, 3, lambda b: b['events'][0].update(receiver_robot_id=3))
        with self.assertRaisesRegex(ValueError, 'custody'): RealOracle(other, 3)

    def test_duplicate_robot_or_robot_custody_disagreement_is_rejected(self):
        mutate_batch(self.source, 2, lambda b: b['robots'][1].update(robot_id=1))
        with self.assertRaisesRegex(ValueError, '100 unique'): RealOracle(self.source, 2)
        other = self.folder/'other.sqlite'; make_journal(other)
        mutate_batch(other, 3, lambda b: b['robots'][1].update(carrying=False, cargo_id=None))
        with self.assertRaisesRegex(ValueError, 'event ledger'): RealOracle(other, 3)

    def test_missing_event_and_nonfinite_or_typed_state_fail(self):
        mutate_batch(self.source, 2, lambda b: b['events'].pop(0))
        with self.assertRaisesRegex(ValueError, 'event sequence'): RealOracle(self.source, 2)
        for field, value in [('battery', float('nan')), ('carrying', 'false'), ('robot_id', True)]:
            state = deepcopy(checkpoint(1)['robots'][0]['state']); state[field] = value
            with self.assertRaises(ValueError): real_vector(state, 1)

    def test_valid_hash_cross_task_cargo_future_task_and_wrong_typed_sequence_fail(self):
        def wrong_task(batch):
            batch['tasks'].append({**batch['tasks'][0], 'task_id': 'T2', 'goods_id': 'G2'})
            batch['events'][0]['task_id'] = 'T2'
        mutate_batch(self.source, 3, wrong_task)
        with self.assertRaisesRegex(ValueError, 'task goods'): RealOracle(self.source, 3)
        other = self.folder/'future.sqlite'; make_journal(other)
        mutate_batch(other, 2, lambda b: b['robots'][4].update(task_id='future-task'))
        with self.assertRaisesRegex(ValueError, 'future/missing'): RealOracle(other, 2)
        typed = self.folder/'typed.sqlite'; make_journal(typed)
        mutate_batch(typed, 1, lambda b: b.update(seq=True))
        with self.assertRaisesRegex(ValueError, 'batch sequence'): RealOracle(typed, 1)

    def test_real_cross_robot_completion_then_earlier_charge_time_keeps_emission_order(self):
        # Mirrors source commit338: a completion at now+1 can be emitted before
        # a later robot's charge_started at now. This is not a future event.
        def source_context(batch):
            batch['events'][0].update(sim_time=10, tick=10)
            batch['events'][0]['event_seq'] = 5
            batch['events'][0]['event_id'] = f'{RUN}:event:5'
            batch['events'].append({'event_id': f'{RUN}:event:6', 'event_seq': 6, 'tick': 9,
                'sim_time': 9, 'kind': 'charge_started', 'robot_id': 39, 'task_id': None,
                'dock_id': 'C-A39', 'battery': 30.})
            batch['events'].append({'event_id': f'{RUN}:event:7', 'event_seq': 7, 'tick': 10,
                'sim_time': 10, 'kind': 'charge_reserved', 'robot_id': 40, 'task_id': None,
                'dock_id': 'C-A40'})
        # Use seq4, whose source delivery leaves the cargo ledger SHIPPED.
        mutate_batch(self.source, 4, source_context)
        with sqlite3.connect(self.source) as db:
            sha, blob = db.execute('SELECT checksum,checkpoint FROM heads').fetchone()
            head = json.loads(zlib.decompress(blob)); head['event_sequence'] = 7
            raw = canonical(head)
            db.execute('UPDATE heads SET checksum=?,checkpoint=?', (hashlib.sha256(raw).hexdigest(), zlib.compress(raw)))
        db.close()
        with RealOracle(self.source, 4) as oracle:
            self.assertEqual(oracle.identity['events'], 7)
            rows = oracle._db().execute('SELECT event_seq,payload FROM events WHERE event_seq>=5 ORDER BY event_seq').fetchall()
            self.assertEqual([json.loads(payload)['sim_time'] for _, payload in rows], [10, 9, 10])
            self.assertEqual(oracle.goods_history(4, 'G1')['current_as_of']['location_id'], 'SHIPPED')

    def test_true_future_negative_time_and_missing_emission_sequence_are_rejected(self):
        for tag, edit in [('future', lambda b: b['events'][1].update(sim_time=6)),
                          ('negative', lambda b: b['events'][1].update(sim_time=-1)),
                          ('gap', lambda b: b['events'][1].update(event_seq=4))]:
            path = self.folder/(tag+'.sqlite'); make_journal(path)
            mutate_batch(path, 2, edit)
            with self.assertRaisesRegex(ValueError, 'event sequence'): RealOracle(path, 2)

    def test_exact_pilot_picked_shipped_charge_started_context(self):
        # Immutable source evidence: pilot-graph commit338/time1685. The code
        # path emits completion markers at now+1, then a later robot's charge
        # start at now. Do not sort, retime, or require cross-robot time order.
        context = [
            {'event_seq': 1426, 'kind': 'picked', 'robot_id': 2, 'sim_time': 1683,
             'tick': 1683, 'task_id': 'V3-T-0000100', 'goods_id': 'INITIAL-00618', 'location_id': 'ROBOT-2'},
            {'event_seq': 1427, 'kind': 'shipped', 'robot_id': 30, 'sim_time': 1683,
             'tick': 1683, 'task_id': 'V3-T-0000137', 'goods_id': 'INITIAL-00885', 'location_id': 'SHIPPED'},
            {'event_seq': 1428, 'kind': 'charge_started', 'robot_id': 39, 'sim_time': 1682,
             'tick': 1682, 'task_id': None, 'dock_id': 'C-A39', 'battery': 54.04340980139244},
        ]
        original = deepcopy(context); previous = 1425
        for event in context:
            previous = _observe_event(event, previous, 1685)
        self.assertEqual(previous, 1428)
        self.assertEqual(context, original)
        for edit in [{'sim_time': 1686}, {'sim_time': -1}, {'event_seq': 1429}]:
            with self.assertRaises(ValueError):
                _observe_event({**context[-1], **edit}, 1427, 1685)

    def test_parameter_bounds_do_not_query_future_or_boolean_id(self):
        for prefix in [0, True, 5]:
            with self.assertRaises(ValueError): RealOracle(self.source, prefix)
        with RealOracle(self.source) as oracle:
            for fn in [lambda: oracle.state(4, True), lambda: oracle.history(4, 1, 0),
                       lambda: oracle.cooperation(4, after_event_seq=True),
                       lambda: oracle.goods_history(4, 'G1', after_event_seq=-1)]:
                with self.assertRaises(ValueError): fn()

    def test_public_oracle_protocol_matches_complete_query_validator(self):
        from backend.real_scale.benchmark import make_case, normalized_score, validate_result
        with RealOracle(self.source, 4) as oracle:
            case = make_case(oracle, 2, 1, 1, 100, oracle.handoff_points())
            seq, robot = case['seq'], case['robot_id']
            query = oracle.vector(seq, robot)
            candidates = []
            for number in range(1, seq+1):
                for owner in range(1, 101):
                    embedding = oracle.vector(number, owner)
                    candidates.append({'key': f'{RUN}:{number}:{owner}', 'run_id': RUN,
                        'seq': number, 'robot_id': owner, 'real_embedding': embedding,
                        'real_payload_sha256': digest(oracle.state(number, owner)),
                        'score': normalized_score(query, embedding)})
            candidates.sort(key=lambda r: r['score'], reverse=True)
            state = oracle.state(seq, robot)
            result = {'run_id': RUN, 'seq': seq, 'robot_id': robot, 'graph_committed_seq': 4,
                'metadata': oracle.identity['metadata'], 'config': oracle.identity['config'],
                'real_identity': {'real_scale_version': VERSION, 'real_source_kind': SOURCE_KIND,
                    'real_source_run': RUN, 'real_dimensions': 32, 'real_vector_kind': VECTOR_KIND},
                'batch_checksum': oracle.batch_checksum(seq), 'robot': state, 'robot_sha256': digest(state),
                'task': oracle.task(seq, state.get('task_id')), 'focus_task_id': case['focus_task_id'],
                'focus_task': oracle.task(seq, case['focus_task_id']), 'query_vector': query,
                'history': oracle.history(seq, robot), 'similar': candidates[:5],
                'goods_history': oracle.goods_history(seq, case['goods_id']),
                'cooperation': oracle.cooperation(seq, case['focus_task_id'])}
            self.assertTrue(validate_result(json.loads(json.dumps(result)), oracle, case)['focus_task_present'])


if __name__ == '__main__':
    unittest.main()
