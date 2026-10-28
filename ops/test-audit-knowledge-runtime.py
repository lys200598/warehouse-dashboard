"""Independent, offline tamper tests for live control-chain source auditing."""
from copy import deepcopy
import importlib.util
import json
from pathlib import Path
import sqlite3
import sys
import tempfile
import unittest
import zlib


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT if (ROOT/'backend').is_dir() else ROOT/'warehouse-dashboard'))
spec = importlib.util.spec_from_file_location('knowledge_runtime_auditor',
    Path(__file__).with_name('audit-knowledge-runtime.py'))
audit = importlib.util.module_from_spec(spec)
spec.loader.exec_module(audit)
from backend.knowledge_runtime.policy import authorize


TEMP = ROOT/'research/knowledge-runtime-audit-tests-20261028'
TEMP.mkdir(parents=True, exist_ok=True)
RUN, TASK, GOODS = 'audit-live-unit', 'TASK-1', 'GOODS-1'


class MiniOracle:
    """Separate source model; reproduces SQLite REAL timestamps deliberately."""
    run_id, prefix = RUN, 4

    def __init__(self, records):
        self.records = records
        self.task_payload = {'task_id': TASK, 'goods_id': GOODS, 'status': 'carrying', 'robot_id': 7}
        self.db = sqlite3.connect(':memory:')
        self.db.execute('CREATE TABLE events(event_seq INTEGER,payload TEXT)')
        self.refresh()

    def refresh(self):
        self.db.execute('DELETE FROM events')
        for r in self.records:
            self.db.execute('INSERT INTO events VALUES (?,?)', (r['event']['event_seq'], audit.canonical(r['event'])))

    def _db(self):
        return self.db

    def task(self, seq, task_id):
        return deepcopy(self.task_payload) if task_id == TASK else None

    def state(self, seq, owner):
        return {'robot_id': owner, 'task_id': TASK if owner == 7 else None,
                'cargo_id': GOODS if owner == 7 else None, 'carrying': owner == 7}

    def batch_checksum(self, seq):
        return chr(96+seq)*64

    def goods_history(self, seq, goods_id):
        rows = [{'event_seq': r['event']['event_seq'], 'seq': r['commit_seq'],
                 'sim_time': float(r['event']['sim_time']), 'kind': r['event']['kind'],
                 'location_id': r['event']['location_id'], 'event_key': r['event']['event_id']}
                for r in self.records if r['commit_seq'] <= seq and r['event'].get('goods_id') == goods_id
                and 'location_id' in r['event']]
        current = None if not rows else {k: rows[-1][k] for k in ['location_id','event_seq','sim_time']}
        return {'run_id': RUN, 'goods_id': goods_id, 'seq': seq, 'graph_committed_seq': self.prefix,
                'rows': rows, 'current_as_of': current, 'has_more': False, 'next_after_event_seq': None}

    def cooperation(self, seq, task_id):
        return {'run_id': RUN, 'task_id': task_id, 'seq': seq, 'graph_committed_seq': self.prefix,
                'rows': [{'event_seq': r['event']['event_seq'], 'seq': r['commit_seq'], 'event': deepcopy(r['event'])}
                         for r in self.records if r['commit_seq'] <= seq and r['event'].get('task_id') == task_id
                         and r['event']['kind'] in {'assistance_requested','handoff_reserved','cargo_handoff'}],
                'has_more': False, 'next_after_event_seq': None}

    def close(self):
        self.db.close()


def fixture():
    def event(number, kind, at, **extra):
        return {'event_seq': number, 'event_id': f'{RUN}:event:{number}', 'kind': kind,
                'sim_time': at, 'tick': at, 'robot_id': None, 'task_id': TASK, **extra}
    records = [
        {'commit_seq': 1, 'event': event(1, 'initial_stock', 0, goods_id=GOODS, location_id='SLOT-1')},
        {'commit_seq': 1, 'event': event(2, 'picked', 9, robot_id=7, goods_id=GOODS, location_id='ROBOT-7')},
        {'commit_seq': 2, 'event': event(3, 'knowledge_decision', 10, robot_id=7,
            decision='authorize_assistance_planning', knowledge_seq=1, knowledge_batch_checksum='a'*64,
            receiver_robot_id=None, mode='required')},
        {'commit_seq': 2, 'event': event(4, 'assistance_requested', 10, robot_id=7)},
        {'commit_seq': 2, 'event': event(5, 'handoff_reserved', 10, robot_id=7, receiver_robot_id=68, handoff_at=13)},
        {'commit_seq': 2, 'event': event(6, 'knowledge_action_applied', 10, robot_id=7,
            action='handoff_plan_reserved', knowledge_seq=1, receiver_robot_id=68)},
        {'commit_seq': 4, 'event': event(7, 'knowledge_decision', 12, robot_id=7,
            decision='authorize_transfer', knowledge_seq=3, knowledge_batch_checksum='c'*64,
            receiver_robot_id=68, mode='required')},
        {'commit_seq': 4, 'event': event(8, 'cargo_handoff', 13, robot_id=68, goods_id=GOODS,
            donor_id=7, donor_robot_id=7, receiver_id=68, receiver_robot_id=68, location_id='ROBOT-68')},
        {'commit_seq': 4, 'event': event(9, 'knowledge_action_applied', 13, robot_id=68,
            action='cargo_transferred', knowledge_seq=3, donor_robot_id=7, receiver_robot_id=68, goods_id=GOODS)}]
    oracle = MiniOracle(records)
    batches = {1: {'checksum': 'a'*64, 'sim_time': 10}, 2: {'checksum': 'b'*64, 'sim_time': 10},
               3: {'checksum': 'c'*64, 'sim_time': 12}, 4: {'checksum': 'd'*64, 'sim_time': 13}}
    # Construct the expected wire/provider context independently of the auditor.
    # Original event times are known integers; the Oracle goods API emits REAL.
    for index in (2, 6):
        e = records[index]['event']; seq = e['knowledge_seq']
        goods = oracle.goods_history(seq, GOODS); coop = oracle.cooperation(seq, TASK)
        for h in (goods, coop): h['graph_committed_seq'] = seq
        for row in goods['rows']:
            row['sim_time'] = records[row['event_seq']-1]['event']['sim_time']
        goods['current_as_of']['sim_time'] = records[goods['current_as_of']['event_seq']-1]['event']['sim_time']
        ctx = dict(run_id=RUN, seq=seq, graph_committed_seq=seq,
            batch_checksum=batches[seq]['checksum'], task_id=TASK, goods_id=GOODS,
            focus_task=oracle.task(seq, TASK), request_cursors={'goods_after':0,'cooperation_after':0},
            goods_history=goods, cooperation=coop, source_sim_time=batches[seq]['sim_time'],
            observation_lag_seconds=e['sim_time']-batches[seq]['sim_time'])
        decision = authorize(ctx, run=RUN, donor=7, task=oracle.task(seq, TASK), goods=GOODS,
                             receiver=e['receiver_robot_id'])
        e.update(context_sha256=decision['context_sha256'], history_event_seqs=decision['history_event_seqs'])
    oracle.refresh()
    source = {'run_id': RUN, 'prefix': 4, 'batches': batches, 'events': records,
              'task_hashes': {TASK: [(1, audit.digest(oracle.task_payload))]}}
    return source, oracle


class RuntimeAuditTests(unittest.TestCase):
    def setUp(self):
        self.source, self.oracle = fixture()

    def tearDown(self):
        self.oracle.close()

    def check_mutation(self, mutate):
        source = deepcopy(self.source)
        mutate(source)
        result = audit.audit_events(source, self.oracle)
        self.assertFalse(result['passed'], result)
        return result

    def test_complete_grant_reservation_physical_action_chain_passes(self):
        result = audit.audit_events(self.source, self.oracle)
        self.assertTrue(result['passed'], result['errors'])
        self.assertEqual(result['counts']['decisions'], 2)
        self.assertEqual(result['counts']['authorized_transfers_applied'], 1)
        self.assertEqual(result['plans'][0]['reservation_event_seq'], 5)
        self.assertEqual(result['transfers'][0]['decision_event_seq'], 7)
        self.assertEqual(result['transfers'][0]['cargo_handoff_event_seq'], 8)
        self.assertEqual(result['transfers'][0]['applied_event_seq'], 9)

    def test_context_restores_original_json_time_type_before_hashing(self):
        ctx = audit.reconstruct_context(self.oracle, 3, TASK, GOODS, 12, self.source['batches'])
        self.assertIs(type(ctx['goods_history']['rows'][0]['sim_time']), int)
        self.assertIs(type(ctx['goods_history']['current_as_of']['sim_time']), int)
        self.assertEqual(audit.digest(ctx), self.source['events'][6]['event']['context_sha256'])
        ctx['goods_history']['rows'][0]['sim_time'] = 0.0
        self.check_mutation(lambda s: s['events'][6]['event'].update(context_sha256=audit.digest(ctx)))

    def test_missing_plan_transfer_or_applied_event_fails_even_with_other_counts_present(self):
        for number in (3, 5, 6, 7, 8, 9):
            with self.subTest(event_seq=number):
                self.check_mutation(lambda s: s.update(events=[r for r in s['events'] if r['event']['event_seq'] != number]))

    def test_future_source_stale_observation_wrong_checksum_and_context_rejected(self):
        mutations = [lambda s: s['events'][6]['event'].update(knowledge_seq=4),
                     lambda s: s['events'][6]['event'].update(knowledge_batch_checksum='0'*64),
                     lambda s: s['events'][6]['event'].update(context_sha256='0'*64),
                     lambda s: s['events'][6]['event'].update(history_event_seqs=[1,2,99999]),
                     lambda s: s['events'][2]['event'].update(sim_time=9),
                     lambda s: s['batches'][1].update(sim_time=0)]
        for mutation in mutations:
            with self.subTest(mutation=mutation): self.check_mutation(mutation)

    def test_stale_source_is_rejected_before_fingerprint_validation(self):
        result = self.check_mutation(lambda s: s['events'][2]['event'].update(sim_time=100))
        self.assertIn('future/stale decision knowledge', result['errors'][0]['error'])

    def test_foreign_participants_goods_task_or_mode_never_authorize_action(self):
        mutations = [lambda s: s['events'][8]['event'].update(goods_id='FOREIGN'),
                     lambda s: s['events'][8]['event'].update(donor_robot_id=8),
                     lambda s: s['events'][8]['event'].update(task_id='FOREIGN'),
                     lambda s: s['events'][6]['event'].update(mode='rules'),
                     lambda s: s['events'][7]['event'].update(receiver_id=25),
                     lambda s: s['events'][6]['event'].update(robot_id=True)]
        for mutation in mutations:
            with self.subTest(mutation=mutation): self.check_mutation(mutation)

    def test_duplicate_physical_action_cannot_consume_one_grant_twice(self):
        def duplicate(source):
            entry = deepcopy(source['events'][-1])
            entry['event'].update(event_seq=10, event_id=f'{RUN}:event:10')
            source['events'].append(entry)
        result = self.check_mutation(duplicate)
        self.assertIn('reused', result['errors'][0]['error'])

    def test_unapplied_extra_authorization_is_not_silent_success(self):
        def add(source):
            extra = deepcopy(source['events'][6])
            extra['event'].update(event_seq=10, event_id=f'{RUN}:event:10')
            source['events'].append(extra)
        result = self.check_mutation(add)
        self.assertEqual(result['unused_authorization_event_seqs'], [10])

    def test_wrong_step_or_action_causality_fails(self):
        self.check_mutation(lambda s: s['events'][7]['event'].update(sim_time=14))
        self.check_mutation(lambda s: s['events'][8]['event'].update(sim_time=12))


class SourceBoundaryTests(unittest.TestCase):
    def journal(self, root, mutate=None, bad_checksum=False):
        path = root/'journal.sqlite'
        metadata, config = {'domain': 'warehouse-wide-inbound-outbound'}, {}
        event = dict(event_seq=1, event_id=f'{RUN}:event:1', sim_time=0,
                     kind='initial_stock', goods_id=GOODS, location_id='SLOT-1')
        batch = dict(run_id=RUN, seq=1, sim_time=0, metadata=metadata, config=config, tasks=[], events=[event])
        if mutate: mutate(batch)
        checkpoint = dict(run_id=RUN, knowledge_seq=1, event_sequence=1, run_metadata=metadata, config=config)
        db = sqlite3.connect(path)
        try:
            db.executescript('CREATE TABLE heads(run_id TEXT,seq INTEGER,checksum TEXT,checkpoint BLOB,mirrored_seq INTEGER);'
                            'CREATE TABLE commits(run_id TEXT,seq INTEGER,checksum TEXT,batch BLOB);')
            db.execute('INSERT INTO heads VALUES(?,?,?,?,?)',
                (RUN,1,audit.digest(checkpoint),zlib.compress(audit.canonical(checkpoint).encode()),1))
            db.execute('INSERT INTO commits VALUES(?,?,?,?)',
                (RUN,1,'0'*64 if bad_checksum else audit.digest(batch),zlib.compress(audit.canonical(batch).encode())))
            db.commit()
        finally:
            db.close()
        return path

    def test_actual_sqlite_source_sha_identity_and_future_time_are_fail_closed(self):
        for mutation, bad in [(None, False), (None, True),
                              (lambda b: b['events'][0].update(event_id='foreign:event:1'), False),
                              (lambda b: b['events'][0].update(sim_time=1), False)]:
            with self.subTest(mutation=mutation, bad=bad), tempfile.TemporaryDirectory(dir=TEMP) as temp:
                path = self.journal(Path(temp), mutation, bad)
                if mutation is None and not bad:
                    self.assertEqual(audit.source_snapshot(path)['prefix'], 1)
                else:
                    with self.assertRaises(ValueError): audit.source_snapshot(path)

    def test_cli_missing_input_writes_failure_not_success(self):
        with tempfile.TemporaryDirectory(dir=TEMP) as temp:
            root = Path(temp); output = root/'audit.json'
            source = Path(sys.modules['backend'].__file__).resolve().parent.parent
            code = audit.main(['--journal', str(root/'missing.sqlite'), '--results', str(root/'missing.json'),
                               '--output', str(output), '--source-root', str(source)])
            self.assertEqual(code, 2)
            value = json.loads(output.read_text())
            self.assertFalse(value['passed'])
            self.assertIn('FileNotFoundError', value['error'])


if __name__ == '__main__':
    unittest.main()
