"""Independent source/watermark and real-control rejection regression tests."""
from copy import deepcopy
from dataclasses import asdict
from functools import partial
import io
import json
from pathlib import Path
import sqlite3
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch
from contextlib import closing, redirect_stdout, redirect_stderr
import zlib

from backend.knowledge import canonical, digest
from backend.checkpoint import read_checkpoint
from backend.knowledge_runtime.policy import authorize, DecisionRejected
from backend.knowledge_runtime.provider import GraphHistoryProvider
from backend.knowledge_runtime.sim import KnowledgeSimulation
from backend.knowledge_runtime.runner import run as run_live, build_parser
from backend.real_scale.writer import Config
from tests import test_knowledge_runtime as runtime_fixture


class MemorySource:
    """Actual SQLite/zlib/checksum boundary, no graph or external network."""
    def __init__(self, sim, task):
        self.db = sqlite3.connect(':memory:')
        self.db.execute('CREATE TABLE commits(run_id TEXT,seq INTEGER,checksum TEXT,batch BLOB)')
        self.state = {'durable_seq': 7, 'mirrored_seq': 7, 'last_error': None}
        self.batch = {'run_id': sim.run_id, 'seq': 7, 'sim_time': 100,
                      'metadata': deepcopy(sim.run_metadata), 'config': asdict(sim.config),
                      'tasks': [deepcopy(task)], 'events': []}
        self.checkpoint = {'run_id': sim.run_id, 'knowledge_seq': 7, 'elapsed': 100,
                           'run_metadata': deepcopy(sim.run_metadata), 'config': asdict(sim.config),
                           'tasks': {task['task_id']: deepcopy(task)}}
        self.replace_batch()

    def replace_batch(self, *, retain_checksum=False):
        previous = self.db.execute('SELECT checksum FROM commits').fetchone()
        checksum = previous[0] if retain_checksum and previous else digest(self.batch)
        self.db.execute('DELETE FROM commits')
        self.db.execute('INSERT INTO commits VALUES (?,?,?,?)',
                        ('live-source-unit', 7, checksum, zlib.compress(canonical(self.batch))))
        self.db.commit()
        self.checksum = checksum

    def status(self, run):
        return {'run_id': run, 'backend': 'neo4j', **self.state}

    def latest_checkpoint(self, run):
        return deepcopy(self.checkpoint)

    def close(self):
        self.db.close()


def complete_context(sim, task, checksum):
    common = dict(run_id=sim.run_id, seq=7, graph_committed_seq=7,
                  has_more=False, next_after_event_seq=None)
    goods_rows = [dict(event_seq=1, seq=1, sim_time=90, kind='initial_stock',
                       location_id='R01-L1-C1-F1', event_key=sim.run_id+':event:1'),
                  dict(event_seq=2, seq=2, sim_time=95, kind='picked',
                       location_id='ROBOT-7', event_key=sim.run_id+':event:2')]
    reserved = dict(event_seq=4, kind='handoff_reserved', task_id=task['task_id'],
                    goods_id=task['goods_id'], robot_id=7, receiver_robot_id=68,
                    handoff_at=120, sim_time=99)
    return dict(run_id=sim.run_id, seq=7, graph_committed_seq=7, batch_checksum=checksum,
                task_id=task['task_id'], goods_id=task['goods_id'], focus_task=deepcopy(task),
                request_cursors={'goods_after': 0, 'cooperation_after': 0},
                goods_history={**common, 'goods_id': task['goods_id'], 'rows': goods_rows,
                    'current_as_of': {'event_seq': 2, 'location_id': 'ROBOT-7', 'sim_time': 95}},
                cooperation={**common, 'task_id': task['task_id'], 'rows': [
                    {'seq': 6, 'event_seq': 4, 'event': reserved}]})


class ContextStore:
    def __init__(self, value):
        self.value, self.calls, self.closed = value, [], False

    def cooperation_context(self, *args):
        self.calls.append(args)
        return deepcopy(self.value)

    def close(self):
        self.closed = True


class JournalTestMirror:
    """Only acknowledges actual source batches; never a performance/DB proof."""
    def __init__(self):
        self.heads = {}

    @classmethod
    def from_env(cls):
        return cls()

    def committed_point(self, run):
        return self.heads.get(run)

    def apply_many(self, batches):
        for batch in batches:
            self.heads[batch['run_id']] = {'seq': batch['seq'], 'checksum': digest(batch)}

    def close(self):
        pass


class UnavailableHistory:
    def __init__(self, *args, **kwargs):
        pass

    def context(self, sim, task):
        raise TimeoutError('independent live-control outage fixture')

    def close(self):
        pass


class SavedHistoryFixture:
    """Persistent-provider test double, deliberately separate from restored RAM."""
    def __init__(self, events):
        self.events, self.calls = deepcopy(events), 0

    def context(self, sim, task):
        self.calls += 1
        source = SimpleNamespace(run_id=sim.run_id, pending_events=self.events)
        return runtime_fixture.context_for(source, task)


class RuntimeProviderTests(unittest.TestCase):
    def setUp(self):
        self.sim = SimpleNamespace(run_id='live-source-unit', elapsed=105, knowledge_seq=7,
            config=Config(charging_policy='none'), run_metadata={'scene_sha256': 'a'*64,
                'tasks_sha256': 'b'*64, 'initial_inventory_sha256': 'c'*64,
                'manifest_sha256': 'd'*64, 'knowledge_runtime': {'mode': 'required', 'source': 'e'*64}})
        self.task = {'task_id': 'LIVE-T-1', 'goods_id': 'LIVE-G-1',
                     'status': 'carrying', 'robot_id': 7}
        self.source = MemorySource(self.sim, self.task)
        self.store = ContextStore(complete_context(self.sim, self.task, self.source.checksum))
        self.provider = GraphHistoryProvider(self.source, self.store)

    def tearDown(self):
        self.source.close()

    def request(self):
        return self.provider.context(self.sim, self.task)

    def test_exact_source_task_and_committed_cutoff_pass_and_are_provenanced(self):
        value = self.request()
        self.assertEqual(self.store.calls, [(self.sim.run_id, 7, 'LIVE-T-1', 'LIVE-G-1')])
        self.assertEqual(value['source_sim_time'], 100)
        self.assertEqual(value['observation_lag_seconds'], 5)
        decision = authorize(value, run=self.sim.run_id, donor=7,
                             task=self.task, goods=self.task['goods_id'], receiver=68)
        self.assertEqual(decision['action'], 'authorize_transfer')
        self.assertEqual(decision['context_sha256'], digest(value))

    def test_lag_mirror_failure_and_unknown_source_prevent_any_graph_query(self):
        for state in ({'mirrored_seq': 6}, {'durable_seq': 8}, {'mirrored_seq': 0},
                      {'last_error': 'graph unavailable'}):
            with self.subTest(state=state):
                previous = deepcopy(self.source.state)
                self.source.state.update(state)
                with self.assertRaises(DecisionRejected): self.request()
                self.assertEqual(self.store.calls, [])
                self.source.state = previous
        self.source.db.execute('DELETE FROM commits')
        with self.assertRaises(DecisionRejected): self.request()
        self.assertEqual(self.store.calls, [])

    def test_source_batch_checksum_foreign_identity_and_configuration_rejected(self):
        edits = [lambda b: b.update(run_id='foreign'), lambda b: b.update(seq=6),
                 lambda b: b['metadata'].update(scene_sha256='0'*64),
                 lambda b: b['config'].update(robot_speed=b['config'].get('robot_speed', 1)+1)]
        for edit in edits:
            with self.subTest(edit=edit):
                original = deepcopy(self.source.batch)
                edit(self.source.batch)
                self.source.replace_batch()
                with self.assertRaises(DecisionRejected): self.request()
                self.assertEqual(self.store.calls, [])
                self.source.batch = original
                self.source.replace_batch()
        self.source.batch['sim_time'] = 101
        self.source.replace_batch(retain_checksum=True)
        with self.assertRaises(DecisionRejected): self.request()

    def test_maximum_source_lag_is_inclusive_future_and_stale_are_rejected(self):
        for source_time, accepted in [(75, True), (74, False), (106, False)]:
            with self.subTest(source_time=source_time):
                self.source.batch['sim_time'] = source_time
                self.source.replace_batch()
                self.store.value['batch_checksum'] = self.source.checksum
                if accepted:
                    self.assertEqual(self.request()['observation_lag_seconds'], 30)
                else:
                    with self.assertRaises(DecisionRejected): self.request()

    def test_graph_wrong_cutoff_head_run_task_goods_or_checksum_rejected(self):
        edits = [lambda c: c.update(seq=6), lambda c: c.update(seq=8),
                 lambda c: c.update(seq=7.0), lambda c: c.update(graph_committed_seq=8),
                 lambda c: c.update(graph_committed_seq=7.0), lambda c: c.update(run_id='foreign'),
                 lambda c: c.update(task_id='foreign'), lambda c: c.update(goods_id='foreign'),
                 lambda c: c.update(batch_checksum='0'*64)]
        original = deepcopy(self.store.value)
        for edit in edits:
            with self.subTest(edit=edit):
                self.store.value = deepcopy(original)
                edit(self.store.value)
                with self.assertRaises(DecisionRejected): self.request()

    def test_focus_task_entire_payload_and_current_source_checkpoint_are_bound(self):
        self.store.value['focus_task']['unexpected-field'] = False
        with self.assertRaisesRegex(DecisionRejected, 'task payload'): self.request()
        del self.store.value['focus_task']['unexpected-field']
        self.source.checkpoint['knowledge_seq'] = 6
        with self.assertRaisesRegex(DecisionRejected, 'task payload'): self.request()

    def test_provider_exceptions_are_not_silent_rule_fallback(self):
        with patch.object(self.store, 'cooperation_context', side_effect=TimeoutError('bounded graph read')):
            with self.assertRaises(TimeoutError): self.request()
        self.provider.close()
        self.assertTrue(self.store.closed)

    def test_illegal_lag_budgets_are_rejected(self):
        for lag in (True, -1, 1., '30', None):
            with self.subTest(lag=lag), self.assertRaises(ValueError):
                GraphHistoryProvider(self.source, self.store, max_lag_seconds=lag)

    def test_source_watermark_and_clock_have_strict_numeric_types(self):
        self.source.state.update(durable_seq=7.0, mirrored_seq=7.0)
        with self.assertRaises(DecisionRejected): self.request()
        self.source.state.update(durable_seq=7, mirrored_seq=7)
        self.sim.elapsed = 5
        self.source.batch['sim_time'] = True
        self.source.replace_batch()
        self.store.value['batch_checksum'] = self.source.checksum
        with self.assertRaises(DecisionRejected): self.request()


class RuntimePolicyBoundaryTests(unittest.TestCase):
    def setUp(self):
        self.sim = SimpleNamespace(run_id='live-source-unit')
        self.task = {'task_id': 'LIVE-T-1', 'goods_id': 'LIVE-G-1',
                     'status': 'carrying', 'robot_id': 7}
        self.base = complete_context(self.sim, self.task, 'a'*64)

    def authorize(self, value):
        return authorize(value, run=self.sim.run_id, donor=7, task=self.task,
                         goods=self.task['goods_id'], receiver=68)

    def test_foreign_parent_contexts_future_custody_and_unordered_event_rows_rejected(self):
        edits = [lambda c: c['goods_history'].update(goods_id='foreign'),
                 lambda c: c['cooperation'].update(task_id='foreign'),
                 lambda c: c['goods_history']['rows'][-1].update(seq=8),
                 lambda c: c['goods_history']['rows'][-1].update(seq=True),
                 lambda c: c['goods_history']['rows'].insert(0, deepcopy(c['goods_history']['rows'][0])),
                 lambda c: c['cooperation']['rows'][0].update(seq=8),
                 lambda c: c['cooperation']['rows'][0]['event'].update(goods_id='foreign'),
                 lambda c: c['cooperation']['rows'][0]['event'].update(receiver_id=25),
                 lambda c: c['cooperation']['rows'][0]['event'].update(receiver_robot_id=True),
                 lambda c: c['cooperation']['rows'][0]['event'].update(robot_id=7.0),
                 lambda c: c['request_cursors'].update(goods_after=False)]
        for edit in edits:
            with self.subTest(edit=edit):
                value = deepcopy(self.base); edit(value)
                with self.assertRaises(DecisionRejected): self.authorize(value)

    def test_missing_or_other_receiver_reservation_rejects_physical_transfer(self):
        for receiver in (None, 25):
            with self.subTest(receiver=receiver):
                value = deepcopy(self.base)
                if receiver is None: value['cooperation']['rows'] = []
                else: value['cooperation']['rows'][0]['event']['receiver_robot_id'] = receiver
                with self.assertRaises(DecisionRejected): self.authorize(value)

    def test_zero_or_negative_cooperation_commit_is_not_a_past_committed_point(self):
        for seq in (0, -1):
            with self.subTest(seq=seq):
                value = deepcopy(self.base)
                value['cooperation']['rows'][0]['seq'] = seq
                with self.assertRaises(DecisionRejected): self.authorize(value)


class RuntimePhysicalPreflightTests(unittest.TestCase):
    root = Path(__file__).resolve().parents[1]

    def make(self):
        dataset = self.root/'data/examples/warehouse-v5'
        if not dataset.is_dir():
            dataset = self.root/'docs/dataset-contract/sample-input'
        sim = KnowledgeSimulation(self.root/'data/layouts/warehouse100-wide-v2.json',
            dataset, 'live-physical-independent', Config(charging_policy='none'),
            knowledge_mode='required', commit_seconds=5)
        sim.history_provider = runtime_fixture.LocalTestProvider()
        return sim

    def prepare_transfer(self):
        sim = self.make()
        helper = runtime_fixture.RuntimeKnowledgeTests(methodName='runTest')
        donor = helper.carrying(sim)
        self.assertTrue(sim.request_assistance(donor))
        handoff = sim.assists[donor]
        while sim.elapsed < handoff['handoff_at']-1:
            sim.step()
        return sim, donor, handoff['receiver']

    def snapshot(self, sim):
        return deepcopy((sim.elapsed, sim.tick, sim.robots, sim.tasks, sim.jobs,
                         sim.goods, sim.assists, sim.reservations.__dict__))

    def test_invalid_live_receiver_or_reservation_is_rejected_before_any_robot_moves(self):
        for kind in ('receiver-cargo', 'wrong-reservation'):
            with self.subTest(kind=kind):
                sim, donor, receiver = self.prepare_transfer()
                if kind == 'receiver-cargo':
                    sim.robots[receiver].update(carrying=True, cargo_id='UNAUTHORIZED')
                else:
                    sim.assists[donor]['receiver'] = 1 if receiver != 1 else 2
                before = self.snapshot(sim)
                calls = sim.history_provider.calls
                with self.assertRaises(DecisionRejected): sim.step()
                self.assertEqual(before, self.snapshot(sim))
                self.assertEqual(sim.history_provider.calls, calls)
                self.assertEqual(sim.metrics['handoffs'], 0)
                self.assertEqual(sim._transfer_tokens, {})

    def test_second_preflight_rejection_clears_all_first_preflight_tokens(self):
        sim = KnowledgeSimulation.__new__(KnowledgeSimulation)
        sim.elapsed, sim.knowledge_mode = 10, 'required'
        sim.tasks, sim.jobs, sim.assists, sim.robots = {}, {}, {}, {}
        for donor, receiver, task_id in [(1, 2, 'T1'), (3, 4, 'T2')]:
            sim.tasks[task_id] = {'task_id': task_id, 'goods_id': 'G'+task_id,
                                  'status': 'carrying', 'robot_id': donor}
            sim.jobs[receiver] = {'start': 0, 'assist_donor': donor, 'task_id': task_id,
                'parts': [{'end': 11, 'marker': 'handoff_received', 'task_id': task_id}]}
            sim.assists[donor] = {'receiver': receiver, 'task_id': task_id}
            sim.robots[receiver] = {'robot_id': receiver, 'carrying': False,
                                    'cargo_id': None, 'task_id': task_id}
        def decide(donor, task, receiver):
            if donor == 3: raise DecisionRejected('second graph preflight refused')
            return {'seq': 7}
        sim._decision = decide
        with self.assertRaisesRegex(DecisionRejected, 'second graph'): sim.step()
        self.assertEqual(sim._transfer_tokens, {})
        self.assertEqual(sim.elapsed, 10)

    def test_changed_receiver_between_authorization_and_marker_never_transfers_cargo(self):
        sim, donor, receiver = self.prepare_transfer()
        task_id = sim.assists[donor]['task_id']
        goods = sim.tasks[task_id]['goods_id']
        job = sim.jobs[receiver]
        sim._transfer_tokens[(donor, receiver, task_id, sim.elapsed)] = {'seq': 7}
        sim.assists[donor]['receiver'] = 1 if receiver != 1 else 2
        before = deepcopy((sim.goods, sim.tasks[task_id], sim.robots[donor], sim.robots[receiver]))
        with self.assertRaises(DecisionRejected):
            sim.marker(sim.robots[receiver], job, 'handoff_received', task_id)
        self.assertEqual(before, (sim.goods, sim.tasks[task_id], sim.robots[donor], sim.robots[receiver]))
        self.assertEqual(sim.goods[goods], f'ROBOT-{donor}')

    def test_restore_rejects_other_mode_and_source_without_installing_state(self):
        sim = self.make()
        state = sim.checkpoint()
        for field, value in [('mode', 'rules'), ('source', '0'*64)]:
            with self.subTest(field=field):
                corrupt = deepcopy(state)
                corrupt['knowledge_runtime']['identity'][field] = value
                before = self.snapshot(sim)
                with self.assertRaisesRegex(ValueError, 'mode/source'): sim.restore(corrupt)
                self.assertEqual(before, self.snapshot(sim))

    def test_inflight_handoff_restore_requeries_history_and_transfers_exactly_once(self):
        sim, donor, receiver = self.prepare_transfer()
        saved = sim.checkpoint()
        original_digest = digest(saved)
        task_id = sim.assists[donor]['task_id']
        goods = sim.tasks[task_id]['goods_id']
        provider = SavedHistoryFixture(sim.pending_events)
        restored = self.make()
        restored.restore(saved)
        restored.history_provider = provider
        self.assertEqual(restored._transfer_tokens, {})
        restored.step()
        self.assertEqual(provider.calls, 1)
        self.assertEqual(restored.metrics['handoffs'], 1)
        self.assertEqual(restored.knowledge_stats['approved_transfers'], 1)
        self.assertEqual(restored.tasks[task_id]['robot_id'], receiver)
        self.assertEqual(restored.goods[goods], f'ROBOT-{receiver}')
        restored.assert_inventory()
        restored.step()
        self.assertEqual(provider.calls, 1)
        self.assertEqual(restored.metrics['handoffs'], 1)
        self.assertEqual(digest(saved), original_digest)

    def test_graph_timeout_prevents_actual_planning_without_rules_fallback(self):
        sim = self.make()
        helper = runtime_fixture.RuntimeKnowledgeTests(methodName='runTest')
        donor = helper.carrying(sim)
        before = self.snapshot(sim)
        with patch.object(sim.history_provider, 'context', side_effect=TimeoutError('bounded Neo4j timeout')):
            with self.assertRaises(TimeoutError): sim.request_assistance(donor)
        self.assertEqual(before, self.snapshot(sim))
        self.assertEqual(sim.knowledge_stats['approved_plans'], 0)
        self.assertEqual(sim.knowledge_stats['rejected'], 1)

    def test_required_runner_rejects_journal_only_before_creating_output(self):
        with tempfile.TemporaryDirectory(dir=self.root/'tests') as folder:
            output = Path(folder)/'not-created'
            args = build_parser().parse_args(['--scene', str(self.root/'data/layouts/warehouse100-wide-v2.json'),
                '--dataset', str(self.root/'docs/dataset-contract/sample-input'), '--output', str(output),
                '--run-id', 'no-fallback-unit', '--knowledge-mode', 'required', '--knowledge-backend', 'journal'])
            with self.assertRaisesRegex(ValueError, 'graph writer'): run_live(args)
            self.assertFalse(output.exists())

    def test_failed_external_measurement_is_recorded_and_resume_does_not_reinject_it(self):
        with tempfile.TemporaryDirectory(dir=self.root/'tests') as folder:
            folder = Path(folder)
            config = folder/'config.json'
            config.write_text(json.dumps({'charging_policy': 'none'}), encoding='utf-8')
            disturbance = folder/'disturbance.json'
            disturbance.write_text(json.dumps([{'kind': 'soc', 'at': 0, 'robot': 'carrying',
                'soc': 15, 'selection_timeout': 1800}]), encoding='utf-8')
            dataset = self.root/'data/examples/warehouse-v5'
            if not dataset.is_dir(): dataset = self.root/'docs/dataset-contract/sample-input'
            output = folder/'run'
            args = build_parser().parse_args(['--scene', str(self.root/'data/layouts/warehouse100-wide-v2.json'),
                '--dataset', str(dataset), '--output', str(output), '--run-id', 'outage-recovery-independent',
                '--knowledge-mode', 'required', '--knowledge-backend', 'neo4j', '--commit-seconds', '5',
                '--until', '1800', '--max-wall-seconds', '60', '--config-json', str(config),
                '--disturbances-json', str(disturbance)])
            factory = partial(KnowledgeSimulation, commit_seconds=5, knowledge_mode='required')
            with patch('backend.knowledge_runtime.provider.GraphHistoryProvider', UnavailableHistory), \
                    patch('backend.query_v2.service_fragments.FragmentQueryService.from_env',
                          return_value=SimpleNamespace(close=lambda: None)), \
                    redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
                result = run_live(args, simulation_class=factory, config_class=Config,
                    mirror_class=JournalTestMirror, graph_auditor=lambda store, sim: {})
            self.assertEqual(result, 2)
            report = json.loads((output/'results.json').read_text(encoding='utf-8'))
            self.assertEqual(report['status'], 'failed')
            attempt = report['disturbances'][0]
            self.assertFalse(attempt['success'])
            self.assertEqual(attempt['index'], 0)
            self.assertIn('explicit_operator_reconciliation_required', attempt['resolution'])
            state = read_checkpoint(output/'checkpoint.json')
            self.assertEqual(state['experiment']['applied'], report['disturbances'])
            measured = next(r['state'] for r in state['robots'] if r['state']['robot_id']==attempt['owner'])
            self.assertEqual(measured['battery'], 15)
            self.assertEqual(state['knowledge_runtime']['stats']['approved_plans'], 0)
            with closing(sqlite3.connect(output/'journal.sqlite')) as db:
                commits = db.execute('SELECT count(*) FROM commits').fetchone()[0]
            args.resume = True
            with self.assertRaisesRegex(RuntimeError, 'reconciliation'), \
                    redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
                run_live(args, simulation_class=factory, config_class=Config,
                    mirror_class=JournalTestMirror, graph_auditor=lambda store, sim: {})
            with closing(sqlite3.connect(output/'journal.sqlite')) as db:
                self.assertEqual(db.execute('SELECT count(*) FROM commits').fetchone()[0], commits)


if __name__ == '__main__':
    unittest.main()
