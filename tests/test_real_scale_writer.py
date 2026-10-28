from copy import deepcopy
import unittest
from unittest.mock import patch
from pathlib import Path
import tempfile
import json
import hashlib
from types import SimpleNamespace

from backend.knowledge import make_batch, digest
from backend.real_scale import VECTOR_INDEX, VERSION, real_vector
from backend.real_scale.writer import (RealMirror, SELECTED_PARAMETERS, real_identity, main)
from backend.real_scale.writer import RealSimulation
from backend.real_scale.prepare import verify_graph
from backend.scheduling_v5.sim import Config
from backend.wide.scenarios import generate_scenario
from tests.test_real_scale_oracle import checkpoint, events
from tests.test_scheduler_v3_graph import Transaction


class Session:
    def __init__(self): self.calls = []
    def __enter__(self): return self
    def __exit__(self, *exc): pass
    def run(self, query):
        self.calls.append(query)
        return self
    def consume(self): pass


class RealWriterTests(unittest.TestCase):
    def test_same_transaction_adds_only_new_vector_property_and_real_label(self):
        cp = checkpoint(1); value = make_batch(cp, events(1))
        tx = Transaction(); RealMirror._apply(tx, [value])
        writes = [(q, p) for q, p in tx.calls if 'a.real_embedding=' in q]
        self.assertEqual(len(writes), 1)
        query, params = writes[0]
        self.assertIn('SET a:WHRealState', query)
        self.assertNotIn('a.embedding=', query)
        self.assertNotIn('WHScaleArchive', query)
        self.assertEqual(len(params['rows']), 100)
        self.assertEqual(params['rows'][0]['vector'], real_vector(cp['robots'][0]['state'], 1))
        self.assertEqual(params['rows'][0]['sha256'], digest(cp['robots'][0]['state']))
        charger = next(p['rows'] for q, p in tx.calls if 'MERGE (v:WHChargerState' in q)
        self.assertEqual(len(charger), 100)
        provenance = next(p for q, p in tx.calls if 'r.real_source_kind=' in q)
        self.assertEqual(provenance['version'], VERSION)
        self.assertEqual(provenance['checksum'], digest(value))

    def test_identical_replay_does_not_rewrite_vectors_or_provenance(self):
        value = make_batch(checkpoint(1), events(1)); tx = Transaction()
        RealMirror._apply(tx, [value]); count = len(tx.calls)
        RealMirror._apply(tx, [value])
        self.assertFalse(any('real_embedding=' in q or 'r.real_source_kind=' in q for q, _ in tx.calls[count:]))

    def test_bad_source_state_fails_before_parent_write(self):
        for edit in [lambda b: b['robots'].pop(), lambda b: b['robots'][0].update(battery=float('inf')),
                     lambda b: b['robots'][0].update(carrying='false')]:
            value = make_batch(checkpoint(1), events(1)); edit(value); tx = Transaction()
            with self.assertRaises(ValueError): RealMirror._apply(tx, [value])
            self.assertEqual(tx.calls, [])

    def test_new_index_targets_real_label_and_property_only(self):
        session = Session()
        driver = type('Driver', (), {'session': lambda self, **kw: session})()
        mirror = RealMirror(driver)
        with patch('backend.wide.graph.WideMirror.ensure_schema') as base:
            mirror.ensure_schema()
        base.assert_called_once()
        query = next(q for q in session.calls if 'CREATE VECTOR INDEX' in q)
        self.assertIn(VECTOR_INDEX, query)
        self.assertIn('WHRealState', query)
        self.assertIn('real_embedding', query)
        self.assertFalse(any('whscale_archive_vector' in q for q in session.calls))
        self.assertTrue(any('whreal_goods_event' in q for q in session.calls))

    def test_five_second_default_and_frozen_selected_policy(self):
        with patch('backend.real_scale.writer.run', return_value=0) as invoke:
            self.assertEqual(main(['--scene', 'scene', '--dataset', 'input', '--output', 'new', '--run-id', 'r']), 0)
        args = invoke.call_args.args[0]
        self.assertEqual(args.commit_seconds, 5)
        self.assertEqual(args.policy, 'port-guard')
        self.assertEqual(args.planner, 'original-sipp')
        self.assertEqual(SELECTED_PARAMETERS['port_queue_cap'], 6)
        self.assertNotEqual(real_identity(5), real_identity(10))

    def test_wrong_cadence_resume_refused_before_journal_open(self):
        old = {'real_scale': {'identity': real_identity(5)}}
        with patch('backend.real_scale.writer.read_checkpoint', return_value=old), \
             patch('backend.real_scale.writer.run') as invoke:
            with self.assertRaisesRegex(ValueError, 'before opening journal'):
                main(['--scene', 's', '--dataset', 'd', '--output', 'o', '--run-id', 'r',
                      '--resume', '--commit-seconds', '10'])
            invoke.assert_not_called()

    def test_real_wrapper_checkpoint_retains_full_selected_policy_and_cadence(self):
        root = Path(__file__).resolve().parents[1]
        scene = root/'data/layouts/warehouse100-wide-v2.json'
        with tempfile.TemporaryDirectory(dir=root/'data/local-check') as folder:
            data = Path(folder)/'input'
            generate_scenario(scene, data, active_slots=4, duration_seconds=120, slot_interval_seconds=3600)
            sim = RealSimulation(scene, data, 'real-wrapper-test', Config(), commit_seconds=5)
            state = sim.checkpoint()
            self.assertEqual(state['real_scale']['identity']['commit_seconds'], 5)
            self.assertEqual(sim.planner_name, 'original-sipp')
            self.assertEqual(sim.policy_parameters, SELECTED_PARAMETERS)
            self.assertIn('every 5s', sim.run_metadata['sampling'])
            self.assertNotIn('300s', sim.run_metadata['sampling'])
            restored = RealSimulation(scene, data, 'real-wrapper-test', Config(), checkpoint=state, commit_seconds=5)
            self.assertEqual(state['real_scale'], restored.checkpoint()['real_scale'])
            with self.assertRaisesRegex(ValueError, 'sampling/version/source'):
                RealSimulation(scene, data, 'real-wrapper-test', Config(), checkpoint=state, commit_seconds=10)

    def test_scale_verification_does_not_count_old_nodes_or_accept_wrong_vector_index(self):
        cp = checkpoint(4)
        prefix = hashlib.sha256()
        for seq in range(1, 5):
            prefix.update(f'{seq}:{digest({"seq": seq})}\n'.encode())
        oracle = SimpleNamespace(run_id=cp['run_id'], prefix=4,
            identity={'metadata': cp['run_metadata'], 'config': cp['config'],
                      'events': 5, 'task_versions': 4, 'slot_versions': 2, 'prefix_sha256': prefix.hexdigest()},
            batch_checksum=lambda seq: digest({'seq': seq}))
        index = {'name': VECTOR_INDEX, 'state': 'ONLINE', 'type': 'VECTOR',
                 'labelsOrTypes': ['WHRealState'], 'properties': ['real_embedding'],
                 'options': {'indexConfig': {'vector.dimensions': 32, 'vector.similarity_function': 'cosine'}}}

        class Mirror:
            corrupt_middle = False
            def query(self, query, params, timeout=30):
                if 'r.metadata_json AS metadata' in query:
                    return [{'metadata': json.dumps(cp['run_metadata']), 'config': json.dumps(cp['config']),
                        'version': VERSION, 'source_kind': 'actual_runtime_committed_state',
                        'source_run': cp['run_id'], 'vector_kind': '32d_structured_physical_time_identity_features_not_semantic', 'dims': 32}]
                if 's.checksum AS checksum' in query:
                    if 's.seq AS seq' in query:
                        return [{'seq': seq, 'checksum': 'f'*64 if self.corrupt_middle and seq == 2 else digest({'seq': seq})}
                                for seq in range(1, 5)]
                    return [{'checksum': digest({'seq': int(params['key'].split(':')[-1])})}]
                if 'AS commits' in query: return [{'commits': 4, 'bad': 0}]
                for label, count in [('WHEvent', 5), ('WHTaskState', 4), ('WHSlotState', 2), ('WHChargerState', 400)]:
                    if ':'+label in query: return [{'n': count}]
                raise AssertionError(query)

        class Store:
            mirror = Mirror()
            def head(self, run): return {'seq': 4, 'real_last_seq': 4}
            def counts(self, run, prefix):
                return {'states': 400, 'vectors': 400, 'payload_hashes': 400, 'robots': 100,
                    'first_seq': 1, 'last_seq': 4, 'min_dimensions': 32, 'max_dimensions': 32,
                    'archive_states': 0, 'derived_links': 0, 'vector_indexes': [index],
                    'unrelated_old_global_nodes': 1038500}
        with patch('backend.real_scale.benchmark.driver_identity', return_value={'test': True}):
            result = verify_graph(oracle, Store(), min_states=1)
            self.assertEqual(result['status'], 'passed')
            self.assertFalse(result['qualification']['million_actual_agent_states'])
            index['options']['indexConfig']['vector.similarity_function'] = 'COSINE'
            self.assertEqual(verify_graph(oracle, Store(), min_states=1)['status'], 'passed')
            index['options']['indexConfig']['vector.similarity_function'] = None
            self.assertFalse(verify_graph(oracle, Store(), min_states=1)['checks']['exact_real_vector_index_online'])
            index['options']['indexConfig']['vector.similarity_function'] = 'COSINE'
            Store.mirror.corrupt_middle = True
            changed = verify_graph(oracle, Store(), min_states=1)
            self.assertTrue(changed['checks']['first_last_source_batch_checksums'])
            self.assertFalse(changed['checks']['complete_graph_prefix_sha256_matches_journal'])
            Store.mirror.corrupt_middle = False
            index['labelsOrTypes'] = ['WHScaleArchive']
            bad = verify_graph(oracle, Store(), min_states=1)
            self.assertEqual(bad['status'], 'failed')
            self.assertFalse(bad['checks']['exact_real_vector_index_online'])


if __name__ == '__main__':
    unittest.main()
