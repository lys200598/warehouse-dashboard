"""Protocol tests exercise real HTTP and spawned processes; no graph claim."""
from concurrent.futures import ThreadPoolExecutor
import json
import gzip
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from types import SimpleNamespace
from urllib.error import HTTPError

from backend.agent_runtime import AgentKnowledgeService, AgentHttpServer, JournalBackend, AgentFleet, Neo4jBackend
from backend.agent_runtime.fleet import _rpc
from backend.agent_runtime.service import safe_exception


def observation(robot=1, seq=1, sim_time=0, canonical_seq=1):
    return {'run_id': 'test-run', 'robot_id': robot, 'seq': seq, 'sim_time': sim_time,
            'committed_seq': canonical_seq, 'state': {'robot_id': robot, 'stage': 'idle', 'soc': 80},
            'events': [{'kind': 'observation_started', 'sim_time': sim_time}]}


class AgentProtocolTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.service = AgentKnowledgeService(run_id='test-run', robot_ids=[1, 2], backend=JournalBackend(),
            journal_path=Path(self.tmp.name)/'journal.sqlite', batch_wait=.01)
        self.http = AgentHttpServer(self.service, telemetry_path=Path(self.tmp.name)/'http.jsonl')

    def tearDown(self):
        self.http.close(); self.service.close(); self.tmp.cleanup()

    def test_real_http_own_write_then_scoped_query(self):
        result = _rpc(self.http.endpoint, '/api/agent/observation', self.service.tokens[1], observation(), 5, 'one')
        self.assertEqual(result['seq'], 1)
        result = _rpc(self.http.endpoint, '/api/agent/query', self.service.tokens[1],
            {'run_id': 'test-run', 'robot_id': 1, 'at_seq': 1}, 5, 'two')
        self.assertEqual(result['context']['observations'][0], observation())
        self.assertFalse(result['action_applied'])
        self.assertEqual(result['backend'], 'journal-only-not-neo4j')
        with self.http.drained_condition:
            self.assertTrue(self.http.drained_condition.wait_for(lambda: self.http.active_threads == 0, timeout=2))
        self.http.telemetry_queue.join()
        lines = [json.loads(line) for line in (Path(self.tmp.name)/'http.jsonl').read_text().splitlines()]
        self.assertEqual({row['request_id'] for row in lines if row['kind'] == 'api'}, {'one', 'two'})
        self.assertNotIn(self.service.tokens[1], (Path(self.tmp.name)/'http.jsonl').read_text())

    def test_credential_cannot_write_or_query_another_robot(self):
        for endpoint, body in [('/api/agent/observation', observation(2)),
                               ('/api/agent/query', {'run_id': 'test-run', 'robot_id': 2})]:
            with self.assertRaises(HTTPError) as caught:
                _rpc(self.http.endpoint, endpoint, self.service.tokens[1], body, 5, 'forged')
            self.assertEqual(caught.exception.code, 403)
        self.assertEqual(self.service.head(2), 0)

    def test_duplicate_idempotence_and_conflict(self):
        self.service.submit(1, observation())
        self.service.submit(1, observation())
        bad = observation(); bad['state']['soc'] = 50
        with self.assertRaises(ValueError): self.service.submit(1, bad)
        self.assertEqual(self.service.head(1), 1)
        self.assertEqual(self.service.metrics['duplicates'], 1)

    def test_gap_backward_watermark_and_bool_identity_rejected(self):
        with self.assertRaises(ValueError): self.service.submit(1, observation(seq=2))
        self.service.submit(1, observation(sim_time=3, canonical_seq=2))
        with self.assertRaises(ValueError): self.service.submit(1, observation(seq=2, sim_time=2, canonical_seq=3))
        bad = observation(); bad['robot_id'] = True
        with self.assertRaises(PermissionError): self.service.submit(1, bad)

    def test_bad_payload_cannot_assert_custody_or_foreign_events(self):
        bad = observation(); bad['events'] = [{'kind': 'cargo_transferred', 'robot_id': 2, 'sim_time': 0}]
        with self.assertRaises(ValueError): self.service.submit(1, bad)
        bad = observation(); bad['custody_owner'] = 'ROBOT-2'
        with self.assertRaises(ValueError): self.service.submit(1, bad)
        bad = observation(); bad['state']['soc'] = float('nan')
        with self.assertRaises(ValueError): self.service.submit(1, bad)

    def test_future_and_uncommitted_query_fails(self):
        self.service.submit(1, observation())
        with self.assertRaises(ValueError):
            self.service.query(1, {'run_id': 'test-run', 'robot_id': 1, 'at_seq': 2})

    def test_parallel_clients_are_batched(self):
        with ThreadPoolExecutor(max_workers=2) as pool:
            list(pool.map(lambda r: self.service.submit(r, observation(r)), [1, 2]))
        self.assertEqual(self.service.metrics['accepted'], 2)
        self.assertGreaterEqual(self.service.metrics['max_batch'], 2)


class RecoveryTests(unittest.TestCase):
    def test_legacy_futures_timeout_is_normalized_to_builtin(self):
        from concurrent.futures import Future
        class LegacyFuturesTimeout(Exception): pass
        with tempfile.TemporaryDirectory() as directory:
            service = AgentKnowledgeService(run_id='test-run', robot_ids=[1], backend=JournalBackend(),
                journal_path=Path(directory)/'journal.sqlite')
            try:
                with patch('backend.agent_runtime.service.FuturesTimeoutError',LegacyFuturesTimeout):
                    with patch.object(Future,'result',side_effect=LegacyFuturesTimeout('legacy deadline')):
                        with self.assertRaises(TimeoutError) as raised:
                            service.submit(1,observation())
                self.assertIs(type(raised.exception),TimeoutError)
                self.assertIsInstance(raised.exception.__cause__,LegacyFuturesTimeout)
            finally:
                service.close()

    def test_query_budget_timeout_is_bounded(self):
        import time
        class SlowQueryBackend(JournalBackend):
            def query(self, *args):
                time.sleep(.1)
                return super().query(*args)
        with tempfile.TemporaryDirectory() as directory:
            service = AgentKnowledgeService(run_id='test-run', robot_ids=[1, 2], backend=SlowQueryBackend(),
                journal_path=Path(directory)/'journal.sqlite', query_slots=1, query_wait_timeout=.02)
            for r in [1, 2]: service.submit(r, observation(r))
            with ThreadPoolExecutor(max_workers=2) as pool:
                first = pool.submit(service.query, 1, {'run_id': 'test-run', 'robot_id': 1})
                time.sleep(.01)
                with self.assertRaises(TimeoutError):
                    service.query(2, {'run_id': 'test-run', 'robot_id': 2})
                self.assertGreaterEqual(first.result()['metrics']['backend_query_ms'], 90)
            self.assertEqual(service.metrics['query_queue_rejected'], 1)
            service.close()

    def test_ack_timeout_never_reports_success(self):
        import time
        class SlowBackend(JournalBackend):
            def write(self, rows):
                time.sleep(.05)
                super().write(rows)
        with tempfile.TemporaryDirectory() as directory:
            service = AgentKnowledgeService(run_id='test-run', robot_ids=[1], backend=SlowBackend(),
                journal_path=Path(directory)/'journal.sqlite', request_timeout=.01, batch_wait=0.)
            with self.assertRaises(TimeoutError): service.submit(1, observation())
            # The durable write may complete after the client deadline. It is
            # recorded, never silently interpreted as an acknowledged action.
            service.writer.join(timeout=.08)
            self.assertEqual(service.head(1), 1)
            service.close()

    def test_graph_failure_keeps_durable_pending_and_replays(self):
        class BrokenBackend(JournalBackend):
            name = 'broken-test-backend'
            def write(self, rows): raise RuntimeError('graph unavailable')
        class RecoveredBackend(JournalBackend): name = 'broken-test-backend'
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)/'journal.sqlite'
            service = AgentKnowledgeService(run_id='test-run', robot_ids=[1], backend=BrokenBackend(), journal_path=path)
            with self.assertRaises(RuntimeError): service.submit(1, observation())
            self.assertEqual(service.db.execute('SELECT mirrored FROM observations').fetchone()[0], 0)
            with self.assertRaises(RuntimeError): service.submit(1, observation(seq=2))
            service.close()
            recovered = AgentKnowledgeService(run_id='test-run', robot_ids=[1], backend=RecoveredBackend(), journal_path=path)
            self.assertEqual(recovered.query(1, {'run_id': 'test-run', 'robot_id': 1})['at_seq'], 1)
            recovered.close()


class Neo4jApiContractTests(unittest.TestCase):
    def test_fleet_closes_service_even_when_observer_close_fails(self):
        calls=[]
        class Http:
            thread=SimpleNamespace(is_alive=lambda:False)
            def close(self):
                calls.append('http');raise OSError('observer status disk full')
        class Service:
            writer=SimpleNamespace(is_alive=lambda:False)
            backend=SimpleNamespace(name='test-backend')
            metrics={}
            def close(self):calls.append('service')
        with tempfile.TemporaryDirectory() as directory:
            fleet=object.__new__(AgentFleet)
            fleet.closed=False;fleet.connections={};fleet.processes={};fleet.pids={}
            fleet.robot_ids=();fleet.failed=False;fleet.latest_contexts={};fleet.output=Path(directory)
            fleet.http=Http();fleet.service=Service()
            with self.assertRaisesRegex(OSError,'observer status disk full'):fleet.close()
            self.assertEqual(calls,['http','service'])
            closed=json.loads((Path(directory)/'agent-closed-status.json').read_text())
            self.assertTrue(closed['closed'])
            self.assertFalse(closed['writer_alive'])

    def test_mirror_closes_fleet_and_base_when_status_io_fails(self):
        from backend.agent_runtime.integration import agent_mirror_class
        calls=[]
        class BaseMirror:
            def close(self): calls.append('base')
        class Fleet:
            def status(self):return {'agents':100}
            def close(self):calls.append('fleet')
        with tempfile.TemporaryDirectory() as directory:
            mirror=agent_mirror_class(BaseMirror,output_dir=directory)()
            mirror.agent_fleet=Fleet()
            with patch.object(Path,'write_text',side_effect=OSError('full disk')):
                with self.assertRaises(OSError):mirror.close()
        self.assertEqual(calls,['fleet','base'])

    def test_exception_diagnostics_omit_arguments_and_locals(self):
        secret = 'neo4j://user:do-not-record-this-password@host'
        try:
            raise RuntimeError(secret)
        except RuntimeError as exc:
            value = safe_exception(exc)
        self.assertEqual(value['exception_type'], 'RuntimeError')
        self.assertTrue(value['frames'])
        self.assertNotIn(secret, json.dumps(value))

    def test_managed_transaction_only_receives_query_strings(self):
        calls = []
        def unit_of_work(*, timeout):
            def decorate(fn):
                fn.timeout = timeout
                return fn
            return decorate
        class Result(list):
            def consume(self): pass
        class Tx:
            def run(inner, query, **kwargs):
                self.assertIsInstance(query, str, 'official tx.run rejects neo4j.Query objects')
                calls.append(query)
                if len(calls) == 1:
                    return Result([{'key': 'test-run:1:1', 'canonical_payload': json.dumps(observation()['state']), 'old_sha': None}])
                return Result()
        class Session:
            def __enter__(self): return self
            def __exit__(self, *args): pass
            def execute_write(inner, callback):
                self.assertEqual(callback.timeout, 7.)
                return callback(Tx())
        backend = object.__new__(Neo4jBackend)
        backend.driver = SimpleNamespace(session=lambda **kwargs: Session())
        backend.database, backend.timeout = 'neo4j', 7.
        def forbidden_query(*args, **kwargs):
            raise AssertionError('managed transaction must not construct a Query wrapper')
        backend.Query = forbidden_query
        with patch.dict('sys.modules', {'neo4j': SimpleNamespace(unit_of_work=unit_of_work)}):
            backend.write([observation()])
        self.assertEqual(len(calls), 2)


@unittest.skipUnless(os.environ.get('WAREHOUSE_AGENT_TEST_BATCH') and os.environ.get('NEO4J_PASSWORD'),
                     'set WAREHOUSE_AGENT_TEST_BATCH to an actual canonical batch and Neo4j credentials for live graph integration')
class Neo4jOptionalIntegrationTests(unittest.TestCase):
    def test_real_graph_appends_own_observation_and_queries_canonical_context(self):
        # This intentionally appends a new observation to the explicitly
        # selected existing run. It never fabricates/deletes canonical facts.
        source = json.loads(Path(os.environ['WAREHOUSE_AGENT_TEST_BATCH']).read_text(encoding='utf-8'))
        state = source['robots'][0]
        backend = Neo4jBackend.from_env()
        try:
            with backend.driver.session(database=backend.database) as session:
                head = session.run('MATCH (o:WHAgentObservation {run_id:$run,robot_id:$robot}) RETURN coalesce(max(o.agent_seq),0) AS seq',
                    run=source['run_id'], robot=state['robot_id']).single()['seq']
            body = {'run_id': source['run_id'], 'robot_id': state['robot_id'], 'seq': head+1,
                    'sim_time': source['sim_time'], 'committed_seq': source['seq'], 'state': state, 'events': []}
            backend.write([body])
            value = backend.query(source['run_id'], state['robot_id'], head+1, 1)
            self.assertEqual(value['observations'][0], body)
            self.assertEqual(value['canonical_context']['robots'][0]['robot'], state)
        finally:
            backend.close()


class SpawnFleetTests(unittest.TestCase):
    def test_actual_processes_publish_query_cadence_and_fail_closed(self):
        with tempfile.TemporaryDirectory() as directory:
            with AgentFleet(run_id='spawn-test', robot_ids=[1, 2], backend=JournalBackend(),
                            output_dir=directory, timeout=10, startup_timeout=30) as fleet:
                robots = {r: {'robot_id': r, 'stage': 'idle', 'soc': 80} for r in (1, 2)}
                results = fleet.publish(robots, 0, 1)
                self.assertEqual(len(set(row['pid'] for row in results)), 2)
                self.assertNotIn(os.getpid(), [row['pid'] for row in results])
                self.assertTrue(all(row['wrote'] and row['queried'] for row in results))
                results = fleet.publish(robots, 1, 2)
                self.assertFalse(any(row['wrote'] or row['queried'] for row in results))
                robots[1]['stage'] = 'to_source'
                results = fleet.publish(robots, 2, 3)
                self.assertTrue(results[0]['queried'])
                self.assertFalse(results[1]['queried'])
                client_rows = [json.loads(row) for row in (Path(directory)/'agent-0001-http.jsonl').read_text().splitlines()]
                self.assertEqual(len(client_rows), 4)
                self.assertTrue(all(row['status'] == 200 and row['body_complete'] for row in client_rows))
                self.assertTrue(all(row['start_ns'] <= row['headers_ns'] <= row['body_end_ns'] <= row['parse_end_ns'] <= row['end_ns'] for row in client_rows))
                from backend.agent_runtime.service import checksum
                with gzip.open(Path(directory)/'agent-0001-responses.jsonl.gz', 'rt', encoding='utf-8') as stream:
                    full_records = [json.loads(row) for row in stream]
                self.assertEqual(len(full_records), 4)
                self.assertTrue(all(row['response_sha256'] == checksum(row['response']) for row in full_records))
                self.assertTrue(all(row['request_sha256'] == checksum(row['request']) for row in full_records))
                self.assertTrue(all(row['observer_after_http'] for row in full_records))
                self.assertEqual(full_records[1]['response']['context']['observations'][0]['state'],
                                 {'robot_id': 1, 'stage': 'idle', 'soc': 80})
                fleet.processes[1].terminate(); fleet.processes[1].join()
                with self.assertRaises(RuntimeError): fleet.publish(robots, 3, 4)
                self.assertTrue(fleet.failed)
                self.assertFalse(json.loads((Path(directory)/'agent-failure.json').read_text())['next_step_authorized'])
            closed = json.loads((Path(directory)/'agent-closed-status.json').read_text())
            self.assertTrue(closed['closed'])
            self.assertTrue(closed['all_owned_processes_stopped'])
            self.assertEqual(closed['live_processes'], 0)
            self.assertFalse(closed['http_service_alive'])
            self.assertFalse(closed['writer_alive'])


if __name__ == '__main__': unittest.main()
