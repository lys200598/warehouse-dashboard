import argparse
import asyncio
import gzip
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

from ops import load_harness as h
from ops.load_probe import LoadProbeMiddleware


class Handler(BaseHTTPRequestHandler):
    def do_POST(self):
        body = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
        if body.get('sleep'):
            time.sleep(body['sleep'])
        raw = json.dumps({'run_id': body.get('run_id'), 'robot_id': body.get('robot_id')}).encode()
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(raw)))
        self.send_header('X-Warehouse-Service-Ms', '0.7')
        self.send_header('X-Warehouse-Queue-Ms', '0.2')
        self.end_headers()
        self.wfile.write(raw)

    def log_message(self, *args):
        pass


class LoadTests(unittest.TestCase):
    def test_templates_and_redaction(self):
        self.assertEqual(h.substitute({'robot': '{{client_id}}', 'name': 'r{{client_id}}'}, {'client_id': 3}),
                         {'robot': 3, 'name': 'r3'})
        self.assertEqual(h.redact({'authorization': 'secret', 'nested': [{'token': 'abc'}]}),
                         {'authorization': '[REDACTED]', 'nested': [{'token': '[REDACTED]'}]})

    def test_metric_and_equal_interval_edges(self):
        self.assertEqual(h.peak([(1, 3), (3, 5), (2, 4)]), 2)
        values = h.metrics([1, 2, 500, 501])
        self.assertEqual(values['over_500ms'], 1)
        self.assertEqual(values['p99_ms'], 501)
        self.assertIsNone(h.metrics([])['max_ms'])

    def test_identity_headers_not_logged(self):
        service = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        thread = threading.Thread(target=service.serve_forever, daemon=True)
        thread.start()
        try:
            import http.client
            connection = http.client.HTTPConnection('127.0.0.1', service.server_port)
            case = {'name': 'self', 'path': '/', 'body': {'robot_id': '{{client_id}}', 'run_id': '{{run_id}}'},
                    'headers_env': {'Authorization': 'WAREHOUSE_AGENT_AUTH_{client_id}'}}
            with patch.dict('os.environ', {'WAREHOUSE_AGENT_AUTH_2': 'Bearer SUPER_PRIVATE'}):
                row = h.request_once(connection, case, {'client_id': 2, 'request_index': 1, 'run_id': 'run'}, 'r1')
            self.assertEqual(row['outcome'], 'ok')
            self.assertEqual(row['queue_ms'], .2)
            self.assertNotIn('SUPER_PRIVATE', json.dumps(row))
            self.assertEqual(row['response']['robot_id'], 2)
            self.assertGreaterEqual(row['http_phases']['body_complete_or_error_ns'], row['http_phases']['start_ns'])
            connection.close()
        finally:
            service.shutdown()
            service.server_close()

    def test_invalid_workload(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / 'workload.json'
            path.write_text(json.dumps({'version': 'warehouse-load-v1', 'cases': [
                {'name': 'bad', 'path': '//other/path'}]}))
            with self.assertRaises(ValueError):
                h.read_workload(path)

    def test_redacts_echoed_credentials(self):
        self.assertEqual(h.redact_known_values({'message': 'Bearer PRIVATE leaked'}, ['Bearer PRIVATE', 'PRIVATE']),
                         {'message': '[REDACTED] leaked'})

    def test_probe_records_real_intervals(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / 'server.jsonl'
            async def app(scope, receive, send):
                await receive()
                await asyncio.sleep(.01)
                await send({'type': 'http.response.start', 'status': 200})
                await send({'type': 'http.response.body', 'body': b'{}'})
            probe = LoadProbeMiddleware(app, output=path, source_identity={'run_id': 'x'})
            async def once(ident):
                scope = {'type': 'http', 'path': '/api/knowledge/query', 'headers': [(b'x-probe-id', ident.encode())]}
                async def receive():
                    return {'type': 'http.request', 'body': b'{}'}
                async def send(value):
                    pass
                await probe(scope, receive, send)
            async def run():
                await asyncio.gather(once('1'), once('2'))
            asyncio.run(run())
            probe.close()
            rows = list(h.records(path))
            api = [r for r in rows if r['kind'] == 'api']
            self.assertEqual(h.peak((r['start_ns'], r['end_ns']) for r in api), 2)
            self.assertTrue(all(r['body_send_completed'] for r in api))
            self.assertTrue(json.loads(Path(str(path) + '.status.json').read_text())['complete'])

    def test_analyze_does_not_hide_failures(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / 'identity.json').write_text('{}')
            (root / 'run.json').write_text(json.dumps({'complete': True, 'configured_clients': 1}))
            with gzip.open(root / 'client-0001.jsonl.gz', 'wt') as out:
                for i, outcome in enumerate(('ok', 'transport_error', 'load_generator_busy')):
                    out.write(json.dumps({'request_id': str(i), 'client_id': 1, 'outcome': outcome,
                                          'response': {'run_id': 'x'}, 'elapsed_ms': 4 if outcome == 'ok' else None,
                                          'start_ns': 1 if outcome == 'ok' else None,
                                          'end_ns': 5 if outcome == 'ok' else None}) + '\n')
            args = argparse.Namespace(input=root, output=root / 'analysis.json', validator=None,
                                      require_run='x', server_trace=None)
            self.assertEqual(h.analyze(args), 2)
            result = json.loads(args.output.read_text())
            self.assertEqual(result['planned_arrivals'], 3)
            self.assertFalse(result['million_prefix_qualified'])
            self.assertFalse(result['server_peak_qualified'])
            self.assertIsNone(result['actual_server_api_peak'])

    def _run_process_http(self, clients=3, mode='closed', sleep=0):
        ThreadingHTTPServer.request_queue_size = 256
        service = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        thread = threading.Thread(target=service.serve_forever, daemon=True)
        thread.start()
        try:
            with tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                workload = root / 'workload.json'
                workload.write_text(json.dumps({'version': 'warehouse-load-v1', 'cases': [
                    {'name': 'self', 'path': '/api/knowledge/query', 'query_class': 'light',
                     'body': {'robot_id': '{{client_id}}', 'run_id': '{{run_id}}', 'sleep': sleep}}]}))
                result = h.main(['run', '--base-url', f'http://127.0.0.1:{service.server_port}',
                    '--workload', str(workload), '--output', str(root / 'run'), '--session', 'test',
                    '--run-id', 'real-http', '--clients', str(clients), '--processes', str(clients),
                    '--duration', '20', '--requests', '4', '--mode', mode, '--interval', '.01'])
                self.assertEqual(result, 0)
                meta = json.loads((root / 'run' / 'run.json').read_text())
                self.assertEqual(len(meta['unique_client_pids']), clients)
                rows = [row for path in (root / 'run').glob('client-*.jsonl.gz') for row in h.records(path)]
                self.assertEqual(len(rows), clients * 4)
                if mode == 'closed':
                    self.assertTrue(all(row['outcome'] == 'ok' for row in rows))
                else:
                    self.assertTrue(any(row['outcome'] == 'load_generator_busy' for row in rows))
        finally:
            service.shutdown()
            service.server_close()

    def test_spawn_processes_real_http(self):
        self._run_process_http()

    def test_slow_periodic_records_missed_arrivals(self):
        self._run_process_http(clients=2, mode='periodic', sleep=.08)

    @unittest.skipUnless(os.environ.get('WAREHOUSE_TEST_100') == '1', 'opt-in 100 real OS processes')
    def test_hundred_os_processes(self):
        self._run_process_http(clients=100)


if __name__ == '__main__':
    unittest.main()
