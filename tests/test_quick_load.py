import argparse
from copy import deepcopy
from contextlib import redirect_stdout, redirect_stderr
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import io
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import threading
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from ops import quick_load as q


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def send(self, value, status=200):
        raw = json.dumps(value).encode()
        self.send_response(status)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def do_GET(self):
        self.server.paths.append(self.path)
        if self.path == '/api/health':
            return self.send({'run_id': 'test-only-fixture', 'knowledge': {'mirrored_seq': 7},
                              'graph_queries_enabled': self.server.graph})
        if self.path == '/api/knowledge/catalog':
            return self.send({'queries': {'self': 'fixture', 'global': 'fixture'}})
        self.send({'error': 'not found'}, 404)

    def do_POST(self):
        self.server.paths.append(self.path)
        request = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
        self.server.requests.append(request)
        if self.server.fail_after_preflight and len(self.server.requests) > 1:
            return self.send({'error': 'fixture deliberate failure'}, 503)
        if self.server.bad_shape:
            return self.send({'wrong_response_shape': True})
        robot = {'robot_id': request['robot_id']}
        from backend.knowledge import digest
        value = {'run_id': request['run_id'], 'seq': request['seq'],
                 'query_id': request['query_id'], 'result': {}}
        if request['query_id'] == 'self':
            value['result']['context'] = {'run_id': request['run_id'], 'seq': request['seq'],
                'graph_committed_seq': request['seq'], 'robots': [{
                    'robot_id': robot['robot_id'], 'robot': robot, 'robot_sha256': digest(robot)}]}
        self.send(value)


class QuickLoadTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(dir=q.ROOT/'tests', prefix='tmp-quick-')
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.config = q.load_config(q.ROOT/'config/pressure-test.json')

    def args(self, profile='quick', output=None):
        return argparse.Namespace(output=output or self.root/'result', server_trace=[])

    def server(self):
        class TestServer(ThreadingHTTPServer):
            request_queue_size = 256
        server = TestServer(('127.0.0.1', 0), Handler)
        server.paths, server.requests = [], []
        server.graph, server.bad_shape, server.fail_after_preflight = True, False, False
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        return server, f'http://127.0.0.1:{server.server_port}'

    def execute(self, args, base, profile='quick'):
        with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
            code = q.run(args, self.config, base, profile)
        return code, json.loads((args.output/'summary.json').read_text())

    def test_point_contract_and_boolean_seq_rejected(self):
        h = {'run_id': 'r', 'knowledge': {'mirrored_seq': 2}, 'graph_queries_enabled': True}
        self.assertEqual(q.point_from_health(h, self.config), ('r', 2))
        for seq in (True, 0, '2', None):
            h['knowledge']['mirrored_seq'] = seq
            with self.assertRaises(q.SetupError):
                q.point_from_health(h, self.config)

    def test_disabled_or_missing_point(self):
        for health in ({}, {'graph_queries_enabled': False}):
            with self.assertRaises(q.SetupError):
                q.point_from_health(health, self.config)

    def test_degraded_service_refused(self):
        with self.assertRaises(q.SetupError):
            q.point_from_health({'status': 'degraded'}, self.config)

    def test_field_mapping_is_configurable(self):
        cfg = {**self.config, 'run_id_path': ['active', 'id'], 'seq_path': ['active', 'seq']}
        value = {'active': {'id': 'renamed', 'seq': 4}, 'graph_queries_enabled': True}
        self.assertEqual(q.point_from_health(value, cfg), ('renamed', 4))

    def test_no_write_workload_or_new_post_bypass(self):
        for name in q.PROFILES:
            spec = q.workload(self.config, name, 'r', 7)
            self.assertEqual(spec['cases'][0]['kind'], 'read')
            self.assertEqual(spec['cases'][0]['body']['seq'], 7)
            path = self.root/(name+'.json')
            q.save(path, spec)
            q.harness.read_workload(path)
        cfg = {**self.config, 'query_path': '/new-post-query'}
        path = self.root/'changed.json'
        q.save(path, q.workload(cfg, 'quick', 'r', 7))
        with self.assertRaises(ValueError):
            q.harness.read_workload(path)

    def test_invalid_config_version(self):
        path = self.root/'config.json'
        q.save(path, {**self.config, 'version': 'other'})
        with self.assertRaises(q.SetupError):
            q.load_config(path)

    def test_windows_console_uses_utf8(self):
        class Stream(io.StringIO):
            def reconfigure(self, **options):
                self.options = options
        out, err = Stream(), Stream()
        with patch.object(q.sys, 'platform', 'win32'), patch.object(q.sys, 'stdout', out), \
                patch.object(q.sys, 'stderr', err):
            q.configure_console()
        self.assertEqual(out.options['encoding'], 'utf-8')
        self.assertEqual(err.options['encoding'], 'utf-8')

    def test_optimized_mode_does_not_disable_validation(self):
        with patch.object(q.sys, 'flags', SimpleNamespace(optimize=1)), redirect_stderr(io.StringIO()):
            self.assertEqual(q.main(['--profile', 'quick']), 1)

    def test_refuse_url_credentials(self):
        with patch.object(q.sys.stdin, 'isatty', return_value=False), redirect_stderr(io.StringIO()):
            self.assertEqual(q.main(['--url', 'http://user:password@localhost:9999', '--profile', 'quick']), 1)

    def test_nonquick_requires_explicit_consent_before_network(self):
        with patch.object(q.sys.stdin, 'isatty', return_value=False), patch.object(q, 'fetch_json') as fetch, \
                redirect_stderr(io.StringIO()):
            self.assertEqual(q.main(['--url', 'http://127.0.0.1:9', '--profile', 'heavy']), 1)
        fetch.assert_not_called()

    def test_interactive_menu_defaults_to_small_check(self):
        with patch.object(q.sys.stdin, 'isatty', return_value=True), patch('builtins.input', side_effect=['', '']), \
                patch.object(q, 'run', return_value=0) as run, redirect_stdout(io.StringIO()):
            self.assertEqual(q.main([]), 0)
        self.assertEqual(run.call_args.args[3], 'quick')

    def test_existing_output_not_overwritten(self):
        output = self.root/'existing'; output.mkdir(); (output/'keep').write_text('original')
        with self.assertRaises(FileExistsError):
            q.run(self.args(output=output), self.config, 'http://127.0.0.1:9', 'quick')
        self.assertEqual((output/'keep').read_text(), 'original')

    def test_disabled_graph_saves_friendly_report(self):
        server, base = self.server(); server.graph = False
        args = self.args(); code, summary = self.execute(args, base)
        self.assertEqual(code, 1)
        self.assertEqual(summary['status'], 'setup_or_tool_error')
        self.assertFalse(server.requests)
        self.assertIn('尚未启用', (args.output/'report.md').read_text())

    def test_wrong_shape_stops_before_load(self):
        server, base = self.server(); server.bad_shape = True
        code, summary = self.execute(self.args(), base)
        self.assertEqual(code, 1)
        self.assertEqual(len(server.requests), 1)
        self.assertEqual(summary['status'], 'setup_or_tool_error')
        self.assertIn('返回结构', summary['error'])

    def test_renamed_post_path_reported_without_request(self):
        server, base = self.server()
        self.config['query_path'] = '/new-post-query'
        code, summary = self.execute(self.args(), base)
        self.assertEqual(code, 1)
        self.assertEqual(server.requests, [])
        self.assertIn('只读白名单', summary['error'])

    def test_real_spawn_quick_keeps_qualification_false(self):
        server, base = self.server()
        args = self.args(); code, summary = self.execute(args, base)
        self.assertIn(code, (0, 2))  # Timing is not asserted on a shared machine.
        self.assertEqual(summary['status'], 'measured')
        self.assertEqual(summary['outcomes'], {'ok': 3})
        self.assertEqual(len(server.requests), 4)  # One explicit preflight.
        self.assertEqual({r['seq'] for r in server.requests}, {7})
        for key in ('server_peak_qualified', 'million_prefix_qualified',
                    'business_hundred_verified', 'independent_source_verified'):
            self.assertFalse(summary[key])
        self.assertTrue((args.output/'analysis.json').exists())
        self.assertFalse(any(p in server.paths for p in ('/api/control', '/api/agent/observation')))

    def test_later_http_failures_retained_without_retry(self):
        server, base = self.server(); server.fail_after_preflight = True
        code, summary = self.execute(self.args(), base)
        self.assertEqual(code, 2)
        self.assertEqual(summary['outcomes'], {'http_error': 3})
        self.assertEqual(len(server.requests), 4)
        self.assertFalse(summary['all_requests_under_500ms'])

    def test_interrupt_preserved_and_no_analysis(self):
        server, base = self.server()
        def interrupted(arguments, log):
            output = Path(arguments[arguments.index('--output')+1]);output.mkdir()
            q.save(output/'run.json', {'interrupted': True})
            return 2
        with patch.object(q, 'invoke', side_effect=interrupted):
            code, summary = self.execute(self.args(), base)
        self.assertEqual(code, 130)
        self.assertEqual(summary['status'], 'interrupted')

    @unittest.skipUnless(os.environ.get('WAREHOUSE_QUICK_LOAD_TEST_100') == '1', 'opt-in 100 process wizard test')
    def test_hundred_process_preset_on_fixture(self):
        server, base = self.server()
        code, summary = self.execute(self.args(), base, 'burst')
        self.assertIn(code, (0, 2))
        self.assertEqual(summary['outcomes'], {'ok': 200})
        self.assertEqual(len(server.requests), 201)
        self.assertEqual(summary['configured_clients'], 100)
        self.assertFalse(summary['server_peak_qualified'])
        self.assertFalse(summary['million_prefix_qualified'])

    @unittest.skipUnless(os.name == 'posix' and os.environ.get('WAREHOUSE_QUICK_LOAD_TEST_100') == '1',
                         'opt-in POSIX real 100-process interrupt test')
    def test_real_interrupt_drains_owned_clients(self):
        server, base = self.server()
        output = self.root/'interrupted-run'
        with (self.root/'interrupt.log').open('x') as log:
            child = subprocess.Popen([sys.executable, str(q.ROOT/'pressure_test.py'),
                '--url', base, '--profile', 'normal', '--yes', '--output', str(output)],
                cwd=q.ROOT, stdout=log, stderr=log, start_new_session=True)
            try:
                deadline = time.monotonic()+60
                while len(server.requests) < 4:
                    if child.poll() is not None or time.monotonic() > deadline:
                        self.fail('wizard did not reach its owned client load')
                    time.sleep(.02)
                child.send_signal(signal.SIGINT)
                self.assertEqual(child.wait(40), 130)
            finally:
                if child.poll() is None:
                    os.killpg(child.pid, signal.SIGTERM)
                    child.wait(10)
        summary = json.loads((output/'summary.json').read_text())
        meta = json.loads((output/'client/run.json').read_text())
        self.assertEqual(summary['status'], 'interrupted')
        self.assertTrue(meta['interrupted'])
        self.assertFalse(meta['complete'])
        self.assertEqual(len(meta['unique_client_pids']), 100)
        for pid in meta['unique_client_pids']:
            with self.assertRaises(ProcessLookupError):
                os.kill(pid, 0)


if __name__ == '__main__':
    unittest.main()
