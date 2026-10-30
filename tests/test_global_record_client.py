"""Real loopback HTTP tests; no third-party packages or Neo4j required."""
import json
import socket
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from backend.global_record_client import BufferFull, ClientError, GlobalRecordClient


class FakeService:
    def __init__(self):
        self.posts = []
        self.post_codes = []
        self.post_gate = threading.Event()
        self.post_gate.set()
        self.post_entered = threading.Event()
        self.health_gate = threading.Event()
        self.health_gate.set()
        self.health_entered = threading.Event()
        self.last_tick = -1
        self.auto_write = True
        self.errors = 0
        self.worker_alive = True
        self.health_code = 200
        self.redirect_hits = 0
        service = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *_args):
                pass

            def reply(self, code, body):
                raw = json.dumps(body).encode()
                try:
                    self.send_response(code)
                    if code == 307:
                        self.send_header('Location', '/redirected')
                    self.send_header('Content-Length', str(len(raw)))
                    self.end_headers()
                    self.wfile.write(raw)
                except (BrokenPipeError, ConnectionResetError):
                    pass

            def do_POST(self):
                if self.path == '/redirected':
                    service.redirect_hits += 1
                record = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
                service.posts.append((time.monotonic(), record))
                service.post_entered.set()
                service.post_gate.wait(10)
                code = service.post_codes.pop(0) if service.post_codes else 202
                if code == 0:  # uncertain request outcome / connection loss
                    self.connection.shutdown(socket.SHUT_RDWR)
                    self.connection.close()
                    return
                if code == 202 and service.auto_write:
                    service.last_tick = record['tick']
                self.reply(code, {'detail': 'fake response'})

            def do_GET(self):
                service.health_entered.set()
                service.health_gate.wait(10)
                self.reply(service.health_code, {'queue': {
                    'last_tick': service.last_tick, 'errors': service.errors,
                    'worker_alive': service.worker_alive}})

        self.server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever,
                                       kwargs={'poll_interval': 0.01}, daemon=True)
        self.thread.start()
        self.url = f'http://127.0.0.1:{self.server.server_port}'

    def close(self):
        self.post_gate.set()
        self.health_gate.set()
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(2)


class ClientTests(unittest.TestCase):
    def setUp(self):
        self.service = FakeService()
        self.clients = []

    def tearDown(self):
        self.service.post_gate.set()
        self.service.health_gate.set()
        for client in self.clients:
            client.close()
        self.service.close()

    def client(self, **kwargs):
        client = GlobalRecordClient(self.service.url, 'test-run', **kwargs)
        self.clients.append(client)
        return client

    @staticmethod
    def record(tick):
        return {'run_id': 'test-run', 'tick': tick, 'events': [{'value': 'original'}]}

    def wait_until(self, predicate, timeout=3):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if predicate():
                return
            time.sleep(0.01)
        self.fail('condition not reached')

    def test_waits_for_202_and_snapshots_nested_record(self):
        self.service.post_gate.clear()
        client = self.client()
        record = self.record(0)
        client.submit(record)
        self.assertTrue(self.service.post_entered.wait(2))
        queued_record = self.record(1)
        client.submit(queued_record)
        record['events'][0]['value'] = 'mutated'
        queued_record['events'][0]['value'] = 'mutated before worker sees it'
        time.sleep(0.1)
        self.assertEqual(len(self.service.posts), 1)
        self.assertEqual(client.health()['buffer_length'], 2)
        self.assertEqual(client.health()['last_sent_tick'], -1)
        self.service.post_gate.set()
        client.flush(3)
        self.assertEqual([r['tick'] for _, r in self.service.posts], [0, 1])
        self.assertEqual(self.service.posts[0][1]['events'][0]['value'], 'original')
        self.assertEqual(self.service.posts[1][1]['events'][0]['value'], 'original')
        self.assertEqual(client.health()['last_sent_tick'], 1)

    def test_503_retries_same_head_with_capped_backoff(self):
        self.service.post_codes = [503] * 5 + [202]
        client = self.client()
        client.submit(self.record(8))
        client.submit(self.record(9))
        client.flush(20)
        self.assertEqual([r['tick'] for _, r in self.service.posts], [8] * 6 + [9])
        times = [t for t, _ in self.service.posts]
        for actual, expected in zip([b - a for a, b in zip(times, times[1:])], [0.5, 1, 2, 4, 4]):
            self.assertGreaterEqual(actual, expected - 0.03)
            self.assertLess(actual, expected + 1.5)
        self.assertIsNone(client.health()['error'])

    def test_400_stops_and_preserves_queue(self):
        self.service.post_gate.clear()
        self.service.post_codes = [400]
        client = self.client()
        client.submit(self.record(1))
        client.submit(self.record(2))
        self.service.post_gate.set()
        with self.assertRaisesRegex(ClientError, 'HTTP 400'):
            client.flush(3)
        time.sleep(0.1)
        self.assertEqual(len(self.service.posts), 1)
        self.assertEqual(client.health()['buffer_length'], 2)
        self.assertEqual(client.health()['last_sent_tick'], -1)
        with self.assertRaises(ClientError):
            client.submit(self.record(3))

    def test_capacity_includes_inflight_and_rejected_tick_can_be_retried(self):
        self.service.post_gate.clear()
        client = self.client(max_buffer=2)
        client.submit(self.record(1))
        self.assertTrue(self.service.post_entered.wait(2))
        client.submit(self.record(2))
        with self.assertRaises(BufferFull):
            client.submit(self.record(3))
        self.service.post_gate.set()
        client.flush(3)
        client.submit(self.record(3))
        client.flush(3)
        self.assertEqual([r['tick'] for _, r in self.service.posts], [1, 2, 3])

    def test_default_capacity_is_300(self):
        self.service.post_gate.clear()
        client = self.client()
        for tick in range(300):
            client.submit(self.record(tick))
        with self.assertRaises(BufferFull):
            client.submit(self.record(300))
        self.assertEqual(client.health()['buffer_length'], 300)
        self.service.post_gate.set()
        client.flush(5)

    def test_duplicate_backwards_and_wrong_run_rejected_gaps_allowed(self):
        client = self.client()
        client.submit(self.record(10))
        for tick in (10, 9, -1, True, 11.0):
            with self.assertRaises(ValueError):
                client.submit(self.record(tick))
        with self.assertRaises(ValueError):
            client.submit({'run_id': 'other', 'tick': 11})
        client.submit(self.record(12))
        client.flush(3)
        self.assertEqual([r['tick'] for _, r in self.service.posts], [10, 12])

    def test_flush_waits_for_write_not_202_and_timeout_keeps_delivery(self):
        self.service.auto_write = False
        client = self.client()
        client.submit(self.record(1))
        with self.assertRaises(TimeoutError):
            client.flush(0.2)
        self.assertEqual(client.health()['last_sent_tick'], 1)
        self.assertEqual(client.health()['last_flushed_tick'], -1)
        self.service.last_tick = 1
        client.flush(2)
        self.assertEqual(client.health()['last_flushed_tick'], 1)

    def test_flush_total_timeout_even_when_health_http_is_blocked(self):
        self.service.health_gate.clear()
        client = self.client(request_timeout=3)
        client.submit(self.record(1))
        start = time.monotonic()
        with self.assertRaises(TimeoutError):
            client.flush(0.2)
        self.assertLess(time.monotonic() - start, 0.7)
        self.assertTrue(self.service.health_entered.is_set())
        self.service.health_gate.set()
        client.flush(2)

    def test_flush_timeout_during_post_preserves_head(self):
        self.service.post_gate.clear()
        client = self.client(request_timeout=3)
        client.submit(self.record(1))
        with self.assertRaises(TimeoutError):
            client.flush(0.1)
        self.assertEqual(client.health()['buffer_length'], 1)
        self.service.post_gate.set()
        client.flush(2)

    def test_writer_drop_or_dead_worker_fails_even_with_high_watermark(self):
        for field, value in (('errors', 1), ('worker_alive', False)):
            with self.subTest(field=field):
                self.service.errors = 0
                self.service.worker_alive = True
                setattr(self.service, field, value)
                client = self.client()
                client.submit(self.record(1))
                with self.assertRaises(ClientError):
                    client.flush(2)
                self.assertEqual(client.health()['last_flushed_tick'], -1)

    def test_network_failure_retries_without_advancing(self):
        self.service.post_codes = [0, 202]
        client = self.client()
        client.submit(self.record(1))
        client.submit(self.record(2))
        client.flush(3)
        self.assertEqual([r['tick'] for _, r in self.service.posts], [1, 1, 2])

    def test_unexpected_status_and_redirect_are_fatal(self):
        for code in (200, 307, 422, 500):
            with self.subTest(code=code):
                self.service.post_codes = [code]
                client = self.client()
                client.submit(self.record(1))
                with self.assertRaisesRegex(ClientError, f'HTTP {code}'):
                    client.flush(2)
                self.assertEqual(client.health()['buffer_length'], 1)
        self.assertEqual(self.service.redirect_hits, 0)

    def test_invalid_health_does_not_confirm(self):
        self.service.auto_write = False
        self.service.last_tick = '100'
        client = self.client()
        client.submit(self.record(1))
        with self.assertRaisesRegex(ClientError, 'last_tick'):
            client.flush(2)

    def test_close_interrupts_retry_preserves_records_and_rejects_use(self):
        self.service.post_codes = [503] * 10
        client = self.client()
        client.submit(self.record(1))
        self.wait_until(lambda: client.health()['error'] is not None)
        client.close(0.5)
        self.assertFalse(client.health()['worker_alive'])
        self.assertEqual(client.health()['buffer_length'], 1)
        with self.assertRaises(ClientError):
            client.submit(self.record(2))
        with self.assertRaises(ClientError):
            client.flush(1)

    def test_empty_flush_and_invalid_json_leave_queue_unchanged(self):
        client = self.client()
        client.flush(0)
        record = self.record(1)
        record['bad'] = float('nan')
        with self.assertRaises(ValueError):
            client.submit(record)
        self.assertEqual(client.health()['buffer_length'], 0)
        client.submit(self.record(1))
        client.flush(2)


if __name__ == '__main__':
    unittest.main()
