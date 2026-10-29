"""Observer-only ASGI intervals; no query alteration, retries or cache.

Wrap only the outer ingress used by clients. Do not combine ingress and sidecar
intervals as if they were different requests. A bounded observer queue fails
qualification on overflow instead of silently claiming complete evidence.
"""
import json
import os
from pathlib import Path
import queue
import socket
import threading
import time


class LoadProbeMiddleware:
    def __init__(self, app, *, output, source_identity=None, paths=None,
                 queue_size=65536):
        self.app = app
        self.output = Path(output)
        self.output.parent.mkdir(parents=True, exist_ok=True)
        # O_EXCL preserves previous experiments. One output per server process.
        self.stream = self.output.open('x', encoding='utf-8', buffering=1024 * 1024)
        self.events = queue.Queue(maxsize=queue_size)
        self.paths = set(paths or ['/api/knowledge/query', '/api/agent/query',
                                   '/api/agent/observation'])
        self.dropped = 0
        self.sink_error = None
        self.active = 0
        self.closed = False
        self.thread = threading.Thread(target=self._sink, name='load-probe-sink', daemon=True)
        self.thread.start()
        self._put({'kind': 'probe_identity', 'pid': os.getpid(),
                   'hostname': socket.gethostname(),
                   'clock': 'perf_counter_ns_same_host_only',
                   'source_identity': source_identity,
                   'paths': sorted(self.paths)})

    def _put(self, value):
        try:
            self.events.put_nowait(value)
        except queue.Full:
            self.dropped += 1

    def _sink(self):
        try:
            while True:
                item = self.events.get()
                if item is None:
                    break
                self.stream.write(json.dumps(item, separators=(',', ':'), allow_nan=False) + '\n')
                if self.events.empty():
                    self.stream.flush()
        except Exception as exc:
            self.sink_error = type(exc).__name__
        finally:
            self.stream.close()

    def close(self):
        if self.closed:
            return
        self.closed = True
        # Called after server shutdown, outside measured request intervals.
        if self.thread.is_alive():
            try:
                self.events.put(None, timeout=10)
            except queue.Full:
                self.sink_error = 'observer drain timeout'
            self.thread.join(15)
        status = {'kind': 'probe_status', 'pid': os.getpid(), 'dropped': self.dropped,
                  'sink_error': self.sink_error, 'drained': not self.thread.is_alive(),
                  'complete': not self.dropped and self.sink_error is None and not self.thread.is_alive()}
        self.output.with_suffix(self.output.suffix + '.status.json').write_text(
            json.dumps(status, indent=2), encoding='utf-8')

    async def __call__(self, scope, receive, send):
        if scope['type'] != 'http' or scope.get('path') not in self.paths:
            return await self.app(scope, receive, send)
        started = time.perf_counter_ns()
        self.active += 1
        active = self.active
        rid = dict(scope.get('headers', [])).get(b'x-probe-id', b'').decode('ascii', 'replace')[:160]
        status = None
        finished = False
        body_end = None

        async def observed_receive():
            nonlocal body_end
            value = await receive()
            if value['type'] == 'http.request' and not value.get('more_body', False):
                body_end = time.perf_counter_ns()
            return value

        def finish(completed):
            nonlocal finished
            if finished:
                return
            ended = time.perf_counter_ns()
            finished = True
            self.active -= 1
            self._put({'kind': 'api', 'request_id': rid, 'start_ns': started,
                       'end_ns': ended, 'request_body_end_ns': body_end,
                       'status': status, 'body_send_completed': completed,
                       'inflight_at_start': active, 'pid': os.getpid(),
                       'path': scope['path'], 'service_kind': 'ingress'})

        async def observed_send(value):
            nonlocal status
            if value['type'] == 'http.response.start':
                status = value['status']
            await send(value)
            if value['type'] == 'http.response.body' and not value.get('more_body', False):
                finish(True)

        try:
            return await self.app(scope, observed_receive, observed_send)
        finally:
            finish(False)
