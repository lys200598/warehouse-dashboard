"""Ordered, bounded HTTP transport for one run's global records (stdlib only).

Usage::

    client = GlobalRecordClient('http://127.0.0.1:14591', run_id)
    try:
        client.submit(record)  # build_global_record(...), plus explicit Step
        client.flush(30.0)     # pause producers at a checkpoint
    finally:
        client.close()

submit snapshots JSON before returning. Ticks must be strictly increasing;
gaps are allowed, duplicates/backwards ticks and mixed runs are rejected.
BufferFull means PAUSE the simulation and retry the same unaccepted record.
The in-flight head counts toward capacity. 503 and transport failures retry
forever after 0.5, 1, 2, 4, 4, ... seconds; other HTTP statuses are fatal.
An uncertain POST may be repeated: the server must provide idempotent writes.

flush uses queue.last_tick, queue.errors and queue.worker_alive from health.
At baseline 63f90a5 this watermark is SERVICE-WIDE, resets on service restart,
and the writer can drop a record after ten failed writes. Use a fresh dedicated
service with this client as its only producer, one run, and no service restarts
during a checkpoint. Any reported writer errors fail the checkpoint. This is
not a per-run or crash-durable guarantee, nor proof that an optional Step exists;
shared services require a server-side per-run durable acknowledgement first.

All buffering is in memory. close stops the worker without clearing pending
records (inspect health); it does NOT flush. Call flush before normal shutdown.
Timeouts never remove records or cancel delivery. No automatic process-crash
recovery is provided. Caller must not mutate a record until submit returns.
"""
from __future__ import annotations

from collections import deque
import http.client
import json
import math
import threading
import time
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener


class BufferFull(BufferError):
    """Record was not accepted: pause simulation, then retry this record."""


class ClientError(RuntimeError):
    """Terminal sender/server error; queued records remain inspectable in health."""


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


class GlobalRecordClient:
    """One worker and one strictly ordered queue; public methods are thread-safe."""

    def __init__(self, base_url: str, run_id: str, *, max_buffer: int = 300,
                 request_timeout: float = 2.0):
        url = urlsplit(base_url)
        if url.scheme not in ('http', 'https') or not url.netloc or url.query or url.fragment:
            raise ValueError('base_url must be an HTTP(S) service URL')
        if not isinstance(run_id, str) or not run_id:
            raise ValueError('run_id must be a non-empty string')
        if type(max_buffer) is not int or max_buffer < 1:
            raise ValueError('max_buffer must be a positive integer')
        if not math.isfinite(request_timeout) or request_timeout <= 0:
            raise ValueError('request_timeout must be positive and finite')
        self.base_url = base_url.rstrip('/')
        self.run_id = run_id
        self.max_buffer = max_buffer
        self.request_timeout = request_timeout
        self._opener = build_opener(_NoRedirect())
        self._cv = threading.Condition()
        self._records = deque()
        self._last_submitted = self._last_sent = self._confirmed = -1
        self._flush_target = -1
        self._error = self._transient_error = None
        self._closed = False
        self._thread = threading.Thread(target=self._run, name='global-record-http', daemon=True)
        self._thread.start()

    def submit(self, record: dict) -> None:
        """Accept a JSON snapshot or raise; never silently evict an older record."""
        with self._cv:
            self._check_open()
            if not isinstance(record, dict) or record.get('run_id') != self.run_id:
                raise ValueError('record must be a dict for this run_id')
            tick = record.get('tick')
            if type(tick) is not int or tick < 0 or tick <= self._last_submitted:
                raise ValueError('tick must be a non-negative, strictly increasing integer')
            if len(self._records) >= self.max_buffer:
                raise BufferFull('global record buffer full; pause simulation and retry this record')
            payload = json.dumps(record, ensure_ascii=False, allow_nan=False).encode('utf-8')
            self._records.append((tick, payload))
            self._last_submitted = tick
            self._cv.notify_all()

    def health(self) -> dict:
        """Local snapshot, without network I/O; last_sent_tick means HTTP 202 only."""
        with self._cv:
            return {'last_sent_tick': self._last_sent, 'buffer_length': len(self._records),
                    'last_flushed_tick': self._confirmed,
                    'error': self._error or self._transient_error,
                    'closed': self._closed, 'worker_alive': self._thread.is_alive()}

    def flush(self, timeout: float) -> None:
        """Confirm all records submitted at entry within a real total timeout.

        Producers should pause at checkpoints. HTTP I/O runs only in the worker,
        so a slow response body cannot extend this caller's timeout. An empty
        client returns immediately. TimeoutError leaves delivery active.
        """
        if not math.isfinite(timeout) or timeout < 0:
            raise ValueError('timeout must be non-negative and finite')
        deadline = time.monotonic() + timeout
        with self._cv:
            self._check_open()
            target = self._last_submitted
            self._flush_target = max(self._flush_target, target)
            self._cv.notify_all()
            while self._confirmed < target:
                self._check_open()
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError(f'global record flush timed out at tick {target}')
                self._cv.wait(remaining)

    def close(self, timeout: float = 5.0) -> None:
        """Stop without discarding pending records; flush first for normal exit."""
        if not math.isfinite(timeout) or timeout < 0:
            raise ValueError('timeout must be non-negative and finite')
        with self._cv:
            self._closed = True
            self._cv.notify_all()
        self._thread.join(timeout)
        if self._thread.is_alive():
            raise TimeoutError('HTTP worker still finishing an in-flight request')

    def _check_open(self):
        if self._error:
            raise ClientError(self._error)
        if self._closed:
            raise ClientError('global record client is closed')

    def _wait(self, seconds):
        with self._cv:
            self._cv.wait_for(lambda: self._closed, timeout=seconds)

    def _request(self, path, payload=None):
        request = Request(self.base_url + path, data=payload,
                          headers={'Content-Type': 'application/json'},
                          method='GET' if payload is None else 'POST')
        try:
            with self._opener.open(request, timeout=self.request_timeout) as response:
                return response.status, response.read(65536)
        except HTTPError as exc:
            with exc:
                return exc.code, exc.read(8192)

    def _run(self):
        attempt = 0
        try:
            while True:
                with self._cv:
                    self._cv.wait_for(lambda: self._closed or self._records or
                                      self._confirmed < self._flush_target)
                    if self._closed:
                        return
                    check_health = self._confirmed < self._flush_target <= self._last_sent
                    tick, payload = (self._flush_target, None) if check_health else self._records[0]
                try:
                    status, body = self._request('/v1/health' if check_health else '/v1/records', payload)
                except (OSError, URLError, http.client.HTTPException) as exc:
                    with self._cv:
                        self._transient_error = f'transport error: {exc}'
                    self._wait((0.5, 1.0, 2.0, 4.0)[min(attempt, 3)])
                    attempt += 1
                    continue
                if status == 503:
                    with self._cv:
                        self._transient_error = 'HTTP 503: service unavailable; retaining queue head'
                    self._wait((0.5, 1.0, 2.0, 4.0)[min(attempt, 3)])
                    attempt += 1
                    continue
                expected = 200 if check_health else 202
                if status != expected:
                    raise ClientError(f'HTTP {status}: {body.decode("utf-8", errors="replace")}')
                attempt = 0
                if check_health:
                    state = json.loads(body)['queue']
                    if type(state.get('errors')) is not int or state['errors'] != 0:
                        raise ClientError('service writer errors reported; checkpoint is unsafe')
                    if state.get('worker_alive') is not True:
                        raise ClientError('service writer is not alive')
                    last_tick = state.get('last_tick')
                    if type(last_tick) is not int:
                        raise ClientError('service health has no integer queue.last_tick')
                    with self._cv:
                        self._transient_error = None
                        if last_tick >= tick:
                            self._confirmed = tick
                        self._cv.notify_all()
                    if last_tick < tick:
                        self._wait(0.1)
                else:
                    with self._cv:
                        self._records.popleft()  # only a 202 releases capacity
                        self._last_sent = tick
                        self._transient_error = None
                        self._cv.notify_all()
        except Exception as exc:
            with self._cv:
                self._error = f'{type(exc).__name__}: {exc}'
                self._cv.notify_all()
