"""Bounded FIFO of immutable robot frames, retained until durable Step acknowledgement.

Only the simulation owner submits. The worker never receives the simulation;
its process callback must confirm the run-specific Step before returning.
"""
from collections import deque
from copy import deepcopy
import math
import threading
import time


class GraphQueueError(RuntimeError):
    pass


class GraphWaitCancelled(RuntimeError):
    """The owner left a safe capacity boundary; accepted graph work remains queued."""
    pass


def inflight_cleanup_status(sim, close_error):
    """After failed close, deletion requires positive evidence that writes stopped."""
    if close_error is None:
        return None
    try:
        probe = getattr(sim, 'graph_inflight_active', None)
        if probe is not None and not probe():
            return None
        detail = 'graph writes may still be active' if probe is not None else 'graph write state is unknown'
    except Exception as exc:
        detail = f'graph write state could not be checked: {type(exc).__name__}: {exc}'
    return {'status': 'blocked_inflight', 'error': f'{type(close_error).__name__}: {close_error}',
            'detail': detail, 'resolution': 'preserve run and queue; flush or stop and verify writers before cleanup'}


class AsyncRobotGraphQueue:
    def __init__(self, process, *, capacity=8, on_wait=None):
        if type(capacity) is not int or not 1 <= capacity <= 128:
            raise ValueError('robot_graph_backlog must be within 1..128')
        self.process, self.capacity, self.on_wait = process, capacity, on_wait
        self._cv = threading.Condition()
        self._frames = deque()
        self._last_tick = self._confirmed_tick = -1
        self._max_pending = self._pause_count = 0
        self._pause_seconds = 0.
        self._waiting_since = None
        self._error = None
        self._closing = False
        self._thread = threading.Thread(target=self._run, name='robot-graph-fifo', daemon=True)
        self._thread.start()

    def _check(self):
        if self._error is not None:
            raise GraphQueueError(f'robot graph pipeline failed: {self._error}') from self._error
        if self._closing:
            raise GraphQueueError('robot graph pipeline is closed')

    def _notify_wait(self):
        if self.on_wait is not None:
            self.on_wait(self.snapshot())

    def reserve(self, cancel_event=None):
        """Single-producer capacity boundary before advancing the next physical tick."""
        return self.wait_for_capacity(cancel_event)

    def wait_for_capacity(self, cancel_event=None):
        """At the high watermark pause new physical ticks until completely caught up."""
        with self._cv:
            self._check()
            if cancel_event is not None and cancel_event.is_set():
                raise GraphWaitCancelled('robot graph capacity wait cancelled by owner control')
            if len(self._frames) < self.capacity:
                return
            self._pause_count += 1
            self._waiting_since = time.monotonic()
        try:
            self._notify_wait()
            with self._cv:
                while self._frames:
                    self._check()
                    if cancel_event is not None and cancel_event.is_set():
                        raise GraphWaitCancelled('robot graph capacity wait cancelled by owner control')
                    self._cv.wait(.1)
        finally:
            with self._cv:
                self._pause_seconds += time.monotonic() - self._waiting_since
                self._waiting_since = None
            self._notify_wait()

    def submit(self, frame):
        """Accept one already-built frame; includes the active frame in the limit."""
        with self._cv:
            self._check()
            if len(self._frames) >= self.capacity:
                raise GraphQueueError('graph capacity must be reserved before advancing a physical tick')
            tick = frame.get('tick')
            if type(tick) is not int or tick <= self._last_tick:
                raise ValueError('asynchronous robot frames require strictly increasing integer ticks')
            frozen = deepcopy(frame)
            self._frames.append(frozen)
            self._last_tick = tick
            self._max_pending = max(self._max_pending, len(self._frames))
            self._cv.notify_all()

    def snapshot(self):
        with self._cv:
            active = 0. if self._waiting_since is None else time.monotonic() - self._waiting_since
            return {'pending': len(self._frames), 'max_pending': self._max_pending,
                    'limit': self.capacity, 'confirmed_tick': self._confirmed_tick,
                    'pause_count': self._pause_count, 'pause_seconds': self._pause_seconds + active,
                    'waiting': self._waiting_since is not None,
                    'error': None if self._error is None else f'{type(self._error).__name__}: {self._error}'}

    def inflight_active(self):
        with self._cv:
            return self._error is None and self._thread.is_alive() and bool(self._frames)

    def flush(self, timeout=60.):
        if not math.isfinite(timeout) or timeout < 0:
            raise ValueError('graph flush timeout must be finite and non-negative')
        deadline = time.monotonic() + timeout
        with self._cv:
            while self._frames:
                if self._error is not None:
                    raise GraphQueueError(f'robot graph pipeline failed: {self._error}') from self._error
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError(f'robot graph flush timed out with {len(self._frames)} frames retained')
                self._cv.wait(remaining)
            if self._error is not None:
                raise GraphQueueError(f'robot graph pipeline failed: {self._error}') from self._error

    def close(self, timeout=60.):
        began = time.monotonic()
        try:
            self.flush(timeout)
        finally:
            with self._cv:
                self._closing = True
                self._cv.notify_all()
        self._thread.join(max(0., timeout - (time.monotonic() - began)))
        if self._thread.is_alive():
            raise TimeoutError('robot graph worker is still finishing its accepted frames')

    def _run(self):
        while True:
            with self._cv:
                self._cv.wait_for(lambda: self._frames or self._closing)
                if not self._frames:
                    return
                frame = self._frames[0]  # retained and counted while in flight
            try:
                self.process(frame)
            except Exception as exc:
                with self._cv:
                    self._error = exc
                    self._cv.notify_all()
                return
            with self._cv:
                self._confirmed_tick = frame['tick']
                self._frames.popleft()
                self._cv.notify_all()
