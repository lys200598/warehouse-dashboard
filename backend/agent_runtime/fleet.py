"""One spawned OS process per robot; each issues its own authenticated HTTP I/O."""
from copy import deepcopy
import json
import gzip
import multiprocessing as mp
from multiprocessing.connection import wait
import os
import sys
from pathlib import Path
import time
from urllib.request import Request
from urllib.error import HTTPError

from .service import AgentKnowledgeService, AgentHttpServer, canonical, checksum


def _rpc(endpoint, path, token, body, timeout, request_id, trace_path=None, evidence_stream=None):
    request = Request(endpoint+path, data=canonical(body).encode(), method='POST',
                      headers={'Authorization': 'Bearer '+token, 'Content-Type': 'application/json',
                               'X-Probe-ID': request_id})
    # Ignore system proxy settings for the loopback-only shared service.
    from urllib.request import ProxyHandler, build_opener
    started = time.perf_counter_ns()
    telemetry = {'kind': 'agent_http_client', 'pid': os.getpid(), 'robot_id': body.get('robot_id'),
                 'request_id': request_id, 'path': path, 'start_ns': started,
                 'status': None, 'error': None, 'body_complete': False}
    value, raw = None, None
    try:
        with build_opener(ProxyHandler({})).open(request, timeout=timeout) as response:
            telemetry.update(status=response.status, headers_ns=time.perf_counter_ns())
            raw = response.read()
            telemetry.update(body_end_ns=time.perf_counter_ns(), response_bytes=len(raw), body_complete=True)
            value = json.loads(raw)
            telemetry['parse_end_ns'] = time.perf_counter_ns()
        if value.get('ok') is not True: raise RuntimeError('agent request was not acknowledged')
        return value
    except HTTPError as exc:
        telemetry.update(status=exc.code, error=type(exc).__name__, headers_ns=time.perf_counter_ns())
        raw = exc.read()
        telemetry.update(body_end_ns=time.perf_counter_ns(), response_bytes=len(raw), body_complete=True)
        raise
    except Exception as exc:
        telemetry['error'] = type(exc).__name__
        raise
    finally:
        telemetry['end_ns'] = time.perf_counter_ns()
        telemetry['duration_ms'] = (telemetry['end_ns']-started)/1e6
        if trace_path:
            with Path(trace_path).open('a', encoding='utf-8') as stream:
                stream.write(canonical(telemetry)+'\n')
        if evidence_stream is not None:
            # Full request/response evidence is serialized/compressed AFTER
            # the measured HTTP/body/parse interval. One persistent raw file
            # per worker; complete concatenated gzip members survive a later
            # worker death without losing earlier acknowledged records.
            import hashlib
            record = {'request_id': request_id, 'path': path, 'pid': os.getpid(),
                'robot_id': body.get('robot_id'), 'request': body, 'request_sha256': checksum(body),
                'response': value, 'response_sha256': checksum(value) if value is not None else None,
                'response_body_sha256': hashlib.sha256(raw).hexdigest() if raw is not None else None,
                'error_response_utf8': raw.decode('utf-8', 'backslashreplace') if value is None and raw is not None else None,
                'http': telemetry, 'observer_after_http': True}
            evidence_stream.write(gzip.compress((canonical(record)+'\n').encode(), compresslevel=1, mtime=0))
            evidence_stream.flush()


def _agent_worker(connection, *, endpoint, token, run_id, robot_id, initial_seq,
                  query_interval, observation_interval, timeout, trace_path):
    evidence_path = Path(trace_path).with_name(f'agent-{robot_id:04d}-responses.jsonl.gz')
    with evidence_path.open('ab', buffering=256*1024) as evidence_stream:
        return _agent_worker_loop(connection, endpoint=endpoint, token=token, run_id=run_id,
            robot_id=robot_id, initial_seq=initial_seq, query_interval=query_interval,
            observation_interval=observation_interval, timeout=timeout, trace_path=trace_path,
            evidence_stream=evidence_stream)


def _agent_worker_loop(connection, *, endpoint, token, run_id, robot_id, initial_seq,
                  query_interval, observation_interval, timeout, trace_path, evidence_stream):
    # Agent processes need their own HTTP credential, never the database key.
    for name in list(os.environ):
        if name.startswith('NEO4J_'): os.environ.pop(name, None)
    last, seq, last_write, last_query = None, initial_seq, None, None
    connection.send({'ready': True, 'robot_id': robot_id, 'pid': os.getpid()})
    while True:
        message = connection.recv()
        if message is None:
            connection.close(); return
        try:
            state, sim_time, committed_seq = message['state'], message['sim_time'], message['committed_seq']
            if state['robot_id'] != robot_id: raise ValueError('foreign sensor state')
            # Motion coordinates change each second. They do not justify
            # emitting a graph query each second: action/ownership boundaries
            # and periodic source samples determine demand instead.
            changed = last is None or any(state.get(key) != last.get(key)
                for key in ('stage', 'task_id', 'cargo_id', 'carrying', 'fault'))
            low = float(state.get('battery', state.get('soc', 100))) < 35
            was_low = last is not None and float(last.get('battery', last.get('soc', 100))) < 35
            changed = changed or low != was_low
            # Fixed per-robot phases reduce periodic wake-up herds when the
            # canonical commit cadence is finer than the query period. Initial
            # publication and semantic state changes are never delayed.
            observation_phase = (robot_id*17) % observation_interval
            query_phase = (robot_id*13) % query_interval
            due_write = (changed or last_write is None or
                (sim_time+observation_phase)//observation_interval > (last_write+observation_phase)//observation_interval)
            due_query = (changed or last_query is None or
                (sim_time+query_phase)//query_interval > (last_query+query_phase)//query_interval)
            result = {'robot_id': robot_id, 'pid': os.getpid(), 'sim_time': sim_time,
                      'wrote': False, 'queried': False, 'use': 'no_query_needed', 'action_applied': False}
            if due_write or due_query:
                events = []
                if last is None: events.append({'kind': 'observation_started', 'sim_time': sim_time})
                elif last.get('stage') != state.get('stage'):
                    events.append({'kind': 'stage_changed', 'sim_time': sim_time,
                                   'from_stage': last.get('stage'), 'to_stage': state.get('stage')})
                if last is not None and last.get('task_id') != state.get('task_id'):
                    events.append({'kind': 'task_changed', 'sim_time': sim_time, 'task_id': state.get('task_id')})
                if low and not was_low:
                    events.append({'kind': 'low_energy_observed', 'sim_time': sim_time})
                body = {'run_id': run_id, 'robot_id': robot_id, 'seq': seq+1,
                        'sim_time': sim_time, 'committed_seq': committed_seq,
                        'state': state, 'events': events}
                response = _rpc(endpoint, '/api/agent/observation', token, body, timeout,
                                f'agent-{robot_id}-{seq+1}-write', trace_path, evidence_stream)
                if (response.get('seq') != seq+1 or response.get('robot_id') != robot_id
                        or response.get('payload_sha256') != checksum(body)):
                    raise RuntimeError('agent write acknowledgement differs from source')
                seq += 1; last_write = sim_time
                result.update(wrote=True, agent_seq=seq, payload_sha256=response['payload_sha256'])
            if due_query:
                response = _rpc(endpoint, '/api/agent/query', token,
                    {'run_id': run_id, 'robot_id': robot_id, 'at_seq': seq, 'limit': 8},
                    timeout, f'agent-{robot_id}-{seq}-query', trace_path, evidence_stream)
                latest = response['context']['observations'][0]
                if latest['robot_id'] != robot_id or latest['seq'] != seq or latest['state'] != state:
                    raise RuntimeError('agent received foreign or stale self context')
                last_query = sim_time
                result.update(queried=True, use=response['use'], backend=response['backend'],
                              context_sha256=checksum(response['context']), context=response['context'])
            last = state
            connection.send(result)
        except Exception as exc:
            connection.send({'robot_id': robot_id, 'pid': os.getpid(), 'error': type(exc).__name__,
                             'sim_time': message.get('sim_time'), 'action_applied': False})
            # Do not keep issuing requests after ambiguous writes/timeouts.
            connection.close(); return


class AgentFleet:
    def __init__(self, *, run_id, robot_ids, backend, output_dir, query_interval=30,
                 observation_interval=30, timeout=30., startup_timeout=120., port=0,
                 queue_size=512, batch_size=100, query_slots=16, query_wait_timeout=1.):
        if type(query_interval) is not int or query_interval <= 0 or type(observation_interval) is not int or observation_interval <= 0:
            raise ValueError('positive integer agent cadences required')
        self.output = Path(output_dir); self.output.mkdir(parents=True, exist_ok=True)
        self.robot_ids = tuple(robot_ids)
        self.service = AgentKnowledgeService(run_id=run_id, robot_ids=self.robot_ids, backend=backend,
            journal_path=self.output/'agent-journal.sqlite', queue_size=queue_size, batch_size=batch_size,
            request_timeout=timeout, query_slots=query_slots, query_wait_timeout=query_wait_timeout)
        self.http = AgentHttpServer(self.service, port=port, telemetry_path=self.output/'agent-http.jsonl')
        self.timeout = float(timeout)
        self.connections, self.processes, self.pids = {}, {}, {}
        self.failed, self.closed = False, False
        self.latest_contexts = {}
        context = mp.get_context('spawn')
        try:
            for robot in self.robot_ids:
                parent, child = context.Pipe()
                process = context.Process(target=_agent_worker, kwargs={
                    'connection': child, 'endpoint': self.http.endpoint,
                    'token': self.service.tokens[robot], 'run_id': run_id, 'robot_id': robot,
                    'initial_seq': self.service.head(robot), 'query_interval': query_interval,
                    'observation_interval': observation_interval, 'timeout': timeout,
                    'trace_path': str(self.output/f'agent-{robot:04d}-http.jsonl')}, name=f'warehouse-agent-{robot}')
                process.start(); child.close()
                self.connections[robot], self.processes[robot] = parent, process
            ready = self._receive(set(self.robot_ids), startup_timeout)
            for row in ready:
                if row.get('ready') is not True: raise RuntimeError('agent startup acknowledgement missing')
                self.pids[row['robot_id']] = row['pid']
            if len(set(self.pids.values())) != len(self.robot_ids):
                raise RuntimeError('agent process identities are not unique')
            self._write('agent-processes.json', {'run_id': run_id, 'backend': backend.name,
                'agents': [{'robot_id': r, 'pid': self.pids[r]} for r in self.robot_ids],
                'endpoint': self.http.endpoint, 'secrets_recorded': False})
        except Exception:
            self.close(); raise

    def _write(self, name, row):
        (self.output/name).write_text(json.dumps(row, ensure_ascii=False, indent=2), encoding='utf-8')

    def _receive(self, robots, timeout):
        remaining, rows, deadline = set(robots), [], time.monotonic()+timeout
        while remaining:
            pipes = [self.connections[r] for r in remaining]
            if os.name == 'nt' and len(pipes) > 60:
                # Windows WaitForMultipleObjects accepts at most 63 handles.
                # Poll bounded groups; do not call wait(100 handles), nor
                # permanently serialize reception by robot order.
                ready = [pipe for start in range(0, len(pipes), 60)
                         for pipe in wait(pipes[start:start+60], timeout=0)]
                if not ready and time.monotonic() < deadline:
                    time.sleep(.005); continue
            else:
                ready = wait(pipes, timeout=max(0., deadline-time.monotonic()))
            if not ready: raise TimeoutError('robot process deadline exceeded; no further simulation action authorized')
            lookup = {self.connections[r]: r for r in remaining}
            for pipe in ready:
                robot = lookup[pipe]
                try: row = pipe.recv()
                except EOFError: raise RuntimeError(f'robot {robot} process exited before acknowledgement') from None
                if row.get('robot_id') != robot or row.get('error'):
                    raise RuntimeError(f'robot {robot} I/O rejected: '+str(row.get('error', 'identity mismatch')))
                rows.append(row); remaining.remove(robot)
        return sorted(rows, key=lambda row: row['robot_id'])

    def publish(self, robots, sim_time, committed_seq):
        """Call AFTER the canonical run commit succeeds, at its sample cadence.

        Exact sensor payloads are sent only to the corresponding process.
        A dead process or failed write/read raises; callers must pause/abort
        before the next physical step rather than silently run stale agents.
        """
        if self.failed or self.closed: raise RuntimeError('agent fleet unavailable')
        if set(robots) != set(self.robot_ids): raise ValueError('fleet sensor IDs differ from owned robots')
        started = time.time_ns()
        try:
            for robot in self.robot_ids:
                if not self.processes[robot].is_alive(): raise RuntimeError(f'robot {robot} process is not alive')
                self.connections[robot].send({'state': deepcopy(robots[robot]),
                                             'sim_time': sim_time, 'committed_seq': committed_seq})
            rows = self._receive(set(self.robot_ids), self.timeout+5)
            for row in rows:
                if row.get('context'):
                    self.latest_contexts[row['robot_id']] = row['context']
            # Full contexts remain in the durable agent journal/graph; the
            # control evidence carries their hashes and explicit advisory use.
            with (self.output/'agent-results.jsonl').open('a', encoding='utf-8') as stream:
                for row in rows:
                    evidence = {key: value for key, value in row.items() if key != 'context'}
                    evidence.update(committed_seq=committed_seq, published_at_ns=started, acknowledged_at_ns=time.time_ns())
                    stream.write(canonical(evidence)+'\n')
            return rows
        except Exception as exc:
            self.failed = True
            self._write('agent-failure.json', {'sim_time': sim_time, 'committed_seq': committed_seq,
                'error': type(exc).__name__, 'detail': str(exc), 'next_step_authorized': False})
            raise

    def status(self):
        return {'agents': len(self.robot_ids), 'live_processes': sum(p.is_alive() for p in self.processes.values()),
                'unique_pids': len(set(self.pids.values())), 'backend': self.service.backend.name,
                'failed': self.failed, 'contexts_available': len(self.latest_contexts),
                'advisory_only': True, 'metrics': dict(self.service.metrics)}

    def close(self):
        if self.closed: return
        self.closed = True
        for pipe in self.connections.values():
            try: pipe.send(None)
            except (BrokenPipeError, EOFError, OSError): pass
        deadline = time.monotonic()+5
        for process in self.processes.values(): process.join(timeout=max(0., deadline-time.monotonic()))
        for process in self.processes.values():
            if process.is_alive(): process.terminate(); process.join(timeout=5)
        for pipe in self.connections.values(): pipe.close()
        try:
            try:
                self.http.close()
            finally:
                self.service.close()
        finally:
            active_error = sys.exc_info()[1]
            try:
                self._write('agent-closed-status.json', {**self.status(),
                    'http_service_alive': self.http.thread.is_alive(),
                    'writer_alive': self.service.writer.is_alive(),
                    'all_owned_processes_stopped': not any(p.is_alive() for p in self.processes.values()),
                    'closed': True})
            except Exception:
                # Keep the first cleanup failure, while never skipping cleanup
                # merely because the evidence filesystem is unavailable.
                if active_error is None:
                    raise

    def __enter__(self): return self
    def __exit__(self, *args): self.close()
