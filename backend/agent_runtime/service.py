"""Bounded, durable robot-owned facts; authoritative custody stays central.

Tokens bind an HTTP client to ONE robot. Tokens and request bodies are never
written to HTTP telemetry. Acknowledgements follow a durable journal commit AND
the selected backend commit. Failed graph writes remain pending for replay.
"""
from concurrent.futures import Future, TimeoutError as FuturesTimeoutError
from copy import deepcopy
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import hashlib
import hmac
import json
import math
from pathlib import Path
import queue
import secrets
import sqlite3
import threading
import time
import traceback

VERSION = 'robot-agent-shared-service-v2-async-observer'


def canonical(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(',', ':'), allow_nan=False)


def checksum(value):
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def integer(value, name, minimum=0):
    if type(value) is not int or value < minimum:
        raise ValueError(name + ' must be an integer >= ' + str(minimum))
    return value


def safe_exception(exc):
    """Diagnostic identity and stack locations only; never exception args/locals."""
    known = {
        'requested same-run robot points absent or duplicated': 'canonical_context_missing',
        'agent observation differs from its canonical source': 'observation_canonical_mismatch',
        'graph query did not return acknowledged own source': 'query_ack_source_mismatch',
        'requested own observation is not committed': 'own_observation_pending',
        'bounded agent query queue deadline exceeded': 'query_queue_deadline',
        'bounded agent write queue full': 'write_queue_full',
    }
    code = getattr(exc, 'code', None)
    return {'exception_type': type(exc).__name__,
            'code': code if isinstance(code, str) and all(c.isalnum() or c in '._' for c in code) else None,
            'reason_code': known.get(str(exc), 'unclassified'),
            'cause_type': type(exc.__cause__).__name__ if exc.__cause__ else None,
            'frames': [{'file': Path(frame.filename).name, 'function': frame.name, 'line': frame.lineno}
                       for frame in traceback.extract_tb(exc.__traceback__)]}


class JournalBackend:
    """Protocol/recovery tests only. This backend provides NO graph qualification."""
    name = 'journal-only-not-neo4j'

    def __init__(self):
        self.rows = {}
        self.lock = threading.Lock()

    def source_times(self, rows):
        # Protocol tests only: there is no authoritative committed state here.
        return None

    def write(self, rows):
        with self.lock:
            for row in rows:
                key = (row['run_id'], row['robot_id'], row['seq'])
                if key in self.rows and checksum(self.rows[key]) != checksum(row):
                    raise ValueError('backend sequence conflict')
                self.rows[key] = deepcopy(row)

    def query(self, run_id, robot_id, at_seq, limit):
        with self.lock:
            rows = [deepcopy(row) for (run, robot, seq), row in self.rows.items()
                    if run == run_id and robot == robot_id and seq <= at_seq]
        return {'observations': sorted(rows, key=lambda row: row['seq'], reverse=True)[:limit]}

    def close(self):
        pass


class Neo4jBackend:
    name = 'neo4j'

    def __init__(self, driver, database='neo4j', *, timeout=10.):
        self.driver, self.database, self.timeout = driver, database, float(timeout)
        from neo4j import Query
        self.Query = Query
        from ..knowledge_graph import Neo4jMirror
        from ..query_v2.service_fragments import FragmentQueryService
        self.context_reader = FragmentQueryService(Neo4jMirror(driver, database), timeout=self.timeout)
        with driver.session(database=database) as session:
            for query in (
                'CREATE CONSTRAINT wh_agent_observation_key IF NOT EXISTS FOR (n:WHAgentObservation) REQUIRE n.key IS UNIQUE',
                'CREATE INDEX wh_agent_observation_history IF NOT EXISTS FOR (n:WHAgentObservation) ON (n.run_id,n.robot_id,n.agent_seq)',
                'CREATE CONSTRAINT wh_agent_runtime_event_key IF NOT EXISTS FOR (n:WHAgentRuntimeEvent) REQUIRE n.key IS UNIQUE',
            ):
                session.run(Query(query, timeout=self.timeout)).consume()

    @classmethod
    def from_env(cls):
        import os
        from neo4j import GraphDatabase
        uri = os.environ.get('NEO4J_URI', 'bolt://127.0.0.1:7687')
        user = os.environ.get('NEO4J_USER', 'neo4j')
        password = os.environ.get('NEO4J_PASSWORD')
        if not password:
            raise ValueError('NEO4J_PASSWORD required for robot graph I/O')
        driver = GraphDatabase.driver(uri, auth=(user, password), max_connection_pool_size=64,
            connection_timeout=5., connection_acquisition_timeout=5., max_transaction_retry_time=0.)
        try:
            return cls(driver, os.environ.get('NEO4J_DATABASE', 'neo4j'))
        except Exception:
            driver.close(); raise

    def source_times(self, rows):
        """Authoritative sim_time of each cited committed robot state."""
        keys = sorted({f"{r['run_id']}:{r['committed_seq']}:{r['robot_id']}" for r in rows})
        if not keys:
            return {}
        with self.driver.session(database=self.database) as session:
            found = session.run(self.Query('UNWIND $keys AS k MATCH (s:WHAgentState {key:k}) '
                                           'RETURN k AS key, s.sim_time AS sim_time', timeout=self.timeout),
                                keys=keys).data()
        return {row['key']: row['sim_time'] for row in found}

    def write(self, rows):
        if not rows:
            return
        from neo4j import unit_of_work
        packed = [{'key': f"{r['run_id']}:{r['robot_id']}:{r['seq']}",
                   'run_id': r['run_id'], 'robot_id': r['robot_id'], 'seq': r['seq'],
                   'canonical_seq': r['committed_seq'], 'payload': canonical(r), 'sha': checksum(r),
                   'sim_time': r['sim_time'], 'state_sha': checksum(r['state']), 'events': [
                       {'key': f"{r['run_id']}:{r['robot_id']}:{r['seq']}:{i}", 'payload': canonical(event),
                        'kind': event['kind']} for i, event in enumerate(r['events'])]} for r in rows]

        @unit_of_work(timeout=self.timeout)
        def transaction(tx):
            # Validate the whole batch before mutating any fact. The central
            # run/robot and its source watermark must already exist.
            checked = list(tx.run('''UNWIND $rows AS p
                MATCH (run:WHRun {run_id:p.run_id}), (robot:WHRobot {key:p.run_id+':'+toString(p.robot_id)})
                WHERE run.committed_seq >= p.canonical_seq
                MATCH (state:WHAgentState {key:p.run_id+':'+toString(p.canonical_seq)+':'+toString(p.robot_id)})
                OPTIONAL MATCH (old:WHAgentObservation {key:p.key})
                RETURN p.key AS key,state.payload AS canonical_payload,old.payload_sha256 AS old_sha''', rows=packed))
            expected = {row['key']: row for row in packed}
            if (len(checked) != len(packed) or {row['key'] for row in checked} != set(expected)
                    or any((row['old_sha'] is not None and row['old_sha'] != expected[row['key']]['sha'])
                           or checksum(json.loads(row['canonical_payload'])) != expected[row['key']]['state_sha']
                           for row in checked)):
                raise ValueError('canonical robot/source watermark missing or agent sequence conflicts')
            tx.run('''UNWIND $rows AS p
                MATCH (run:WHRun {run_id:p.run_id}), (robot:WHRobot {key:p.run_id+':'+toString(p.robot_id)})
                MERGE (o:WHAgentObservation {key:p.key})
                ON CREATE SET o.run_id=p.run_id,o.robot_id=p.robot_id,o.agent_seq=p.seq,
                  o.committed_seq=p.canonical_seq,o.sim_time=p.sim_time,o.payload_json=p.payload,o.payload_sha256=p.sha
                MERGE (o)-[:OBSERVED_BY]->(robot) MERGE (o)-[:IN_RUN]->(run)
                WITH o,p UNWIND p.events AS e
                MERGE (event:WHAgentRuntimeEvent {key:e.key})
                ON CREATE SET event.run_id=p.run_id,event.robot_id=p.robot_id,event.agent_seq=p.seq,
                  event.sim_time=p.sim_time,event.kind=e.kind,event.payload_json=e.payload
                MERGE (event)-[:FROM_OBSERVATION]->(o)''', rows=packed).consume()

        with self.driver.session(database=self.database) as session:
            session.execute_write(transaction)

    def query(self, run_id, robot_id, at_seq, limit):
        with self.driver.session(database=self.database) as session:
            rows = session.run(self.Query('''MATCH (o:WHAgentObservation)
                WHERE o.run_id=$run AND o.robot_id=$robot AND o.agent_seq <= $seq
                RETURN o.payload_json AS payload ORDER BY o.agent_seq DESC LIMIT $limit''', timeout=self.timeout),
                run=run_id, robot=robot_id, seq=at_seq, limit=limit)
            observations = [json.loads(row['payload']) for row in rows]
        if not observations:
            return {'observations': []}
        canonical_seq = observations[0]['committed_seq']
        canonical_context = self.context_reader.robot_context(run_id, canonical_seq, [robot_id])
        if canonical_context['robots'][0]['robot'] != observations[0]['state']:
            raise RuntimeError('agent observation differs from its canonical source')
        return {'observations': observations, 'canonical_context': canonical_context}

    def close(self):
        self.driver.close()


class AgentKnowledgeService:
    def __init__(self, *, run_id, robot_ids, backend, journal_path, queue_size=512,
                 batch_size=100, batch_wait=.005, request_timeout=20., query_slots=16,
                 query_wait_timeout=1.):
        if not isinstance(run_id, str) or not run_id or len(run_id) > 128:
            raise ValueError('bounded nonempty run_id required')
        robot_ids = tuple(robot_ids)
        if not robot_ids or len(set(robot_ids)) != len(robot_ids):
            raise ValueError('unique robot IDs required')
        for rid in robot_ids:
            integer(rid, 'robot_id', 1)
        integer(queue_size, 'queue_size', 1); integer(batch_size, 'batch_size', 1)
        integer(query_slots, 'query_slots', 1)
        if not math.isfinite(query_wait_timeout) or query_wait_timeout <= 0:
            raise ValueError('positive finite query queue timeout required')
        if not math.isfinite(batch_wait) or batch_wait < 0 or not math.isfinite(request_timeout) or request_timeout <= 0:
            raise ValueError('invalid timing bounds')
        self.run_id, self.backend = run_id, backend
        self.tokens = {rid: secrets.token_urlsafe(32) for rid in robot_ids}
        self.path = Path(journal_path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(self.path, check_same_thread=False)
        self.db.execute('PRAGMA journal_mode=WAL')
        self.db.execute('PRAGMA synchronous=FULL')
        self.db.execute('CREATE TABLE IF NOT EXISTS metadata(key TEXT PRIMARY KEY,value TEXT NOT NULL)')
        self.db.execute('CREATE TABLE IF NOT EXISTS observations(robot INTEGER,seq INTEGER,sha TEXT,payload TEXT,mirrored INTEGER NOT NULL DEFAULT 0,PRIMARY KEY(robot,seq))')
        identity = canonical({'run_id': run_id, 'robots': sorted(robot_ids), 'backend': backend.name, 'version': VERSION})
        old = self.db.execute("SELECT value FROM metadata WHERE key='identity'").fetchone()
        if old and old[0] != identity:
            self.db.close(); raise ValueError('agent journal identity differs; use separate output')
        self.db.execute("INSERT OR IGNORE INTO metadata VALUES('identity',?)", (identity,)); self.db.commit()
        self.lock = threading.RLock()
        self.queue = queue.Queue(maxsize=queue_size)
        self.batch_size, self.batch_wait, self.request_timeout = batch_size, batch_wait, request_timeout
        self.query_slots = threading.BoundedSemaphore(query_slots)
        self.query_wait_timeout = query_wait_timeout
        self.error = None
        self.closed = False
        self.metrics = {'accepted': 0, 'duplicates': 0, 'queries': 0, 'query_queue_rejected': 0,
                        'write_batches': 0, 'max_batch': 0}
        # Retry pending graph commits before workers can claim a ready service.
        try:
            pending = [json.loads(row[0]) for row in self.db.execute('SELECT payload FROM observations WHERE mirrored=0 ORDER BY seq,robot')]
            if isinstance(backend, JournalBackend):
                all_rows = [json.loads(row[0]) for row in self.db.execute('SELECT payload FROM observations ORDER BY seq,robot')]
                if all_rows: backend.write(all_rows)
            for start in range(0, len(pending), batch_size):
                self._mirror(pending[start:start+batch_size])
        except Exception:
            self.db.close(); backend.close(); raise
        self.writer = threading.Thread(target=self._writer, name='agent-shared-batched-writer', daemon=True)
        self.writer.start()

    def authenticate(self, token):
        for robot, expected in self.tokens.items():
            if isinstance(token, str) and hmac.compare_digest(token, expected):
                return robot
        raise PermissionError('invalid agent credential')

    def _identity(self, robot, body):
        if (type(robot) is not int or robot not in self.tokens or body.get('run_id') != self.run_id
                or type(body.get('robot_id')) is not int or body.get('robot_id') != robot):
            raise PermissionError('agent may access its own run and robot only')

    def _validate(self, robot, body):
        self._identity(robot, body)
        if set(body) != {'run_id', 'robot_id', 'seq', 'sim_time', 'committed_seq', 'state', 'events'}:
            raise ValueError('unexpected observation fields')
        for name in ('seq', 'committed_seq'):
            integer(body[name], name, 1)
        integer(body['sim_time'], 'sim_time')
        state = body['state']
        if not isinstance(state, dict) or type(state.get('robot_id')) is not int or state.get('robot_id') != robot:
            raise PermissionError('foreign or missing state robot_id')
        if not isinstance(body['events'], list) or len(body['events']) > 8:
            raise ValueError('at most eight local events allowed')
        for event in body['events']:
            if not isinstance(event, dict) or set(event) - {'kind', 'sim_time', 'from_stage', 'to_stage', 'task_id'}:
                raise ValueError('only local observation event fields allowed')
            if event.get('kind') not in {'observation_started', 'stage_changed', 'task_changed', 'low_energy_observed'}:
                raise ValueError('unsupported local event kind')
            if event.get('sim_time') != body['sim_time']:
                raise ValueError('event time differs from observation')
        if len(canonical(body).encode()) > 60000:
            raise ValueError('observation too large')
        return deepcopy(body)

    def head(self, robot):
        with self.lock:
            row = self.db.execute('SELECT coalesce(max(seq),0) FROM observations WHERE robot=?', (robot,)).fetchone()
            return row[0]

    def submit(self, robot, body):
        row = self._validate(robot, body)
        if self.closed or self.error:
            raise RuntimeError('shared writer unavailable')
        done = Future()
        done.enqueued_ns = time.perf_counter_ns()
        try:
            self.queue.put_nowait((row, done))
        except queue.Full:
            raise RuntimeError('bounded agent write queue full') from None
        try:
            return done.result(timeout=self.request_timeout)
        except FuturesTimeoutError as exc:
            # Python 3.10 exposes a distinct concurrent.futures.TimeoutError;
            # newer Python aliases it to the builtin. Keep one public contract.
            raise TimeoutError('agent write acknowledgement deadline exceeded') from exc

    def _mirror(self, rows):
        try:
            self.backend.write(rows)
        except Exception as exc:
            self.error = type(exc).__name__
            raise
        with self.lock:
            self.db.executemany('UPDATE observations SET mirrored=1 WHERE robot=? AND seq=?',
                                [(row['robot_id'], row['seq']) for row in rows])
            self.db.commit()

    def _reject_wrong_source_times(self, items):
        """Drop observations whose sim_time differs from their committed state.

        Checked before the local journal insert, so a wrong time never becomes a
        head that later correct observations could not move past. Only the
        offending request fails; other robots in the same batch continue.
        """
        times = self.backend.source_times([row for row, _ in items])
        if times is None:
            return items
        kept = []
        for row, future in items:
            key = f"{row['run_id']}:{row['committed_seq']}:{row['robot_id']}"
            if key in times and times[key] != row['sim_time']:
                self.metrics['source_time_rejections'] = self.metrics.get('source_time_rejections', 0) + 1
                future.set_exception(ValueError('observation sim_time differs from the authoritative committed state'))
            else:
                kept.append((row, future))
        return kept

    def _commit(self, items):
        rows = []
        items = self._reject_wrong_source_times(items)
        if not items:
            return
        with self.lock:
            try:
                for row, future in items:
                    robot, seq, sha = row['robot_id'], row['seq'], checksum(row)
                    old = self.db.execute('SELECT sha,mirrored FROM observations WHERE robot=? AND seq=?', (robot, seq)).fetchone()
                    if old:
                        if old[0] != sha:
                            raise ValueError('sequence payload conflict')
                        self.metrics['duplicates'] += 1
                        if not old[1]: rows.append(row)
                        continue
                    last = self.db.execute('SELECT seq,payload FROM observations WHERE robot=? ORDER BY seq DESC LIMIT 1', (robot,)).fetchone()
                    expected = last[0]+1 if last else 1
                    if seq != expected:
                        raise ValueError('noncontiguous agent sequence')
                    if last:
                        previous = json.loads(last[1])
                        if row['sim_time'] < previous['sim_time'] or row['committed_seq'] < previous['committed_seq']:
                            raise ValueError('agent source watermark moved backwards')
                    self.db.execute('INSERT INTO observations VALUES(?,?,?,?,0)', (robot, seq, sha, canonical(row)))
                    rows.append(row)
                self.db.commit()
            except Exception:
                self.db.rollback(); raise
        if rows:
            # Distinct robots normally share this transaction. Do not rewrite
            # a global robot head node for each individual state.
            unique = {(row['robot_id'], row['seq']): row for row in rows}
            self._mirror(list(unique.values()))
        with self.lock:
            self.metrics['accepted'] += len(items)
            self.metrics['write_batches'] += 1
            self.metrics['max_batch'] = max(self.metrics['max_batch'], len(items))
        for row, future in items:
            future.set_result({'ok': True, 'run_id': self.run_id, 'robot_id': row['robot_id'],
                               'seq': row['seq'], 'payload_sha256': checksum(row), 'backend': self.backend.name,
                               'metrics': {'queue_and_commit_ms': (time.perf_counter_ns()-future.enqueued_ns)/1e6}})

    def _writer(self):
        while True:
            item = self.queue.get()
            if item is None:
                self.queue.task_done(); return
            items = [item]
            deadline = time.monotonic()+self.batch_wait
            while len(items) < self.batch_size:
                try:
                    value = self.queue.get(timeout=max(0., deadline-time.monotonic()))
                except queue.Empty:
                    break
                if value is None:
                    self.queue.task_done(); self.queue.put(None); break
                items.append(value)
            try:
                self._commit(items)
            except Exception as exc:
                # Validation errors reject only this batch; a backend error
                # closes the admission gate with pending journal rows retained.
                if not isinstance(exc, ValueError): self.error = type(exc).__name__
                for _, future in items:
                    if not future.done():
                        future.set_exception(exc)
            finally:
                for _ in items: self.queue.task_done()

    def query(self, robot, body):
        started = time.perf_counter_ns()
        self._identity(robot, body)
        if set(body) - {'run_id', 'robot_id', 'at_seq', 'limit'}:
            raise ValueError('unexpected query fields')
        limit = integer(body.get('limit', 8), 'limit', 1)
        if limit > 100: raise ValueError('limit exceeds 100')
        at_seq = integer(body.get('at_seq', self.head(robot)), 'at_seq', 1)
        with self.lock:
            latest = self.db.execute('SELECT sha,payload FROM observations WHERE robot=? AND seq=? AND mirrored=1', (robot, at_seq)).fetchone()
        if latest is None:
            raise ValueError('requested own observation is not committed')
        queue_started = time.perf_counter_ns()
        if not self.query_slots.acquire(timeout=self.query_wait_timeout):
            with self.lock: self.metrics['query_queue_rejected'] += 1
            raise TimeoutError('bounded agent query queue deadline exceeded')
        backend_started = time.perf_counter_ns()
        try:
            value = self.backend.query(self.run_id, robot, at_seq, limit)
        finally:
            backend_ended = time.perf_counter_ns()
            self.query_slots.release()
        rows = value.get('observations')
        if not rows or rows[0]['seq'] != at_seq or checksum(rows[0]) != latest[0]:
            raise RuntimeError('graph query did not return acknowledged own source')
        for row in rows:
            self._identity(robot, row)
            if row['seq'] > at_seq:
                raise RuntimeError('graph query crossed requested agent fence')
        with self.lock: self.metrics['queries'] += 1
        return {'ok': True, 'backend': self.backend.name, 'run_id': self.run_id,
                'robot_id': robot, 'at_seq': at_seq, 'context': value,
                'use': 'advisory_available', 'action_applied': False,
                'metrics': {'query_queue_ms': (backend_started-queue_started)/1e6,
                            'backend_query_ms': (backend_ended-backend_started)/1e6,
                            'service_ms': (time.perf_counter_ns()-started)/1e6}}

    def close(self):
        if self.closed: return
        self.closed = True
        self.queue.put(None, timeout=self.request_timeout)
        self.writer.join(timeout=self.request_timeout)
        if self.writer.is_alive():
            raise TimeoutError('agent writer did not stop; journal retained')
        self.backend.close(); self.db.close()


class AgentHttpServer:
    def __init__(self, service, *, port=0, max_connections=128, telemetry_path=None):
        integer(max_connections, 'max_connections', 1)
        self.service, self.telemetry_path = service, Path(telemetry_path) if telemetry_path else None
        self.telemetry_lock = threading.Lock()
        self.telemetry_error = False
        self.telemetry_dropped = 0
        self.transport_rejections = 0
        self.telemetry_queue = queue.Queue(maxsize=8192)
        self.telemetry_thread = None
        self.prior_trace_incomplete = False
        self.active_threads = 0
        self.drained_condition = threading.Condition()
        self.slots = threading.BoundedSemaphore(max_connections)
        self.connection_slots = threading.BoundedSemaphore(max_connections)
        outer = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = 'HTTP/1.0'
            def log_message(self, *args): pass
            def do_POST(self):
                started, status, robot = time.perf_counter_ns(), 500, None
                body_sent = False
                request_id = self.headers.get('X-Probe-ID', '')[:128]
                admitted = outer.slots.acquire(blocking=False)
                try:
                    if not admitted: raise RuntimeError('bounded agent request capacity full')
                    self.connection.settimeout(service.request_timeout)
                    auth = self.headers.get('Authorization', '')
                    robot = service.authenticate(auth[7:] if auth.startswith('Bearer ') else '')
                    if self.headers.get('Transfer-Encoding'):
                        raise ValueError('chunked requests unsupported')
                    length = int(self.headers.get('Content-Length', '-1'))
                    if not 0 < length <= 65536: raise ValueError('bounded JSON body required')
                    body = json.loads(self.rfile.read(length))
                    if not isinstance(body, dict): raise ValueError('JSON object required')
                    if self.path == '/api/agent/observation': result = service.submit(robot, body)
                    elif self.path == '/api/agent/query': result = service.query(robot, body)
                    else: raise LookupError('unknown agent endpoint')
                    status = 200
                except PermissionError:
                    status, result = 403, {'ok': False, 'error': 'agent identity rejected'}
                except (ValueError, TypeError, KeyError, json.JSONDecodeError) as exc:
                    outer._record({'kind': 'agent_error', 'request_id': request_id, 'robot_id': robot,
                                   'path': self.path, 'error': safe_exception(exc)})
                    status, result = 400, {'ok': False, 'error': 'invalid agent request'}
                except LookupError:
                    status, result = 404, {'ok': False, 'error': 'unknown endpoint'}
                except Exception as exc:
                    outer._record({'kind': 'agent_error', 'request_id': request_id, 'robot_id': robot,
                                   'path': self.path, 'error': safe_exception(exc)})
                    status, result = 503, {'ok': False, 'error': 'agent service unavailable; no action authorized'}
                try:
                    raw = canonical(result).encode()
                    self.send_response(status); self.send_header('Content-Type', 'application/json')
                    self.send_header('Content-Length', str(len(raw))); self.end_headers()
                    self.wfile.write(raw); self.wfile.flush()
                    body_sent = True
                finally:
                    ended = time.perf_counter_ns()
                    if admitted: outer.slots.release()
                    if outer.telemetry_path:
                        import os
                        row = {'kind': 'api', 'request_id': request_id, 'pid': os.getpid(), 'robot_id': robot,
                               'path': self.path, 'status': status, 'start_ns': started, 'end_ns': ended,
                               'duration_ms': (ended-started)/1e6, 'body_send_completed': body_sent}
                        outer._record(row)

        class Server(ThreadingHTTPServer):
            daemon_threads = True
            request_queue_size = 256
            def process_request(self, request, client_address):
                if not outer.connection_slots.acquire(blocking=False):
                    # An admission rejection occurs before parsing an auth
                    # token/request ID. Preserve failure of strict tracing.
                    outer.telemetry_error = True
                    outer.transport_rejections += 1
                    started = time.perf_counter_ns()
                    try:
                        request.sendall(b'HTTP/1.0 503 Service Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n')
                    finally:
                        outer._record({'kind': 'api', 'request_id': '', 'robot_id': None,
                            'start_ns': started, 'end_ns': time.perf_counter_ns(), 'status': 503,
                            'body_send_completed': True, 'reason_code': 'transport_capacity_rejected'})
                        self.shutdown_request(request)
                    return
                try:
                    with outer.drained_condition: outer.active_threads += 1
                    super().process_request(request, client_address)
                except Exception:
                    with outer.drained_condition:
                        outer.active_threads -= 1; outer.drained_condition.notify_all()
                    outer.connection_slots.release(); raise
            def process_request_thread(self, request, client_address):
                try:
                    super().process_request_thread(request, client_address)
                finally:
                    outer.connection_slots.release()
                    with outer.drained_condition:
                        outer.active_threads -= 1; outer.drained_condition.notify_all()
        self.server = Server(('127.0.0.1', port), Handler)
        self.endpoint = 'http://127.0.0.1:' + str(self.server.server_address[1])
        if self.telemetry_path:
            import os
            import socket
            self.telemetry_path.parent.mkdir(parents=True, exist_ok=True)
            # One owned, bounded sink; response threads never reopen a file or
            # wait for observer filesystem I/O while holding transport slots.
            if self.telemetry_path.exists():
                status_path = self.telemetry_path.with_suffix(self.telemetry_path.suffix+'.status.json')
                self.prior_trace_incomplete = (not status_path.exists() or
                    json.loads(status_path.read_text(encoding='utf-8')).get('complete') is not True)
            self.telemetry_stream = self.telemetry_path.open('a', encoding='utf-8', buffering=1024*1024)
            self.telemetry_thread = threading.Thread(target=self._sink, name='agent-http-trace-sink', daemon=True)
            self.telemetry_thread.start()
            self._record({'kind': 'probe_identity', 'pid': os.getpid(),
                    'hostname': socket.gethostname(), 'clock': 'perf_counter_ns',
                    'version': VERSION, 'prior_trace_incomplete': self.prior_trace_incomplete,
                    'clock_info': vars(time.get_clock_info('perf_counter'))})
        self.thread = threading.Thread(target=self.server.serve_forever, name='agent-http-service', daemon=True)
        self.thread.start()

    def _record(self, row):
        if self.telemetry_path is None: return
        try:
            self.telemetry_queue.put_nowait(row)
        except queue.Full:
            self.telemetry_dropped += 1
            self.telemetry_error = True

    def _sink(self):
        try:
            while True:
                row = self.telemetry_queue.get()
                try:
                    if row is None: return
                    self.telemetry_stream.write(canonical(row)+'\n')
                    if self.telemetry_queue.empty(): self.telemetry_stream.flush()
                finally:
                    self.telemetry_queue.task_done()
        except Exception:
            self.telemetry_error = True
        finally:
            self.telemetry_stream.close()

    def close(self):
        self.server.shutdown(); self.server.server_close(); self.thread.join(timeout=5)
        with self.drained_condition:
            drained = self.drained_condition.wait_for(lambda: self.active_threads == 0,
                                                      timeout=self.service.request_timeout+1)
        if self.telemetry_path:
            if self.telemetry_thread.is_alive():
                try:
                    self.telemetry_queue.put(None, timeout=5)
                except queue.Full:
                    self.telemetry_error = True
                self.telemetry_thread.join(timeout=10)
            sink_drained = not self.telemetry_thread.is_alive()
            with self.telemetry_lock:
                self.telemetry_path.with_suffix(self.telemetry_path.suffix+'.status.json').write_text(canonical({
                    'complete': not self.telemetry_error and not self.prior_trace_incomplete and drained and sink_drained and not self.thread.is_alive(),
                    'dropped': self.telemetry_dropped, 'transport_rejections': self.transport_rejections,
                    'prior_trace_incomplete': self.prior_trace_incomplete,
                    'drained': drained and sink_drained and not self.thread.is_alive()}), encoding='utf-8')
