"""Streaming SHA-verified actual-journal oracle, indexed on disk not in a million-row dict."""
from collections import Counter
from copy import deepcopy
import hashlib
import json
import math
from pathlib import Path
import sqlite3
import tempfile
import threading
import zlib

from ..knowledge import canonical, digest
from ..scale_benchmark.model import identity as check_run
from ..wide.queries import COOPERATION_KINDS
from . import VERSION, VECTOR_KIND, positive_integer, real_vector, validate_state


def _json(raw):
    def reject(value):
        raise ValueError('nonfinite JSON constant: ' + value)
    return json.loads(raw, parse_constant=reject)


def _number(value, name):
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        raise ValueError('finite ' + name + ' required')
    return value


def _observe_event(event, previous_event, commit_time):
    """Emission order is strict; different robots' effective times may overlap."""
    number = event['event_seq']
    event_time = _number(event['sim_time'], 'event simulation time')
    if type(number) is not int or number != previous_event+1 or not 0 <= event_time <= commit_time:
        raise ValueError('event sequence gap or future event')
    return number


def _transfer(ledger, holders, event, task):
    """Independent event-state machine; no graph mutable-current properties used."""
    if 'goods_id' not in event or 'location_id' not in event:
        return
    goods, location, kind = event['goods_id'], event['location_id'], event['kind']
    if not isinstance(goods, str) or not goods or not isinstance(location, str) or not location:
        raise ValueError('invalid cargo identity/location')
    before = ledger.get(goods)
    if kind != 'initial_stock' and (task is None or task.get('goods_id') != goods):
        raise ValueError('cargo event references a different/missing task goods')
    if kind == 'initial_stock':
        if before is not None:
            raise ValueError('initial cargo identity reused')
    elif kind == 'received':
        if before is not None or task is None or location != task['source_id']:
            raise ValueError('invalid received cargo dependency')
    elif kind == 'picked':
        if task is None or before != task['source_id'] or location != f"ROBOT-{event['robot_id']}":
            raise ValueError('pick violates unique source custody')
    elif kind in {'placed', 'shipped'}:
        target = 'SHIPPED' if kind == 'shipped' else task['target_id'] if task else None
        if before != f"ROBOT-{event['robot_id']}" or location != target:
            raise ValueError('delivery violates unique carrier custody')
    elif kind == 'cargo_handoff':
        donor = event.get('donor_robot_id', event.get('donor_id'))
        receiver = event.get('receiver_robot_id', event.get('receiver_id'))
        if ('donor_id' in event and event.get('donor_robot_id', event['donor_id']) != event['donor_id'] or
                'receiver_id' in event and event.get('receiver_robot_id', event['receiver_id']) != event['receiver_id']):
            raise ValueError('conflicting handoff participant aliases')
        positive_integer(donor, 'handoff donor', 100)
        positive_integer(receiver, 'handoff receiver', 100)
        if donor == receiver or before != f'ROBOT-{donor}' or location != f'ROBOT-{receiver}':
            raise ValueError('handoff violates unique donor/receiver custody')
    else:
        raise ValueError('unknown cargo transition ' + str(kind))
    if before and before.startswith('ROBOT-'):
        holders[int(before[6:])].remove(goods)
    if location.startswith('ROBOT-'):
        owner = int(location[6:])
        positive_integer(owner, 'cargo holder', 100)
        holders[owner].add(goods)
        if len(holders[owner]) != 1:
            raise ValueError('one-box robot has multiple goods')
    ledger[goods] = location


SCHEMA = '''
CREATE TABLE meta(key TEXT PRIMARY KEY,value TEXT NOT NULL);
CREATE TABLE batches(seq INTEGER PRIMARY KEY,checksum TEXT NOT NULL,sim_time REAL NOT NULL,tick INTEGER NOT NULL);
CREATE TABLE states(seq INTEGER NOT NULL,robot INTEGER NOT NULL,payload TEXT NOT NULL,sha256 TEXT NOT NULL,
    sim_time REAL NOT NULL,tick INTEGER NOT NULL,task_id TEXT,cargo_id TEXT,stage TEXT,carrying INTEGER NOT NULL,
    PRIMARY KEY(seq,robot)) WITHOUT ROWID;
CREATE INDEX state_history ON states(robot,seq);
CREATE TABLE tasks(task_id TEXT NOT NULL,seq INTEGER NOT NULL,goods_id TEXT NOT NULL,payload TEXT NOT NULL,
    PRIMARY KEY(task_id,seq)) WITHOUT ROWID;
CREATE INDEX task_by_seq ON tasks(seq,task_id);
CREATE TABLE events(event_seq INTEGER PRIMARY KEY,seq INTEGER NOT NULL,kind TEXT NOT NULL,
    robot_id INTEGER,task_id TEXT,goods_id TEXT,payload TEXT NOT NULL);
CREATE INDEX event_task_order ON events(task_id,event_seq);
CREATE INDEX event_commit_order ON events(seq,event_seq);
CREATE TABLE goods(goods_id TEXT NOT NULL,event_seq INTEGER NOT NULL,seq INTEGER NOT NULL,
    sim_time REAL NOT NULL,kind TEXT NOT NULL,location_id TEXT NOT NULL,event_key TEXT NOT NULL,
    PRIMARY KEY(goods_id,event_seq)) WITHOUT ROWID;
CREATE INDEX goods_by_seq ON goods(seq,event_seq);
'''


class RealOracle:
    """Freeze and verify journal prefix W while its original writer may continue.

    Building writes only a NEW caller-owned SQLite index. The source journal is
    opened mode=ro in a consistent read transaction; queries use per-thread
    read-only connections. Logical goods ledger is bounded by cargo identities,
    never by the number of historical agent states.
    """
    def __init__(self, journal_path, prefix=None, *, index_path=None, run_id=None):
        source = Path(journal_path).resolve()
        if source.is_dir():
            source /= 'journal.sqlite'
        # Keep auto-created files beside this caller-owned source, rather than
        # a platform temp directory which can have different sandbox ACLs.
        self._temporary = tempfile.TemporaryDirectory(prefix='whreal-oracle-', dir=source.parent) if index_path is None else None
        destination = Path(self._temporary.name)/'oracle.sqlite' if index_path is None else Path(index_path).resolve()
        if destination.exists():
            raise FileExistsError('preserve previous oracle index; use RealOracle.open() or a new path')
        destination.parent.mkdir(parents=True, exist_ok=True)
        try:
            self._build(source, prefix, destination, run_id)
            self._initialize(destination)
        except Exception:
            if self._temporary is not None:
                self._temporary.cleanup()
            raise

    @classmethod
    def open(cls, index_path):
        """Open an already verified immutable index in a worker process."""
        value = cls.__new__(cls)
        value._temporary = None
        value._initialize(Path(index_path).resolve())
        return value

    from_index = open

    def _initialize(self, path):
        self.index_path = path
        self._local = threading.local()
        self._connections = []
        self._lock = threading.Lock()
        row = self._db().execute("SELECT value FROM meta WHERE key='identity'").fetchone()
        if not row:
            raise ValueError('oracle index incomplete/unverified')
        self._identity = _json(row[0])
        if self._identity.get('version') != VERSION or self._identity.get('verified') is not True:
            raise ValueError('wrong oracle version or incomplete verification')
        self.run_id = self._identity['run_id']
        self.prefix = self._identity['prefix']
        self.min_task_seq = self._identity['min_task_seq']
        self._metadata_sha256 = digest(self._identity['metadata'])
        self._config_sha256 = digest(self._identity['config'])

    def _db(self):
        db = getattr(self._local, 'db', None)
        if db is None:
            db = sqlite3.connect(self.index_path.as_uri()+'?mode=ro&immutable=1', uri=True,
                                 check_same_thread=False, timeout=30)
            self._local.db = db
            with self._lock:
                self._connections.append(db)
        return db

    @property
    def identity(self):
        return deepcopy(self._identity)

    def check_source_identity(self, metadata, config):
        """Compare complete, canonical source fields without copying the scene.

        No private data escapes: callers still receive defensive copies through
        ``identity``. Original digests are cached once; actual fields are fully
        encoded each time. Python's True == 1 / 1 == 1.0 aliases must not pass.
        """
        try:
            actual_metadata, actual_config = digest(metadata), digest(config)
        except (TypeError, ValueError, UnicodeError) as exc:
            raise ValueError('immutable source metadata/config mismatch') from exc
        if actual_metadata != self._metadata_sha256 or actual_config != self._config_sha256:
            raise ValueError('immutable source metadata/config mismatch')

    def _cutoff(self, seq):
        return positive_integer(seq, 'committed cutoff', self.prefix)

    @staticmethod
    def _build(source, prefix, destination, requested_run):
        db = sqlite3.connect(source.as_uri()+'?mode=ro', uri=True, timeout=30)
        out = None
        try:
            db.execute('BEGIN')
            heads = db.execute('SELECT run_id,seq,checksum,checkpoint,mirrored_seq FROM heads' +
                               (' WHERE run_id=?' if requested_run is not None else ''),
                               (requested_run,) if requested_run is not None else ()).fetchall()
            if len(heads) != 1:
                raise ValueError('select exactly one real journal run')
            run, head_seq, head_sha, head_blob, mirrored_seq = heads[0]
            check_run(run)
            head_raw = zlib.decompress(head_blob)
            if hashlib.sha256(head_raw).hexdigest() != head_sha:
                raise ValueError('journal head checksum mismatch')
            head = _json(head_raw)
            if head.get('run_id') != run or head.get('knowledge_seq') != head_seq:
                raise ValueError('journal head identity mismatch')
            prefix = head_seq if prefix is None else positive_integer(prefix, 'prefix', head_seq)
            positive_integer(prefix, 'prefix', head_seq)
            out = sqlite3.connect(destination)
            out.executescript(SCHEMA)
            out.execute('BEGIN')
            previous_seq = previous_event = 0
            previous_time = -1
            metadata = config = None
            rolling = hashlib.sha256()
            ledger = {}
            holders = {i: set() for i in range(1, 101)}
            stages = Counter()
            carrying = slot_versions = 0
            last_states = None
            for seq, checksum, blob in db.execute('SELECT seq,checksum,batch FROM commits WHERE run_id=? AND seq<=? ORDER BY seq', (run, prefix)):
                raw = zlib.decompress(blob)
                if hashlib.sha256(raw).hexdigest() != checksum:
                    raise ValueError('journal batch SHA mismatch')
                batch = _json(raw)
                positive_integer(batch.get('seq'), 'batch sequence')
                if seq != previous_seq+1 or batch.get('seq') != seq or batch.get('run_id') != run:
                    raise ValueError('journal sequence gap or wrong run')
                sim_time = _number(batch.get('sim_time'), 'simulation time')
                if sim_time < max(0, previous_time):
                    raise ValueError('simulation time moved backwards')
                if type(batch.get('tick')) is not int or batch['tick'] < 0:
                    raise ValueError('nonnegative integer source tick required')
                if metadata is None:
                    metadata, config = batch['metadata'], batch['config']
                    if metadata.get('domain') not in {'warehouse-inbound-outbound-day1', 'warehouse-wide-inbound-outbound'}:
                        raise ValueError('source is not a real warehouse runtime')
                    if metadata.get('scale_version') or 'synthetic_resampled' in str(metadata.get('scale_kind', '')):
                        raise ValueError('archive resampling is not real runtime history')
                elif metadata != batch['metadata'] or config != batch['config']:
                    raise ValueError('source configuration/metadata changed inside frozen prefix')
                states = batch['robots']
                slot_versions += len(batch['slots'])
                ids = [s.get('robot_id') for s in states]
                if len(ids) != 100 or set(ids) != set(range(1, 101)) or any(isinstance(v, bool) for v in ids):
                    raise ValueError('each actual commit must contain all 100 unique robot states')
                for task in batch['tasks']:
                    task_id, goods_id = task['task_id'], task['goods_id']
                    if not isinstance(task_id, str) or not task_id or not isinstance(goods_id, str) or not goods_id:
                        raise ValueError('invalid task/cargo identity')
                    out.execute('INSERT INTO tasks VALUES (?,?,?,?)', (task_id, seq, goods_id, canonical(task).decode()))
                task_cache = {}

                def task_at(task_id):
                    if task_id is None:
                        return None
                    if task_id not in task_cache:
                        found = out.execute('SELECT payload FROM tasks WHERE task_id=? AND seq<=? ORDER BY seq DESC LIMIT 1', (task_id, seq)).fetchone()
                        task_cache[task_id] = _json(found[0]) if found else None
                    return task_cache[task_id]

                for event in batch['events']:
                    # event_seq is emission/observation order. A completion at
                    # now+1 for an earlier robot can precede another robot's
                    # charging-start event at now in this same simulation step.
                    # Preserve both times; do not sort or impose a false global
                    # effective-time monotonicity contract on different robots.
                    number = _observe_event(event, previous_event, sim_time)
                    previous_event = number
                    task = task_at(event.get('task_id'))
                    _transfer(ledger, holders, event, task)
                    out.execute('INSERT INTO events VALUES (?,?,?,?,?,?,?)',
                        (number, seq, event['kind'], event.get('robot_id'), event.get('task_id'), event.get('goods_id'), canonical(event).decode()))
                    if 'goods_id' in event and 'location_id' in event:
                        out.execute('INSERT INTO goods VALUES (?,?,?,?,?,?,?)',
                            (event['goods_id'], number, seq, event['sim_time'], event['kind'], event['location_id'], event['event_id']))
                for state in states:
                    owner = validate_state(state)
                    current_task = task_at(state.get('task_id'))
                    if state.get('task_id') is not None and current_task is None:
                        raise ValueError('robot references a future/missing task state')
                    if state['carrying'] and (current_task is None or current_task.get('goods_id') != state['cargo_id']):
                        raise ValueError('robot cargo differs from its current task goods')
                    expected = {state.get('cargo_id')} if state.get('carrying') else set()
                    if None in expected or holders[owner] != expected:
                        raise ValueError('actual robot payload disagrees with independent cargo event ledger')
                    stages[state.get('stage', 'unknown')] += 1
                    carrying += bool(state.get('carrying'))
                    out.execute('INSERT INTO states VALUES (?,?,?,?,?,?,?,?,?,?)',
                        (seq, owner, canonical(state).decode(), digest(state), sim_time, batch['tick'],
                         state.get('task_id'), state.get('cargo_id'), state.get('stage'), int(bool(state.get('carrying')))))
                out.execute('INSERT INTO batches VALUES (?,?,?,?)', (seq, checksum, sim_time, batch['tick']))
                rolling.update(f'{seq}:{checksum}\n'.encode())
                previous_seq, previous_time, last_states = seq, sim_time, states
                if seq % 100 == 0:
                    out.commit()
                    out.execute('BEGIN')
            if previous_seq != prefix:
                raise ValueError('requested prefix is incomplete in source journal')
            if prefix == head_seq:
                if ledger != head['goods'] or sum(v != 'SHIPPED' for v in ledger.values()) != head['inside']:
                    raise ValueError('final checkpoint disagrees with independent cargo replay')
                if previous_event != head['event_sequence']:
                    raise ValueError('final event archive is incomplete')
                if {s['robot_id']: s for s in last_states} != {r['state']['robot_id']: r['state'] for r in head['robots']}:
                    raise ValueError('final robot state differs from checkpoint')
                count = 0
                for task_id, payload in out.execute('''SELECT t.task_id,t.payload FROM tasks t
                    WHERE t.seq=(SELECT max(v.seq) FROM tasks v WHERE v.task_id=t.task_id)'''):
                    count += 1
                    if head['tasks'].get(task_id) != _json(payload):
                        raise ValueError('final task delta replay differs from checkpoint')
                if count != len(head['tasks']):
                    raise ValueError('final task archive incomplete')
            min_task = out.execute('SELECT min(seq) FROM tasks').fetchone()[0]
            logical_tasks = out.execute('SELECT count(DISTINCT task_id) FROM tasks').fetchone()[0]
            source_keys = ['scene_sha256', 'tasks_sha256', 'initial_inventory_sha256', 'manifest_sha256',
                           'code_sha256', 'execution_code_sha256', 'scheduler_v3_code_sha256', 'wide_code_sha256']
            identity = {'version': VERSION, 'verified': True, 'run_id': run, 'prefix': prefix,
                'prefix_sha256': rolling.hexdigest(), 'source_journal_path': str(source),
                'source_head_at_snapshot': head_seq, 'source_mirrored_seq_at_snapshot': mirrored_seq,
                'head_checkpoint_sha256': head_sha, 'metadata': metadata, 'config': config,
                'source_identity': {k: metadata[k] for k in source_keys if k in metadata},
                'states': 100*prefix, 'events': previous_event, 'logical_tasks': logical_tasks,
                'task_versions': out.execute('SELECT count(*) FROM tasks').fetchone()[0],
                'slot_versions': slot_versions,
                'min_task_seq': min_task, 'state_stage_counts': dict(stages), 'carrying_state_rows': carrying,
                'vector_kind': VECTOR_KIND, 'last_sim_time': previous_time}
            out.execute('INSERT INTO meta VALUES (?,?)', ('identity', canonical(identity).decode()))
            out.commit()
        finally:
            if out is not None:
                out.close()
            db.close()

    def state(self, seq, robot):
        self._cutoff(seq)
        positive_integer(robot, 'robot ID', 100)
        row = self._db().execute('SELECT payload FROM states WHERE seq=? AND robot=?', (seq, robot)).fetchone()
        if not row:
            raise ValueError('source robot state missing')
        return _json(row[0])

    def history(self, seq, robot, limit=5):
        self._cutoff(seq)
        positive_integer(robot, 'robot ID', 100)
        positive_integer(limit, 'history limit', 1000)
        return [{'key': f'{self.run_id}:{number}:{robot}', 'seq': number, 'robot_id': robot,
                 'payload': payload, 'real_payload_sha256': checksum}
                for number, payload, checksum in self._db().execute(
                    'SELECT seq,payload,sha256 FROM states WHERE robot=? AND seq<=? ORDER BY seq DESC LIMIT ?', (robot, seq, limit))]

    def task(self, seq, task_id):
        self._cutoff(seq)
        if task_id is None:
            return None
        if not isinstance(task_id, str) or not task_id:
            raise ValueError('invalid task ID')
        row = self._db().execute('SELECT payload FROM tasks WHERE task_id=? AND seq<=? ORDER BY seq DESC LIMIT 1', (task_id, seq)).fetchone()
        return _json(row[0]) if row else None

    def vector(self, seq, robot):
        return real_vector(self.state(seq, robot), seq, robot)

    def batch_checksum(self, seq):
        self._cutoff(seq)
        return self._db().execute('SELECT checksum FROM batches WHERE seq=?', (seq,)).fetchone()[0]

    def focus(self, seq, robot=None):
        self._cutoff(seq)
        if robot is not None:
            task = self.task(seq, self.state(seq, robot).get('task_id'))
            if task:
                return {'task_id': task['task_id'], 'goods_id': task['goods_id']}
        row = self._db().execute('''SELECT t.task_id,t.goods_id FROM tasks t WHERE t.seq<=?
            AND EXISTS(SELECT 1 FROM goods g WHERE g.goods_id=t.goods_id AND g.seq<=?)
            ORDER BY t.seq DESC,t.task_id LIMIT 1''', (seq, seq)).fetchone()
        return {'task_id': row[0], 'goods_id': row[1]} if row else None

    def goods_history(self, seq, goods_id, after_event_seq=0, limit=200):
        self._cutoff(seq)
        positive_integer(limit, 'goods limit', 1000)
        if not isinstance(goods_id, str) or not goods_id or type(after_event_seq) is not int or after_event_seq < 0:
            raise ValueError('invalid goods identity or event cursor')
        current = self._db().execute('''SELECT location_id,event_seq,sim_time FROM goods
            WHERE goods_id=? AND seq<=? ORDER BY event_seq DESC LIMIT 1''', (goods_id, seq)).fetchone()
        rows = self._db().execute('''SELECT event_seq,seq,sim_time,kind,location_id,event_key FROM goods
            WHERE goods_id=? AND seq<=? AND event_seq>? ORDER BY event_seq LIMIT ?''', (goods_id, seq, after_event_seq, limit+1)).fetchall()
        more = len(rows) > limit
        value = [dict(zip(('event_seq', 'seq', 'sim_time', 'kind', 'location_id', 'event_key'), r)) for r in rows[:limit]]
        return {'run_id': self.run_id, 'goods_id': goods_id, 'seq': seq, 'graph_committed_seq': self.prefix,
            'current_as_of': dict(zip(('location_id', 'event_seq', 'sim_time'), current)) if current else None,
            'rows': value, 'has_more': more,
            'next_after_event_seq': value[-1]['event_seq'] if more else None}

    def cooperation(self, seq, task_id=None, limit=100, after_event_seq=0):
        self._cutoff(seq)
        positive_integer(limit, 'cooperation limit', 1000)
        if task_id is not None and (not isinstance(task_id, str) or not task_id):
            raise ValueError('invalid task ID')
        if type(after_event_seq) is not int or after_event_seq < 0:
            raise ValueError('invalid event cursor')
        kinds = ','.join('?' for _ in COOPERATION_KINDS)
        query = f'SELECT event_seq,seq,payload FROM events WHERE seq<=? AND event_seq>? AND kind IN ({kinds})'
        values = [seq, after_event_seq, *COOPERATION_KINDS]
        if task_id is not None:
            query += ' AND task_id=?'
            values.append(task_id)
        query += ' ORDER BY event_seq LIMIT ?'
        rows = self._db().execute(query, [*values, limit+1]).fetchall()
        more = len(rows) > limit
        result = [{'event_seq': number, 'seq': commit, 'event': _json(payload)} for number, commit, payload in rows[:limit]]
        return {'run_id': self.run_id, 'task_id': task_id, 'seq': seq, 'graph_committed_seq': self.prefix,
                'rows': result, 'has_more': more,
                'next_after_event_seq': result[-1]['event_seq'] if more else None}

    def handoff_points(self, limit=100):
        positive_integer(limit, 'handoff limit', 10000)
        result = []
        for seq, number, payload in self._db().execute("SELECT seq,event_seq,payload FROM events WHERE kind='cargo_handoff' ORDER BY event_seq LIMIT ?", (limit,)):
            event = _json(payload)
            result.append({'seq': seq, 'before_seq': max(1, seq-1), 'event_seq': number,
                'task_id': event['task_id'], 'goods_id': event['goods_id'],
                'donor_robot_id': event.get('donor_robot_id', event.get('donor_id')),
                'receiver_robot_id': event.get('receiver_robot_id', event.get('receiver_id'))})
        return result

    def close(self):
        for db in self._connections:
            db.close()
        self._connections.clear()
        if self._temporary is not None:
            self._temporary.cleanup()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
