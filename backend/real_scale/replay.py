"""Bounded actual-journal graph preload/tail replay, NOT live robot execution.

The source is opened SQLite mode=ro, in short autocommit reads. Original IDs,
times, payloads and metadata are never changed or resampled. Each invocation
uses a NEW evidence directory; --resume permits an existing verified graph,
not overwriting old evidence. Use one graph writer for this source run.

max-wall-seconds is checked between operations; an in-flight atomic Neo4j
transaction retains RealMirror's 60s server timeout. Use an outer supervisor
with a suitable grace period when an absolute process deadline is required.
"""
import argparse
from copy import deepcopy
import hashlib
import json
import math
import os
from pathlib import Path
import sqlite3
import time
import zlib

from ..knowledge import digest
from . import VERSION, SOURCE_KIND, VECTOR_KIND, DIMENSIONS, positive_integer
from .oracle import _json, _number, _observe_event
from .store import identity
from .writer import RealMirror


SCOPE = 'actual_journal_replay_not_live_robot_simulation'
REPLAY_VERSION = 'warehouse-real-journal-replay-v2'
COUNTERS = ('nodes_created', 'nodes_deleted', 'relationships_created',
            'relationships_deleted', 'properties_set')


def _save(path, value):
    with Path(path).open('x', encoding='utf-8') as stream:
        json.dump(value, stream, ensure_ascii=False, indent=2, allow_nan=False)


class Source:
    """SHA/identity-checked forward cursor; never holds a read transaction."""
    def __init__(self, path, run_id):
        identity(run_id)
        self.path = Path(path).resolve()
        if self.path.is_dir():
            self.path /= 'journal.sqlite'
        if not self.path.is_file():
            raise FileNotFoundError('source journal does not exist')
        self.run_id = run_id
        self.db = sqlite3.connect(self.path.as_uri()+'?mode=ro', uri=True,
                                  isolation_level=None, timeout=10)
        self.db.execute('PRAGMA query_only=ON')
        self.metadata = self.config = None
        self._cached_head = self._cached_blob_sha256 = None
        self.maximum_head = self.verified_seq = self.event_seq = 0
        self.sim_time = self.tick = -1
        self.rolling = hashlib.sha256()
        try:
            self.initial_head = self.head()
            rows = self.db.execute('SELECT seq,checksum,batch FROM commits WHERE run_id=? AND seq=1',
                                   (run_id,)).fetchall()
            if len(rows) != 1:
                raise ValueError('source first commit missing')
            self.first_checksum = rows[0][1]
            self._decode(rows[0], 1)
            self.source_identity = {'run_id': run_id, 'metadata': self.metadata,
                                    'config': self.config, 'first_batch_sha256': self.first_checksum}
            self.identity_sha256 = digest(self.source_identity)
        except Exception:
            self.close()
            raise

    def close(self):
        self.db.close()

    def head(self):
        rows = self.db.execute('SELECT seq,checksum,checkpoint,mirrored_seq FROM heads WHERE run_id=?',
                               (self.run_id,)).fetchall()
        if len(rows) != 1:
            raise ValueError('requested source run is absent')
        seq, checksum, blob, mirrored = rows[0]
        positive_integer(seq, 'source durable head')
        if seq < self.maximum_head:
            raise ValueError('source durable head moved backwards')
        blob_sha256 = hashlib.sha256(blob).hexdigest()
        if self._cached_head is not None and seq == self._cached_head['seq']:
            if checksum != self._cached_head['checkpoint_sha256']:
                raise ValueError('source checkpoint checksum changed at the same durable sequence')
            if blob_sha256 != self._cached_blob_sha256:
                raise ValueError('source checkpoint blob changed at the same durable sequence/checksum')
            # These exact compressed bytes already passed decompression, full
            # raw SHA, run/seq and immutable config checks. Cache only one head;
            # mirrored_seq is allowed to move independently of business data.
            return {**self._cached_head, 'source_mirrored_seq': mirrored}
        raw = zlib.decompress(blob)
        if hashlib.sha256(raw).hexdigest() != checksum:
            raise ValueError('source checkpoint SHA mismatch')
        cp = _json(raw)
        if cp.get('run_id') != self.run_id or type(cp.get('knowledge_seq')) is not int or cp['knowledge_seq'] != seq:
            raise ValueError('source checkpoint run/sequence mismatch')
        metadata, config = cp.get('run_metadata'), cp.get('config')
        if self.metadata is None:
            if not isinstance(metadata, dict):
                raise ValueError('source checkpoint metadata must be an object')
            marker = metadata.get('real_scale', {})
            if not isinstance(marker, dict):
                raise ValueError('source real_scale identity must be an object')
            if (metadata.get('domain') not in {'warehouse-wide-inbound-outbound', 'warehouse-inbound-outbound-day1'}
                    or marker.get('version') != VERSION or marker.get('source_kind') != SOURCE_KIND
                    or marker.get('vector_kind') != VECTOR_KIND or metadata.get('scale_version')
                    or 'synthetic_resampled' in str(metadata.get('scale_kind', ''))):
                raise ValueError('source is not a real_scale actual runtime journal')
            positive_integer(marker.get('commit_seconds'), 'source cadence', 300)
            if not isinstance(config, dict) or config.get('robot_count') != 100 or config.get('chargers') != 100:
                raise ValueError('source must retain 100 actual robots and bays')
            self.metadata, self.config = metadata, config
        elif metadata != self.metadata or config != self.config:
            raise ValueError('source immutable metadata/config changed')
        self.maximum_head = seq
        self._cached_head = {'seq': seq, 'checkpoint_sha256': checksum,
            'sim_time': _number(cp.get('elapsed'), 'checkpoint time'),
            'tick': cp.get('tick'), 'source_mirrored_seq': mirrored, 'running': cp.get('running')}
        self._cached_blob_sha256 = blob_sha256
        return dict(self._cached_head)

    def checksum(self, seq):
        rows = self.db.execute('SELECT checksum FROM commits WHERE run_id=? AND seq=?',
                               (self.run_id, seq)).fetchall()
        if len(rows) != 1:
            raise ValueError('graph head references a missing source commit')
        return rows[0][0]

    def _decode(self, row, expected):
        seq, checksum, blob = row
        raw = zlib.decompress(blob)
        if hashlib.sha256(raw).hexdigest() != checksum:
            raise ValueError('source batch SHA mismatch')
        batch = _json(raw)
        if (type(seq) is not int or seq != expected or type(batch.get('seq')) is not int
                or batch['seq'] != seq or batch.get('run_id') != self.run_id):
            raise ValueError('source commit run/continuous sequence mismatch')
        if digest(batch) != checksum:
            raise ValueError('source batch is not its original canonical checksum')
        if batch.get('metadata') != self.metadata or batch.get('config') != self.config:
            raise ValueError('source batch immutable identity mismatch')
        states = batch.get('robots', [])
        ids = [s.get('robot_id') for s in states]
        if len(ids) != 100 or set(ids) != set(range(1, 101)) or any(type(i) is not int for i in ids):
            raise ValueError('source commit needs exactly 100 actual entities')
        return batch

    def read(self, through, limit=20):
        """Read at most limit contiguous rows, release SQLite cursor before graph I/O."""
        positive_integer(limit, 'batch size', 50)
        count = min(limit, through-self.verified_seq)
        if count <= 0:
            return []
        rows = self.db.execute('SELECT seq,checksum,batch FROM commits WHERE run_id=? AND seq>? AND seq<=? ORDER BY seq LIMIT ?',
                               (self.run_id, self.verified_seq, through, count)).fetchall()
        if len(rows) != count:
            raise ValueError('source committed prefix contains missing rows')
        batches = []
        for row in rows:
            batch = self._decode(row, self.verified_seq+1)
            now = _number(batch.get('sim_time'), 'source simulation time')
            tick = batch.get('tick')
            if now < max(0, self.sim_time) or type(tick) is not int or tick < max(0, self.tick):
                raise ValueError('source commit time/tick moved backwards')
            for event in batch['events']:
                self.event_seq = _observe_event(event, self.event_seq, now)
                if event.get('event_id') != f'{self.run_id}:event:{self.event_seq}':
                    raise ValueError('source event identity is not scoped to this run/sequence')
            self.verified_seq, self.sim_time, self.tick = batch['seq'], now, tick
            self.rolling.update(f'{batch["seq"]}:{row[1]}\n'.encode())
            batches.append(batch)
        return batches


class _Result:
    def __init__(self, result, counters):
        self.result, self.counters, self.consumed = result, counters, False

    def __getattr__(self, key):
        return getattr(self.result, key)

    def consume(self):
        summary = self.result.consume()
        if not self.consumed:
            for key in COUNTERS:
                self.counters[key] += getattr(summary.counters, key, 0)
            self.consumed = True
        return summary


class _Transaction:
    def __init__(self, tx, counters):
        self.tx, self.counters = tx, counters

    def run(self, *args, **kwargs):
        return _Result(self.tx.run(*args, **kwargs), self.counters)


class ReplayMirror(RealMirror):
    """Keep RealMirror.apply_many transactions; observe genuine write counters."""
    def _apply(self, tx, batches):
        counters = {key: 0 for key in COUNTERS}
        RealMirror._apply(_Transaction(tx, counters), batches)
        self.last_counters = counters

    def apply_many(self, batches):
        self.last_counters = None
        return super().apply_many(batches)


def connect(*, ensure_schema=False):
    from neo4j import GraphDatabase
    driver = GraphDatabase.driver(os.environ['NEO4J_URI'],
        auth=(os.environ.get('NEO4J_USER', 'neo4j'), os.environ['NEO4J_PASSWORD']),
        max_connection_pool_size=4, connection_timeout=10,
        connection_acquisition_timeout=10, max_transaction_retry_time=0)
    try:
        driver.verify_connectivity()
        mirror = ReplayMirror(driver, os.environ.get('NEO4J_DATABASE', 'neo4j'))
        if ensure_schema:
            mirror.ensure_schema()
        return mirror
    except Exception:
        driver.close()
        raise


def _graph_metadata(mirror, run):
    return mirror.query('''MATCH (r:WHRun {run_id:$run}) RETURN r.metadata_json AS metadata,
        r.config_json AS config,r.real_scale_version AS version,r.real_source_kind AS source_kind,
        r.real_source_run AS source_run,r.real_vector_kind AS vector_kind,r.real_dimensions AS dimensions,
        r.real_last_seq AS seq,r.real_last_batch_sha256 AS checksum''', {'run': run}, timeout=15)


def _verify_existing(mirror, source, resume, deadline, clock):
    point = mirror.committed_point(source.run_id)
    if point is None:
        orphan = mirror.query('''CALL () { MATCH (s:WHStep {run_id:$run}) RETURN count(s) AS steps }
            CALL () { MATCH (a:WHAgentState {run_id:$run}) RETURN count(a) AS states } RETURN steps,states''',
            {'run': source.run_id}, timeout=15)
        if len(orphan) != 1 or orphan[0]['steps'] or orphan[0]['states']:
            raise ValueError('orphan graph history without a run head')
        return {'seq': 0, 'checksum': None}
    if not resume:
        raise ValueError('graph run already exists; explicit --resume required with NEW evidence output')
    positive_integer(point.get('seq'), 'existing graph head')
    if point['seq'] > source.initial_head['seq'] or point.get('checksum') != source.checksum(point['seq']):
        raise ValueError('foreign/ahead/conflicting graph head')
    rows = _graph_metadata(mirror, source.run_id)
    if len(rows) != 1:
        raise ValueError('graph real source identity absent')
    value = rows[0]
    if (_json(value['metadata']) != source.metadata or _json(value['config']) != source.config
            or value.get('version') != VERSION or value.get('source_kind') != SOURCE_KIND
            or value.get('source_run') != source.run_id or value.get('vector_kind') != VECTOR_KIND
            or value.get('dimensions') != DIMENSIONS or value.get('seq') != point['seq']
            or value.get('checksum') != point['checksum']):
        raise ValueError('graph metadata/real-source marker mismatch')
    steps = mirror.query('''MATCH (s:WHStep {run_id:$run}) USING INDEX s:WHStep(run_id)
        WHERE s.seq>=1 AND s.seq<=$seq RETURN s.seq AS seq,s.checksum AS checksum,s.key AS key ORDER BY s.seq''',
        {'run': source.run_id, 'seq': point['seq']}, timeout=120)
    if len(steps) != point['seq']:
        raise ValueError('existing graph prefix contains missing steps')
    while source.verified_seq < point['seq']:
        if clock() >= deadline:
            raise TimeoutError('wall bound reached while verifying existing prefix')
        batches = source.read(point['seq'], 50)
        for batch in batches:
            seq = batch['seq']
            row = steps[seq-1]
            if (type(row.get('seq')) is not int or row['seq'] != seq or
                    row.get('key') != f'{source.run_id}:{seq}' or row.get('checksum') != digest(batch)):
                raise ValueError('existing graph prefix step/checksum conflicts with source')
    return point


def run(args, mirror, *, clock=time.monotonic, sleep=time.sleep):
    """Bounded writer. The caller owns mirror; errors retain raw evidence, exit2."""
    positive_integer(args.batch_size, 'batch size', 50)
    for value, name, high in [(args.max_wall_seconds, 'wall limit', 21600),
                              (args.interval_seconds, 'interval', 60)]:
        if (isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value)
                or value < 0 or value > high or name=='wall limit' and value==0):
            raise ValueError('invalid bounded '+name)
    if args.interval_seconds and args.batch_size != 1:
        raise ValueError('paced tail replay requires --batch-size 1')
    if args.stop_after_seq is not None:
        positive_integer(args.stop_after_seq, 'stop sequence')
    output = Path(args.output_dir)
    output.mkdir(parents=True, exist_ok=False)
    started, source = clock(), None
    deadline, next_start = started+args.max_wall_seconds, started
    report = {'scope': SCOPE, 'version': REPLAY_VERSION, 'status': 'failed',
        'replay_mode': 'paced_tail' if args.interval_seconds else 'bulk_preload',
        'live_robot_simulation': False, 'query_latency_measured': False,
        'source_readonly': True, 'errors': 0, 'first_source_seq': None, 'last_source_seq': None,
        'maximum_schedule_delay_seconds': 0., 'maximum_scheduled_supply_lag_commits': 0,
        'output_dir': str(output.resolve()), 'run_id': args.run_id, 'transactions': 0,
        'replayed_commits': 0, 'batch_size': args.batch_size, 'interval_seconds': args.interval_seconds,
        'stop_after_seq': args.stop_after_seq, 'follow': args.follow,
        'replay_code_sha256': hashlib.sha256(Path(__file__).read_bytes()).hexdigest()}
    point = {'seq': 0, 'checksum': None}
    try:
        with (output/'transactions.jsonl').open('x', encoding='utf-8') as raw:
            source = Source(args.journal, args.run_id)
            point = _verify_existing(mirror, source, args.resume, deadline, clock)
            report.update(source_path=str(source.path), source_identity_sha256=source.identity_sha256,
                          source_head_at_start=source.initial_head, graph_head_at_start=deepcopy(point),
                          source_first_batch_sha256=source.first_checksum,
                          verified_existing_prefix_sha256=source.rolling.hexdigest(),
                          graph_prefix_sha256=source.rolling.hexdigest())
            if args.stop_after_seq is not None and point['seq'] > args.stop_after_seq:
                raise ValueError('existing graph is already beyond requested stop sequence; never rewinding')
            if not args.follow and args.stop_after_seq is not None and args.stop_after_seq > source.initial_head['seq']:
                raise ValueError('stop sequence unavailable in source; use --follow or a committed prefix')
            _save(output/'contract.json', {**report, 'source_identity': source.source_identity,
                'resume': args.resume, 'schema_requested': bool(args.ensure_schema),
                'graph_scope': 'only missing commits of this exact existing source run; one graph writer',
                'lag_definitions': {'source_backlog_commits': 'durable source head minus graph head; pregenerated backlog, NOT live simulation lag',
                    'lag_seconds': 'completed wall timestamp minus nominal scheduled timestamp, clipped at zero',
                    'scheduled_supply_lag_commits': 'available nominally due sequence minus actual graph head, clipped at zero'},
                'timing': 'replay transaction/publication lag, NOT query latency or live simulation',
                'deadline': 'checked between atomic operations; existing RealMirror transaction timeout60s; use outer supervisor'})
            target = args.stop_after_seq if args.stop_after_seq is not None else (None if args.follow else source.initial_head['seq'])
            schedule_start, schedule_ns, initial_seq = clock(), time.time_ns(), point['seq']
            next_start = schedule_start
            while True:
                if target is not None and point['seq'] == target:
                    report['status'] = 'complete'
                    break
                if clock() >= deadline:
                    report['status'] = 'wall-time-limit'
                    break
                if clock() < next_start:
                    sleep(min(next_start-clock(), deadline-clock()))
                    continue
                head = source.head()
                through = min(head['seq'], target) if target is not None else head['seq']
                if through <= point['seq']:
                    if not args.follow:
                        report['status'] = 'complete'
                        break
                    sleep(min(.2, max(0, deadline-clock())))
                    continue
                before = mirror.committed_point(args.run_id) or {'seq': 0, 'checksum': None}
                if before != point:
                    raise ValueError('another graph writer changed this run during replay')
                batches = source.read(min(through, point['seq']+args.batch_size), args.batch_size)
                if clock() >= deadline:
                    report['status'] = 'wall-time-limit'
                    break
                begin, start_ns = clock(), time.time_ns()
                scheduled_ns = (schedule_ns+round((batches[0]['seq']-initial_seq-1)*args.interval_seconds*1e9)
                                if args.interval_seconds else start_ns)
                txrow = {'scope': SCOPE, 'run_id': args.run_id, 'transaction': report['transactions']+1,
                    'replay_mode': report['replay_mode'], 'batch_size': len(batches), 'scheduled_ns': scheduled_ns,
                    'start_ns': start_ns, 'source_identity_sha256': source.identity_sha256,
                    'source_head_before': head, 'graph_head_before': point,
                    'source_seq': batches[-1]['seq'], 'source_sim_time': batches[-1]['sim_time'],
                    'graph_before_seq': point['seq'], 'batch_checksum': digest(batches[-1]),
                    'first_seq': batches[0]['seq'], 'last_seq': batches[-1]['seq'],
                    'source_batch_checksums': [digest(b) for b in batches],
                    'source_sim_times': [b['sim_time'] for b in batches], 'ok': False, 'error': None}
                try:
                    mirror.apply_many(batches)
                    point = mirror.committed_point(args.run_id)
                    wanted = {'seq': batches[-1]['seq'], 'checksum': digest(batches[-1])}
                    if point != wanted:
                        raise ValueError('post-transaction graph head/checksum mismatch or foreign writer')
                    after_head = source.head()
                    counters = getattr(mirror, 'last_counters', None)
                    if not isinstance(counters, dict) or any(type(counters.get(k)) is not int or counters[k]<0 for k in COUNTERS):
                        raise ValueError('committed graph write counters unavailable')
                    txrow.update(ok=True, graph_head_after=point, source_head_after=after_head,
                        graph_after_seq=point['seq'],
                        actual_neo4j_counters=counters,
                        actual_agent_states_in_source_batches=100*len(batches),
                        source_backlog_commits=after_head['seq']-point['seq'],
                        source_backlog_simulation_seconds=after_head['sim_time']-batches[-1]['sim_time'])
                    due = min(after_head['seq'], initial_seq+1+int((clock()-schedule_start)/args.interval_seconds)) if args.interval_seconds else point['seq']
                    if target is not None:
                        due = min(due, target)
                    txrow.update(scheduled_due_seq=due, scheduled_supply_lag_commits=max(0, due-point['seq']))
                    report['transactions'] += 1
                    report['replayed_commits'] += len(batches)
                    if report['first_source_seq'] is None:
                        report['first_source_seq'] = batches[0]['seq']
                    report['last_source_seq'] = batches[-1]['seq']
                    report['graph_prefix_sha256'] = source.rolling.hexdigest()
                except Exception as exc:
                    txrow['error'] = type(exc).__name__+': '+str(exc)[:1500]
                    # A lost ACK may follow a committed transaction. Never assume rollback.
                    txrow['graph_head_after_error'] = mirror.committed_point(args.run_id)
                    raise
                finally:
                    txrow.update(end_ns=time.time_ns(), transaction_wall_ms=(clock()-begin)*1000)
                    txrow['lag_seconds'] = max(0., (txrow['end_ns']-scheduled_ns)/1e9)
                    report['maximum_schedule_delay_seconds'] = max(report['maximum_schedule_delay_seconds'], txrow['lag_seconds'])
                    report['maximum_scheduled_supply_lag_commits'] = max(report['maximum_scheduled_supply_lag_commits'], txrow.get('scheduled_supply_lag_commits', 0))
                    raw.write(json.dumps(txrow, ensure_ascii=False, separators=(',', ':'), allow_nan=False)+'\n')
                    raw.flush()
                # Absolute nominal deadlines: source/graph preparation after a
                # sleep must not shift every subsequent deadline. If slow, do
                # not sleep to catch up; keep every original batch and its lag.
                next_start = (schedule_start+(point['seq']-initial_seq)*args.interval_seconds
                              if args.interval_seconds else clock())
    except Exception as exc:
        report.update(status='failed', errors=1, error=type(exc).__name__+': '+str(exc)[:2000])
    finally:
        if source is not None:
            try:
                report['source_head_at_end'] = source.head()
                report['graph_head_at_end'] = mirror.committed_point(args.run_id)
                report['verified_source_prefix'] = source.verified_seq
                report['verified_source_prefix_sha256'] = source.rolling.hexdigest()
                if report['graph_head_at_end'] is not None and report['graph_head_at_end']['seq'] == source.verified_seq:
                    report['graph_prefix_sha256'] = source.rolling.hexdigest()
                if report['graph_head_at_end'] is not None:
                    report['source_backlog_commits_at_end'] = report['source_head_at_end']['seq']-report['graph_head_at_end']['seq']
            except Exception as exc:
                report.update(status='failed', errors=report['errors']+1, final_probe_error=type(exc).__name__+': '+str(exc)[:1500])
            source.close()
        report['wall_seconds'] = clock()-started
        report['raw_sha256'] = hashlib.sha256((output/'transactions.jsonl').read_bytes()).hexdigest()
        _save(output/'report.json', report)
    return report


def parser():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--journal', type=Path, required=True)
    p.add_argument('--run-id', required=True)
    p.add_argument('--output-dir', type=Path, required=True)
    p.add_argument('--batch-size', type=int, default=20)
    p.add_argument('--stop-after-seq', type=int)
    p.add_argument('--follow', action='store_true')
    p.add_argument('--resume', action='store_true')
    p.add_argument('--ensure-schema', action='store_true')
    p.add_argument('--max-wall-seconds', type=float, default=600)
    p.add_argument('--interval-seconds', type=float, default=0)
    return p


def main(argv=None):
    args = parser().parse_args(argv)
    mirror = connect(ensure_schema=args.ensure_schema)
    try:
        report = run(args, mirror)
        print(json.dumps({key: report.get(key) for key in ('scope', 'status', 'transactions',
                         'replayed_commits', 'wall_seconds', 'error')}, ensure_ascii=False))
        return 0 if report['status'] == 'complete' else 2
    finally:
        mirror.close()


if __name__ == '__main__':
    raise SystemExit(main())
