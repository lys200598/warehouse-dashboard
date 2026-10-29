"""Isolated, read-only-source derived summary prototype; never a live default.

Builds from original compressed journal batches, independently of RealOracle
and the post-verifier. Full API responses still read graph point/fleet/events
and a complete current graph task-entity budget. Source checks run afterwards.
"""
from collections import defaultdict
from contextlib import closing
import argparse
import gzip
import hashlib
import heapq
import json
import math
import os
from pathlib import Path
import sqlite3
import time
import traceback
import zlib

from backend.knowledge import digest
from backend.query_catalog import QueryCatalogService
from backend.query_catalog.service import IncompleteContext


VERSION = 'warehouse-summary-sidecar-prototype-v1'
BUDGET_QUERY = '''MATCH (r:WHRun {run_id:$run}) WHERE r.committed_seq >= $seq
    MATCH (cutoff:WHStep {key:$run+':'+toString($seq)})
    CALL (r) { MATCH (r)<-[:IN_RUN]-(t:WHTask) RETURN count(t) AS entity_count }
    RETURN r.run_id AS run_id,r.committed_seq AS head,toString(r.created_at) AS created_at,
        cutoff.checksum AS batch_checksum,entity_count'''


def sha(path):
    h = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            h.update(block)
    return h.hexdigest()


def canonical(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False,
                      separators=(',', ':'), allow_nan=False)


class SummaryAccumulator:
    """Only original task deltas and clock advancement; stale heap entries expire."""
    def __init__(self, zones):
        self.zones = zones
        self.tasks = {}
        self.counts = defaultdict(lambda: [0, 0])
        self.heap = []
        self.clock = -1

    def consume(self, tasks, clock, seq):
        if clock < self.clock:
            raise ValueError('source simulation time regressed')
        changed_by_id = {}
        while self.heap and self.heap[0][0] < clock:
            _, version, task_id = heapq.heappop(self.heap)
            current = self.tasks.get(task_id)
            if current and current['version'] == version and not current['overdue']:
                current['overdue'] = True
                self.counts[(current['zone'], current['status'])][1] += 1
                changed_by_id[task_id] = current
        seen = set()
        for task in tasks:
            key = task['task_id']
            if key in seen:
                raise ValueError('duplicate task delta in original batch')
            seen.add(key)
            previous = self.tasks.get(key)
            if previous:
                group = self.counts[(previous['zone'], previous['status'])]
                group[0] -= 1
                group[1] -= int(previous['overdue'])
            due = task['due_second']
            if type(due) not in (int, float) or not math.isfinite(due):
                raise ValueError('invalid task deadline')
            row = {'task_id': key, 'status': task['status'], 'slot_id': task['slot_id'],
                   'due_second': due, 'zone': self.zones.get(task['slot_id'], 'unmapped'),
                   'source_seq': seq, 'version': 1 if previous is None else previous['version'] + 1,
                   'overdue': task['status'] != 'completed' and due < clock}
            self.tasks[key] = row
            group = self.counts[(row['zone'], row['status'])]
            group[0] += 1
            group[1] += int(row['overdue'])
            if row['status'] != 'completed' and not row['overdue']:
                heapq.heappush(self.heap, (due, row['version'], key))
            changed_by_id[key] = row
        self.clock = clock
        counts = [{'zone': z, 'status': s, 'tasks': n, 'overdue': late}
                  for (z, s), (n, late) in sorted(self.counts.items()) if n]
        if any(r['tasks'] < 0 or not 0 <= r['overdue'] <= r['tasks'] for r in counts):
            raise ValueError('derived counts inconsistent')
        return counts, list(changed_by_id.values())


def build_sidecar(journal, output, created_version):
    journal, output = Path(journal).resolve(), Path(output).resolve()
    if output.exists() or Path(str(output) + '.building').exists():
        raise FileExistsError(output)
    output.parent.mkdir(parents=True, exist_ok=True)
    original_sha = sha(journal)
    started = time.perf_counter()
    building = Path(str(output) + '.building')
    db = sqlite3.connect(building)
    db.executescript('''CREATE TABLE meta(key TEXT PRIMARY KEY,value TEXT NOT NULL);
        CREATE TABLE latest_task(task_id TEXT PRIMARY KEY,payload TEXT NOT NULL);
        CREATE TABLE summary(seq INTEGER PRIMARY KEY,payload TEXT NOT NULL,checksum TEXT NOT NULL,cache_key TEXT NOT NULL);''')
    source = sqlite3.connect(journal.as_uri() + '?mode=ro', uri=True)
    first = None
    timing = []
    try:
        heads = source.execute('SELECT run_id,seq FROM heads').fetchall()
        if len(heads) != 1:
            raise ValueError('one original run required')
        run, source_head = heads[0]
        prefix = hashlib.sha256()
        previous = 0
        for seq, checksum, blob in source.execute(
                'SELECT seq,checksum,batch FROM commits WHERE run_id=? ORDER BY seq', (run,)):
            tick = time.perf_counter()
            raw = zlib.decompress(blob)
            batch = json.loads(raw)
            if (seq != previous + 1 or sha_bytes(raw) != checksum or digest(batch) != checksum
                    or batch['seq'] != seq or batch['run_id'] != run):
                raise ValueError('original batch continuity/hash/run invalid')
            if first is None:
                metadata, config = batch['metadata'], batch['config']
                manifest = {'version': VERSION, 'run_id': run, 'created_version': created_version,
                            'metadata_sha256': digest(metadata), 'scene_sha256': digest(metadata['scene']),
                            'config_sha256': digest(config), 'source_journal_sha256': original_sha,
                            'source_head': source_head, 'first_batch_sha256': checksum}
                racks = {r['id']: r['zone'] for r in metadata['scene']['racks']}
                zones = {s['slot_id']: racks.get(s['rack_id'], 'unmapped') for s in batch['slots']}
                accumulator = SummaryAccumulator(zones)
                first = batch
            if (digest(batch['metadata']) != manifest['metadata_sha256']
                    or digest(batch['config']) != manifest['config_sha256']):
                raise ValueError('source scene/config/metadata changed within this prototype')
            counts, changed = accumulator.consume(batch['tasks'], batch['sim_time'], seq)
            if sum(r['tasks'] for r in counts if r['status'] == 'completed') != batch['completed']:
                raise ValueError('derived complete tasks differ from original Step.completed')
            record = {'run_id': run, 'seq': seq, 'sim_time': batch['sim_time'],
                      'completed': batch['completed'], 'batch_checksum': checksum,
                      'task_entity_count': len(accumulator.tasks), 'counts': counts}
            cache_key = digest({'run_id': run, 'created_version': created_version,
                                'scene_sha256': manifest['scene_sha256'], 'config_sha256': manifest['config_sha256'],
                                'seq': seq, 'batch_checksum': checksum})
            db.execute('INSERT INTO summary VALUES (?,?,?,?)',
                       (seq, canonical(record), digest(record), cache_key))
            db.executemany('INSERT OR REPLACE INTO latest_task VALUES (?,?)',
                           [(r['task_id'], canonical(r)) for r in changed])
            prefix.update(canonical([seq, checksum]).encode())
            timing.append((time.perf_counter() - tick) * 1000)
            previous = seq
        if previous != source_head or first is None:
            raise ValueError('source head not fully consumed')
        manifest['consumed_prefix_sha256'] = prefix.hexdigest()
        manifest_checksum = digest(manifest)
        db.execute('INSERT INTO meta VALUES (?,?)', ('manifest', canonical(manifest)))
        db.execute('INSERT INTO meta VALUES (?,?)', ('manifest_sha256', manifest_checksum))
        db.commit()
        if db.execute('PRAGMA quick_check').fetchone()[0] != 'ok':
            raise ValueError('derived SQLite integrity failed')
    finally:
        db.close()
        source.close()
    if sha(journal) != original_sha:
        raise ValueError('frozen source changed while building')
    if output.exists():
        raise FileExistsError(output)
    os.rename(building, output)
    return {'manifest': manifest, 'manifest_sha256': manifest_checksum,
            'sidecar_sha256': sha(output), 'sidecar_bytes': output.stat().st_size,
            'build_wall_seconds': time.perf_counter() - started,
            'batch_processing_ms': {'count': len(timing), 'total': sum(timing),
                                    'mean': sum(timing) / len(timing), 'max': max(timing)},
            'generation_uses_oracle_or_verifier': False}


def sha_bytes(raw):
    return hashlib.sha256(raw).hexdigest()


class SidecarReader:
    def __init__(self, path, expected_manifest_sha256):
        self.path = Path(path).resolve()
        with closing(self.open()) as db:
            if db.execute('PRAGMA quick_check').fetchone()[0] != 'ok':
                raise ValueError('derived SQLite corrupt')
            values = dict(db.execute('SELECT key,value FROM meta'))
        self.manifest = json.loads(values['manifest'])
        if digest(self.manifest) != expected_manifest_sha256 or values['manifest_sha256'] != expected_manifest_sha256:
            raise ValueError('derived manifest identity mismatch')

    def open(self):
        return sqlite3.connect(self.path.as_uri() + '?mode=ro', uri=True)

    def read(self, run, seq, fleet):
        m = self.manifest
        if (m['run_id'] != run or m['created_version'] != fleet['token'][1]
                or m['scene_sha256'] != digest(fleet['metadata']['scene'])
                or m['config_sha256'] != digest(fleet['config'])):
            raise ValueError('derived cache run-creation/scene/config identity mismatch')
        with closing(self.open()) as db:
            row = db.execute('SELECT payload,checksum,cache_key FROM summary WHERE seq=?', (seq,)).fetchone()
        if row is None:
            raise IncompleteContext('derived summary does not contain exact committed cutoff')
        result = json.loads(row[0])
        key = digest({'run_id': run, 'created_version': m['created_version'],
                      'scene_sha256': m['scene_sha256'], 'config_sha256': m['config_sha256'],
                      'seq': seq, 'batch_checksum': fleet['token'][0]})
        if (digest(result) != row[1] or row[2] != key or result['run_id'] != run or result['seq'] != seq
                or result['batch_checksum'] != fleet['token'][0]
                or result['sim_time'] != fleet['step']['sim_time'] or result['completed'] != fleet['step']['completed']):
            raise ValueError('derived summary checksum/exact-point mismatch')
        return result


class SidecarCatalog(QueryCatalogService):
    def __init__(self, store, sidecar):
        super().__init__(store)
        self.sidecar = sidecar

    def _tasks(self, run, seq, fleet, maximum):
        rows = self._read(BUDGET_QUERY, {'run': run, 'seq': seq})
        if len(rows) != 1:
            raise ValueError('actual graph entity budget point absent')
        self._point(rows[0], run, seq, fleet['token'])
        entities = rows[0]['entity_count']
        if type(entities) is not int or entities < 0:
            raise ValueError('invalid graph entity budget')
        if entities > maximum:
            raise IncompleteContext('task entity budget exceeded; raise max_tasks explicitly')
        result = self.sidecar.read(run, seq, fleet)
        counts = result['counts']
        if (any(type(r['tasks']) is not int or type(r['overdue']) is not int
                or not 0 <= r['overdue'] <= r['tasks'] for r in counts)
                or sum(r['tasks'] for r in counts) != result['task_entity_count']
                or result['task_entity_count'] > entities
                or sum(r['tasks'] for r in counts if r['status'] == 'completed') != fleet['step']['completed']):
            raise IncompleteContext('derived complete task counts/Step/budget inconsistent')
        return counts


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--diagnostic-dir', type=Path, required=True)
    p.add_argument('--output', type=Path, required=True)
    args = p.parse_args(argv)
    args.output.mkdir(parents=True, exist_ok=False)
    diagnostic = json.loads((args.diagnostic_dir / 'diagnostic.json').read_text())
    run, seq = diagnostic['run_id'], diagnostic['cutoff']
    baseline = QueryCatalogService.from_env(timeout=30, pool_size=3)
    row = baseline.store.mirror.query('MATCH (r:WHRun {run_id:$run}) '
                                    'RETURN toString(r.created_at) AS created', {'run': run})
    build = build_sidecar(args.diagnostic_dir / 'journal-snapshot.sqlite',
                          args.output / 'derived-summary.sqlite', row[0]['created'])
    with (args.output / 'build-report.json').open('x', encoding='utf-8') as out:
        json.dump(build, out, ensure_ascii=False, indent=2)
    print(json.dumps({'build_wall_seconds': build['build_wall_seconds'],
                      'sidecar_bytes': build['sidecar_bytes']}), flush=True)
    sidecar = SidecarCatalog(baseline.store,
        SidecarReader(args.output / 'derived-summary.sqlite', build['manifest_sha256']))
    records = []
    with (args.output / 'requests.jsonl').open('x', encoding='utf-8') as requests, \
            gzip.open(args.output / 'responses.jsonl.gz', 'wt', encoding='utf-8') as responses:
        for kind in ('global', 'local_awareness'):
            for pair in range(4):
                order = ('baseline', 'sidecar') if pair % 2 == 0 else ('sidecar', 'baseline')
                for method in order:
                    request = {'query_id': kind, 'run_id': run, 'seq': seq, 'robot_id': 1,
                               'task_id': diagnostic['selected_actual_task_id'], 'start_seq': max(1, seq - 3),
                               'event_limit': 10000, 'history_limit': 1000, 'limit': 5}
                    error = None
                    value = None
                    begin = time.time_ns()
                    tick = time.perf_counter_ns()
                    try:
                        value = (baseline if method == 'baseline' else sidecar).execute(request)
                    except Exception as exc:
                        error = type(exc).__name__ + ': ' + str(exc)
                        traceback.print_exc()
                    elapsed = (time.perf_counter_ns() - tick) / 1e6
                    end = time.time_ns()
                    request_id = f'summary-{kind}-p{pair+1}-{method}'
                    record = {'request_id': request_id, 'round': pair + 1, 'client_id': 1,
                              'run_id': run, 'seq': seq, 'point_kind': 'historical', 'start_ns': begin,
                              'end_ns': end, 'ok': error is None, 'response_sha256': digest(value),
                              'request': request, 'error': error}
                    requests.write(canonical(record) + '\n');requests.flush()
                    responses.write(canonical({'request_id': request_id, 'result': value}) + '\n');responses.flush()
                    records.append({'request_id': request_id, 'query_id': kind, 'method': method,
                                    'pair': pair + 1, 'service_ms': elapsed, 'error': error})
                    print(json.dumps(records[-1]), flush=True)
    baseline.close()
    report = {'version': VERSION, 'scope': 'static snapshot876-derived sidecar; graph remains live; no latest publication integration',
              'source_generation_uses_oracle': False, 'production_default_changed': False,
              'build': build, 'records': records, 'complete_api_requests': len(records),
              'concurrent_100_global_tested': False,
              'concurrency_reason': 'avoid a new heavyweight 100-global workload during the formal live seven-day writer/query phases',
              'source_verification_in_query_timer': False}
    with (args.output / 'report.json').open('x', encoding='utf-8') as out:
        json.dump(report, out, ensure_ascii=False, indent=2, allow_nan=False)
    return 0 if all(r['error'] is None for r in records) else 2


if __name__ == '__main__':
    raise SystemExit(main())
