"""Read-only catalog diagnostic at one actual committed historical cutoff.

Keeps every cold/failed response and real timing, then uses SQLite's online
backup API for an immutable source snapshot. Source verification is a separate
post-processing step and never included in API service timings.
"""
import argparse
from collections import Counter
import gzip
import hashlib
import json
from pathlib import Path
import sqlite3
import time
import traceback

from backend.knowledge import digest
from backend.query_catalog import QueryCatalogService


def sha(path):
    h = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            h.update(block)
    return h.hexdigest()


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--run-id', required=True)
    p.add_argument('--journal', type=Path, required=True)
    p.add_argument('--output', type=Path, required=True)
    p.add_argument('--repeats', type=int, default=1)
    args = p.parse_args(argv)
    if not 1 <= args.repeats <= 10:
        raise ValueError('repeats must be in [1,10]')
    args.output.mkdir(parents=True, exist_ok=False)
    service = QueryCatalogService.from_env(timeout=30, pool_size=3)
    calls = []
    timings = []
    original_query = service.store.mirror.query

    def profile_query(query, params=None, **kwargs):
        begin = time.time_ns()
        tick = time.perf_counter_ns()
        error = None
        values = None
        try:
            values = original_query(query, params, **kwargs)
            return values
        except Exception as exc:
            error = type(exc).__name__ + ': ' + str(exc)
            raise
        finally:
            calls.append({'query_sha256': hashlib.sha256(query.encode()).hexdigest(),
                          'query': query, 'params': params, 'start_ns': begin,
                          'end_ns': time.time_ns(), 'elapsed_ms': (time.perf_counter_ns() - tick) / 1e6,
                          'rows_returned': len(values) if isinstance(values, list) else None,
                          'error': error})

    try:
        # Initial discovery is explicitly separate from all timed API calls.
        observed = service.store.mirror.query('MATCH (r:WHRun {run_id:$run}) '
            'RETURN r.committed_seq AS seq', {'run': args.run_id})
        cutoff = int(observed[0]['seq'])
        cutoff_clock = time.time_ns()
        rows = service.store.mirror.query('''MATCH (e:WHEvent {run_id:$run})
            WHERE e.commit_seq<=$seq AND e.task_id IS NOT NULL
            RETURN e.task_id AS task ORDER BY e.event_seq DESC LIMIT 1''',
            {'run': args.run_id, 'seq': cutoff})
        task_id = rows[0]['task']
        service.store.mirror.query = profile_query
        with (args.output / 'requests.jsonl').open('x', encoding='utf-8') as requests, \
                gzip.open(args.output / 'responses.jsonl.gz', 'wt', encoding='utf-8') as responses:
            for repeat in range(1, args.repeats + 1):
                for kind in service.describe()['queries']:
                    request = {'query_id': kind, 'run_id': args.run_id, 'seq': cutoff,
                               'robot_id': 1, 'task_id': task_id, 'start_seq': max(1, cutoff - 3),
                               'event_limit': 10000, 'history_limit': 1000, 'window_steps': 60,
                               'max_candidates': 10000, 'limit': 5}
                    request_id = f'midrun-r{repeat}-{kind}'
                    error = None
                    value = None
                    calls.clear()
                    begin = time.time_ns()
                    tick = time.perf_counter_ns()
                    try:
                        value = service.execute(request)
                    except Exception as exc:
                        error = type(exc).__name__ + ': ' + str(exc)
                        traceback.print_exc()
                    elapsed_ms = (time.perf_counter_ns() - tick) / 1e6
                    end = time.time_ns()
                    index = {'request_id': request_id, 'round': repeat, 'client_id': 1,
                             'run_id': args.run_id, 'seq': cutoff, 'point_kind': 'historical',
                             'start_ns': begin, 'end_ns': end, 'ok': error is None,
                             'response_sha256': digest(value), 'request': request, 'error': error}
                    requests.write(json.dumps(index, ensure_ascii=False, allow_nan=False) + '\n')
                    responses.write(json.dumps({'request_id': request_id, 'result': value},
                                               ensure_ascii=False, allow_nan=False) + '\n')
                    requests.flush()
                    responses.flush()
                    details = {'request_id': request_id, 'repeat': repeat, 'query_id': kind,
                               'service_ms': elapsed_ms, 'database_call_ms': sum(c['elapsed_ms'] for c in calls),
                               'database_statements': len(calls), 'database_calls': list(calls),
                               'catalog_metrics': service.latest_metrics,
                               'full_response_json_bytes': len(json.dumps(value, ensure_ascii=False).encode()),
                               'error': error}
                    timings.append(details)
                    print(json.dumps({k: details[k] for k in ('request_id', 'service_ms', 'database_call_ms',
                                                              'database_statements', 'error')}), flush=True)
    finally:
        service.close()
    backup_start = time.time_ns()
    snapshot = args.output / 'journal-snapshot.sqlite'
    # A plain cp of an actively written DB is deliberately never used.
    with sqlite3.connect(args.journal.resolve().as_uri() + '?mode=ro', uri=True, timeout=60) as source, \
            sqlite3.connect(snapshot) as destination:
        source.backup(destination, pages=500, sleep=.1)
    with sqlite3.connect(snapshot.resolve().as_uri() + '?mode=ro', uri=True) as connection:
        heads = connection.execute('SELECT run_id,seq FROM heads').fetchall()
        integrity = connection.execute('PRAGMA quick_check').fetchone()[0]
    if len(heads) != 1 or heads[0][0] != args.run_id or heads[0][1] < cutoff or integrity != 'ok':
        raise AssertionError('backup does not contain the selected source cutoff intact')
    report = {'version': 'warehouse-catalog-midrun-diagnostic-v1',
              'scope': 'historical immutable-cutoff diagnostic; not final full-week joint acceptance',
              'run_id': args.run_id, 'cutoff': cutoff, 'cutoff_observed_at_ns': cutoff_clock,
              'selected_actual_task_id': task_id, 'request_count': len(timings),
              'errors': [t for t in timings if t['error']], 'over_500ms': [t['request_id'] for t in timings if t['service_ms'] > 500],
              'queries_by_id': dict(Counter(t['query_id'] for t in timings)), 'timings': timings,
              'source_snapshot': {'path': str(snapshot), 'sha256': sha(snapshot), 'head': heads[0][1],
                                  'method': 'sqlite3-online-backup', 'integrity': integrity,
                                  'started_ns': backup_start, 'end_ns': time.time_ns()},
              'post_verification_timed_as_service': False,
              'instrumentation': 'external per-instance mirror.query wrapper; source bytes unchanged; includes driver row materialization'}
    with (args.output / 'diagnostic.json').open('x', encoding='utf-8') as out:
        json.dump(report, out, ensure_ascii=False, indent=2, allow_nan=False)
    return 0 if not report['errors'] else 2


if __name__ == '__main__':
    raise SystemExit(main())
