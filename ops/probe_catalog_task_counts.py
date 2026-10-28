"""Read-only AB/BA experiment for complete cutoff-bound task aggregates.

This external prototype never changes the production catalog/default, graph
schema, source tree, original query packets, or journal. Cold requests and
PROFILE plans remain separately visible.
"""
import argparse
from collections import defaultdict
import hashlib
import importlib.util
import json
from pathlib import Path
import time
import traceback

from backend.knowledge import digest
from backend.query_catalog import QueryCatalogService
from backend.query_catalog.service import TASK_COUNTS_QUERY
from backend.real_scale.oracle import RealOracle


STREAM_QUERY = '''MATCH (r:WHRun {run_id:$run}) WHERE r.committed_seq >= $seq
    MATCH (cutoff:WHStep {key:$run+':'+toString($seq)})
    CALL (r) {
        MATCH (r)<-[:IN_RUN]-(t:WHTask)
        RETURN count(t) AS entity_count }
    CALL (r) {
        MATCH (v:WHTaskState {run_id:$run})
        USING INDEX v:WHTaskState(run_id,task_id,seq)
        WHERE v.task_id IS NOT NULL AND v.seq <= $seq
        WITH r,v ORDER BY v.run_id DESC,v.task_id DESC,v.seq DESC
        WITH r,v.task_id AS task_id,head(collect(v)) AS v
        MATCH (t:WHTask {key:$run+':'+task_id})-[:IN_RUN]->(r)
        WITH v.slot_id AS slot_id,v.status AS status,count(*) AS tasks,
            sum(CASE WHEN v.status<>'completed' AND v.due_second<$clock THEN 1 ELSE 0 END) AS overdue
        OPTIONAL MATCH (slot:WHSlot {key:$run+':'+slot_id})-[:IN_RACK]->(rack:WHRack)
        WITH coalesce(rack.zone,'unmapped') AS zone,status,sum(tasks) AS tasks,sum(overdue) AS overdue
        RETURN collect({zone:zone,status:status,tasks:tasks,overdue:overdue}) AS counts }
    RETURN r.run_id AS run_id,r.committed_seq AS head,toString(r.created_at) AS created_at,
        cutoff.checksum AS batch_checksum,entity_count,counts'''

_ORIGINAL_SLOT_JOIN = '''    OPTIONAL MATCH (slot:WHSlot {key:$run+':'+v.slot_id})-[:IN_RACK]->(rack:WHRack)
        WITH coalesce(rack.zone,'unmapped') AS zone,v.status AS status,
            count(*) AS tasks,sum(CASE WHEN v.status<>'completed' AND v.due_second < $clock THEN 1 ELSE 0 END) AS overdue'''
_PREAGGREGATED_SLOT_JOIN = '''    WITH v.slot_id AS slot_id,v.status AS status,count(*) AS tasks,
            sum(CASE WHEN v.status<>'completed' AND v.due_second < $clock THEN 1 ELSE 0 END) AS overdue
        OPTIONAL MATCH (slot:WHSlot {key:$run+':'+slot_id})-[:IN_RACK]->(rack:WHRack)
        WITH coalesce(rack.zone,'unmapped') AS zone,status,sum(tasks) AS tasks,sum(overdue) AS overdue'''
if TASK_COUNTS_QUERY.count(_ORIGINAL_SLOT_JOIN) != 1:
    raise ValueError('baseline slot join changed; inspect before generating prototype')
PREAGGREGATE_QUERY = TASK_COUNTS_QUERY.replace(_ORIGINAL_SLOT_JOIN, _PREAGGREGATED_SLOT_JOIN)


def sha(path):
    h = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            h.update(block)
    return h.hexdigest()


def ordered_counts(values):
    return sorted(values, key=lambda r: (r['zone'], r['status']))


def source_counts(tasks, racks, clock):
    zones = {r['id']: r['zone'] for r in racks}
    counts = defaultdict(lambda: [0, 0])
    for task in tasks:
        key = zones.get(task['slot_id'].split('-')[0], 'unmapped'), task['status']
        counts[key][0] += 1
        counts[key][1] += int(task['status'] != 'completed' and task['due_second'] < clock)
    return [{'zone': zone, 'status': status, 'tasks': size, 'overdue': overdue}
            for (zone, status), (size, overdue) in sorted(counts.items())]


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--diagnostic-dir', required=True, type=Path)
    p.add_argument('--verifier', required=True, type=Path)
    p.add_argument('--output', required=True, type=Path)
    p.add_argument('--pairs', type=int, default=4)
    p.add_argument('--candidate', choices=('ordered_stream', 'per_task_preaggregate'), default='ordered_stream')
    args = p.parse_args(argv)
    if not 2 <= args.pairs <= 10:
        raise ValueError('paired runs in [2,10] required')
    args.output.mkdir(parents=True, exist_ok=False)
    diagnostic = json.loads((args.diagnostic_dir / 'diagnostic.json').read_text())
    validation = json.loads((args.diagnostic_dir / 'source-verification.json').read_text())
    if not validation['passed'] or validation['error_count']:
        raise ValueError('complete catalog source validation must pass first')
    journal = args.diagnostic_dir / 'journal-snapshot.sqlite'
    if sha(journal) != validation['source_journal_sha256']:
        raise ValueError('immutable original snapshot identity changed')
    run, seq = diagnostic['run_id'], diagnostic['cutoff']
    spec = importlib.util.spec_from_file_location('independent_catalog_verifier', args.verifier)
    verifier = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(verifier)
    oracle = RealOracle.open(args.diagnostic_dir / 'oracle/oracle.sqlite')
    truth = verifier.JournalTruth(journal, oracle)
    try:
        tasks = truth.all_tasks(seq)
        clock = truth.batch(seq)['sim_time']
        expected = source_counts(tasks, truth.metadata['scene']['racks'], clock)
        checksum = oracle.batch_checksum(seq)
    finally:
        truth.close()
        oracle.close()
    global_timing = next(t for t in diagnostic['timings'] if t['query_id'] == 'global')
    params = global_timing['database_calls'][-1]['params']
    if params['run'] != run or params['seq'] != seq or params['clock'] != clock:
        raise ValueError('original diagnostic request/domain differs from oracle')
    maximum = params['limit'] - 1
    if len(tasks) > maximum:
        raise ValueError('source task entity budget exceeded; never truncate task history')
    service = QueryCatalogService.from_env(timeout=30, pool_size=3)
    records = []
    plans = []
    expected_token = None
    queries = {'baseline': TASK_COUNTS_QUERY, args.candidate: STREAM_QUERY if args.candidate == 'ordered_stream' else PREAGGREGATE_QUERY}
    try:
        budget = service.store.mirror.query('''MATCH (r:WHRun {run_id:$run})
            MATCH (r)<-[:IN_RUN]-(t:WHTask) RETURN count(t) AS entities''', {'run': run})
        if len(budget) != 1 or budget[0]['entities'] > maximum:
            raise ValueError('complete graph entity budget exceeded; no prototype query started')
        if budget[0]['entities'] < len(tasks):
            raise ValueError('graph entities omit tasks present at the historical source cutoff')

        def check(rows):
            nonlocal expected_token
            if len(rows) != 1:
                raise AssertionError('one complete aggregate row required')
            row = rows[0]
            if row['run_id'] != run or type(row['head']) is not int or row['head'] < seq:
                raise AssertionError('foreign/future cutoff source')
            # Current task entities can include tasks first committed AFTER
            # the selected immutable cutoff while the live producer advances.
            # They count against the entity scan budget, with no historical
            # task version in the returned aggregate at this cutoff.
            if (row['batch_checksum'] != checksum or type(row['entity_count']) is not int
                    or not len(tasks) <= row['entity_count'] <= maximum):
                raise AssertionError('aggregate source identity/budget mismatch')
            if ordered_counts(row['counts']) != expected:
                raise AssertionError('full aggregate differs from independent source oracle')
            token = row['created_at'], row['batch_checksum']
            if expected_token is not None and token != expected_token:
                raise AssertionError('run recreation/mixed immutable point')
            expected_token = token

        for pair in range(args.pairs):
            order = tuple(queries) if pair % 2 == 0 else tuple(reversed(queries))
            for method in order:
                error = None
                rows = None
                begin = time.time_ns()
                tick = time.perf_counter_ns()
                try:
                    rows = service.store.mirror.query(queries[method], params, timeout=30)
                except Exception as exc:
                    error = type(exc).__name__ + ': ' + str(exc)
                    traceback.print_exc()
                elapsed = (time.perf_counter_ns() - tick) / 1e6
                end = time.time_ns()
                try:
                    if error is None:
                        check(rows)  # strictly after the measured DB request
                except Exception as exc:
                    error = type(exc).__name__ + ': ' + str(exc)
                record = {'pair': pair + 1, 'method': method, 'start_ns': begin, 'end_ns': end,
                          'query_ms': elapsed, 'first_call_for_method': not any(r['method'] == method for r in records),
                          'query': queries[method], 'params': params, 'rows': rows, 'error': error}
                records.append(record)
                with (args.output / 'requests-and-full-rows.jsonl').open('a', encoding='utf-8') as out:
                    out.write(json.dumps(record, ensure_ascii=False, allow_nan=False) + '\n')
                print(json.dumps({k: record[k] for k in ('pair', 'method', 'query_ms', 'first_call_for_method', 'error')}), flush=True)
        # PROFILE is a separately identified diagnostic call, never included
        # in the AB/BA speed comparison and never treated as production time.
        from neo4j import Query, READ_ACCESS
        for method, query in queries.items():
            begin = time.time_ns()
            error = None
            plan = None
            rows = None
            try:
                with service.store.mirror.driver.session(database=service.store.mirror.database,
                        default_access_mode=READ_ACCESS) as session:
                    result = session.run(Query('PROFILE ' + query, timeout=30), params)
                    rows = [dict(r) for r in result]
                    summary = result.consume()
                    plan = summary.profile
                check(rows)
            except Exception as exc:
                error = type(exc).__name__ + ': ' + str(exc)
                traceback.print_exc()
            plans.append({'method': method, 'start_ns': begin, 'end_ns': time.time_ns(),
                          'query': query, 'params': params, 'rows': rows, 'profile': plan, 'error': error})
    finally:
        service.close()
    errors = [r for r in records + plans if r['error']]
    report = {'version': 'warehouse-task-counts-stream-prototype-v1', 'passed': not errors,
              'scope': 'read-only query-only prototype, historical same cutoff; production catalog unchanged',
              'run_id': run, 'cutoff': seq, 'source_task_count': len(tasks), 'entity_budget': maximum,
              'initial_graph_entity_count': budget[0]['entities'],
              'entity_count_semantics': 'current graph entities, including those first committed after historical cutoff; complete cutoff aggregate is independently source-verified',
              'expected_counts': expected, 'source_journal_sha256': sha(journal),
              'source_verification_sha256': sha(args.diagnostic_dir / 'source-verification.json'),
              'source_oracle_sha256': sha(args.diagnostic_dir / 'oracle/oracle.sqlite'),
              'verification_inside_query_interval': False,
              'records': records, 'plans': plans, 'errors': errors,
              'statistics': {method: {'requests': len([r for r in records if r['method'] == method]),
                  'mean_ms': sum(r['query_ms'] for r in records if r['method'] == method) / args.pairs,
                  'max_ms': max(r['query_ms'] for r in records if r['method'] == method),
                  'over_500ms': sum(r['query_ms'] > 500 for r in records if r['method'] == method)} for method in queries}}
    with (args.output / 'report.json').open('x', encoding='utf-8') as out:
        json.dump(report, out, ensure_ascii=False, indent=2, allow_nan=False)
    print(json.dumps({'passed': report['passed'], 'statistics': report['statistics']}, ensure_ascii=False))
    return 0 if report['passed'] else 2


if __name__ == '__main__':
    raise SystemExit(main())
