"""Compare full production reads separately from correctness-tool overhead.

Both implementations return the unchanged complete RealStore contract. Inline
validation reproduces the old measured workload. Deferred validation keeps every
result until ALL worker query groups have finished, then validates EVERY result.
No ADDITIONAL benchmark result serialization, oracle call or fingerprint runs
in the deferred query phase. The service's own full JSON materialization remains
inside service latency. This is not HTTP or robot-controller acceptance.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
import gzip
import hashlib
import json
import math
import multiprocessing
import os
from pathlib import Path
import queue
import statistics
import threading
import time
import traceback

from ..knowledge import digest
from ..real_scale.benchmark import (business_coverage, canonical, driver_identity,
                                    make_case, validate_result)
from ..real_scale.oracle import RealOracle
from ..real_scale.store import RealStore


MIB = 1024 * 1024
MAX_PHASE_REQUESTS = 6000


def save(path, value):
    with Path(path).open('x', encoding='utf-8') as stream:
        json.dump(value, stream, ensure_ascii=False, indent=2, allow_nan=False)


def file_sha256(path):
    checksum = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for block in iter(lambda: stream.read(1024*1024), b''):
            checksum.update(block)
    return checksum.hexdigest()


def source_manifest():
    backend = Path(__file__).resolve().parents[1]
    names = ['knowledge.py', 'knowledge_graph.py',
             'real_scale/__init__.py', 'real_scale/store.py',
             'real_scale/oracle.py', 'real_scale/benchmark.py']
    names += [str(p.relative_to(backend)).replace('\\', '/')
              for p in sorted((backend/'query_v2').glob('*.py'))]
    return {name: hashlib.sha256((backend/name).read_bytes()).hexdigest()
            for name in names}


def interval_peak(rows, start='service_start_ns', end='service_end_ns'):
    """Sweep real half-open intervals; never infer service overlap from full_ms."""
    points = []
    for row in rows:
        a, b = row[start], row[end]
        if type(a) is not int or type(b) is not int or not 0 < a <= b:
            raise ValueError('invalid independent query interval')
        if a != b:
            points.extend([(a, 1), (b, -1)])
    active = peak = 0
    for _, change in sorted(points):
        active += change
        peak = max(peak, active)
    return peak


def summarize_values(values):
    values = sorted(values)
    q = lambda fraction: values[max(0, math.ceil(len(values)*fraction)-1)] if values else None
    return {'count': len(values), 'mean_ms': statistics.mean(values) if values else None,
            'p50_ms': q(.5), 'p95_ms': q(.95), 'p99_ms': q(.99),
            'max_ms': max(values) if values else None,
            'over_200ms': sum(v > 200 for v in values),
            'over_500ms': sum(v > 500 for v in values)}


def summarize_rows(rows, *, clients, expected_requests, phase_start_ns,
                   phase_end_ns, process_errors=(), stability_seconds=0.):
    """Recompute report from raw request evidence, including failed attempts."""
    for row in rows:
        if not row['start_ns'] <= row['service_start_ns'] <= row['service_end_ns'] <= row['end_ns']:
            raise ValueError('service interval must be inside its request interval')
        for field, start, end in [('service_ms', 'service_start_ns', 'service_end_ns'),
                                  ('full_ms', 'start_ns', 'end_ns')]:
            if (isinstance(row[field], bool) or not isinstance(row[field], (int, float)) or
                    not math.isfinite(row[field]) or
                    abs(row[field]-(row[end]-row[start])/1e6) > 1e-6):
                raise ValueError('latency must match its own independent absolute interval')
    successes = [r for r in rows if r.get('ok')]
    interval_peak(rows)  # Invalid evidence must never silently disappear.
    starts = [r['service_start_ns'] for r in rows]
    ends = [r['service_end_ns'] for r in rows]
    span = (max(ends)-min(starts))/1e9 if rows else 0.
    phase_seconds = (phase_end_ns-phase_start_ns)/1e9 if phase_end_ns >= phase_start_ns > 0 else 0.
    actual_clients = {r['client_id'] for r in rows}
    expected_pairs = {(client, sample) for client in range(1, clients+1)
                      for sample in range(expected_requests//clients)}
    pairs = [(r['client_id'], r['sample']) for r in rows]
    sample_complete = (len(rows) == expected_requests and len(set(pairs)) == len(pairs)
                       and set(pairs) == expected_pairs)
    duration_complete = bool(rows) and span >= stability_seconds if stability_seconds else None
    complete = (not process_errors and actual_clients == set(range(1, clients+1)) and
                (duration_complete if stability_seconds else sample_complete))
    return {'requests': len(rows), 'successes': len(successes),
            'failures': len(rows)-len(successes),
            'actual_clients_observed': sorted(actual_clients),
            'peak_active_service_requests': interval_peak(rows),
            'peak_active_full_inline_requests': interval_peak(rows, 'start_ns', 'end_ns'),
            'actual_service_span_seconds': span, 'query_phase_wall_seconds': phase_seconds,
            'successful_service_requests_per_second': len(successes)/span if span else 0.,
            'expected_requests': expected_requests if not stability_seconds else None,
            'sample_count_complete': sample_complete,
            'duration_complete': duration_complete,
            'completed_requested_workload': complete,
            'all_correct': complete and len(successes) == len(rows),
            'service': summarize_values([r['service_ms'] for r in rows]),
            'validation': summarize_values([r.get('validation_ms', 0.) for r in rows]),
            'fingerprint': summarize_values([r.get('fingerprint_ms', 0.) for r in rows]),
            'full': summarize_values([r['full_ms'] for r in rows]),
            'service_cpu': summarize_values([r['service_thread_cpu_ms'] for r in rows]),
            'result_json_bytes': sum(r.get('result_json_bytes', 0) for r in rows),
            'process_errors': list(process_errors)}


def build_plan(oracle, *, clients, samples, rounds, stability_seconds=0.):
    specials = oracle.handoff_points(limit=100)
    phases = []
    for round_id in range(1, rounds+1+bool(stability_seconds)):
        count = MAX_PHASE_REQUESTS//clients if round_id > rounds else samples
        cases = [{'client_id': client, 'sample': sample,
                  **make_case(oracle, client, sample, round_id, clients, specials)}
                 for client in range(1, clients+1) for sample in range(count)]
        phases.append({'round': round_id, 'stability_seconds': stability_seconds if round_id > rounds else 0.,
                       'samples_per_client': count, 'cases': cases})
    return {'run_id': oracle.run_id, 'prefix': oracle.prefix,
            'clients': clients, 'phases': phases}


class _Release:
    def __init__(self, wall, monotonic):
        self.wall, self.monotonic = wall, monotonic

    def __call__(self):
        self.monotonic.value = time.perf_counter()
        self.wall.value = time.time_ns()


def _construct_store(implementation, timeout, pool_size):
    if implementation == 'baseline':
        return RealStore.from_env(timeout=timeout)
    if implementation == 'optimized':
        from .service import QueryService
        return QueryService.from_env(timeout=timeout, pool_size=pool_size)
    if implementation == 'fragments':
        from .service_fragments import FragmentQueryService
        return FragmentQueryService.from_env(timeout=timeout, pool_size=pool_size)
    if implementation == 'tail':
        from .service_tail import TailQueryService
        return TailQueryService.from_env(timeout=timeout, pool_size=pool_size)
    raise ValueError('unknown explicit query implementation')


def _validate(row, result, oracle, case):
    start = time.perf_counter()
    row['validation_start_ns'] = time.time_ns()
    try:
        row.update(validate_result(result, oracle, case))
        checked = time.perf_counter()
        row['validation_ms'] = (checked-start)*1000
        row['validation_end_ns'] = time.time_ns()
        row['fingerprint_start_ns'] = time.time_ns()
        row['result_sha256'] = digest(result)
        row['fingerprint_ms'] = (time.perf_counter()-checked)*1000
        row['fingerprint_end_ns'] = time.time_ns()
        row['similar_keys'] = [n['key'] for n in result['similar']]
        row['retrieval_path'] = result['retrieval_path']
        row['bolt_statements'] = result['bolt_statements']
        row['ok'] = True
    except Exception as exc:
        row['validation_ms'] = (time.perf_counter()-start)*1000
        row['validation_end_ns'] = time.time_ns()
        row['validation_error'] = type(exc).__name__+': '+str(exc)[:1200]
        row['ok'] = False


def _worker(index, ids, options, start_barrier, finish_barrier,
            release_ns, release_monotonic, finished_ns, finished_monotonic,
            reports, store_factory=None, oracle_factory=None):
    """Independent worker, with an all-worker finish barrier before deferred QA."""
    store = oracle = None
    report = {'worker': index, 'pid': os.getpid(), 'client_ids': ids,
              'client_ready_ns': {}, 'error': None, 'local_barrier_parties': len(ids)}
    rows, pending, lock = [], [], threading.Lock()
    started_cpu = time.process_time()
    folder = Path(options['folder'])
    trace_path = folder/f'worker-{index}-requests.jsonl'
    result_path = folder/f'worker-{index}-responses.jsonl.gz'
    reserved_bytes = 0
    try:
        oracle = (oracle_factory or RealOracle.open)(options['oracle_index'])
        store = store_factory() if store_factory else _construct_store(
            options['implementation'], options['timeout'], options['pool_size'])
        report['oracle_identity'] = oracle.identity
        by_client = {client: [c for c in options['phase']['cases'] if c['client_id'] == client]
                     for client in ids}
        def ready_group():
            report['group_ready_ns'] = time.time_ns()
            start_barrier.wait(timeout=90)
        local_ready = threading.Barrier(len(ids), action=ready_group)
        def client(client_id):
            nonlocal reserved_bytes
            with lock:
                report['client_ready_ns'][str(client_id)] = time.time_ns()
            local_ready.wait(timeout=90)
            stop = release_monotonic.value+options['phase']['stability_seconds']
            for case in by_client[client_id]:
                if time.perf_counter() > options['deadline']:
                    with lock:
                        report['request_limit_reason'] = 'max_wall_seconds'
                    break
                if options['phase']['stability_seconds'] and time.perf_counter() >= stop:
                    break
                with lock:
                    if reserved_bytes+options['reserve_bytes_per_result'] > options['pending_budget_per_worker']:
                        report['request_limit_reason'] = 'pending_memory_reservation_budget'
                        break
                    reserved_bytes += options['reserve_bytes_per_result']
                row = {'round': options['phase']['round'], **case,
                       'worker': index, 'worker_pid': os.getpid(),
                       'ok': False, 'validation_mode': options['validation']}
                begin, cpu, absolute = time.perf_counter(), time.thread_time(), time.time_ns()
                row['start_ns'] = row['service_start_ns'] = absolute
                result = None
                try:
                    result = store.full_query(oracle.run_id, case['seq'], case['robot_id'],
                                             case['goods_id'], case['focus_task_id'], mode='ordered')
                    row['service_ok'] = True
                except Exception as exc:
                    row['service_ok'] = False
                    row['service_error'] = type(exc).__name__+': '+str(exc)[:1200]
                finally:
                    fetched = time.perf_counter()
                    row['service_end_ns'] = time.time_ns()
                    row['service_monotonic_ms'] = (fetched-begin)*1000
                    row['service_ms'] = (row['service_end_ns']-row['service_start_ns'])/1e6
                    row['service_thread_cpu_ms'] = (time.thread_time()-cpu)*1000
                    metrics = getattr(store, 'latest_metrics', None)
                    if metrics is not None:
                        row['service_components'] = dict(metrics)
                if result is not None and options['validation'] == 'inline':
                    _validate(row, result, oracle, case)
                row['end_ns'] = time.time_ns()
                row['full_ms'] = (row['end_ns']-row['start_ns'])/1e6
                row['full_monotonic_ms'] = (time.perf_counter()-begin)*1000
                # No additional benchmark JSON serialization, fingerprint,
                # oracle or disk write here in deferred mode: the service's
                # complete JSON materialization has already been timed above.
                with lock:
                    rows.append(row)
                    if result is not None:
                        pending.append((row, result, case))
        with ThreadPoolExecutor(max_workers=len(ids)) as executor:
            list(executor.map(client, ids))
        report['group_query_finished_ns'] = time.time_ns()
        finish_barrier.wait(timeout=120)
        report['global_query_finished_ns'] = finished_ns.value
        exact_bytes = 0
        with gzip.open(result_path, 'xt', encoding='utf-8', compresslevel=1) as output:
            for row, result, case in pending:
                if options['validation'] == 'deferred':
                    _validate(row, result, oracle, case)
                serialized = canonical(result)
                size = len(serialized.encode('utf-8'))
                exact_bytes += size
                row['result_json_bytes'] = size
                if size > options['reserve_bytes_per_result']:
                    report['memory_budget_contract_error'] = 'actual response bytes exceed reserved per-result bytes'
                if exact_bytes > options['pending_budget_per_worker']:
                    report['memory_budget_contract_error'] = 'actual response bytes exceed worker pending budget'
                output.write(canonical({'round': row['round'], 'client_id': row['client_id'],
                    'sample': row['sample'], 'case': case, 'result': result})+'\n')
        report['actual_response_json_bytes'] = exact_bytes
        report['reserved_pending_bytes'] = reserved_bytes
        report['retained_results'] = len(pending)
        report['driver'] = driver_identity()
        report['source_manifest'] = source_manifest()
        if report.get('memory_budget_contract_error'):
            report['error'] = report['memory_budget_contract_error']
    except BaseException as exc:
        report.update(error=type(exc).__name__+': '+str(exc)[:2000], traceback=traceback.format_exc())
        for barrier in (start_barrier, finish_barrier):
            try:
                barrier.abort()
            except Exception:
                pass
    finally:
        # Preserve even incomplete/error request records, never only successes.
        with trace_path.open('x', encoding='utf-8') as stream:
            for row in rows:
                stream.write(canonical(row)+'\n')
        for item in (store, oracle):
            if item is not None:
                try:
                    item.close()
                except Exception as exc:
                    report['error'] = report['error'] or 'close failed: '+str(exc)[:500]
        report['process_cpu_seconds'] = time.process_time()-started_cpu
        report['responses_file'] = result_path.name if result_path.exists() else None
        report['trace_file'] = trace_path.name
        reports.put(report)


def execute_phase(options, *, store_factory=None, oracle_factory=None):
    folder = Path(options['folder'])
    folder.mkdir(parents=True, exist_ok=False)
    ctx = multiprocessing.get_context('spawn')
    process_count = options['worker_processes']
    release_ns, release_monotonic = ctx.RawValue('q', 0), ctx.RawValue('d', 0.)
    finished_ns, finished_monotonic = ctx.RawValue('q', 0), ctx.RawValue('d', 0.)
    start = ctx.Barrier(process_count, action=_Release(release_ns, release_monotonic))
    finish = ctx.Barrier(process_count, action=_Release(finished_ns, finished_monotonic))
    reports_queue = ctx.Queue()
    groups, next_id = [], 1
    for index in range(process_count):
        count = options['clients']//process_count+int(index < options['clients'] % process_count)
        groups.append(list(range(next_id, next_id+count)))
        next_id += count
    args = [(i, ids, options, start, finish, release_ns, release_monotonic,
             finished_ns, finished_monotonic, reports_queue, store_factory, oracle_factory)
            for i, ids in enumerate(groups)]
    # One process uses the same implementation in this process. Eight workers
    # use spawn, each with its own native driver and immutable oracle handle.
    processes = []
    if process_count == 1:
        _worker(*args[0])
    else:
        processes = [ctx.Process(target=_worker, args=a) for a in args]
        for process in processes:
            process.start()
    workers = []
    grace_end = max(options['deadline'], time.perf_counter()+60)+240
    while len(workers) < process_count and time.perf_counter() < grace_end:
        try:
            workers.append(reports_queue.get(timeout=1))
        except queue.Empty:
            if processes and all(not p.is_alive() for p in processes):
                break
    errors = []
    for index, process in enumerate(processes):
        process.join(timeout=1)
        if process.is_alive():
            process.terminate()  # Only this invocation's own stuck child.
            process.join(timeout=5)
        if process.exitcode != 0:
            errors.append({'worker': index, 'error': 'nonzero worker exit', 'exitcode': process.exitcode})
    reports_queue.close()
    reports_queue.join_thread()
    if len(workers) != process_count:
        errors.append({'error': 'missing worker report', 'expected': process_count, 'received': len(workers)})
    errors.extend({'worker': w['worker'], 'error': w['error']} for w in workers if w.get('error'))
    errors.extend({'worker': w['worker'], 'error': w['request_limit_reason']}
                  for w in workers if w.get('request_limit_reason'))
    ready = [int(client) for worker in workers for client in worker['client_ready_ns']]
    all_ready = (len(ready) == options['clients'] and set(ready) == set(range(1, options['clients']+1))
                 and all(type(ns) is int and 0 < ns <= release_ns.value for worker in workers
                         for ns in worker['client_ready_ns'].values()))
    if not all_ready:
        errors.append({'error': 'client-readiness evidence incomplete'})
    rows = []
    for path in sorted(folder.glob('worker-*-requests.jsonl')):
        rows.extend(json.loads(line) for line in path.read_text(encoding='utf-8').splitlines())
    rows.sort(key=lambda r: (r['client_id'], r['sample']))
    if options['validation'] == 'deferred':
        if any(r.get('validation_start_ns', finished_ns.value) < finished_ns.value for r in rows):
            errors.append({'error': 'deferred validation overlapped global query phase'})
    summary = summarize_rows(rows, clients=options['clients'],
        expected_requests=len(options['phase']['cases']), phase_start_ns=release_ns.value,
        phase_end_ns=finished_ns.value, process_errors=errors,
        stability_seconds=options['phase']['stability_seconds'])
    summary.update(round=options['phase']['round'], validation_mode=options['validation'],
        implementation=options['implementation'], worker_processes=process_count,
        worker_group_sizes=[len(g) for g in groups], worker_identities=workers,
        total_clients_ready_observed=len(ready), all_clients_ready_before_release=all_ready,
        barrier_release_ns=release_ns.value, global_query_finished_ns=finished_ns.value,
        memory_budget_bytes=options['pending_budget_per_worker']*process_count,
        reserve_bytes_per_result=options['reserve_bytes_per_result'],
        deferred_validator_started_after_all_groups=(options['validation'] == 'deferred' and
            all(r.get('validation_start_ns', finished_ns.value) >= finished_ns.value for r in rows)))
    save(folder/'summary.json', summary)
    return rows, summary


def analyze_legacy(path, output_dir):
    """Descriptive decomposition only; NOT an isolated production-speed result."""
    output_dir.mkdir(parents=True, exist_ok=False)
    rows = [json.loads(line) for line in path.read_text(encoding='utf-8').splitlines()]
    fields = ['ms', 'fetch_materialize_ms', 'oracle_ms', 'fingerprint_ms']
    groups = {}
    for round_id in sorted({r.get('round') for r in rows}):
        selected = [r for r in rows if r.get('round') == round_id]
        groups[str(round_id)] = {'requests': len(selected), 'errors': sum(not r.get('ok') for r in selected),
            'phases': {field: summarize_values([r[field] for r in selected if field in r]) for field in fields}}
    report = {'source': str(path.resolve()), 'source_sha256': hashlib.sha256(path.read_bytes()).hexdigest(),
        'requests': len(rows), 'rounds': groups,
        'scope': 'old INLINE mixed-workload decomposition; fetch duration is still affected by simultaneous verification contention; not a deferred service benchmark'}
    save(output_dir/'legacy-analysis.json', report)
    return report


def run(args, oracle, store):
    args.output_dir.mkdir(parents=True, exist_ok=False)
    if args.clients*args.samples > MAX_PHASE_REQUESTS:
        raise ValueError('at most 6000 requests may be retained per phase')
    counts = store.counts(oracle.run_id, oracle.prefix)
    if (counts['states'] != oracle.prefix*100 or counts['states'] < args.min_states or
            counts['robots'] != 100 or counts['vectors'] != counts['states'] or
            counts['payload_hashes'] != counts['states'] or counts['derived_links'] or counts['archive_states'] or
            counts['first_seq'] != 1 or counts['last_seq'] != oracle.prefix or
            counts['min_dimensions'] != 32 or counts['max_dimensions'] != 32):
        raise ValueError('actual same-run source/vector count contract failed')
    indexes = counts['vector_indexes']
    if len(indexes) != 1:
        raise ValueError('one actual-state vector index required')
    index = indexes[0]
    configuration = index.get('options', {}).get('indexConfig', {})
    if (index['state'] != 'ONLINE' or index['type'] != 'VECTOR' or
            index.get('labelsOrTypes') != ['WHRealState'] or index.get('properties') != ['real_embedding'] or
            configuration.get('vector.dimensions') != 32 or
            str(configuration.get('vector.similarity_function')).lower() != 'cosine'):
        raise ValueError('actual-state vector index identity/ONLINE contract failed')
    before_source = source_manifest()
    plan = build_plan(oracle, clients=args.clients, samples=args.samples, rounds=args.rounds,
                      stability_seconds=args.stability_seconds)
    save(args.output_dir/'case-plan.json', plan)
    plan_sha = digest(plan)
    # Exact immutable source metadata length, conservatively reserved at 8x
    # JSON bytes plus dynamic rows. Actual JSON byte totals are checked after
    # each phase. Count/byte budget exhaustion FAILS; it never qualifies soak.
    identity_bytes = len(canonical({'metadata': oracle.identity['metadata'],
                                    'config': oracle.identity['config']}).encode('utf-8'))
    reserve = identity_bytes*8+128*1024
    contract = {'implementation': args.implementation, 'validation': args.validation,
        'oracle_identity': oracle.identity, 'counts': counts, 'source_manifest': before_source,
        'case_plan_sha256': plan_sha, 'clients': args.clients, 'rounds': args.rounds,
        'oracle_index_file': {'path': str(oracle.index_path), 'bytes': oracle.index_path.stat().st_size,
            'sha256': file_sha256(oracle.index_path)},
        'samples': args.samples, 'worker_processes': args.worker_processes,
        'max_pending_bytes': args.max_pending_bytes, 'max_requests_per_phase': MAX_PHASE_REQUESTS,
        'reserve_bytes_per_result': reserve,
        'timing': 'service_start/end is complete unchanged Python/Bolt/JSON API; inline validation separately timed; deferred every result validated only after ALL query groups finish',
        'scope': 'full Python/Bolt production-read comparison, no HTTP/edge/latest-decision certification',
        'warmup': 'no discarded warmup requests; count/index/source checks outside timing may warm DB caches; each phase uses fresh worker stores'}
    save(args.output_dir/'contract.json', contract)
    all_rows, summaries = [], []
    deadline = time.perf_counter()+args.max_wall_seconds
    for phase in plan['phases']:
        options = {'phase': phase, 'folder': str(args.output_dir/f"round-{phase['round']}"),
            'oracle_index': str(oracle.index_path), 'implementation': args.implementation,
            'validation': args.validation, 'clients': args.clients, 'worker_processes': args.worker_processes,
            'timeout': args.query_timeout, 'pool_size': args.pool_size, 'deadline': deadline,
            'pending_budget_per_worker': args.max_pending_bytes//args.worker_processes,
            'reserve_bytes_per_result': reserve}
        rows, summary = execute_phase(options)
        all_rows.extend(rows)
        summaries.append(summary)
        print(canonical({k: v for k, v in summary.items() if k != 'worker_identities'}), flush=True)
        if summary['process_errors']:
            break
    source_after = source_manifest()
    raw = args.output_dir/'requests.jsonl'
    with raw.open('x', encoding='utf-8') as stream:
        for row in all_rows:
            stream.write(canonical(row)+'\n')
    formal = summaries[:args.rounds]
    stability = summaries[args.rounds] if len(summaries) > args.rounds else None
    report = {'implementation': args.implementation, 'validation_mode': args.validation,
        'clients': args.clients, 'worker_processes': args.worker_processes,
        'rounds': formal, 'stability': stability,
        'case_plan_sha256': plan_sha, 'source_manifest_before': before_source,
        'source_manifest_after': source_after, 'counts': counts, 'oracle_identity': oracle.identity,
        'driver': driver_identity(), 'business_coverage': business_coverage(oracle, all_rows),
        'raw_sha256': hashlib.sha256(raw.read_bytes()).hexdigest(),
        'qualification': {'same_run_actual_million_states': counts['states'] >= 1000000,
            'three_rounds': len(formal) >= 3,
            'all_correct': len(formal) == args.rounds and all(r['all_correct'] for r in summaries),
            'source_unchanged': source_after == before_source,
            'hundred_service_inflight_each_round': args.clients == 100 and len(formal) == args.rounds
                and all(r['peak_active_service_requests'] >= 100 for r in formal),
            'service_mean_under_200ms': args.clients == 1 and len(formal) == args.rounds
                and all(r['service']['mean_ms'] is not None and r['service']['mean_ms'] <= 200 for r in formal),
            'service_mean_under_500ms': args.clients == 100 and len(formal) == args.rounds
                and all(r['service']['mean_ms'] is not None and r['service']['mean_ms'] <= 500 for r in formal),
            'stability_requested': bool(args.stability_seconds),
            'stability_complete': stability is not None and stability['all_correct'] and stability['duration_complete'],
            'complete_api_contract_unchanged': True, 'http': False, 'edge_device': False}}
    save(args.output_dir/'report.json', report)
    return report


def parser():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--oracle-index', type=Path)
    p.add_argument('--output-dir', type=Path, required=True)
    p.add_argument('--implementation', choices=['baseline', 'optimized', 'fragments', 'tail'], default='baseline')
    p.add_argument('--clients', type=int, choices=[1, 100], default=100)
    p.add_argument('--worker-processes', type=int, choices=[1, 8], default=8)
    p.add_argument('--rounds', type=int, default=3)
    p.add_argument('--samples', type=int, default=20)
    p.add_argument('--validation', choices=['inline', 'deferred'], default='deferred')
    p.add_argument('--stability-seconds', type=float, default=0.)
    p.add_argument('--max-pending-bytes', type=int, default=2*1024*MIB)
    p.add_argument('--min-states', type=int, default=1000000)
    p.add_argument('--query-timeout', type=float, default=15.)
    p.add_argument('--pool-size', type=int, default=120)
    p.add_argument('--max-wall-seconds', type=float, default=900.)
    p.add_argument('--analyze-legacy', type=Path)
    return p


def main(argv=None):
    p = parser(); args = p.parse_args(argv)
    if args.analyze_legacy:
        analyze_legacy(args.analyze_legacy, args.output_dir)
        return 0
    if args.oracle_index is None:
        p.error('--oracle-index is required unless --analyze-legacy is used')
    if args.clients == 1 and args.worker_processes != 1:
        p.error('one client requires --worker-processes 1')
    if (not 1 <= args.rounds <= 6 or not 1 <= args.samples <= 1000 or
            args.clients*args.samples > MAX_PHASE_REQUESTS or not 0 <= args.stability_seconds <= 180 or
            not 16*MIB <= args.max_pending_bytes <= 64*1024*MIB or
            not 1 <= args.min_states <= 10000000 or not 0 < args.query_timeout <= 120 or
            not 1 <= args.pool_size <= 256 or not 10 <= args.max_wall_seconds <= 3600):
        p.error('bounded samples/rounds/pending-memory/time required')
    oracle, store = RealOracle.open(args.oracle_index), None
    absent = not args.output_dir.exists()
    try:
        store = _construct_store(args.implementation, args.query_timeout, args.pool_size)
        report = run(args, oracle, store)
        return 0 if report['qualification']['all_correct'] and report['qualification']['source_unchanged'] else 2
    except Exception as exc:
        if absent and args.output_dir.is_dir():
            save(args.output_dir/'failure.json', {'type': type(exc).__name__, 'message': str(exc)[:2000]})
        raise
    finally:
        if store is not None:
            store.close()
        oracle.close()


if __name__ == '__main__':
    raise SystemExit(main())
