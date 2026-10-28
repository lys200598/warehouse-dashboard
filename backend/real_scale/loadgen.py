"""Optional four/eight-process full-API load generator, not a DB optimization.

Each process owns its driver and read-only oracle. Local client-ready barriers
feed a cross-process coordinator barrier. Timed request bodies and deterministic
cases are identical to the one-process reader; per-request times remain real.
"""
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
import multiprocessing
import os
from pathlib import Path
import queue
import threading
import time
import traceback


class _RecordRelease:
    """Barrier action runs once while all ready parties are still waiting."""
    def __init__(self, monotonic_time, wall_ns):
        self.monotonic_time, self.wall_ns = monotonic_time, wall_ns

    def __call__(self):
        self.monotonic_time.value = time.perf_counter()
        self.wall_ns.value = time.time_ns()


def overlap(rows):
    """Actual full-API interval overlap, never a sum of per-process peaks."""
    events = []
    for row in rows:
        start, end = row['start_ns'], row['end_ns']
        if type(start) is not int or type(end) is not int or end < start:
            raise ValueError('invalid absolute request interval')
        if end == start:  # Coarse host clocks cannot prove overlap for an empty interval.
            continue
        events.extend([(start, 1), (end, -1)])
    active = peak = 0
    for _, change in sorted(events):  # Ends first for half-open intervals.
        active += change
        peak = max(peak, active)
    return peak


def _child(index, ids, options, barrier, release_time, release_ns, results, store_factory, oracle_factory):
    from ..knowledge import digest
    from .benchmark import canonical, driver_identity, make_case, validate_result
    from .oracle import RealOracle
    from .store import RealStore
    started, cpu = time.perf_counter(), time.process_time()
    oracle = store = None
    report = {'worker': index, 'pid': os.getpid(), 'client_ids': ids, 'error': None,
              'local_thread_barrier_parties': len(ids), 'client_ready_ns': {}}
    trace = Path(options['trace_dir'])/f'process-{index}.jsonl'
    try:
        oracle = (oracle_factory or RealOracle.open)(options['oracle_index'])
        store = store_factory() if store_factory else RealStore.from_env(timeout=options['timeout'])
        report.update(oracle_identity=oracle.identity, oracle_index=options['oracle_index'])
        specials = oracle.handoff_points(limit=100)
        lock = threading.Lock()
        with trace.open('x', encoding='utf-8') as stream:
            def release_group():
                report['group_ready_ns'] = time.time_ns()
                barrier.wait(timeout=60)
            local_ready = threading.Barrier(len(ids), action=release_group)
            def client(client_id):
                # Plan preparation was always outside the API timer. Finish the
                # first case before the cross-process barrier so every party
                # is actually ready to enter its full request on release.
                try:
                    initial_case = make_case(oracle, client_id, 0, options['round_id'], options['clients'], specials)
                    initial_ready_ns = time.time_ns()
                    with lock:
                        report['client_ready_ns'][str(client_id)] = initial_ready_ns
                except BaseException:
                    local_ready.abort()
                    barrier.abort()
                    raise
                local_ready.wait(timeout=60)
                local_end = options['deadline']
                if options['stability_seconds']:
                    local_end = min(local_end, release_time.value+options['stability_seconds'])
                for sample in range(options['samples']):
                    if time.perf_counter() >= local_end:
                        break
                    case = initial_case if sample == 0 else make_case(
                        oracle, client_id, sample, options['round_id'], options['clients'], specials)
                    case_ready_ns = initial_ready_ns if sample == 0 else time.time_ns()
                    begin, thread_cpu, start_ns = time.perf_counter(), time.thread_time(), time.time_ns()
                    row = {'round': options['round_id'], 'client_id': client_id, 'sample': sample, **case,
                           'start_ns': start_ns, 'ok': False, 'query_mode': options['mode'],
                           'worker_process': index, 'worker_pid': os.getpid(), 'case_ready_ns': case_ready_ns}
                    try:
                        result = store.full_query(oracle.run_id, case['seq'], case['robot_id'],
                            case['goods_id'], case['focus_task_id'], mode=options['mode'])
                        fetched = time.perf_counter()
                        detail = validate_result(result, oracle, case)
                        checked = time.perf_counter()
                        fingerprint = digest(result)
                        ended = time.perf_counter()
                        row.update(ok=True, **detail, result_sha256=fingerprint,
                            fetch_materialize_ms=(fetched-begin)*1000, oracle_ms=(checked-fetched)*1000,
                            fingerprint_ms=(ended-checked)*1000,
                            similar_keys=[n['key'] for n in result['similar']],
                            retrieval_path=result['retrieval_path'], bolt_statements=result['bolt_statements'])
                    except Exception as exc:
                        row['error'] = type(exc).__name__+': '+str(exc)[:700]
                    finally:
                        row.update(ms=(time.perf_counter()-begin)*1000, end_ns=time.time_ns(),
                                   thread_cpu_ms=(time.thread_time()-thread_cpu)*1000)
                    # Crash-surviving raw evidence, outside the full API timer.
                    with lock:
                        stream.write(canonical(row)+'\n')
                        stream.flush()
            with ThreadPoolExecutor(max_workers=len(ids)) as executor:
                list(executor.map(client, ids))
        report['driver'] = driver_identity()
        folder = Path(__file__).resolve().parent
        report['reader_sha256'] = {name: hashlib.sha256((folder/name).read_bytes()).hexdigest()
            for name in ('__init__.py', 'oracle.py', 'prepare.py', 'store.py', 'benchmark.py', 'profile.py', 'loadgen.py')}
    except BaseException as exc:
        report.update(error=type(exc).__name__+': '+str(exc)[:1500], traceback=traceback.format_exc())
        try:
            barrier.abort()
        except Exception:
            pass
    finally:
        for resource in (store, oracle):
            if resource is not None:
                try:
                    resource.close()
                except Exception as exc:
                    report['error'] = report['error'] or type(exc).__name__+': '+str(exc)[:1500]
        report.update(wall_seconds=time.perf_counter()-started,
                      process_cpu_seconds=time.process_time()-cpu)
        results.put(report)


def parallel_round(store, oracle, *, clients, samples, round_id, mode, deadline, writer_samples,
                   stability_seconds=0., phase_hook=None, output_dir=None,
                   worker_processes=4, _store_factory=None, _oracle_factory=None):
    """Four or eight independent workers; production CLI requires clients=100.

    Factories are explicit local test seams; the benchmark CLI never supplies
    them. There is no global patching, shared driver, or whole-history cache.
    """
    from .benchmark import distribution
    if worker_processes not in (4, 8) or clients < worker_processes or output_dir is None or phase_hook is not None:
        raise ValueError('four/eight balanced worker groups and a new trace directory required')
    started, parent_cpu = time.perf_counter(), time.process_time()
    trace_dir = Path(output_dir)/f'worker-round-{round_id}'
    trace_dir.mkdir(exist_ok=False)
    ctx = multiprocessing.get_context('spawn')
    release_time, release_ns = ctx.RawValue('d', 0.), ctx.RawValue('q', 0)
    barrier = ctx.Barrier(worker_processes, action=_RecordRelease(release_time, release_ns))
    results = ctx.Queue()
    options = {'oracle_index': str(oracle.index_path), 'timeout': store.timeout,
        'clients': clients, 'samples': samples, 'round_id': round_id, 'mode': mode,
        'deadline': deadline, 'stability_seconds': stability_seconds, 'trace_dir': str(trace_dir)}
    groups, next_client = [], 1
    for index in range(worker_processes):
        count = clients//worker_processes + int(index < clients % worker_processes)
        groups.append(list(range(next_client, next_client+count)))
        next_client += count
    processes = [ctx.Process(target=_child, args=(i, ids, options, barrier, release_time,
        release_ns, results, _store_factory, _oracle_factory)) for i, ids in enumerate(groups)]
    for process in processes:
        process.start()
    workers = []
    grace_end = max(deadline, time.perf_counter()+60)+90
    while len(workers) < len(processes) and time.perf_counter() < grace_end:
        try:
            workers.append(results.get(timeout=min(1., max(.01, grace_end-time.perf_counter()))))
        except queue.Empty:
            if all(not p.is_alive() for p in processes):
                break
    for process in processes:
        process.join(timeout=1)
        if process.is_alive():
            process.terminate()  # Only this invocation's own stuck worker.
            process.join(timeout=5)
    errors = [w for w in workers if w.get('error')]
    if len(workers) != worker_processes:
        errors.append({'error': 'missing process completion evidence', 'received': len(workers),
                       'expected': worker_processes})
    for index, process in enumerate(processes):
        if process.exitcode != 0:
            errors.append({'worker': index, 'error': 'nonzero worker exit', 'exit_code': process.exitcode})
    ready_ids = [int(client) for w in workers for client in w.get('client_ready_ns', {})]
    all_ready = (len(ready_ids) == clients and set(ready_ids) == set(range(1, clients+1)) and
                 all(type(ns) is int and 0 < ns <= release_ns.value for w in workers
                     for ns in w.get('client_ready_ns', {}).values()))
    if not all_ready:
        errors.append({'error': 'hierarchical client-readiness provenance incomplete'})
    results.close()
    results.join_thread()
    rows = []
    for file in sorted(trace_dir.glob('process-*.jsonl')):
        with file.open(encoding='utf-8') as stream:
            for line in stream:
                rows.append(json.loads(line))
    rows.sort(key=lambda r: (r['client_id'], r['sample']))
    elapsed = time.perf_counter()-started
    first = min((r['start_ns'] for r in rows), default=0)
    last = max((r['end_ns'] for r in rows), default=0)
    inside = [r for r in writer_samples if rows and 'seq' in r and
              first <= r['start_ns'] and r['end_ns'] <= last]
    cpu = sum(w.get('process_cpu_seconds', 0.) for w in workers)+time.process_time()-parent_cpu
    return rows, {'round': round_id, **distribution(rows, elapsed),
        'peak_active_full_api_requests': overlap(rows), 'process_cpu_seconds': cpu,
        'actual_request_span_seconds': (last-first)/1e9 if rows else 0.,
        'writer_observations_inside_span': len(inside),
        'writer_commits_observed_inside_span': len(inside)>1 and max(r['seq'] for r in inside)>min(r['seq'] for r in inside),
        'expected_requests': None if stability_seconds else clients*samples,
        'completed_requested_sample_count': not errors and (bool(stability_seconds) or len(rows)==clients*samples),
        'worker_processes': worker_processes,
        'threads_per_process': clients//worker_processes if clients % worker_processes == 0 else None,
        'worker_group_sizes': [len(ids) for ids in groups],
        'barrier_implementation': f'hierarchical_{worker_processes}_groups',
        'global_start_barrier_parties': worker_processes,
        'total_clients': clients, 'total_clients_ready_observed': len(ready_ids),
        'all_clients_ready_before_release': all_ready, 'barrier_release_ns': release_ns.value,
        'worker_identities': workers, 'process_errors': errors,
        'loadgen_scope': 'independent client processes, same complete API+oracle; not a database optimization'}
