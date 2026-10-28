"""Reproducible complete API benchmarks against a fixed actual-run prefix.

Three measured rounds, optional timed stability and separate ANN recall probes.
All latency samples include Bolt, payload/JSON materialization and full oracle
checks; failed/slow requests are retained. No HTTP/browser/edge-device claim.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from functools import partial
import hashlib
import importlib.metadata
import json
import math
import os
from pathlib import Path
import platform
import statistics
import sys
import threading
import time

from ..knowledge import digest
from . import real_vector
from .store import RealStore, DIMENSIONS, MODES


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False, allow_nan=False)


def save(path, value):
    with Path(path).open('x', encoding='utf-8') as stream:
        json.dump(value, stream, ensure_ascii=False, indent=2, allow_nan=False, default=str)


def vector(value):
    if not isinstance(value, (list, tuple)) or len(value) != DIMENSIONS or any(
            isinstance(v, bool) or not isinstance(v, (float, int)) or abs(v) > 1e6 or not math.isfinite(v) for v in value):
        raise ValueError('invalid finite 32d vector')
    if not sum(v*v for v in value):
        raise ValueError('zero vector')
    return value


def normalized_score(a, b):
    return (1 + sum(x*y for x, y in zip(a, b))/math.sqrt(sum(x*x for x in a)*sum(y*y for y in b))) / 2


def typed_json_equal(left, right):
    """Typed JSON equality, allowing legitimate SQLite REAL/int equivalents.

    A boolean is never a numeric ID, time, cursor or quantity. Unlike original
    complete JSON payloads, normalized goods columns can validly be 101.0/101.
    """
    if isinstance(left, bool) or isinstance(right, bool):
        return type(left) is bool and type(right) is bool and left == right
    if isinstance(left, (int, float)) and isinstance(right, (int, float)):
        return left == right
    if type(left) is not type(right):
        return False
    if isinstance(left, dict):
        return left.keys() == right.keys() and all(typed_json_equal(left[k], right[k]) for k in left)
    if isinstance(left, list):
        return len(left) == len(right) and all(typed_json_equal(a, b) for a, b in zip(left, right))
    return left == right


def validate_similar(rows, query_vector, oracle, cutoff):
    if len(rows) != 5 or len({r['key'] for r in rows}) != 5:
        raise ValueError('ANN completeness/duplicate error')
    previous = math.inf
    for row in rows:
        seq, robot = row['seq'], row['robot_id']
        if type(seq) is not int or not 1 <= seq <= cutoff or type(robot) is not int or not 1 <= robot <= 100 or row['run_id'] != oracle.run_id or row['key'] != f'{oracle.run_id}:{seq}:{robot}':
            raise ValueError('ANN returned wrong-run/future/bad-identity state')
        embedded = vector(row['real_embedding'])
        original = oracle.state(seq, robot)
        expected = real_vector(original, seq, robot)
        if any(abs(a-b) > 1e-10 for a, b in zip(embedded, expected)) or row['real_payload_sha256'] != digest(original):
            raise ValueError('ANN vector/payload provenance mismatch')
        score = row['score']
        if isinstance(score, bool) or not isinstance(score, (int, float)) or not math.isfinite(score) or not 0 <= score <= 1.000001 or score > previous + 1e-7 or abs(score-normalized_score(query_vector, embedded)) > 2e-4:
            raise ValueError('ANN score/order mismatch')
        previous = score


def validate_result(result, oracle, case):
    seq, robot = case['seq'], case['robot_id']
    head = result['graph_committed_seq']
    if type(head) is not int or head < oracle.prefix or (result['run_id'], result['seq'], result['robot_id']) != (oracle.run_id, seq, robot):
        raise ValueError('run/point/watermark mismatch')
    oracle.check_source_identity(result['metadata'], result['config'])
    marker = result['real_identity']
    if marker.get('real_scale_version') != 'warehouse-real-scale-v1' or marker.get('real_source_kind') != 'actual_runtime_committed_state' or marker.get('real_source_run') != oracle.run_id or marker.get('real_dimensions') != DIMENSIONS or marker.get('real_vector_kind') != '32d_structured_physical_time_identity_features_not_semantic':
        raise ValueError('not a verified same-run actual-history graph')
    if result['batch_checksum'] != oracle.batch_checksum(seq):
        raise ValueError('source step checksum mismatch')
    expected = oracle.state(seq, robot)
    source_robot_sha = digest(expected)
    source_task = oracle.task(seq, expected.get('task_id'))
    if (digest(result['robot']) != source_robot_sha or result['robot_sha256'] != source_robot_sha
            or digest(result['task']) != digest(source_task)):
        raise ValueError('robot/current historical task payload mismatch')
    source_focus = oracle.task(seq, case['focus_task_id'])
    if result['focus_task_id'] != case['focus_task_id'] or digest(result['focus_task']) != digest(source_focus):
        raise ValueError('focus task payload mismatch')
    if result['focus_task'] is None or result['focus_task']['goods_id'] != case['goods_id']:
        raise ValueError('missing focus task or task/goods mismatch')
    history = oracle.history(seq, robot, limit=5)
    if len(result['history']) != len(history):
        raise ValueError('history missing rows')
    for got, want in zip(result['history'], history):
        for key in ['key', 'seq', 'robot_id', 'real_payload_sha256']:
            if type(got[key]) is not type(want[key]) or got[key] != want[key]:
                raise ValueError('history identity/order/hash mismatch')
        payload = json.loads(got['payload']) if isinstance(got['payload'], str) else got['payload']
        original = json.loads(want['payload']) if isinstance(want['payload'], str) else want['payload']
        source_payload_sha = digest(original)
        if digest(payload) != source_payload_sha or got['real_payload_sha256'] != source_payload_sha:
            raise ValueError('history complete payload mismatch')
    query = vector(result['query_vector'])
    if any(abs(a-b) > 1e-10 for a, b in zip(query, real_vector(expected, seq, robot))):
        raise ValueError('query vector source mismatch')
    validate_similar(result['similar'], query, oracle, seq)
    for field, reference in [('goods_history', oracle.goods_history(seq, case['goods_id'])),
                             ('cooperation', oracle.cooperation(seq, case['focus_task_id']))]:
        got, want = deepcopy(result[field]), deepcopy(reference)
        if got.pop('graph_committed_seq') != head:
            raise ValueError('subquery watermark mismatch')
        want.pop('graph_committed_seq', None)
        if not typed_json_equal(got, want):
            raise ValueError(field + ' temporal/pagination/payload mismatch')
    return {'current_task_present': result['task'] is not None,
            'focus_task_present': True, 'goods_present': result['goods_history']['current_as_of'] is not None,
            'goods_rows': len(result['goods_history']['rows']),
            'cooperation_rows': len(result['cooperation']['rows']),
            'cargo_handoff_event_seqs': [row['event_seq'] for row in result['cooperation']['rows']
                if row['event'].get('kind')=='cargo_handoff' and
                row['event'].get('task_id')==case['focus_task_id'] and
                row['event'].get('goods_id')==case['goods_id']],
            'self_retrieved': any(r['key'] == f'{oracle.run_id}:{seq}:{robot}' for r in result['similar'])}


def distribution(rows, elapsed):
    values = sorted(r['ms'] for r in rows)
    quantile = lambda q: values[max(0, math.ceil(q*len(values))-1)] if values else None
    return {'requests': len(rows), 'successes': sum(r['ok'] for r in rows),
        'failures': sum(not r['ok'] for r in rows),
        'mean_ms': statistics.mean(values) if values else None,
        'p50_ms': quantile(.5), 'p95_ms': quantile(.95), 'p99_ms': quantile(.99),
        'max_ms': max(values) if values else None, 'over_200ms': sum(v>200 for v in values),
        'over_500ms': sum(v>500 for v in values), 'wall_seconds': elapsed,
        'successful_requests_s': sum(r['ok'] for r in rows)/elapsed if elapsed else 0.,
        'current_task_present': sum(r.get('current_task_present', False) for r in rows),
        'focus_task_present': sum(r.get('focus_task_present', False) for r in rows),
        'goods_present': sum(r.get('goods_present', False) for r in rows),
        'cooperation_nonempty': sum(r.get('cooperation_rows', 0)>0 for r in rows),
        'boundary_cases': {kind: sum(r['ok'] and r.get('boundary')==kind for r in rows)
                           for kind in ['handoff_before', 'handoff_at', 'handoff_after']},
        'retrieval_paths': {p: sum(r.get('retrieval_path')==p for r in rows)
                            for p in sorted({r['retrieval_path'] for r in rows if 'retrieval_path' in r})}}


def business_coverage(oracle, rows):
    """Coverage is independent of per-request correctness and latency gates.

Only successful formal requests count. A label without the corresponding
source handoff task/goods/event is not collaboration evidence. No-handoff
inputs are N/A, not successful collaboration demonstrations.
    """
    points = oracle.handoff_points(limit=10000)
    successes = [r for r in rows if r['ok']]
    task_present = any(r.get('focus_task_present') and r.get('current_task_present') for r in successes)
    goods_present = any(r.get('goods_present') and r.get('goods_rows', 0)>0 for r in successes)
    checked = []
    for point in points:
        matching = [r for r in successes if r.get('handoff_event_seq')==point['event_seq'] and
                    r.get('focus_task_id')==point['task_id'] and r.get('goods_id')==point['goods_id'] and
                    r.get('robot_id') in {point['donor_robot_id'], point['receiver_robot_id']}]
        phase_seq = {'handoff_before': point['before_seq'], 'handoff_at': point['seq'],
                     'handoff_after': point['seq']+1}
        phases = {kind: any(r.get('boundary')==kind and r.get('seq')==number for r in matching) and
                  (kind!='handoff_before' or number<point['seq']) for kind, number in phase_seq.items()}
        returned = any(point['event_seq'] in r.get('cargo_handoff_event_seqs', []) for r in matching)
        checked.append({'source_event_seq': point['event_seq'], 'task_id': point['task_id'],
            'goods_id': point['goods_id'], **phases, 'actual_cargo_handoff_returned': returned,
            'covered': all(phases.values()) and returned})
    possibly_truncated = len(points)==10000
    sufficient = (task_present and goods_present and all(p['covered'] for p in checked)
                  and not possibly_truncated) if points else None
    return {'cooperation_applicable': bool(points), 'source_handoffs_examined': len(points),
            'source_handoff_limit_may_truncate': possibly_truncated,
            'task_nonempty_covered': task_present, 'goods_nonempty_covered': goods_present,
            'handoffs': checked, 'business_coverage_sufficient': sufficient}


def driver_identity():
    versions = {}
    for package in ['neo4j', 'neo4j-rust-ext']:
        try:
            versions[package] = importlib.metadata.version(package)
        except importlib.metadata.PackageNotFoundError:
            versions[package] = None
    loaded = {}
    for name, module in tuple(sys.modules.items()):
        path = getattr(module, '__file__', None)
        if path and ('neo4j' in name or '_rust' in name or 'rust' in str(path)) and str(path).endswith(('.so', '.pyd')):
            file = Path(path)
            loaded[name] = {'path': str(file), 'sha256': hashlib.sha256(file.read_bytes()).hexdigest() if file.is_file() else None}
    return {'versions': versions, 'loaded_native_modules': loaded,
            'rust_native_loaded': any('rust' in name or 'rust' in value['path'] for name, value in loaded.items()),
            'python_executable': sys.executable, 'python': platform.python_version(),
            'platform': platform.platform(), 'hostname': platform.node(), 'logical_cpus': os.cpu_count()}


def make_case(oracle, client, sample, round_id, clients, specials=()):
    low = max(oracle.min_task_seq, 1)
    if low > oracle.prefix:
        raise ValueError('real source prefix has no task context')
    robot = client if clients == 100 else 1 + ((sample*13 + round_id*17 + client-1) % 100)
    seq = low + ((client*997 + sample*4999 + round_id*1297) % (oracle.prefix-low+1))
    boundary, boundary_focus, boundary_event = None, None, None
    points = list(specials)
    if points:
        offset = (round_id-1) % len(points)
        points = points[offset:] + points[:offset]
    for point in points:
        ids = [point['donor_robot_id'], point['receiver_robot_id']]
        if sample < 3 and (clients == 1 or robot in ids):
            wanted = [point['before_seq'], point['seq'], point['seq']+1][sample]
            if not low <= wanted <= oracle.prefix or sample==0 and wanted>=point['seq'] or oracle.task(wanted, point['task_id']) is None:
                continue
            robot = ids[(sample+round_id) % 2] if clients == 1 else robot
            seq = wanted
            boundary = ['handoff_before', 'handoff_at', 'handoff_after'][sample]
            boundary_focus = {'task_id': point['task_id'], 'goods_id': point['goods_id']}
            boundary_event = point['event_seq']
            break
    focus = boundary_focus or oracle.focus(seq, robot)
    if not focus or not focus.get('task_id') or not focus.get('goods_id'):
        raise ValueError('query plan must include nonempty independent task/goods context')
    return {'seq': seq, 'robot_id': robot, 'focus_task_id': focus['task_id'],
            'goods_id': focus['goods_id'], 'boundary': boundary,
            'handoff_event_seq': boundary_event}


def one_round(store, oracle, *, clients, samples, round_id, mode, deadline, writer_samples,
              stability_seconds=0., phase_hook=None):
    specials = oracle.handoff_points(limit=100)
    barrier, lock = threading.Barrier(clients), threading.Lock()
    active = peak = 0
    started, cpu = time.perf_counter(), time.process_time()
    local_end = min(deadline, started+stability_seconds) if stability_seconds else deadline
    def client(client_id):
        nonlocal active, peak
        rows = []
        barrier.wait(timeout=60)
        for sample in range(samples):
            if time.perf_counter() >= local_end:
                break
            case = make_case(oracle, client_id, sample, round_id, clients, specials)
            with lock:
                active += 1
                peak = max(peak, active)
            begin, thread_cpu, start_ns = time.perf_counter(), time.thread_time(), time.time_ns()
            row = {'round': round_id, 'client_id': client_id, 'sample': sample, **case,
                   'start_ns': start_ns, 'ok': False, 'query_mode': mode}
            try:
                result = store.full_query(oracle.run_id, case['seq'], case['robot_id'],
                    case['goods_id'], case['focus_task_id'], mode=mode)
                fetched = time.perf_counter()
                details = validate_result(result, oracle, case)
                checked = time.perf_counter()
                fingerprint = digest(result)
                ended = time.perf_counter()
                row.update(ok=True, **details, result_sha256=fingerprint,
                    fetch_materialize_ms=(fetched-begin)*1000, oracle_ms=(checked-fetched)*1000,
                    fingerprint_ms=(ended-checked)*1000, similar_keys=[n['key'] for n in result['similar']],
                    retrieval_path=result['retrieval_path'], bolt_statements=result['bolt_statements'])
                if phase_hook:
                    phase_hook(row)
            except Exception as exc:
                row['error'] = type(exc).__name__ + ': ' + str(exc)[:700]
            finally:
                row.update(ms=(time.perf_counter()-begin)*1000, end_ns=time.time_ns(),
                           thread_cpu_ms=(time.thread_time()-thread_cpu)*1000)
                with lock:
                    active -= 1
            rows.append(row)
        return rows
    with ThreadPoolExecutor(max_workers=clients) as executor:
        rows = [row for batch in executor.map(client, range(1, clients+1)) for row in batch]
    elapsed = time.perf_counter()-started
    first = min((v['start_ns'] for v in rows), default=0)
    last = max((v['end_ns'] for v in rows), default=0)
    inside = [r for r in writer_samples if rows and 'seq' in r and
              first <= r['start_ns'] and r['end_ns'] <= last]
    return rows, {'round': round_id, 'peak_active_full_api_requests': peak,
        **distribution(rows, elapsed), 'process_cpu_seconds': time.process_time()-cpu,
        'writer_observations_inside_span': len(inside),
        'writer_commits_observed_inside_span': len(inside)>1 and max(r['seq'] for r in inside)>min(r['seq'] for r in inside),
        'expected_requests': None if stability_seconds else clients*samples,
        'completed_requested_sample_count': bool(stability_seconds) or len(rows)==clients*samples}


def run(args, store, oracle):
    args.output_dir.mkdir(parents=True, exist_ok=False)
    worker_processes = getattr(args, 'worker_processes', 1)
    round_runner = one_round
    if worker_processes in (4, 8):
        if args.clients != 100:
            raise ValueError('multiprocess formal reader requires all 100 clients')
        from .loadgen import parallel_round
        round_runner = partial(parallel_round, output_dir=args.output_dir, worker_processes=worker_processes)
    elif worker_processes != 1:
        raise ValueError('only one-, four-, or eight-process client architectures are supported')
    counts = store.counts(oracle.run_id, oracle.prefix)
    if counts['states'] < args.min_states or counts['states'] != oracle.prefix*100 or counts['vectors'] != counts['states'] or counts['payload_hashes'] != counts['states'] or counts['robots'] != 100 or counts['first_seq'] != 1 or counts['last_seq'] != oracle.prefix or counts['derived_links'] or counts['archive_states'] or counts['min_dimensions'] != DIMENSIONS or counts['max_dimensions'] != DIMENSIONS:
        raise ValueError('same-run actual-history count/source/vector contract failed')
    indexes = counts['vector_indexes']
    similarity = indexes[0].get('options', {}).get('indexConfig', {}).get('vector.similarity_function') if len(indexes)==1 else None
    if len(indexes)!=1 or indexes[0]['state']!='ONLINE' or indexes[0]['type']!='VECTOR' or indexes[0].get('labelsOrTypes')!=['WHRealState'] or indexes[0].get('properties')!=['real_embedding'] or indexes[0].get('options', {}).get('indexConfig', {}).get('vector.dimensions')!=DIMENSIONS or not isinstance(similarity, str) or similarity.lower()!='cosine':
        raise ValueError('real vector index not ONLINE')
    save(args.output_dir/'contract.json', {'oracle_identity': oracle.identity, 'counts': counts,
        'mode': args.mode, 'clients': args.clients, 'rounds': args.rounds, 'samples': args.samples,
        'worker_processes': worker_processes,
        'complete_api': 'same-run metadata/config + agent/task + ordered history + real vector ANN + focus task + goods custody + cooperation + JSON + independent oracle',
        'scope': 'actual runtime committed histories; structured features, not semantic multimodal vectors',
        'timing': 'all requests retained, no discarded warmup; count/index checks may warm caches',
        'writer_scope': 'same real run beyond frozen query prefix; watermark observation only, correlate actual writer log'} )
    observations, stop = [], threading.Event()
    def watch():
        while not stop.is_set():
            begin = time.time_ns()
            try:
                value = store.head(oracle.run_id)
                observations.append({'start_ns': begin, 'end_ns': time.time_ns(), **value})
            except Exception as exc:
                observations.append({'start_ns': begin, 'end_ns': time.time_ns(), 'error': str(exc)[:300]})
            stop.wait(.2)
    watcher = threading.Thread(target=watch, daemon=True) if args.observe_writer else None
    if watcher:
        watcher.start()
    rounds, coverage_rows = [], []
    raw = args.output_dir/'requests.jsonl'
    deadline = time.perf_counter()+args.max_wall_seconds
    try:
        with raw.open('x', encoding='utf-8') as stream:
            for round_id in range(1, args.rounds+1):
                before = store.head(oracle.run_id)
                rows, summary = round_runner(store, oracle, clients=args.clients, samples=args.samples,
                    round_id=round_id, mode=args.mode, deadline=deadline, writer_samples=observations)
                summary.update(writer_before=before, writer_after=store.head(oracle.run_id))
                rounds.append(summary)
                coverage_rows.extend(rows)
                for row in rows:
                    stream.write(canonical(row)+'\n')
                stream.flush()
                print(canonical(summary), flush=True)
            stability = None
            if args.stability_seconds:
                rows, stability = round_runner(store, oracle, clients=args.clients, samples=100000,
                    round_id=args.rounds+1, mode=args.mode, deadline=deadline,
                    writer_samples=observations, stability_seconds=args.stability_seconds)
                for row in rows:
                    stream.write(canonical(row)+'\n')
                stream.flush()
    finally:
        stop.set()
        if watcher:
            watcher.join(timeout=20)
        save(args.output_dir/'writer-observations.json', observations)
    recall = []
    for sample in range(args.recall_samples):
        case = make_case(oracle, 1, sample+7, 99, 1, ())
        begin = time.perf_counter()
        try:
            result = store.full_query(oracle.run_id, case['seq'], case['robot_id'], case['goods_id'], case['focus_task_id'], mode=args.mode)
            validate_result(result, oracle, case)
            exact = store.exact_neighbors(oracle.run_id, case['seq'], result['query_vector'], timeout=60)
            validate_similar(exact, result['query_vector'], oracle, case['seq'])
            wanted = {n['key'] for n in exact}
            recall.append({**case, 'ok': True, 'recall_at_5': len(wanted & {n['key'] for n in result['similar']})/5,
                'ann_keys': [n['key'] for n in result['similar']], 'exact_keys': [n['key'] for n in exact],
                'wall_seconds': time.perf_counter()-begin})
        except Exception as exc:
            recall.append({**case, 'ok': False, 'error': type(exc).__name__+': '+str(exc)[:600]})
    coverage = business_coverage(oracle, coverage_rows)
    report = {'oracle_identity': oracle.identity, 'counts': counts, 'query_mode': args.mode,
        'clients': args.clients, 'worker_processes': worker_processes,
        'rounds': rounds, 'stability': stability, 'ann_recall_samples': recall,
        'business_coverage': coverage,
        'environment': driver_identity(), 'raw_sha256': hashlib.sha256(raw.read_bytes()).hexdigest(),
        'database_components': store.mirror.query('CALL dbms.components() YIELD name,versions,edition RETURN name,versions,edition', {}),
        'qualification': {'same_run_actual_million_states': counts['states']>=1000000,
            'three_rounds': len(rounds)>=3,
            'business_coverage_sufficient': coverage['business_coverage_sufficient'],
            'cooperation_coverage_applicable': coverage['cooperation_applicable'],
            'formal_rounds_correct': all(r['failures']==0 and r['completed_requested_sample_count'] for r in rounds),
            'all_correct': all(r['failures']==0 and r['completed_requested_sample_count'] for r in rounds) and
                (stability is None or stability['failures']==0 and stability['requests']>0 and not stability.get('process_errors')),
            'stability_duration_met': stability is not None and stability['requests']>0 and
                stability.get('actual_request_span_seconds', stability['wall_seconds'])>=args.stability_seconds,
            'hundred_clients_configured': args.clients==100,
            'hundred_inflight_observed': args.clients==100 and all(r['peak_active_full_api_requests']>=100 for r in rounds),
            'single_mean_under_200ms': args.clients==1 and all(r['mean_ms'] is not None and r['mean_ms']<=200 for r in rounds),
            'concurrent_mean_under_500ms': args.clients==100 and all(r['mean_ms'] is not None and r['mean_ms']<=500 for r in rounds),
            'all_requests_under_500ms': all(r['over_500ms']==0 for r in rounds),
            'writer_progress_each_round': args.observe_writer and all(r['writer_commits_observed_inside_span'] for r in rounds),
            'semantic_multimodal': False, 'edge_device': False, 'hardware_robot': False}}
    save(args.output_dir/'report.json', report)
    return report


def parser():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--oracle-index', required=True, type=Path)
    p.add_argument('--output-dir', required=True, type=Path)
    p.add_argument('--mode', choices=MODES, default='baseline')
    p.add_argument('--clients', type=int, default=100)
    p.add_argument('--rounds', type=int, default=3)
    p.add_argument('--samples', type=int, default=10)
    p.add_argument('--min-states', type=int, default=1000000)
    p.add_argument('--observe-writer', action='store_true')
    p.add_argument('--worker-processes', type=int, choices=[1, 4, 8], default=1)
    p.add_argument('--stability-seconds', type=float, default=20)
    p.add_argument('--recall-samples', type=int, default=2)
    p.add_argument('--max-wall-seconds', type=float, default=600)
    return p


def main(argv=None):
    p = parser(); args = p.parse_args(argv)
    if not 1 <= args.clients <= 100 or not 1 <= args.rounds <= 10 or not 1 <= args.samples <= 1000 or not 1 <= args.min_states <= 10000000 or not 0 <= args.stability_seconds <= 180 or not 0 <= args.recall_samples <= 5 or not 10 <= args.max_wall_seconds <= 3600:
        p.error('bounded clients/rounds/samples/stability/recall/time limits required')
    if args.worker_processes > 1 and args.clients != 100:
        p.error('multiprocess load generator requires exactly 100 clients')
    from .oracle import RealOracle
    oracle, store = RealOracle.open(args.oracle_index), None
    output_was_absent = not args.output_dir.exists()
    try:
        store = RealStore.from_env()
        report = run(args, store, oracle)
        return 0 if report['qualification']['all_correct'] else 2
    except Exception as exc:
        if output_was_absent and args.output_dir.is_dir():
            save(args.output_dir/'failure.json', {'type': type(exc).__name__, 'message': str(exc)[:2000]})
        raise
    finally:
        if store:
            store.close()


if __name__ == '__main__':
    raise SystemExit(main())
