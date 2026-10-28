"""Bounded diagnostic-only full API/driver/Neo4j profile; no schema writes."""
import argparse
import cProfile
from concurrent.futures import ThreadPoolExecutor
import io
import json
from pathlib import Path
import pstats
import threading
import time

from ..knowledge import digest
from ..wide.queries import COOPERATION_KINDS
from .benchmark import make_case, validate_result, distribution, driver_identity, save, canonical
from .store import RealStore, build_query, VECTOR_INDEX, MODES


def inspect_server(store, oracle, mode):
    from neo4j import Query, READ_ACCESS
    case = make_case(oracle, 28, 3, 1, 100, ())
    run, seq, robot = oracle.run_id, case['seq'], case['robot_id']
    params = {'run': run, 'seq': seq, 'robot': robot, 'step_key': f'{run}:{seq}',
        'state_key': f'{run}:{seq}:{robot}', 'robot_key': f'{run}:{robot}',
        'goods': case['goods_id'], 'cargo_key': f'{run}:{case["goods_id"]}',
        'focus_task': case['focus_task_id'], 'goods_after': 0, 'coop_after': 0,
        'goods_limit': 201, 'coop_limit': 101, 'index': VECTOR_INDEX,
        'ann_count': 100, 'kinds': COOPERATION_KINDS}
    query = 'PROFILE ' + build_query(mode)
    started = time.perf_counter()
    with store.mirror.driver.session(database=store.mirror.database, default_access_mode=READ_ACCESS) as session:
        response = session.run(Query(query, timeout=15), params)
        rows = [dict(row) for row in response]
        summary = response.consume()
    return {'case': case, 'rows': len(rows), 'wall_seconds': time.perf_counter()-started,
        'result_available_after_ms': summary.result_available_after,
        'result_consumed_after_ms': summary.result_consumed_after, 'profile': summary.profile,
        'query_sha256': digest(query), 'note': 'PROFILE overhead; not formal latency'}


def run(args, store, oracle):
    args.output_dir.mkdir(parents=True, exist_ok=False)
    if store.head(oracle.run_id)['seq'] < oracle.prefix:
        raise ValueError('diagnostic requires the committed oracle prefix')
    if args.server_profile:
        try:
            value = inspect_server(store, oracle, args.mode)
        except Exception as exc:
            value = {'error': type(exc).__name__+': '+str(exc)[:1200]}
        save(args.output_dir/'server-profile.json', value)
    barrier = threading.Barrier(args.clients)
    started, cpu = time.perf_counter(), time.process_time()
    deadline = started + args.max_wall_seconds
    def client(client_id):
        profiler = cProfile.Profile() if client_id == args.profile_client else None
        rows = []
        barrier.wait(timeout=60)
        if profiler:
            profiler.enable()
        try:
            for sample in range(args.samples):
                if time.perf_counter() >= deadline:
                    break
                case = make_case(oracle, client_id, sample, 1, args.clients, ())
                begin, thread_cpu = time.perf_counter(), time.thread_time()
                row = {'client_id': client_id, 'sample': sample, **case, 'ok': False,
                       'diagnostic_only': True}
                try:
                    result = store.full_query(oracle.run_id, case['seq'], case['robot_id'],
                        case['goods_id'], case['focus_task_id'], mode=args.mode)
                    fetched = time.perf_counter()
                    detail = validate_result(result, oracle, case)
                    checked = time.perf_counter()
                    checksum = digest(result)
                    finished = time.perf_counter()
                    row.update(ok=True, **detail, fetch_materialize_ms=(fetched-begin)*1000,
                        oracle_ms=(checked-fetched)*1000, fingerprint_ms=(finished-checked)*1000,
                        result_sha256=checksum, retrieval_path=result['retrieval_path'],
                        bolt_statements=result['bolt_statements'])
                except Exception as exc:
                    row['error'] = type(exc).__name__+': '+str(exc)[:700]
                row.update(ms=(time.perf_counter()-begin)*1000,
                           thread_cpu_ms=(time.thread_time()-thread_cpu)*1000)
                rows.append(row)
        finally:
            if profiler:
                profiler.disable()
        return rows, profiler
    with ThreadPoolExecutor(max_workers=args.clients) as executor:
        returns = list(executor.map(client, range(1, args.clients+1)))
    rows = [row for batch, _ in returns for row in batch]
    elapsed = time.perf_counter()-started
    profiler = next(p for _, p in returns if p is not None)
    output = io.StringIO()
    stats = pstats.Stats(profiler, stream=output)
    stats.dump_stats(str(args.output_dir/'worker.pstats'))
    stats.strip_dirs().sort_stats('cumulative').print_stats(100)
    (args.output_dir/'worker-top100.txt').write_text(output.getvalue(), encoding='utf-8')
    with (args.output_dir/'requests.jsonl').open('x', encoding='utf-8') as stream:
        for row in rows:
            stream.write(canonical(row)+'\n')
    report = {'diagnostic_only': True, 'query_mode': args.mode, 'clients': args.clients,
        'samples': args.samples, **distribution(rows, elapsed),
        'process_cpu_seconds': time.process_time()-cpu,
        'cpu_core_equivalents': (time.process_time()-cpu)/max(elapsed, 1e-9),
        'phase_mean_ms': {field: sum(r.get(field, 0.) for r in rows)/max(1, sum(r['ok'] for r in rows))
                          for field in ['fetch_materialize_ms', 'oracle_ms', 'fingerprint_ms']},
        'oracle_identity': oracle.identity, 'environment': driver_identity(),
        'scope': 'same complete API and oracle; one worker cProfile adds overhead; not acceptance'}
    save(args.output_dir/'profile-report.json', report)
    return report


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--oracle-index', type=Path, required=True)
    parser.add_argument('--output-dir', type=Path, required=True)
    parser.add_argument('--mode', choices=MODES, default='ordered')
    parser.add_argument('--clients', type=int, default=100)
    parser.add_argument('--samples', type=int, default=3)
    parser.add_argument('--profile-client', type=int, default=28)
    parser.add_argument('--max-wall-seconds', type=float, default=60)
    parser.add_argument('--server-profile', action='store_true')
    args = parser.parse_args(argv)
    if not 1 <= args.clients <= 100 or not 1 <= args.samples <= 3 or not 1 <= args.profile_client <= args.clients or not 1 <= args.max_wall_seconds <= 60:
        parser.error('diagnostic limited to 100 clients x3 samples and 60 seconds')
    from .oracle import RealOracle
    oracle, store = RealOracle.open(args.oracle_index), None
    try:
        store = RealStore.from_env(timeout=5)
        run(args, store, oracle)
    finally:
        if store:
            store.close()


if __name__ == '__main__':
    main()
