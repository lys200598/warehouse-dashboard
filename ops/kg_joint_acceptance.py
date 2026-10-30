"""C-label joint acceptance using the same context client as robot actors.

The simulator must already be advancing the selected run. It is only controlled
when an explicit --resume-live-url requests a coordinated speed restoration.
Optional negative-tick AgentState history is removed after measurement; no
current Robot or Step properties are changed.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import multiprocessing as mp
import os
from pathlib import Path
import queue
import statistics
import threading
import time
import traceback
import uuid
from urllib.request import Request, urlopen

LABELS = ('Robot', 'AgentState', 'Execution', 'ExecutionState', 'RuntimeEvent',
          'Step', 'Handoff', 'Assist', 'RunStat', 'DispatchState', 'OwnershipChange')


def connection_config():
    def env(name, fallback=None):
        return os.environ.get('WAREHOUSE_' + name) or os.environ.get(name) or fallback
    return dict(uri=env('NEO4J_URI', 'bolt://127.0.0.1:17692'), user=env('NEO4J_USER', 'neo4j'),
                password=env('NEO4J_PASSWORD'), database=env('NEO4J_DATABASE', 'neo4j'))


def driver_for(config):
    from neo4j import GraphDatabase
    return GraphDatabase.driver(config['uri'], auth=(config['user'], config['password']),
                                connection_timeout=10, connection_acquisition_timeout=30,
                                max_connection_pool_size=2)


def ensure_context_schema(driver, database):
    from backend.query_c import SCHEMA
    with driver.session(database=database) as session:
        for query in SCHEMA:
            session.run(query).consume()
        session.run('CALL db.awaitIndexes(120)').consume()


def resume_writer(base_url, speed, driver, database, run_id):
    before = observe(driver, database, run_id)['latest_step']
    request = Request(base_url.rstrip('/')+'/api/control', method='POST',
        data=json.dumps(dict(action='speed', value=speed, expected_run_id=run_id)).encode(),
        headers={'Content-Type': 'application/json'})
    with urlopen(request, timeout=30) as response:
        if response.status != 200:
            raise RuntimeError('writer speed control was not accepted')
    deadline = time.monotonic() + 30
    while observe(driver, database, run_id)['latest_step'] <= before:
        if time.monotonic() >= deadline:
            raise TimeoutError('writer did not commit a Step after restoring speed')
        time.sleep(.05)
    print(f'[writer ready] speed={speed}, new Step committed', flush=True)


def observe(driver, database, run_id, *, count_nodes=False, interval=None):
    with driver.session(database=database, default_access_mode='READ') as session:
        row = session.run('MATCH (r:Run {id:$run}) RETURN r.latest_step AS tick,r.status AS status',
                          run=run_id).single()
        if row is None:
            raise RuntimeError(f'Run not found: {run_id}')
        result = dict(run_id=run_id, latest_step=row['tick'], status=row['status'],
                      observed_at_ms=time.time_ns() // 1_000_000)
        if count_nodes:
            counts = {label: session.run(f'MATCH (n:{label} {{run_id:$run}}) RETURN count(n) AS n',
                                         run=run_id).single()['n'] for label in LABELS}
            counts['Run'] = 1
            exact = session.run('MATCH (n) WHERE n.run_id=$run OR (n:Run AND n.id=$run) '
                                'RETURN count(n) AS n', run=run_id).single()['n']
            result.update(node_count=exact, nodes_by_label=counts)
        if interval:
            result['write_steps_in_interval'] = session.run(
                'MATCH (s:Step {run_id:$run}) WHERE s.updated_at_ms >= $lo AND s.updated_at_ms <= $hi '
                'RETURN count(s) AS n', run=run_id, lo=interval[0], hi=interval[1]).single()['n']
        return result


def add_prefix(driver, database, run_id, nodes, tag, batch_size=1000):
    if nodes <= 0:
        return 0
    if not run_id.startswith('wz-test-'):
        raise ValueError('history preparation requires a wz-test- run')
    with driver.session(database=database) as session:
        robots = session.run('MATCH (r:Robot {run_id:$run}) RETURN count(r) AS n', run=run_id).single()['n']
        if robots != 100:
            raise ValueError(f'expected 100 existing robots, got {robots}')
        # No relationship to Robot: bulk history must not lock live robot writes.
        lo = 0
        while lo < nodes:
            hi = min(nodes - 1, lo + batch_size - 1)
            try:
                session.run('UNWIND range($lo,$hi) AS n '
                    'CREATE (:AgentState {id:$tag+":"+toString(n),run_id:$run,joint_prefix_id:$tag, '
                    'robot_id:1+(n % 100),tick:-1-toInteger(n/100),x:1.0,y:1.0,battery:80.0, '
                    'stage:"idle",synthetic_history:true})', lo=lo, hi=hi, tag=tag, run=run_id).consume()
            except Exception as exc:
                # A failed Neo4j statement rolls its transaction back. Reduce
                # only the failed batch; committed batches are never replayed.
                if 'memory' not in str(exc).lower() and 'heap' not in str(exc).lower() or batch_size <= 100:
                    raise
                batch_size = max(100, batch_size // 2)
                print(f'[prefix backoff] batch={batch_size}, next={lo}', flush=True)
                time.sleep(.25)
                continue
            lo = hi + 1
            if hi + 1 == nodes or (hi + 1) % 100000 == 0:
                print(f'[prefix] {hi + 1}/{nodes}', flush=True)
    return nodes


def remove_prefix(driver, database, run_id, tag):
    with driver.session(database=database) as session:
        batch = 1000
        while True:
            try:
                session.run('MATCH (s:AgentState {run_id:$run}) WHERE s.joint_prefix_id=$tag '
                    f'CALL (s) {{ DELETE s }} IN TRANSACTIONS OF {batch} ROWS', run=run_id, tag=tag).consume()
                break
            except Exception as exc:
                if ('memory' not in str(exc).lower() and 'heap' not in str(exc).lower()) or batch <= 50:
                    raise
                batch = max(50, batch // 2)
                print(f'[prefix cleanup backoff] batch={batch}', flush=True)
        remaining = session.run('MATCH (s:AgentState {run_id:$run}) WHERE s.joint_prefix_id=$tag '
                                'RETURN count(s) AS n', run=run_id, tag=tag).single()['n']
        if remaining:
            raise RuntimeError(f'{remaining} history prefix nodes remain')
    return remaining


def stats(values):
    ordered = sorted(values)
    if not ordered:
        return dict(count=0, mean_ms=None, p99_ms=None, max_ms=None, over_500ms=0)
    return dict(count=len(ordered), mean_ms=statistics.fmean(ordered),
                p99_ms=ordered[min(len(ordered)-1, int(.99*(len(ordered)-1)))],
                max_ms=ordered[-1], over_500ms=sum(v > 500 for v in ordered))


def overlap_peak(intervals):
    points = [(start, 1) for start, _ in intervals] + [(end, -1) for _, end in intervals]
    active = peak = 0
    for _, change in sorted(points, key=lambda point: (point[0], point[1])):
        active += change
        peak = max(peak, active)
    return peak


def observed_advances(trace, interval):
    """Count new Step commits between two reads entirely inside the query window."""
    rows = [row for row in trace if not row.get('error') and
            row['started_wall_ms'] >= interval[0] and row['observed_wall_ms'] <= interval[1]]
    return sum(max(0, b['latest_step']-a['latest_step']) for a, b in zip(rows, rows[1:]))


def watch_writer(driver, database, run_id, stop, trace):
    while not stop.is_set():
        began = time.time_ns() // 1_000_000
        try:
            point = observe(driver, database, run_id)
            trace.append(dict(started_wall_ms=began, observed_wall_ms=point['observed_at_ms'], **point))
        except Exception as exc:
            trace.append(dict(started_wall_ms=began, observed_wall_ms=time.time_ns()//1_000_000,
                              error=str(exc)))
        stop.wait(.02)


def reader_process(robot_id, config, run_id, ready, results, starts, duration, request_cap):
    """Each robot has one persistent OS process, driver and context client."""
    client = None
    try:
        from backend.query_c import RobotContextClient
        client = RobotContextClient(**config)
        warm = client.query(run_id, robot_id)
        ready.put(dict(robot_id=robot_id, pid=os.getpid(), warmed=True))
        for round_number, start in enumerate(starts, 1):
            if not start.wait(180):
                raise TimeoutError('round start barrier timed out')
            stop = time.monotonic() + duration
            samples = []
            while time.monotonic() < stop and len(samples) < request_cap:
                began_ns, began_wall = time.perf_counter_ns(), time.time_ns()
                metric = client.query(run_id, robot_id)
                ended_ns, ended_wall = time.perf_counter_ns(), time.time_ns()
                samples.append(dict(total_ms=(ended_ns-began_ns)/1e6,
                    start_ns=began_ns, end_ns=ended_ns, start_wall_ms=began_wall//1_000_000,
                    end_wall_ms=ended_wall//1_000_000, tick=metric['tick'],
                    pack_ms=metric['pack_ms'], pack_rows=metric['pack_rows']))
            results.put(dict(round=round_number, robot_id=robot_id, pid=os.getpid(), samples=samples))
    except BaseException as exc:
        results.put(dict(error=f'{type(exc).__name__}: {exc}', traceback=traceback.format_exc(),
                         robot_id=robot_id, pid=os.getpid()))
        ready.put(dict(robot_id=robot_id, pid=os.getpid(), warmed=False))
    finally:
        if client is not None:
            client.close()


def summarize_round(index, before, after, records, clients):
    samples = [sample for record in records for sample in record.get('samples', [])]
    errors = [record for record in records if record.get('error')]
    timing = stats([sample['total_ms'] for sample in samples])
    packs = sorted({name for sample in samples for name in sample['pack_ms']})
    pids = sorted({record['pid'] for record in records if not record.get('error')})
    peak = overlap_peak([(sample['start_ns'], sample['end_ns']) for sample in samples])
    return dict(round=index, before=before, after=after, **timing,
        requests=timing['count'], unique_worker_pids=pids, clients=clients,
        client_observed_peak=peak, errors=errors,
        writer_step_advance=(after['latest_step'] or 0)-(before['latest_step'] or 0),
        # Direct observations of new commits inside the measured window are
        # authoritative; record.generated_at_ms is only a production timestamp.
        writes_during_round=after.get('observed_commit_advances', 0) > 0,
        packs={name: {**stats([s['pack_ms'][name] for s in samples]),
                      'min_rows': min((s['pack_rows'][name] for s in samples), default=0),
                      'max_rows': max((s['pack_rows'][name] for s in samples), default=0)} for name in packs})


def judge(rounds, clients, required_rounds=3, min_nodes=1_000_000):
    reasons = []
    if len(rounds) < required_rounds:
        reasons.append('fewer than three completed rounds')
    if clients != 100:
        reasons.append('requires 100 independent robot processes')
    for row in rounds:
        prefix = f'round {row["round"]}: '
        if row['before']['node_count'] < max(1_000_000, min_nodes):
            reasons.append(prefix + 'fewer than one million nodes in the measured run')
        if len(row['unique_worker_pids']) != 100 or row['client_observed_peak'] < 100:
            reasons.append(prefix + '100-process query concurrency not observed')
        if not row['writes_during_round']:
            reasons.append(prefix + 'no complete Step write observed inside the measured interval')
        if row['errors'] or not row['count']:
            reasons.append(prefix + 'queries failed or no results were recorded')
    qualified = not reasons
    metric = bool(rounds) and all(row['mean_ms'] is not None and row['mean_ms'] < 500 for row in rounds)
    result = dict(qualified=qualified, not_qualified_reasons=reasons,
                original_metric_mean_under_threshold_every_round=metric,
                threshold_ms=500,
                status='通过' if qualified and metric else '未达标' if qualified else '不满足验收资格')
    result['pass'] = qualified and metric
    return result


def render(result):
    verdict = result['verdict']
    lines = ['# C 套联合验收：' + verdict['status'], '',
        '查询与机器人 actor 使用同一 RobotContextClient；100 个独立 spawn 进程同步开始。',
        '预热不计时。每组端到端耗时包含完整五包结果消费；共享查询去重，分包耗时不能相加作为整组耗时。', '',
        '|轮|同运行节点数|独立进程|并发峰值|请求数|平均 ms|P99 ms|最大 ms|>500 ms|边界Step前进|窗内生产Step|观察到新提交|',
        '|---|---|---|---|---|---|---|---|---|---|---|---|']
    for row in result['rounds']:
        lines.append('|{}|{}|{}|{}|{}|{:.2f}|{:.2f}|{:.2f}|{}|{}|{}|{}|'.format(
            row['round'], row['before']['node_count'], len(row['unique_worker_pids']),
            row['client_observed_peak'], row['count'], row['mean_ms'] or 0, row['p99_ms'] or 0,
            row['max_ms'] or 0, row['over_500ms'], row['writer_step_advance'], row['after'].get('write_steps_in_interval', 0),
            row['after'].get('observed_commit_advances', 0)))
    lines += ['', '原始判据：每轮平均 < 500 ms；P99、最大值与超时次数单独报告。',
              '同时写入以查询窗口中两次完整读取之间观察到的新 Step 提交为准；载荷生产时间仅作辅助。',
              '硬件与配置：`' + json.dumps(result['hardware'], ensure_ascii=False) + '`',
              '查询包分布和逐机器人原始样本见 acceptance.json / round-N.json。']
    if verdict['not_qualified_reasons']:
        lines += ['', '不满足验收资格：'] + ['- ' + reason for reason in verdict['not_qualified_reasons']]
    return '\n'.join(lines) + '\n'


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run-id', required=True)
    parser.add_argument('--clients', type=int, default=100)
    parser.add_argument('--rounds', type=int, default=3)
    parser.add_argument('--duration', type=float, default=2)
    parser.add_argument('--requests', type=int, default=1000, help='maximum per process per round')
    parser.add_argument('--prefix-nodes', type=int, default=0)
    parser.add_argument('--prefix-batch', type=int, default=1000)
    parser.add_argument('--min-nodes', type=int, default=1_000_000)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--ray-cpus', type=int, default=None, help='record the writer Ray cluster CPU allocation')
    parser.add_argument('--ready-file', type=Path, help='optional coordinator gate before starting reader processes')
    parser.add_argument('--ready-timeout', type=float, default=180)
    parser.add_argument('--resume-live-url', help='optional coordinator-authorized speed restoration after reader warm-up')
    parser.add_argument('--resume-speed', type=float, default=4)
    parser.add_argument('--producer-kg-state-source', choices=('memory', 'neo4j'), default=None)
    parser.add_argument('--background-actor-query-interval', type=int, default=0)
    args = parser.parse_args(argv)
    if args.clients < 1 or args.rounds < 1 or args.duration <= 0 or args.requests < 1 or args.prefix_nodes < 0 or args.prefix_batch < 1:
        parser.error('clients, rounds, duration and requests must be positive; prefix nodes must be nonnegative')
    args.output.mkdir(parents=True, exist_ok=False)
    config = connection_config()
    driver = driver_for(config)
    ctx = mp.get_context('spawn')
    ready, results = ctx.Queue(), ctx.Queue()
    starts = [ctx.Event() for _ in range(args.rounds)]
    workers, rows = [], []
    tag = args.run_id + ':joint-prefix:' + uuid.uuid4().hex[:12]
    hardware = dict(hostname=os.uname().nodename, logical_cpus=os.cpu_count(),
        available_cpus=len(os.sched_getaffinity(0)) if hasattr(os, 'sched_getaffinity') else os.cpu_count(),
        neo4j_uri=config['uri'], database=config['database'], independent_processes=args.clients,
        writer_ray_cpus=args.ray_cpus, ray_auth_mode=os.environ.get('RAY_AUTH_MODE'),
        producer_kg_state_source=args.producer_kg_state_source,
        background_actor_query_interval=args.background_actor_query_interval,
        ray_cluster_address=os.environ.get('WAREHOUSE_RAY_TEST_ADDRESS'))
    protocol = dict(clients=args.clients, rounds=args.rounds, duration=args.duration,
        requests=args.requests, query='robot-context-five-packs', prefix_nodes=args.prefix_nodes,
        prefix_batch=args.prefix_batch, writer_target_speed=args.resume_speed if args.resume_live_url else None,
        producer_kg_state_source=args.producer_kg_state_source,
        background_actor_query_interval=args.background_actor_query_interval)
    result = dict(version='warehouse-c-joint-acceptance-v2', run_id=args.run_id,
        generated_at=datetime.now(timezone.utc).isoformat(), hardware=hardware, protocol=protocol,
        rounds=rows, verdict=judge(rows, args.clients, min_nodes=args.min_nodes))
    stage = 'query-indexes'
    try:
        ensure_context_schema(driver, config['database'])
        stage = 'history-prefix'
        add_prefix(driver, config['database'], args.run_id, args.prefix_nodes, tag, args.prefix_batch)
        stage = 'graph-size-check'
        initial = observe(driver, config['database'], args.run_id, count_nodes=True)
        result['initial_graph'] = initial
        print('[graph] ' + json.dumps(initial), flush=True)
        if initial['node_count'] < max(1_000_000, args.min_nodes):
            raise RuntimeError('initial measured run is below one million nodes; reader processes were not started')
        if args.ready_file:
            stage = 'query-plan-readiness'
            print(f'[waiting for query plan] {args.ready_file}', flush=True)
            deadline = time.monotonic() + args.ready_timeout
            while not args.ready_file.is_file():
                if time.monotonic() >= deadline:
                    raise TimeoutError('query-plan readiness gate timed out')
                time.sleep(.1)
        stage = 'reader-warmup'
        for robot_id in range(1, args.clients + 1):
            worker = ctx.Process(target=reader_process, args=(robot_id, config, args.run_id, ready,
                results, starts, args.duration, args.requests), name=f'robot-context-{robot_id}')
            worker.start()
            workers.append(worker)
        warmed = [ready.get(timeout=180) for _ in workers]
        if any(not worker['warmed'] for worker in warmed):
            raise RuntimeError('a context worker failed during warm-up')
        print(f'[readers ready] independent_processes={len(warmed)}', flush=True)
        if args.resume_live_url:
            stage = 'writer-speed-restoration'
            resume_writer(args.resume_live_url, args.resume_speed, driver, config['database'], args.run_id)
        for index, event in enumerate(starts, 1):
            stage = f'round-{index}'
            before = observe(driver, config['database'], args.run_id, count_nodes=True)
            if before['node_count'] < max(1_000_000, args.min_nodes):
                raise RuntimeError(f'round {index} measured run fell below one million nodes')
            trace, watch_stop = [], threading.Event()
            watcher = threading.Thread(target=watch_writer, args=(driver, config['database'],
                args.run_id, watch_stop, trace), daemon=True)
            watcher.start()
            try:
                event.set()
                records = [results.get(timeout=max(60, args.duration * 5)) for _ in workers]
            finally:
                watch_stop.set()
                watcher.join(timeout=5)
            samples = [s for r in records for s in r.get('samples', [])]
            interval = (min(s['start_wall_ms'] for s in samples), max(s['end_wall_ms'] for s in samples)) if samples else None
            after = observe(driver, config['database'], args.run_id, interval=interval)
            # Save raw observations before derived statistics, so a reporting
            # error never discards an already completed expensive measurement.
            (args.output / f'round-{index}.json').write_text(json.dumps(records, indent=2) + '\n')
            (args.output / f'round-{index}-writer.json').write_text(json.dumps(
                dict(before=before, after=after, interval=interval, trace=trace), indent=2) + '\n')
            after['observed_commit_advances'] = observed_advances(trace, interval) if interval else 0
            row = summarize_round(index, before, after, records, args.clients)
            row['commit_observations'] = trace
            row['observed_window_seconds'] = (interval[1]-interval[0])/1000 if interval else 0
            row['observed_step_commits_per_second'] = (after['observed_commit_advances']/row['observed_window_seconds']
                                                     if row['observed_window_seconds'] else 0)
            rows.append(row)
            print('[round] ' + json.dumps({key: row[key] for key in ('round', 'count', 'mean_ms',
                'p99_ms', 'max_ms', 'over_500ms', 'writer_step_advance', 'writes_during_round',
                'client_observed_peak')}), flush=True)
        with driver.session(database=config['database']) as session:
            hardware['neo4j_version'] = session.run('CALL dbms.components() YIELD versions RETURN versions[0] AS v').single()['v']
        result['verdict'] = judge(rows, args.clients, min_nodes=args.min_nodes)
    except BaseException as exc:
        result['failure'] = dict(stage=stage, error=f'{type(exc).__name__}: {exc}', traceback=traceback.format_exc())
        result['verdict'] = judge(rows, args.clients, min_nodes=args.min_nodes)
        result['verdict']['not_qualified_reasons'].append(result['failure']['error'])
        result['verdict'].update(qualified=False, status='不满足验收资格', **{'pass': False})
        print('[measurement failed] ' + json.dumps(result['failure']), flush=True)
    finally:
        deadline = time.monotonic() + 3
        for worker in workers:
            worker.join(timeout=max(0, deadline-time.monotonic()))
            if worker.is_alive():
                worker.terminate()
                worker.join(timeout=5)
        try:
            remaining = remove_prefix(driver, config['database'], args.run_id, tag) if args.prefix_nodes else 0
            if result is not None:
                result['history_prefix_cleanup_remaining'] = remaining
            print(f'[prefix cleanup] remaining={remaining}', flush=True)
        except Exception as exc:
            result['cleanup_error'] = f'{type(exc).__name__}: {exc}'
            result['verdict']['not_qualified_reasons'].append('history prefix cleanup outcome uncertain: ' + str(exc))
            result['verdict'].update(qualified=False, status='不满足验收资格', **{'pass': False})
        finally:
            driver.close()
    if result is not None:
        (args.output / 'acceptance.json').write_text(json.dumps(result, ensure_ascii=False, indent=2) + '\n')
        (args.output / 'report.md').write_text(render(result))
        print(render(result), flush=True)
        return 0 if result['verdict']['pass'] else 2 if not result['verdict']['qualified'] else 1
    return 1


if __name__ == '__main__':
    raise SystemExit(main())
