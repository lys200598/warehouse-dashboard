"""Prepare an all-day workload and record C graph timings in two-hour windows.

prepare reuses the validated wide scenario generator. run executes real ticks;
a bounded --max-ticks probe is explicitly not an all-day acceptance result.
"""
import argparse
import json
import math
import os
from pathlib import Path
import signal
import statistics
import time

PHASES = ('advance_ms', 'write_ms', 'wait_step_ms', 'read_ms', 'dispatch_ms', 'context_ms', 'total_ms')


def write_report(path, report):
    temporary = path.with_suffix('.json.tmp')
    temporary.write_text(json.dumps(report, indent=2) + '\n')
    temporary.replace(path)


def graph_config(sim=None):
    config = getattr(sim, '_graph_config', None) or {}
    return {'uri': config.get('uri') or os.environ.get('WAREHOUSE_NEO4J_URI') or os.environ['NEO4J_URI'],
            'user': config.get('user') or os.environ.get('WAREHOUSE_NEO4J_USER', 'neo4j'),
            'password': config.get('password') or os.environ['WAREHOUSE_NEO4J_PASSWORD'],
            'database': config.get('database') or os.environ.get('WAREHOUSE_NEO4J_DATABASE', 'neo4j')}


def run_exists(config, run_id):
    from neo4j import GraphDatabase
    with GraphDatabase.driver(config['uri'], auth=(config['user'], config['password'])) as driver:
        with driver.session(database=config['database'], default_access_mode='READ') as session:
            return session.run('MATCH (r:Run {id:$run}) RETURN count(r) AS n', run=run_id).single()['n'] > 0


def cleanup_run(args, config):
    from urllib.request import Request, urlopen
    from neo4j import GraphDatabase
    request = Request(args.global_writer_url.rstrip('/') + '/v1/runs/' + args.run_id, method='DELETE',
                      data=json.dumps({'confirm_run_id': args.run_id}).encode(),
                      headers={'Content-Type': 'application/json'})
    with urlopen(request, timeout=600) as response:
        if response.status != 200:
            raise RuntimeError('run deletion failed')
    with GraphDatabase.driver(config['uri'], auth=(config['user'], config['password'])) as driver:
        with driver.session(database=config['database']) as session:
            remaining = session.run('MATCH (n) WHERE n.run_id=$run OR (n:Run AND n.id=$run) '
                                    'RETURN count(n) AS n', run=args.run_id).single()['n']
    if remaining:
        raise RuntimeError(f'{remaining} nodes remain')
    print('remaining=0', flush=True)
    return {'status': 'complete', 'remaining_nodes': 0}


def stop_owned_writer(expected_pid, pid_file):
    """Only stop the exact writer still recorded by the launching job."""
    if not pid_file.exists() or pid_file.read_text().strip() != str(expected_pid):
        print('writer stop skipped: writer.pid changed', flush=True)
        return
    command_file = Path(f'/proc/{expected_pid}/cmdline')
    if not command_file.exists() or not command_file.read_bytes():
        print('writer already stopped', flush=True)
        return
    command = command_file.read_bytes().decode(errors='replace').split('\0')
    module_ok = any(part == '-m' and index + 1 < len(command) and command[index + 1] ==
                    'backend.global_writer_api' for index, part in enumerate(command))
    port_ok = '--port=14591' in command or any(part == '--port' and index + 1 < len(command) and
                  command[index + 1] == '14591' for index, part in enumerate(command))
    if not module_ok or not port_ok:
        print('writer stop skipped: PID command no longer matches 14591 global_writer_api', flush=True)
        return
    os.kill(expected_pid, signal.SIGTERM)
    deadline = time.monotonic() + 20
    while time.monotonic() < deadline:
        if not command_file.exists() or not command_file.read_bytes():
            print('stopped owned writer', expected_pid, flush=True)
            return
        if command_file.read_bytes().decode(errors='replace').split('\0') != command:
            print('writer stop finished: original PID command ended', flush=True)
            return
        time.sleep(.2)
    raise TimeoutError('owned writer did not stop within 20 seconds')


def distribution(values):
    if not values:
        return {'n': 0}
    ordered = sorted(values)
    return {'n': len(values), 'mean_ms': statistics.fmean(values),
            'p99_ms': ordered[max(0, math.ceil(.99 * len(values)) - 1)],
            'max_ms': ordered[-1]}


def run(args):
    from backend.robot_actor.ray_actor import RayRobotProcessSplit
    from backend.wheeled.sim import WheeledConfig, WheeledSimulation
    class Engine(RayRobotProcessSplit, WheeledSimulation):
        ray_address = args.ray_address
        ray_actor_cpus = args.actor_cpus
    if not args.run_id.startswith('wz-test-'):
        raise ValueError('run_id must start with wz-test-')
    manifest = json.loads((args.dataset / 'manifest.json').read_text())
    if manifest['duration_seconds'] != 86400:
        raise ValueError('all-day runner requires a 86400-second dataset')
    args.output.mkdir(parents=True, exist_ok=False)
    sim, owned_run, failure = None, False, None
    buckets = {}
    began = time.monotonic()
    limit = args.max_ticks or 86400
    last_successful_tick = 0
    report = {'run_id': args.run_id, 'tick': 0, 'last_successful_tick': 0, 'status': 'starting',
              'dataset_duration_seconds': 86400, 'all_day_executed': False, 'windows': {},
              'robot_context_interval': args.robot_context_interval, 'requested_ticks': limit,
              'started_at_utc': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
              'last_successful_tick_definition': 'last returned sim.step(); not a durable Neo4j watermark'}
    write_report(args.output / 'report.json', report)

    def progress(status):
        write_report(args.output / 'progress.json', {'run_id': args.run_id, 'status': status,
                     'tick': getattr(sim, 'tick', 0), 'last_successful_tick': last_successful_tick,
                     'wall_seconds': time.monotonic() - began,
                     'updated_at_utc': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())})

    def update_report(status):
        tick = getattr(sim, 'tick', 0)
        report.update(status=status, tick=tick, last_successful_tick=last_successful_tick,
                      all_day_executed=last_successful_tick >= 86400,
                      wall_seconds=time.monotonic() - began,
                      completed_tasks=getattr(sim, 'metrics', {}).get('completed', 0),
                      total_tasks=len(getattr(sim, 'tasks', {})),
                      windows={str(k * 7200) + '-' + str((k + 1) * 7200):
                               {name: distribution(samples) for name, samples in group.items()}
                               for k, group in sorted(buckets.items())})
        write_report(args.output / 'report.json', report)
        progress(status)

    def checkpoint(name):
        from backend.checkpoint import write_checkpoint
        write_checkpoint(args.output / name, sim.checkpoint())

    previous_term_handler = signal.getsignal(signal.SIGTERM)
    def terminated(_signum, _frame):
        raise KeyboardInterrupt('day job received SIGTERM')
    signal.signal(signal.SIGTERM, terminated)
    try:
        cfg = graph_config()
        if run_exists(cfg, args.run_id):
            raise FileExistsError('graph run_id already exists; refusing to overwrite or delete it')
        owned_run = True
        config = WheeledConfig(**json.loads(args.config.read_text()))
        sim = Engine(args.scene, args.dataset, args.run_id, config, robot_graph=True,
                     kg_state_source='neo4j', global_writer_url=args.global_writer_url,
                     robot_context_interval=args.robot_context_interval)
        sim.context_log_path = str(args.output / 'robot-context.jsonl')
        # Ray.init installs its own SIGTERM handler. Restore the finally/cleanup
        # handler after initialization, before the first physical step.
        make_pool = getattr(sim, '_robot_pool', None)
        if make_pool is not None:
            make_pool()
        signal.signal(signal.SIGTERM, terminated)
        update_report('running')
        with (args.output / 'tick-timing.jsonl').open('x', encoding='utf-8') as stream:
            for _ in range(limit):
                mark = time.perf_counter()
                sim.step()
                last_successful_tick = sim.tick
                timing = {key: float(sim.kg_timing.get(key, 0.)) for key in PHASES}
                timing['total_ms'] = (time.perf_counter() - mark) * 1000
                bucket = (sim.tick - 1) // 7200
                values = buckets.setdefault(bucket, {})
                for key, value in timing.items():
                    values.setdefault(key, []).append(value)
                stream.write(json.dumps({'tick': sim.tick, 'window': bucket, **timing}) + '\n')
                if sim.tick % 60 == 0:
                    stream.flush()
                    progress('running')
                if sim.tick % 7200 == 0 or sim.tick == limit:
                    stream.flush()
                    if sim._step_client is not None:
                        sim._step_client.flush(15.)
                    checkpoint(f'checkpoint-{sim.tick:07d}.json')
                    checkpoint('checkpoint.json')
                    update_report('running')
                    print(json.dumps(report), flush=True)
        update_report('complete' if last_successful_tick >= 86400 else 'bounded_complete')
    except BaseException as exc:
        failure = exc
        report['error'] = f'{type(exc).__name__}: {exc}'
        if sim is not None:
            try:
                checkpoint(f'checkpoint-failed-{sim.tick:07d}.json')
            except Exception as checkpoint_error:
                report['failure_checkpoint_error'] = str(checkpoint_error)
        update_report('failed')
        raise
    finally:
        final_errors = []
        if sim is not None:
            try:
                sim.close()
            except BaseException as exc:
                report['close_error'] = f'{type(exc).__name__}: {exc}'
                final_errors.append(exc)
        if args.cleanup and owned_run:
            try:
                report['cleanup'] = cleanup_run(args, graph_config(sim))
            except BaseException as exc:
                report['cleanup'] = {'status': 'failed', 'error': f'{type(exc).__name__}: {exc}'}
                final_errors.append(exc)
        else:
            report['cleanup'] = {'status': 'not_requested' if not args.cleanup else 'run_not_created'}
        if final_errors and failure is None:
            report['error'] = f'{type(final_errors[0]).__name__}: {final_errors[0]}'
            update_report('failed')
        else:
            update_report(report['status'])
        signal.signal(signal.SIGTERM, previous_term_handler)
        if final_errors and failure is None:
            raise final_errors[0]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='action', required=True)
    prepare = commands.add_parser('prepare')
    prepare.add_argument('--scene', type=Path, required=True)
    prepare.add_argument('--output', type=Path, required=True)
    prepare.add_argument('--active-slots', type=int, default=72)
    prepare.add_argument('--date', default='2026-10-30')
    execute = commands.add_parser('run')
    execute.add_argument('--scene', type=Path, required=True)
    execute.add_argument('--dataset', type=Path, required=True)
    execute.add_argument('--config', type=Path, default=Path('config/wheeled-medium-100.json'))
    execute.add_argument('--output', type=Path, required=True)
    execute.add_argument('--run-id', required=True)
    execute.add_argument('--ray-address', default='127.0.0.1:27380')
    execute.add_argument('--actor-cpus', type=float, default=.02)
    execute.add_argument('--global-writer-url', default='http://127.0.0.1:14591')
    execute.add_argument('--robot-context-interval', type=int, default=10)
    execute.add_argument('--max-ticks', type=int)
    execute.add_argument('--cleanup', action='store_true')
    stop = commands.add_parser('stop-writer')
    stop.add_argument('--expected-pid', type=int, required=True)
    stop.add_argument('--pid-file', type=Path, default=Path('/tmp/wz-kg-evidence/writer.pid'))
    args = parser.parse_args()
    if args.action == 'prepare':
        from backend.wide.scenarios import generate_scenario
        manifest, report = generate_scenario(args.scene, args.output, scenario='uniform',
            duration_seconds=86400, active_slots=args.active_slots, slot_interval_seconds=3600,
            seed=20261030, date=args.date)
        if not all(report['hourly_tasks']):
            raise RuntimeError('generated workload does not cover every hour')
        print(json.dumps({'dataset': str(args.output), 'tasks': manifest['tasks'],
                          'duration_seconds': manifest['duration_seconds'],
                          'hourly_tasks': report['hourly_tasks'], 'validation': report['valid']}))
    elif args.action == 'stop-writer':
        if args.expected_pid < 1:
            parser.error('--expected-pid must be positive')
        stop_owned_writer(args.expected_pid, args.pid_file)
    else:
        if args.max_ticks is not None and args.max_ticks < 1:
            parser.error('--max-ticks must be positive')
        run(args)


if __name__ == '__main__':
    main()
