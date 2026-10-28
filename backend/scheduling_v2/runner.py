"""Isolated, bounded A/B runner. Never resets the production dashboard or DB."""
import argparse
from collections import Counter
import json
from pathlib import Path
import sqlite3
import time
import traceback
import zlib

from ..checkpoint import write_checkpoint, read_checkpoint
from ..knowledge import JournalKnowledgeStore, canonical
from ..day1.sim import Simulation as BaselineSimulation, Config as BaselineConfig
from ..day1.runner import quantile
from ..day1.data import sha
from .sim import Simulation, Config
from .audit import audit_graph


def run(args):
    output = Path(args.output)
    checkpoint = output / 'checkpoint.json'
    if output.exists() and any(output.iterdir()) and not args.resume:
        raise FileExistsError('preserve existing experiment; choose a fresh output')
    output.mkdir(parents=True, exist_ok=True)
    mirror = None
    if args.knowledge_backend == 'neo4j':
        from ..day1.graph import DayOneMirror
        mirror = DayOneMirror.from_env()
    store = JournalKnowledgeStore(output / 'journal.sqlite', mirror)
    extra = json.loads(Path(args.config_json).read_text()) if args.config_json else {}
    if args.variant == 'baseline':
        cls, config = BaselineSimulation, BaselineConfig(**extra)
    else:
        cls, config = Simulation, Config(task_chaining=args.variant != 'v2-no-chain', **extra)
    state = read_checkpoint(checkpoint) if args.resume else None
    if state:
        state = store.recovery_payload(state)
    started = time.monotonic()
    sim = cls(args.scene, args.dataset, args.run_id, config, checkpoint=state)
    startup_seconds = time.monotonic() - started
    frames = sqlite3.connect(output / 'frames.sqlite') if args.frames else None
    if frames:
        frames.execute('PRAGMA journal_mode=WAL')
        frames.execute('CREATE TABLE IF NOT EXISTS frames(second INTEGER PRIMARY KEY,payload BLOB NOT NULL)')
    last_log, last_commit, failures = time.monotonic(), sim.elapsed, 0
    status, failure = 'complete', None

    def persist():
        nonlocal failures, last_commit
        sim.knowledge_seq += 1
        # Preserve digest identity across JSON round trips, including baseline's integer job keys.
        payload = json.loads(json.dumps(sim.checkpoint(), allow_nan=False))
        store.append(payload, sim.pending_events)
        sim.pending_events = []
        write_checkpoint(checkpoint, payload)
        last_commit = sim.elapsed
        failures = failures + 1 if store.last_error else 0
        if failures >= 3:
            raise RuntimeError('mirror failed three consecutive commits; journal preserved')

    def frame():
        if frames:
            value = sim.frame()
            value['knowledge'] = store.status(sim.run_id)
            with frames:
                frames.execute('INSERT OR REPLACE INTO frames VALUES (?,?)', (sim.elapsed, zlib.compress(canonical(value), 3)))
            write_checkpoint(output / 'latest.json', value)

    try:
        if not args.resume:
            persist()
            frame()
        while True:
            if time.monotonic() - started >= args.max_wall_seconds:
                status = 'wall-time-limit'
                break
            if sim.elapsed >= args.until:
                if args.until < 86400 or (sim.metrics['completed'] == len(sim.tasks) and not any(j['task_id'] for j in sim.jobs.values())):
                    break
                if sim.elapsed >= args.until + 3600:
                    status = 'unfinished-after-drain'
                    break
            sim.step()
            if sim.elapsed - last_commit >= args.commit_seconds:
                persist()
            if sim.elapsed % 10 == 0:
                frame()
            if time.monotonic() - last_log >= 30:
                print(json.dumps({'variant': args.variant, 'elapsed': sim.elapsed, 'completed': sim.metrics['completed'],
                                  'pending': len(sim.pending), 'counts': sim.counts(),
                                  'planning_seconds': round(sim.metrics.get('planning_seconds', 0), 2),
                                  'wall_seconds': round(time.monotonic() - started, 2)}), flush=True)
                last_log = time.monotonic()
    except Exception as error:
        failure, status = f'{type(error).__name__}: {error}', 'failed'
        traceback.print_exc()
    finally:
        sim.running = False
        try:
            persist()
            frame()
        except Exception as error:
            failure, status = failure or f'final commit: {error}', 'failed'
            traceback.print_exc()

    released = [t for t in sim.tasks.values() if t['status'] != 'scheduled']
    unfinished = [t for t in released if t['status'] != 'completed']
    flow = [t['completed_at'] - t['release_second'] for t in released if t['status'] == 'completed']
    late_unfinished = sum(t['due_second'] < sim.elapsed for t in unfinished)
    result = {'run_id': sim.run_id, 'variant': args.variant, 'status': status, 'error': failure,
              'simulated_seconds': sim.elapsed, 'wall_seconds': round(time.monotonic() - started, 3),
              'startup_seconds': round(startup_seconds, 3), 'registered_robots': len(sim.robots),
              'robots_completed_tasks': sum(r['completed'] > 0 for r in sim.robots.values()),
              'robot_processes': 1, 'input_identity': {k: sim.run_metadata[k] for k in
                  ['scene_sha256', 'tasks_sha256', 'initial_inventory_sha256', 'manifest_sha256']},
              'runner_sha256': sha(Path(__file__)), 'config': sim.checkpoint()['config'],
              'tasks_total': len(sim.tasks), 'released': len(released), 'unfinished_released': len(unfinished),
              'late_unfinished': late_unfinished, 'task_statuses': dict(Counter(t['status'] for t in sim.tasks.values())),
              'flow_seconds': {'mean_completed_only': sum(flow) / max(1, len(flow)), 'p95_completed_only': quantile(flow, .95)},
              'departure_seconds': {'mean_planned': sum(sim.metrics['departure_wait_seconds']) / max(1, len(sim.metrics['departure_wait_seconds'])),
                                    'p95_planned': quantile(sim.metrics['departure_wait_seconds'], .95)},
              'inside': sim.inside, 'counts': sim.counts(), 'metrics': sim.metrics,
              'knowledge': store.status(sim.run_id), 'graph_backend': args.knowledge_backend,
              'acceptance': {'day_completed': sim.elapsed >= 86400 and len(unfinished) == 0 and sim.cursor == len(sim.tasks),
                             'zero_capacity_overflow': sim.metrics['max_inside'] <= 1728,
                             'no_recorded_safety_exception': failure is None,
                             'minimum_soc_above_emergency': sim.metrics['min_soc'] >= 10,
                             'no_graph_lag': mirror is not None and store.status(sim.run_id)['lag'] == 0,
                             'ray_100_workers': False, 'million_node_requirement': False, 'latency_joint_requirement': False}}
    if mirror and failure is None and not store.last_error:
        try:
            result['graph_audit'] = audit_graph(store, sim)
        except Exception as error:
            result['graph_audit_error'], result['status'] = str(error), 'failed'
    (output / 'results.json').write_text(json.dumps(result, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
    print(json.dumps({k: v for k, v in result.items() if k not in {'metrics', 'graph_audit'}}, ensure_ascii=False), flush=True)
    if frames:
        frames.close()
    store.close()
    return 0 if result['status'] == 'complete' else 2


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    for name in ['scene', 'dataset', 'output', 'run-id']:
        parser.add_argument('--' + name, required=True)
    parser.add_argument('--variant', choices=['baseline', 'v2-no-chain', 'v2'], default='v2')
    parser.add_argument('--until', type=int, default=86400)
    parser.add_argument('--max-wall-seconds', type=int, default=1200)
    parser.add_argument('--commit-seconds', type=int, default=300)
    parser.add_argument('--knowledge-backend', choices=['journal', 'neo4j'], default='journal')
    parser.add_argument('--config-json')
    parser.add_argument('--frames', action='store_true')
    parser.add_argument('--resume', action='store_true')
    raise SystemExit(run(parser.parse_args()))
