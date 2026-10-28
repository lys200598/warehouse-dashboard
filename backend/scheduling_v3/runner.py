"""Bounded v3 experiments, unique outputs and durable real events, no UI reset."""
import argparse
from collections import Counter
from dataclasses import asdict
import hashlib
import json
from pathlib import Path
import sqlite3
import time
import traceback
import zlib

from ..checkpoint import read_checkpoint, write_checkpoint
from ..day1.runner import quantile
from ..day1.data import sha
from ..knowledge import JournalKnowledgeStore, canonical
from ..scheduling_v2.audit import audit_graph
from .graph import V3Mirror, audit_helpers
from .layout import audit_roadmap
from .sim import Simulation, Config


def run(args, *, simulation_class=Simulation, config_class=Config, mirror_class=V3Mirror,
        graph_auditor=None):
    if min(args.max_wall_seconds, args.drain_seconds, args.commit_seconds) <= 0 or args.until is not None and args.until < 0:
        raise ValueError('invalid experiment limits')
    output = Path(args.output)
    if output.exists() and any(output.iterdir()) and not args.resume:
        raise FileExistsError('preserve previous experiments; use a new output directory')
    output.mkdir(parents=True, exist_ok=True)
    config = config_class(**(json.loads(Path(args.config_json).read_text()) if args.config_json else {}))
    specs = json.loads(Path(args.disturbances_json).read_text()) if args.disturbances_json else []
    if not isinstance(specs, list):
        raise ValueError('disturbances must be a list')
    identity = hashlib.sha256(canonical(specs)).hexdigest()
    state = read_checkpoint(output / 'checkpoint.json') if args.resume else None
    if state and state['experiment']['disturbance_sha256'] != identity:
        raise ValueError('disturbance identity mismatch on resume')
    mirror = mirror_class.from_env() if args.knowledge_backend == 'neo4j' else None
    store = JournalKnowledgeStore(output / 'journal.sqlite', mirror)
    if state:
        state = store.recovery_payload(state)
    started = time.monotonic()
    sim = simulation_class(args.scene, args.dataset, args.run_id, config, checkpoint=state)
    until = args.until if args.until is not None else sim.manifest['duration_seconds']
    applied = state['experiment']['applied'] if state else []
    done = {e['index'] for e in applied}
    frames = sqlite3.connect(output / 'frames.sqlite') if args.frames else None
    if frames:
        frames.execute('CREATE TABLE IF NOT EXISTS frames(second INTEGER PRIMARY KEY,payload BLOB NOT NULL)')
    last_commit = sim.elapsed
    last_log = time.monotonic()
    status, error, graph_audit = 'complete', None, None
    layout = audit_roadmap(sim.roadmap, circle_probes=False)
    (output / 'layout-audit.json').write_text(json.dumps(layout, ensure_ascii=False, indent=2), encoding='utf-8')

    def persist():
        nonlocal last_commit
        sim.knowledge_seq += 1
        checkpoint = sim.checkpoint()
        checkpoint['experiment'] = {'disturbance_sha256': identity, 'applied': applied}
        store.append(checkpoint, sim.pending_events)
        sim.pending_events = []
        write_checkpoint(output / 'checkpoint.json', checkpoint)
        last_commit = sim.elapsed
        if mirror and store.last_error:
            raise RuntimeError('graph mirror error; journal preserved: ' + str(store.last_error))

    def frame():
        if frames:
            value = sim.frame()
            value['backend'] = sim.run_metadata.get('backend','local-central-prevention-feedback-v3')
            value['knowledge'] = store.status(sim.run_id)
            frames.execute('INSERT OR REPLACE INTO frames VALUES (?,?)', (sim.elapsed, zlib.compress(canonical(value), 3)))
            frames.commit()
            write_checkpoint(output / 'latest.json', value)

    def disturbances():
        changed = False
        for index, spec in enumerate(specs):
            if index in done or sim.elapsed < spec['at']:
                continue
            selector = spec.get('robot', 'carrying')
            if isinstance(selector, int):
                owner = selector if selector in sim.robots else None
            elif selector == 'last_persistent':
                previous = [e['owner'] for e in applied if e['kind']=='persistent_stop' and e['success']]
                owner = previous[-1] if previous else None
            else:
                owners = [i for i, r in sim.robots.items() if
                    (selector == 'carrying' and r['carrying'] and i not in sim.assists or
                     selector == 'moving' and r['stage'] in {'to_source', 'to_target', 'returning'} or
                     selector == 'carrying_moving' and r['carrying'] and r['stage']=='to_target' and i not in sim.assists or
                     selector == 'handoff_receiver' and any(a['receiver'] == i for a in sim.assists.values()))]
                owner = min(owners) if owners else None
            if owner is None:
                if sim.elapsed - spec['at'] > spec.get('selection_timeout', 300):
                    raise RuntimeError(f'disturbance {index} had no eligible robot; not silently skipped')
                continue
            kind = spec['kind']
            if kind == 'delay':
                ok = sim.inject_delay(owner, spec['seconds'])
            elif kind == 'stop':
                ok = sim.inject_stop(owner, spec['seconds'])
                if ok and spec.get('handoff'):
                    ok = sim.request_assistance(owner, reason='finite_drive_fault')
            elif kind == 'persistent_stop':
                ok = sim.inject_stop(owner, None, reason='persistent_drive_fault')
                if ok and sim.robots[owner]['carrying']:
                    ok = sim.request_assistance(owner, reason='persistent_drive_fault')
            elif kind == 'clear_fault':
                ok = sim.clear_fault(owner)
            elif kind == 'soc':
                ok = sim.report_soc(owner, spec['soc'])
            elif kind == 'handoff':
                ok = sim.request_assistance(owner)
            else:
                raise ValueError('unknown disturbance kind')
            record = {'index': index, 'requested_at': spec['at'], 'applied_at': sim.elapsed,
                      'kind': kind, 'owner': owner, 'success': bool(ok)}
            applied.append(record)
            done.add(index)
            changed = True
            if not ok:
                raise RuntimeError(f'disturbance {index} was not safely recovered')
        return changed

    try:
        if not layout['initial_roadmap_usable']:
            raise RuntimeError('layout static safety gate failed')
        if not args.resume:
            persist()
            frame()
        while True:
            if time.monotonic() - started >= args.max_wall_seconds:
                status = 'wall-time-limit'
                break
            complete = sim.metrics['completed'] == len(sim.tasks) and sim.cursor == len(sim.tasks)
            if sim.elapsed >= until and (args.window or complete and not sim.assists):
                break
            if sim.elapsed >= until + args.drain_seconds:
                status = 'unfinished-after-drain'
                break
            if disturbances():
                persist()
            sim.step()
            if sim.elapsed - last_commit >= args.commit_seconds:
                persist()
            if sim.elapsed % 10 == 0:
                frame()
            if time.monotonic() - last_log >= 30:
                print(json.dumps({'elapsed': sim.elapsed, 'completed': sim.metrics['completed'],
                    'counts': sim.counts(), 'wall_seconds': time.monotonic()-started,
                    'handoffs': sim.metrics['handoffs']}), flush=True)
                last_log = time.monotonic()
        if len(done) != len(specs):
            raise RuntimeError('not all requested disturbances were exercised')
    except Exception as exc:
        status, error = 'failed', f'{type(exc).__name__}: {exc}'
        traceback.print_exc()
    finally:
        sim.running = False
        try:
            persist()
            frame()
        except Exception as exc:
            status, error = 'failed', error or f'final commit: {exc}'
            traceback.print_exc()
    if mirror and status == 'complete':
        try:
            if graph_auditor:
                graph_audit = graph_auditor(store,sim)
            else:
                graph_audit = audit_graph(store, sim)
                graph_audit['collaboration'] = audit_helpers(mirror, sim.run_id, expected_handoffs=sim.metrics['handoffs'])
        except Exception as exc:
            status, error = 'failed', f'graph audit: {exc}'
    released = [t for t in sim.tasks.values() if t['status'] != 'scheduled']
    flow = [t['completed_at']-t['release_second'] for t in released if t['status'] == 'completed']
    metrics = dict(sim.metrics)
    for name in ['dispatch_wall_seconds', 'recovery_wall_seconds']:
        values = metrics.pop(name)
        metrics[name] = {'count': len(values), 'total': sum(values), 'p95': quantile(values, .95),
                         'p99': quantile(values, .99), 'max': max(values, default=0.)}
    result = {'run_id': sim.run_id, 'status': status, 'error': error,
        'elapsed': sim.elapsed, 'wall_seconds': time.monotonic()-started,
        'input_identity': {k: sim.run_metadata[k] for k in ['scene_sha256','tasks_sha256','initial_inventory_sha256','manifest_sha256']},
        'code_identity': sim.run_metadata.get('execution_code_sha256',sim.run_metadata.get('wide_code_sha256',sim.run_metadata['scheduler_v3_code_sha256'])), 'config': asdict(config),
        'scenario': sim.manifest.get('scenario'), 'registered_robots': len(sim.robots),
        'robots_completed_tasks': sum(r['completed'] > 0 for r in sim.robots.values()),
        'released': len(released), 'tasks_total': len(sim.tasks), 'unfinished': sum(t['status']!='completed' for t in released),
        'late_unfinished': sum(t['status']!='completed' and t['due_second'] < sim.elapsed for t in released),
        'task_statuses': dict(Counter(t['status'] for t in sim.tasks.values())),
        'flow_seconds': {'mean_completed_only': sum(flow)/max(1,len(flow)), 'p95': quantile(flow,.95),
                         'p99': quantile(flow,.99), 'max': max(flow,default=0)},
        'metrics': metrics, 'disturbances': applied, 'inside': sim.inside,
        'knowledge': store.status(sim.run_id), 'graph_audit': graph_audit,
        'acceptance': {'all_input_completed': sim.metrics['completed']==len(sim.tasks),
            'all_disturbances_exercised': len(done)==len(specs),
            'no_recorded_safety_failure': error is None, 'hardware_calibrated': False,
            'hundred_simultaneously_working': sim.metrics['max_active_business']>=100,
            'hundred_execution_processes': False, 'million_node_joint': False, 'latency_joint': False}}
    if hasattr(sim, 'execution_status'):
        result['scheduler'] = sim.execution_status()
    (output/'results.json').write_text(json.dumps(result, ensure_ascii=False, indent=2, allow_nan=False)+'\n',encoding='utf-8')
    print(json.dumps({k:v for k,v in result.items() if k not in {'metrics','graph_audit'}},ensure_ascii=False),flush=True)
    if frames:
        frames.close()
    store.close()
    return 0 if status == 'complete' else 2


def build_parser():
    p = argparse.ArgumentParser()
    for name in ['scene','dataset','output','run-id']:
        p.add_argument('--'+name, required=True)
    p.add_argument('--until',type=int)
    p.add_argument('--max-wall-seconds',type=int,default=1800)
    p.add_argument('--drain-seconds',type=int,default=7200)
    p.add_argument('--commit-seconds',type=int,default=300)
    p.add_argument('--knowledge-backend',choices=['journal','neo4j'],default='journal')
    p.add_argument('--config-json')
    p.add_argument('--disturbances-json')
    p.add_argument('--window',action='store_true')
    p.add_argument('--frames',action='store_true')
    p.add_argument('--resume',action='store_true')
    return p


if __name__ == '__main__':
    raise SystemExit(run(build_parser().parse_args()))
