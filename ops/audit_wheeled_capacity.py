"""Read-only audit of finished capacity cases; never repairs or reruns them.

Final checkpoint, journal commits and 10-second frames are distinguished from
the runner's per-second swept audit counters. Sparse frames cannot independently
prove continuous collision freedom between samples.
"""
from __future__ import annotations
import argparse
from collections import Counter
import csv
import hashlib
import json
import math
from pathlib import Path
import sqlite3
import sys
from types import SimpleNamespace
import zlib


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def read_json(path):
    return json.loads(Path(path).read_text(encoding='utf-8'),
        parse_constant=lambda x: (_ for _ in ()).throw(ValueError('nonfinite JSON '+x)))


def rows(path):
    with Path(path).open(encoding='utf-8', newline='') as f:
        return list(csv.DictReader(f))


def spatial_snapshot(states, scene):
    if len(states) != 100 or {r['robot_id'] for r in states} != set(range(1, 101)):
        raise AssertionError('snapshot requires 100 unique robots')
    radius = scene['spec']['robotRadius']
    minimum, max_soc, min_soc = math.inf, -math.inf, math.inf
    for i, r in enumerate(states):
        x, y, soc = r['x'], r['y'], r['battery']
        if not all(math.isfinite(v) for v in (x, y, soc)):
            raise AssertionError('nonfinite robot state')
        if not 0 <= x <= scene['spec']['width'] or not 0 <= y <= scene['spec']['depth']:
            raise AssertionError('robot outside warehouse')
        max_soc, min_soc = max(max_soc, soc), min(min_soc, soc)
        for b in states[:i]:
            distance = math.hypot(x-b['x'], y-b['y'])
            minimum = min(minimum, distance)
            if distance < 2*radius-1e-6:
                raise AssertionError(f'snapshot body overlap {r["robot_id"]}/{b["robot_id"]}')
        for box in scene['racks']:
            dx = max(box['x']-x, 0, x-box['x']-box['w'])
            dy = max(box['y']-y, 0, y-box['y']-box['d'])
            if math.hypot(dx, dy) < radius-1e-6:
                raise AssertionError(f'snapshot rack overlap {r["robot_id"]}/{box["id"]}')
    if min_soc < 10-1e-6 or max_soc > 100+1e-6:
        raise AssertionError('snapshot SOC outside hard bounds')
    return {'min_pair_separation_m': minimum, 'minimum_soc': min_soc, 'maximum_soc': max_soc}


def audit_case(case, matrix, source):
    from backend.checkpoint import read_checkpoint
    from backend.knowledge import digest
    from backend.scheduling_v5.invariants import assert_warehouse_inventory
    from backend.wheeled.batteries import BatteryPool
    from backend.scheduling_v3.energy import EnergyProfile
    output = matrix/case['name']
    if not (output/'results.json').exists():
        return {'name': case['name'], 'status': 'pending-no-final-results', 'accepted': False}
    paths = {n: output/n for n in ('results.json', 'checkpoint.json', 'journal.sqlite', 'frames.sqlite') if (output/n).exists()}
    before = {n: sha(p) for n, p in paths.items()}
    result, cp = read_json(output/'results.json'), read_checkpoint(output/'checkpoint.json')
    if result['status'] != 'complete':
        return {'name': case['name'], 'status': result['status'], 'accepted': False, 'raw_sha256': before}
    scene_path, dataset = Path(case['scene']), Path(case['dataset'])
    if result['config'] != cp['config'] or cp['config']['speed'] != case['speed_mps']:
        raise AssertionError('configuration/speed mismatch')
    config_file = Path(case.get('config', str(matrix/(case['name']+'-config.json'))))
    if config_file.exists():
        configured = read_json(config_file)
        if any(cp['config'].get(k) != v for k, v in configured.items()):
            raise AssertionError('configuration file differs from actual runtime configuration')
    scene, initial = read_json(scene_path), rows(dataset/'initial_inventory.csv')
    inputs = {'scene_sha256': sha(scene_path), 'tasks_sha256': sha(dataset/'tasks.csv'),
              'initial_inventory_sha256': sha(dataset/'initial_inventory.csv'), 'manifest_sha256': sha(dataset/'manifest.json')}
    if any(v != result['input_identity'][k] or v != cp['run_metadata'][k] for k, v in inputs.items()):
        raise AssertionError('run input identity mismatch')
    source_hashes = {p.relative_to(source/'backend').as_posix(): sha(p) for p in sorted((source/'backend').rglob('*.py'))}
    source_sha = hashlib.sha256(json.dumps(source_hashes, sort_keys=True).encode()).hexdigest()
    if source_sha != cp['run_metadata']['code_sha256']:
        raise AssertionError('frozen backend source identity mismatch')
    raw_tasks = rows(dataset/'tasks.csv')
    original = rows(source/'data/examples/regression/peak/tasks.csv')
    if len(raw_tasks) != len(original):
        raise AssertionError('input task count changed')
    unchanged = ['task_id', 'task_type', 'goods_id', 'release_time', 'due_time']
    if any(a[k] != b[k] for a, b in zip(raw_tasks, original) for k in unchanged):
        raise AssertionError('task identity/type/timing changed')
    original_initial = rows(source/'data/examples/regression/peak/initial_inventory.csv')
    if [r['goods_id'] for r in initial] != [r['goods_id'] for r in original_initial]:
        raise AssertionError('initial goods identity/count changed')
    slots = {s['slot_id']: s for rack in cp['racks'] for s in rack['slots']}
    robot_states = [r['state'] for r in cp['robots']]
    sim = SimpleNamespace(slots=slots, goods=cp['goods'], tasks=cp['tasks'],
        robots={r['robot_id']: r for r in robot_states}, initial=initial,
        scene=scene, metrics=cp['metrics'], inside=cp['inside'], ports=cp['ports'])
    inventory = assert_warehouse_inventory(sim)
    if len(cp['tasks']) != len(raw_tasks) or any(t['status'] != 'completed' for t in cp['tasks'].values()):
        raise AssertionError('final checkpoint does not complete every input task')
    if result['unfinished'] or not result['acceptance']['all_input_completed']:
        raise AssertionError('runner final task acceptance mismatch')
    final_spatial = spatial_snapshot(robot_states, scene)
    if cp['metrics']['min_soc'] < cp['config']['emergency_reserve_soc']-1e-6:
        raise AssertionError('runtime recorded hard SOC reserve failure')
    pool_result = None
    if 'wheeled_batteries' in cp:
        b = cp['wheeled_batteries']
        profile = {'transport': EnergyProfile, 'legacy': EnergyProfile.legacy,
                   'conservative': EnergyProfile.conservative}[cp['config']['energy_profile']]()
        pool = BatteryPool(range(1, 101), list(b['chargers']), cp['config']['swap_spares_per_station'],
            cp['config']['swap_offline_chargers_per_station'], profile)
        pool.restore(b)
        if b['updated_at'] != cp['elapsed']:
            raise AssertionError('battery pool clock mismatch')
        reserved = [(key, p['owner']) for key, p in pool.packs.items() if p['status'] == 'reserved']
        if len({owner for _, owner in reserved}) != len(reserved):
            raise AssertionError('robot holds multiple battery reservations')
        for token, owner in reserved:
            job = cp['jobs'].get(str(owner), cp['jobs'].get(owner, {}))
            if job.get('swap_pack_id') != token or not any(p.get('marker') == 'swap_complete' for p in job['parts']):
                raise AssertionError('reserved pack lacks actual future physical exchange job')
        pool_result = {'physical_pack_count': len(pool.all_ids), 'installed_count': len(pool.installed),
            'spare_count': len(pool.packs), 'status_counts': dict(Counter(p['status'] for p in pool.packs.values())),
            'station_count': len(pool.chargers), 'offline_channels': sum(pool.chargers.values()),
            'identity_and_inventory_passed': True}
    events, committed = [], []
    journal_min_sep, journal_min_soc, journal_max_soc = math.inf, math.inf, -math.inf
    with sqlite3.connect((output/'journal.sqlite').as_uri()+'?mode=ro', uri=True) as db:
        db.execute('PRAGMA query_only=ON')
        for seq, checksum, raw in db.execute('SELECT seq,checksum,batch FROM commits WHERE run_id=? ORDER BY seq', (cp['run_id'],)):
            b = json.loads(zlib.decompress(raw))
            if b['seq'] != seq or seq != len(committed)+1 or digest(b) != checksum:
                raise AssertionError('journal sequence/checksum mismatch')
            spatial = spatial_snapshot(b['robots'], scene)
            journal_min_sep = min(journal_min_sep, spatial['min_pair_separation_m'])
            journal_min_soc = min(journal_min_soc, spatial['minimum_soc'])
            journal_max_soc = max(journal_max_soc, spatial['maximum_soc'])
            committed.append(seq)
            events.extend(b['events'])
    if len(committed) != cp['knowledge_seq'] or [e['event_seq'] for e in events] != list(range(1, cp['event_sequence']+1)):
        raise AssertionError('journal/event archive incomplete')
    event_counts = Counter(e['kind'] for e in events)
    dangerous = {k: n for k, n in event_counts.items() if any(x in k for x in ('unresolved', 'collision', 'deadlock'))}
    if dangerous:
        raise AssertionError('recorded unresolved/safety events: '+str(dangerous))
    task_racks = {slots[t[k]]['rack_id'] for t in raw_tasks for k in ('source_id', 'target_id') if t[k] in slots}
    all_racks = {r['id'] for r in scene['racks']}
    frames_result = None
    if (output/'frames.sqlite').exists():
        frames_count, frame_min_sep, first, last = 0, math.inf, None, None
        gap = 0
        with sqlite3.connect((output/'frames.sqlite').as_uri()+'?mode=ro', uri=True) as db:
            db.execute('PRAGMA query_only=ON')
            for second, raw in db.execute('SELECT second,payload FROM frames ORDER BY second'):
                frame = json.loads(zlib.decompress(raw))
                spatial = spatial_snapshot(frame['robots'], scene)
                frame_min_sep = min(frame_min_sep, spatial['min_pair_separation_m'])
                if first is None:
                    first = second
                if last is not None:
                    gap = max(gap, second-last)
                last, frames_count = second, frames_count+1
        frames_result = {'count': frames_count, 'first_second': first, 'last_second': last,
            'maximum_gap_seconds': gap, 'sampled_min_separation_m': frame_min_sep,
            'all_sampled_positions_passed': True, 'continuous_between_samples_independently_verified': False}
    after = {n: sha(p) for n, p in paths.items()}
    if before != after:
        raise AssertionError('run artifacts changed during read-only audit')
    return {'name': case['name'], 'status': result['status'], 'accepted_final_state_audit': True,
        'all_input_completed': True, 'completed': inventory['completed'], 'late': cp['metrics']['late'],
        'flow_seconds': result['flow_seconds'],
        'completed_task_slack_seconds': {
            'minimum': min(t['due_second']-t['completed_at'] for t in cp['tasks'].values()),
            'maximum': max(t['due_second']-t['completed_at'] for t in cp['tasks'].values())},
        'elapsed': cp['elapsed'], 'inventory': inventory, 'battery_pool': pool_result,
        'residual_physical_jobs': {'count': len(cp['jobs']),
            'unfinished_swap_jobs': sum(bool(j.get('swap_pack_id')) for j in cp['jobs'].values()),
            'all_robots_parked_and_replenished_claim': False},
        'input_sha256': inputs, 'frozen_backend_sha256': source_sha, 'raw_sha256': before,
        'actual_config_file_sha256': sha(config_file) if config_file.exists() else None,
        'frozen_entrypoint_sha256': {str(p.relative_to(source)): sha(p) for p in (
            source/'ops/run_capacity_matrix.py', source/'backend/wheeled/runner.py',
            source/'backend/warehouse/__main__.py', source/'backend/warehouse/cli.py',
            source/'backend/real_scale/runner.py', source/'backend/scheduling_v3/runner.py') if p.exists()},
        'coverage': {'task_racks': len(task_racks), 'all_racks': len(all_racks),
            'all_racks_referenced_by_tasks': task_racks == all_racks,
            'plan_coverage_claim': case.get('new_racks_coverage_claim')},
        'final_positions_and_soc': final_spatial,
        'journal_snapshot_audit': {'commits': len(committed), 'events': len(events),
            'minimum_pair_separation_m': journal_min_sep, 'minimum_soc': journal_min_soc, 'maximum_soc': journal_max_soc},
        'event_counts': dict(event_counts), 'frames': frames_result,
        'runtime_reported_swept_audit': {k: cp['metrics'].get(k) for k in ('motion_audit_pairs', 'min_separation_m',
            'max_inside', 'min_inside', 'min_soc', 'progress_warnings', 'recovery_rejections')},
        'continuous_trajectory_audited_independently': False,
        'runner_chain_metadata': 'knowledge-runtime/real-scale' if 'knowledge_runtime' in cp else 'standalone-V5/V3-journal',
        'runner_confound': 'v1 cross-case runner differences require separate attribution; physical outcomes are audited, wall time is not a pure planning ablation'}


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--matrix', type=Path, required=True)
    p.add_argument('--source', type=Path, required=True)
    p.add_argument('--plan-json', help='explicit audit-only case definitions when no original plan artifact exists')
    p.add_argument('--output', type=Path)
    p.add_argument('--allow-pending', action='store_true',
                   help='report-only mode: exit 0 even if some cases have no final results (audit errors still exit 1)')
    args = p.parse_args(argv)
    sys.path.insert(0, str(args.source.resolve()))
    plan = json.loads(args.plan_json) if args.plan_json else read_json(args.matrix/'plan.json')
    reports = []
    for case in plan:
        try:
            reports.append(audit_case(case, args.matrix.resolve(), args.source.resolve()))
        except Exception as exc:
            reports.append({'name': case['name'], 'status': 'audit-failed',
                'accepted_final_state_audit': False, 'error': f'{type(exc).__name__}: {exc}'})
    result = {'scope': 'read-only completed capacity cases; sparse snapshots are not full continuous trajectory proof',
        'matrix': str(args.matrix), 'source': str(args.source),
        'plan_sha256': sha(args.matrix/'plan.json') if not args.plan_json else None,
        'audit_case_definition_sha256': hashlib.sha256(json.dumps(plan, sort_keys=True).encode()).hexdigest(),
        'plan_provenance': 'explicit audit-only definitions; not a recorded run command' if args.plan_json else 'original frozen plan.json',
        'cases': reports}
    raw = json.dumps(result, ensure_ascii=False, indent=2, allow_nan=False)+'\n'
    if args.output:
        with args.output.open('x', encoding='utf-8') as out:
            out.write(raw)
    print(raw)
    # Exit codes: 1 = an audit failed; 2 = some case is incomplete/pending or the
    # plan is empty; 0 = every case passed. --allow-pending only relaxes code 2.
    if any(r['status'] == 'audit-failed' for r in reports):
        return 1
    incomplete = not reports or any(r.get('accepted_final_state_audit') is not True for r in reports)
    return 2 if incomplete and not args.allow_pending else 0


if __name__ == '__main__':
    raise SystemExit(main())
