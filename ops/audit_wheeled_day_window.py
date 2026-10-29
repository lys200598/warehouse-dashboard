"""Read-only day WINDOW audit, not a claim that all released work drained.

Future input tasks may remain scheduled. Checks committed inventory histories,
final pack identity and sparse spatial samples without restarting the producer.
"""
from __future__ import annotations
import argparse
from collections import Counter
from copy import deepcopy
import hashlib
import json
from pathlib import Path
import sqlite3
import sys
from types import SimpleNamespace
import zlib


def json_read(path):
    return json.loads(Path(path).read_text(encoding='utf-8'))


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def classify(tasks, at):
    released = [t for t in tasks if t['release_second'] < at]
    completed = [t for t in released if t['status'] == 'completed']
    unfinished = [t for t in released if t['status'] != 'completed']
    overdue = [t for t in unfinished if t['due_second'] < at]
    return {'released': len(released), 'completed': len(completed),
        'completed_late': sum(t['completed_at'] > t['due_second'] for t in completed),
        'unfinished': len(unfinished), 'unfinished_overdue': len(overdue),
        'unfinished_not_yet_due': len(unfinished)-len(overdue),
        'unfinished_due_at_boundary': sum(t['due_second'] == at for t in unfinished),
        'future_or_boundary_release': len(tasks)-len(released)}


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__)
    for key in ('source', 'run', 'scene', 'dataset', 'exit-file'):
        p.add_argument('--'+key, type=Path, required=True)
    p.add_argument('--output', type=Path)
    args = p.parse_args(argv)
    sys.path.insert(0, str(args.source.resolve()))
    from backend.checkpoint import read_checkpoint
    from backend.knowledge import digest
    from backend.wide.data import read_dataset
    from backend.scheduling_v5.invariants import assert_warehouse_inventory
    from backend.scheduling_v3.energy import EnergyProfile
    from backend.wheeled.batteries import BatteryPool
    from ops.audit_wheeled_capacity import spatial_snapshot
    files = {n: args.run/n for n in ('results.json', 'checkpoint.json', 'journal.sqlite', 'frames.sqlite')}
    before = {n: sha(path) for n, path in files.items()}
    result, cp = json_read(files['results.json']), read_checkpoint(files['checkpoint.json'])
    if args.exit_file.read_text().strip() != '0' or result['status'] != 'complete':
        raise AssertionError('producer did not exit naturally with complete window status')
    if cp['elapsed'] != result['elapsed'] or cp['elapsed'] != 86400:
        raise AssertionError('not a completed 24-hour window')
    config_path = args.source/'config/wheeled-medium-100.json'
    configured = json_read(config_path)
    if result['config'] != cp['config'] or any(cp['config'].get(k) != v for k, v in configured.items()):
        raise AssertionError('frozen medium configuration differs from actual runtime')
    manifest, all_tasks, initial = read_dataset(args.scene, args.dataset)
    actual = {'scene_sha256': sha(args.scene), **{n.replace('.csv', '').replace('.json', '')+'_sha256': sha(args.dataset/n)
        for n in ('tasks.csv', 'initial_inventory.csv', 'manifest.json')}}
    if any(result['input_identity'][k] != v or cp['run_metadata'][k] != v for k, v in actual.items()):
        raise AssertionError('run/source input identity mismatch')
    backend_files = {x.relative_to(args.source/'backend').as_posix(): sha(x)
        for x in sorted((args.source/'backend').rglob('*.py'))}
    backend_sha = hashlib.sha256(json.dumps(backend_files, sort_keys=True).encode()).hexdigest()
    if backend_sha != cp['run_metadata']['code_sha256']:
        raise AssertionError('frozen backend source changed')
    live_tasks = {t['task_id']: t for t in all_tasks}
    live_tasks.update(cp['tasks'])
    counts = classify(list(live_tasks.values()), cp['elapsed'])
    if counts['released'] != result['released'] or counts['completed'] != cp['metrics']['completed']:
        raise AssertionError('released/completed count disagrees')
    if counts['completed_late'] != cp['metrics']['late'] or counts['unfinished_overdue'] != result['late_unfinished']:
        raise AssertionError('late/unfinished-overdue count disagrees')
    scene = json_read(args.scene)
    final_slots = {s['slot_id']: s for rack in cp['racks'] for s in rack['slots']}
    final_robots = {r['state']['robot_id']: r['state'] for r in cp['robots']}
    final_sim = SimpleNamespace(scene=scene, initial=initial, slots=final_slots,
        robots=final_robots, tasks=cp['tasks'], goods=cp['goods'], inside=cp['inside'], ports=cp['ports'], metrics=cp['metrics'])
    inventory = assert_warehouse_inventory(final_sim)
    final_spatial = spatial_snapshot(list(final_robots.values()), scene)
    b = cp['wheeled_batteries']
    profile = {'transport': EnergyProfile, 'legacy': EnergyProfile.legacy,
               'conservative': EnergyProfile.conservative}[cp['config']['energy_profile']]()
    pool = BatteryPool(range(1, 101), list(b['chargers']), cp['config']['swap_spares_per_station'],
                       cp['config']['swap_offline_chargers_per_station'], profile)
    pool.restore(b)
    if b['updated_at'] != cp['elapsed']:
        raise AssertionError('pool clock not at window boundary')
    states, task_versions, slot_versions, events = {}, {}, {}, []
    goods = {r['goods_id']: r['location_id'] for r in initial}
    totals = Counter(received=0, shipped=0)
    commits = 0
    with sqlite3.connect(files['journal.sqlite'].as_uri()+'?mode=ro', uri=True) as db:
        db.execute('PRAGMA query_only=ON')
        for seq, checksum, raw in db.execute('SELECT seq,checksum,batch FROM commits WHERE run_id=? ORDER BY seq', (cp['run_id'],)):
            batch = json.loads(zlib.decompress(raw))
            if seq != commits+1 or batch['seq'] != seq or digest(batch) != checksum:
                raise AssertionError('journal sequence/checksum mismatch')
            commits = seq
            states = {r['robot_id']: r for r in batch['robots']}
            spatial_snapshot(list(states.values()), scene)
            for t in batch['tasks']:
                task_versions[t['task_id']] = deepcopy(t)
            for s in batch['slots']:
                slot_versions[s['slot_id']] = deepcopy(s)
            for e in batch['events']:
                events.append(e)
                totals[e['kind']] += 1
                if 'goods_id' in e and 'location_id' in e:
                    goods[e['goods_id']] = e['location_id']
            metrics = {'received': totals['received'], 'shipped': totals['shipped'],
                'completed': sum(t['status'] == 'completed' for t in task_versions.values())}
            ports = Counter(holder for holder in goods.values() if holder.startswith('IN-'))
            snapshot = SimpleNamespace(scene=scene, initial=initial, slots=slot_versions,
                robots=states, tasks=task_versions, goods=goods, inside=len(initial)+totals['received']-totals['shipped'],
                ports=ports, metrics=metrics)
            assert_warehouse_inventory(snapshot)
    if commits != cp['knowledge_seq'] or [e['event_seq'] for e in events] != list(range(1, cp['event_sequence']+1)):
        raise AssertionError('journal/event archive incomplete')
    if task_versions != cp['tasks'] or slot_versions != final_slots or goods != cp['goods']:
        raise AssertionError('journal lifecycle replay differs from final checkpoint')
    installed = {i: f'installed-{i}' for i in range(1, 101)}
    reserved = {}
    for e in events:
        owner = e.get('robot_id')
        if e['kind'] == 'battery_swap_reserved':
            if e['pack_id'] in reserved:
                raise AssertionError('pack double reserved')
            reserved[e['pack_id']] = owner
        elif e['kind'] == 'battery_swap_cancelled':
            if reserved.pop(e['pack_id']) != owner:
                raise AssertionError('battery cancellation mismatch')
        elif e['kind'] == 'battery_swap_completed':
            if reserved.pop(e['installed_pack']) != owner or installed[owner] != e['removed_pack']:
                raise AssertionError('battery exchange identity mismatch')
            installed[owner] = e['installed_pack']
    if {str(k): v for k, v in installed.items()} != b['installed']:
        raise AssertionError('exchange event history differs from final installed pack map')
    actual_reserved = {key: v['owner'] for key, v in b['packs'].items() if v['status'] == 'reserved'}
    if reserved != actual_reserved:
        raise AssertionError('event reservations differ from final inventory')
    for token, owner in reserved.items():
        job = cp['jobs'].get(str(owner), cp['jobs'].get(owner, {}))
        if job.get('swap_pack_id') != token or not any(p.get('marker') == 'swap_complete' for p in job['parts']):
            raise AssertionError('pending reserved battery lacks actual swap plan')
    frames, first, last, gap, minimum = 0, None, None, 0, float('inf')
    with sqlite3.connect(files['frames.sqlite'].as_uri()+'?mode=ro', uri=True) as db:
        db.execute('PRAGMA query_only=ON')
        for second, raw in db.execute('SELECT second,payload FROM frames ORDER BY second'):
            frame = json.loads(zlib.decompress(raw))
            spatial = spatial_snapshot(frame['robots'], scene)
            minimum = min(minimum, spatial['min_pair_separation_m'])
            if first is None:
                first = second
            if last is not None:
                gap = max(gap, second-last)
            last, frames = second, frames+1
    task_racks = {final_slots[t[k]]['rack_id'] for t in cp['tasks'].values() for k in ('source_id', 'target_id') if t[k] in final_slots}
    event_counts = Counter(e['kind'] for e in events)
    for metric, kind in [('swap_reserved', 'battery_swap_reserved'),
                         ('swap_completed', 'battery_swap_completed'),
                         ('swap_cancelled', 'battery_swap_cancelled')]:
        if cp['metrics'][metric] != event_counts[kind]:
            raise AssertionError('swap event/metric disagreement: '+metric)
    unsafe_events = {k: n for k, n in event_counts.items() if any(x in k for x in ('unresolved', 'collision', 'deadlock'))}
    if unsafe_events:
        raise AssertionError('recorded unsafe/unresolved events '+str(unsafe_events))
    after = {n: sha(path) for n, path in files.items()}
    if before != after:
        raise AssertionError('producer artifacts changed during read-only audit')
    backend_after = {x.relative_to(args.source/'backend').as_posix(): sha(x)
        for x in sorted((args.source/'backend').rglob('*.py'))}
    input_after = {'scene_sha256': sha(args.scene), **{n.replace('.csv', '').replace('.json', '')+'_sha256': sha(args.dataset/n)
        for n in ('tasks.csv', 'initial_inventory.csv', 'manifest.json')}}
    if backend_files != backend_after or actual != input_after:
        raise AssertionError('frozen source or inputs changed during read-only audit')
    report = {'scope': '24-hour window; future tasks remain scheduled; not drain-all or 7day acceptance',
        'status': result['status'], 'elapsed': cp['elapsed'], 'input_tasks_total': len(all_tasks), 'counts': counts,
        'config': cp['config'], 'config_file_sha256': sha(config_path),
        'knowledge_runtime': cp['run_metadata'].get('knowledge_runtime'),
        'released_task_statuses': dict(Counter(t['status'] for t in cp['tasks'].values())),
        'final_inventory': inventory, 'final_spatial': final_spatial,
        'released_task_rack_coverage': {'referenced_racks': len(task_racks), 'warehouse_racks': len(scene['racks'])},
        'battery_inventory': {'physical_ids': len(pool.all_ids), 'installed': len(pool.installed), 'spares': len(pool.packs),
            'status_counts': dict(Counter(p['status'] for p in pool.packs.values())),
            'offline_channels': sum(pool.chargers.values()), 'exchange_history_identity_passed': True,
            'offline_pack_soc_minimum': min((p['soc'] for p in pool.packs.values()
                if p['status'] in {'waiting', 'charging'}), default=None),
            'station_status_counts': {s: dict(Counter(p['status'] for p in pool.packs.values()
                if p['station'] == s)) for s in pool.chargers}},
        'replenishment': {k: cp['metrics'].get(k) for k in ('swap_reserved', 'swap_completed', 'swap_cancelled',
            'swap_fallbacks', 'max_swapping', 'charge_sessions', 'max_charging', 'min_soc')},
        'ending_jobs': {'total': len(cp['jobs']), 'stages': dict(Counter(r['stage'] for r in final_robots.values())),
            'pending_swap_jobs': len(reserved), 'all_robots_parked_replenished_claim': False},
        'event_counts': dict(event_counts), 'journal_commits': commits,
        'committed_inventory_histories_passed': True,
        'frames': {'count': frames, 'first_second': first, 'last_second': last, 'maximum_gap': gap,
            'minimum_sampled_separation_m': minimum, 'sampled_positions_passed': True,
            'independent_continuous_trajectory_audit': False},
        'input_sha256': actual, 'backend_source_sha256': backend_sha, 'raw_sha256': before,
        'runtime_reported_safety_metrics': {k: cp['metrics'].get(k) for k in
            ('min_soc', 'min_inside', 'max_inside', 'motion_audit_pairs', 'min_separation_m',
             'progress_warnings', 'recovery_rejections', 'max_active_business')},
        'audited_source': str(args.source), 'exit_file_sha256': sha(args.exit_file),
        'helpers_sha256': {name: sha(args.source/name) for name in ('ops/audit_wheeled_capacity.py',
            'backend/wheeled/batteries.py', 'backend/wheeled/sim.py', 'backend/scheduling_v5/invariants.py')},
        'hardware_calibrated': False, 'knowledge_graph_acceptance': False}
    raw = json.dumps(report, ensure_ascii=False, indent=2, allow_nan=False)+'\n'
    if args.output:
        with args.output.open('x', encoding='utf-8') as f:
            f.write(raw)
    print(raw)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
