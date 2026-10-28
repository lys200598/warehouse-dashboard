"""Read-only deterministic prefix replay against actual saved warehouse frames.

The 804s output is explicitly replay-derived. Original frame samples are used
as anchors; no state is interpolated and the original run is never modified.
"""
from copy import deepcopy
from dataclasses import asdict
import argparse
import hashlib
import itertools
import json
import math
from pathlib import Path
import sqlite3
import sys
import time
import zlib


BUSINESS_STAGES = frozenset({'to_source', 'to_target', 'picking', 'placing',
                             'to_handoff', 'handoff_receive'})
FRAME_FIELDS = ('runId', 'elapsed', 'tick', 'running', 'robots', 'counts', 'inside',
                'pending', 'unreleased', 'occupiedSlots', 'bufferCounts',
                'totalTasks', 'chargerCount', 'reservedChargers', 'specVersion',
                'physicalBayCount', 'scheduler')


def sha(path):
    h = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            h.update(block)
    return h.hexdigest()


def canonical(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False,
                      separators=(',', ':'), allow_nan=False).encode()


def project_frame(frame):
    """All robot fields and explicit physical frame fields, with no tolerance."""
    result = {name: deepcopy(frame[name]) for name in FRAME_FIELDS}
    result['robots'].sort(key=lambda row: row['robot_id'])
    ids = [r['robot_id'] for r in result['robots']]
    if ids != list(range(1, 101)):
        raise AssertionError('frame must contain all 100 distinct robot IDs')
    return result


def compare_sample(actual, replayed):
    a, b = project_frame(actual), project_frame(replayed)
    if a != b:
        different = [key for key in a if a[key] != b[key]]
        raise AssertionError(f'actual frame mismatch at {a["elapsed"]}: {different}')
    return hashlib.sha256(canonical(a)).hexdigest()


def active_business(robots):
    return sum(r['stage'] in BUSINESS_STAGES for r in robots)


def swept_distance(a0, a1, b0, b1):
    """Independent relative-segment closest approach on t in [0,1]."""
    dx, dy = a0['x'] - b0['x'], a0['y'] - b0['y']
    vx = a1['x'] - a0['x'] - b1['x'] + b0['x']
    vy = a1['y'] - a0['y'] - b1['y'] + b0['y']
    vv = vx * vx + vy * vy
    t = max(0., min(1., -(dx * vx + dy * vy) / vv)) if vv else 0.
    return math.hypot(dx + t * vx, dy + t * vy)


def audit_motion(before, after, clearance=1.1):
    if set(before) != set(after) or len(before) != 100:
        raise AssertionError('motion requires exactly the same 100 robots')
    best = (math.inf, None)
    pairs = 0
    for left, right in itertools.combinations(sorted(before), 2):
        distance = swept_distance(before[left], after[left], before[right], after[right])
        pairs += 1
        if distance < best[0]:
            best = distance, [left, right]
        if distance < clearance - 1e-7:
            raise AssertionError(f'unsafe swept separation: {left}/{right}: {distance}')
    return {'checked_pairs': pairs, 'min_separation_m': best[0],
            'closest_pair': best[1], 'required_separation_m': clearance}


def replay(args):
    # Prefer the explicitly selected PYTHONPATH frozen tree. A regular clone
    # can still run the tool directly; no production module is patched.
    try:
        from backend.knowledge_runtime.sim import KnowledgeSimulation
    except ModuleNotFoundError:
        sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
        from backend.knowledge_runtime.sim import KnowledgeSimulation
    from backend.real_scale.writer import Config
    from backend.scheduling_v3.layout import audit_roadmap
    from backend.scheduling_v5.invariants import assert_warehouse_inventory
    import backend

    if args.second < 1 or args.commit_seconds < 1:
        raise ValueError('positive replay second/cadence required')
    target = Path(args.output)
    if target.exists():
        raise FileExistsError(target)
    target.parent.mkdir(parents=True, exist_ok=True)
    frames = Path(args.frames_sqlite).resolve()
    results_path = frames.parent / 'results.json'
    original = json.loads(results_path.read_text(encoding='utf-8'))
    if original['status'] != 'complete' or original['run_id'] != args.run_id:
        raise ValueError('select the completed actual run with the same run ID')
    if original['disturbances'] or original['scheduler']['knowledge_runtime']['stats']['queries']:
        raise ValueError('this replay profile requires the actual no-disturbance/no-history-query run')
    config_data = json.loads(Path(args.config_json).read_text(encoding='utf-8'))
    config = Config(**config_data)
    if asdict(config) != original['config']:
        raise ValueError('config differs from original actual run')
    if original['persistence']['complete_journal_and_graph_cadence_seconds'] != args.commit_seconds:
        raise ValueError('cadence differs from original actual run')
    before_hash = sha(frames)
    start = time.perf_counter()
    sim = KnowledgeSimulation(args.scene, args.dataset, args.run_id, config,
                              commit_seconds=args.commit_seconds, knowledge_mode='required')
    if sim.run_metadata['execution_code_sha256'] != original['code_identity']:
        raise ValueError('backend source/mode identity differs from original run')
    for name, expected in original['input_identity'].items():
        if sim.run_metadata[name] != expected:
            raise ValueError(f'actual input identity differs: {name}')
    layout = audit_roadmap(sim.roadmap, circle_probes=False)
    if not layout['initial_roadmap_usable']:
        raise AssertionError('original layout safety gate failed')
    sim.running = True
    sim.knowledge_seq += 1  # original initial persist, before frame at t=0
    sim.pending_events = []
    last_commit = 0
    expected_seconds = set(range(0, args.second + 1, 10))
    with sqlite3.connect(f'{frames.as_uri()}?mode=ro', uri=True) as db:
        saved = {int(s): json.loads(zlib.decompress(b)) for s, b in db.execute(
            'SELECT second,payload FROM frames WHERE second<=? ORDER BY second', (args.second,))}
    if set(saved) != expected_seconds:
        raise AssertionError('actual sampled prefix lacks expected 10-second anchors')
    checks = []

    def check_sample():
        if sim.elapsed in saved:
            checks.append({'second': sim.elapsed,
                           'physical_frame_sha256': compare_sample(saved[sim.elapsed], sim.frame()),
                           'robots_compared': len(sim.robots)})

    check_sample()
    minimum = math.inf
    all_pairs = 0
    target_motion = None
    previous_frame = None
    while sim.elapsed < args.second:
        previous = deepcopy(sim.robots)
        if sim.knowledge_boundary_due():
            raise AssertionError('unexpected history decision boundary in no-disturbance replay')
        sim.step()
        if sim.elapsed - last_commit >= args.commit_seconds:
            sim.knowledge_seq += 1
            sim.pending_events = []
            last_commit = sim.elapsed
        motion = audit_motion(previous, sim.robots)
        minimum = min(minimum, motion['min_separation_m'])
        all_pairs += motion['checked_pairs']
        if sim.elapsed == args.second:
            previous_frame = {'elapsed': sim.elapsed - 1, 'robots': list(previous.values())}
            target_motion = motion
        check_sample()
    inventory = assert_warehouse_inventory(sim)
    current = sim.frame()
    business = active_business(current['robots'])
    if business != 100:
        raise AssertionError(f'selected second has {business} actual business-active robots, expected 100')
    if sim.knowledge_stats['queries'] != 0:
        raise AssertionError('replay unexpectedly used history queries')
    if sha(frames) != before_hash:
        raise AssertionError('original frame database changed during read-only replay')
    report = {'version': 'warehouse-hundred-active-prefix-replay-v1', 'passed': True,
              'run_id': args.run_id, 'source_kind': 'deterministic_replay_anchored_to_actual_saved_frames',
              'original_saved_target_second': args.second in saved,
              'interpolation_used': False, 'target_second': args.second,
              'source_root': str(Path(backend.__file__).resolve().parents[1]),
              'source_identity': original['code_identity'],
              'real_identity': sim.real_identity,
              'input_identity': original['input_identity'],
              'files': {'actual_frames_sqlite': {'path': str(frames), 'sha256': before_hash},
                        'actual_results': {'path': str(results_path), 'sha256': sha(results_path)},
                        'config': {'path': str(Path(args.config_json).resolve()), 'sha256': sha(args.config_json)},
                        'replay_script': {'path': str(Path(__file__).resolve()), 'sha256': sha(__file__)}},
              'sample_comparisons': checks, 'sample_count': len(checks),
              'robot_sample_comparisons': len(checks) * 100,
              'frame_projection': list(FRAME_FIELDS),
              'excluded_frame_fields': ['backend', 'metrics', 'knowledge', 'reservationChecks'],
              'all_robot_fields_compared_exactly': True,
              'business_stages': sorted(BUSINESS_STAGES), 'business_active_robots': business,
              'stage_counts': {s: sum(r['stage'] == s for r in current['robots'])
                               for s in sorted({r['stage'] for r in current['robots']})},
              'last_step_motion': target_motion,
              'all_prefix_steps_motion': {'steps': args.second, 'checked_pairs': all_pairs,
                                          'min_separation_m': minimum, 'required_separation_m': 1.1},
              'inventory': inventory, 'previous_replay_frame': previous_frame,
              'target_replay_frame': project_frame(current),
              'elapsed_wall_seconds': time.perf_counter() - start}
    with target.open('x', encoding='utf-8', newline='\n') as stream:
        stream.write(json.dumps(report, ensure_ascii=False, indent=2, allow_nan=False) + '\n')
    return report


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__)
    for name in ('scene', 'dataset', 'config-json', 'run-id', 'frames-sqlite', 'output'):
        p.add_argument('--' + name, required=True)
    p.add_argument('--second', type=int, default=804)
    p.add_argument('--commit-seconds', type=int, default=300)
    result = replay(p.parse_args(argv))
    print(json.dumps({k: result[k] for k in ('passed', 'target_second', 'sample_count',
                                           'business_active_robots', 'last_step_motion',
                                           'elapsed_wall_seconds')}))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
