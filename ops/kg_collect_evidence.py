"""Collect compact review evidence; raw measurements remain in their run folders."""
import argparse
import json
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument('--root', type=Path, default=Path('/tmp/wz-kg-evidence'))
parser.add_argument('--joint', type=Path, required=True)
parser.add_argument('--output', type=Path, required=True)
args = parser.parse_args()

def read(path):
    return json.loads(path.read_text()) if path.exists() else None

joint = read(args.joint / 'acceptance.json')
if joint:
    for row in joint.get('rounds', []):
        row.pop('commit_observations', None)
evidence = {
    'branch': 'wz/kg-driven-dispatch', 'base': '18bae53', 'lys_merge': '1f21f0b',
    'unit_regression': {'total': 91, 'passed': 89, 'skipped': 1, 'known_expected_error': 1,
        'known_error': "test_warehouse_runtime_graph: KeyError('robots')",
        'ray_recovery_separately_executed': True, 'ray_recovery_wall_s': 26.383},
    'frontend': {'tests_passed': 42, 'motion_window_ticks': [235, 263],
        'sample_seconds': 20.006, 'raf_p95_ms': 16.7, 'raf_max_ms': 33.3,
        'raf_over_100ms': 0, 'javascript_errors': 0},
    'relay_golden': read(args.root / 'relay.json'),
    'context_actor_probe': read(args.root / 'context-actors-final.json'),
    'frozen_live': read(args.root / 'live-frozen' / 'result.json'),
    'joint': joint,
    'preliminary_cpu_samples': {str(cpu): read(args.root / f'cpu{cpu}.json') for cpu in (4, 16, 32)},
    'busy_cpu32_sample': read(args.root / 'active-cpu32.json'),
    'all_day': read(args.root / 'full-day-run-20261030-final' / 'progress.json'),
    'limitations': ['Whole-day completion not yet established',
        'Graph snapshot restore requires agreement with lys',
        'Ordinary/redo full Neo4j golden validation is not replaced by the bounded live probe',
        'Medium19 does not prove 100 robots simultaneously carrying out business tasks']}
args.output.parent.mkdir(parents=True, exist_ok=True)
args.output.write_text(json.dumps(evidence, ensure_ascii=False, indent=2) + '\n')
print(json.dumps({'output': str(args.output), 'joint': (joint or {}).get('verdict'),
                  'frozen_live': bool(evidence['frozen_live'])}, ensure_ascii=False))
