"""Run the existing old/wide adapters without modifying their algorithms.

This is a whole-layout comparison, NOT a controlled aisle-width ablation:
the wide layout has 100 assigned combined bays, the old layout 24 shared
chargers and 100 separate private homes. All other Config fields, input CSV
bytes, deadlines and requested disturbances are held equal and verified.
"""
from __future__ import annotations

import argparse
from dataclasses import asdict
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import sys
import time

from ..day1.data import sha
from ..scheduling_v3.data import validate_dataset
from ..scheduling_v3.sim import Config
from ..wide.data import migrate_dataset
from .metrics import summarize_run, summarize_common_cutoff, compare_runs


def save_new(path, value):
    with Path(path).open('x', encoding='utf-8') as stream:
        json.dump(value, stream, ensure_ascii=False, indent=2, allow_nan=False)
        stream.write('\n')


def source_identity():
    backend = Path(__file__).parents[1]
    files = {p.relative_to(backend).as_posix(): sha(p) for p in sorted(backend.rglob('*.py'))}
    raw = json.dumps(files, sort_keys=True, separators=(',', ':')).encode()
    return {'backend_tree_sha256': hashlib.sha256(raw).hexdigest(), 'files': files}


def prepare(*, old_scene, wide_scene, source_dataset, output, run_prefix,
            config=None, disturbances=None, max_wall_seconds=900,
            knowledge_backend='journal', frames=False, drain_seconds=7200):
    """Preserve source CSV bytes and deadlines, write a new explicit matrix."""
    output = Path(output).resolve()
    if output.exists():
        raise FileExistsError('preserve previous matrix; choose a fresh output directory')
    if max_wall_seconds < 1 or drain_seconds < 1:
        raise ValueError('positive time bounds required')
    if knowledge_backend not in {'journal', 'neo4j'}:
        raise ValueError('unknown knowledge backend')
    if not run_prefix or not all(c.isalnum() or c in '-_' for c in run_prefix):
        raise ValueError('safe unique run prefix required')
    old_scene, wide_scene, source_dataset = map(lambda p: Path(p).resolve(),
                                               (old_scene, wide_scene, source_dataset))
    validation = validate_dataset(old_scene, source_dataset)
    config = dict(config or {})
    old_config = asdict(Config(**config))
    if old_config['chargers'] != 24:
        raise ValueError('this comparison baseline requires the original 24 chargers')
    wide_config = dict(old_config, chargers=100)
    disturbances = disturbances or []
    if not isinstance(disturbances, list):
        raise ValueError('disturbances must be a list')
    output.mkdir(parents=True, exist_ok=False)
    old_input, wide_input = output / 'input-old', output / 'input-wide'
    old_input.mkdir()
    for filename in ('tasks.csv', 'initial_inventory.csv', 'manifest.json'):
        shutil.copyfile(source_dataset / filename, old_input / filename)
    migrate_dataset(old_scene, wide_scene, source_dataset, wide_input,
                    deadline_policy='preserve')
    for filename in ('tasks.csv', 'initial_inventory.csv'):
        if (old_input / filename).read_bytes() != (wide_input / filename).read_bytes():
            raise AssertionError('input CSV bytes changed')
    save_new(output / 'config-old.json', old_config)
    save_new(output / 'config-wide.json', wide_config)
    save_new(output / 'disturbances.json', disturbances)
    identity = source_identity()
    save_new(output / 'source-identity.json', identity)
    cases = []
    for variant, module, scene, dataset in (
        ('old', 'backend.scheduling_v3.runner', old_scene, old_input),
        ('wide', 'backend.wide.runner', wide_scene, wide_input),
    ):
        args = ['-u', '-m', module, '--scene', str(scene), '--dataset', str(dataset),
                '--output', str(output / ('run-' + variant)),
                '--run-id', run_prefix + '-' + variant,
                '--config-json', str(output / ('config-' + variant + '.json')),
                '--disturbances-json', str(output / 'disturbances.json'),
                '--max-wall-seconds', str(max_wall_seconds),
                '--drain-seconds', str(drain_seconds),
                '--knowledge-backend', knowledge_backend]
        if frames:
            args.append('--frames')
        cases.append({'variant': variant, 'args': args, 'run_id': run_prefix + '-' + variant})
    plan = {
        'schema': 'warehouse-layout-pair-v1', 'run_prefix': run_prefix,
        'output': str(output), 'source_dataset': str(source_dataset),
        'old_scene': str(old_scene), 'wide_scene': str(wide_scene),
        'source_validation': validation,
        'tasks_sha256': sha(old_input / 'tasks.csv'),
        'initial_inventory_sha256': sha(old_input / 'initial_inventory.csv'),
        'disturbances_sha256': sha(output / 'disturbances.json'),
        'source_identity_sha256': sha(output / 'source-identity.json'),
        'max_wall_seconds_per_case': max_wall_seconds,
        'drain_seconds': drain_seconds, 'knowledge_backend': knowledge_backend,
        'config_differences': {'chargers': {'old': 24, 'wide': 100}},
        'scope': 'whole-layout-and-physical-bay-model comparison, not pure aisle-width ablation',
        'physical_differences': [
            'old: 24 shared chargers plus 100 private homes; empty private regions excluded',
            'wide: 100 assigned charging/home canonical bays; empty hardware traversable',
            'wide: persistent terminal robot bodies explicitly reserved',
            'rack/port/home coordinates, aisle width and total area differ'],
        'cases': cases,
    }
    save_new(output / 'plan.json', plan)
    return plan


def execute(plan_path):
    """Sequential children avoid cross-case CPU/cache contention; no retries."""
    plan_path = Path(plan_path)
    plan = json.loads(plan_path.read_text(encoding='utf-8'))
    output = Path(plan['output'])
    for path, expected in [(output / 'source-identity.json', plan['source_identity_sha256']),
                           (output / 'disturbances.json', plan['disturbances_sha256'])]:
        if sha(path) != expected:
            raise ValueError('planned evidence changed: ' + str(path))
    if source_identity() != json.loads((output / 'source-identity.json').read_text(encoding='utf-8')):
        raise ValueError('backend changed since preparation')
    # Refuse before starting either case if any result/output/log already exists.
    for case in plan['cases']:
        if (output / ('run-' + case['variant'])).exists() or (output / (case['variant'] + '.log')).exists():
            raise FileExistsError('previous case exists; automatic retry is forbidden')
    records = []
    for case in plan['cases']:
        variant = case['variant']
        for filename, identity in [('tasks.csv', plan['tasks_sha256']),
                                   ('initial_inventory.csv', plan['initial_inventory_sha256'])]:
            if sha(output / ('input-' + variant) / filename) != identity:
                raise ValueError('input changed after preparation')
        print('START', variant, flush=True)
        started = time.monotonic()
        with (output / (variant + '.log')).open('x', encoding='utf-8') as stream:
            try:
                code = subprocess.run([sys.executable] + case['args'], stdout=stream,
                                      stderr=subprocess.STDOUT,
                                      timeout=plan['max_wall_seconds_per_case'] + 120).returncode
            except subprocess.TimeoutExpired:
                code = 124
        record = {'variant': variant, 'exit_code': code,
                  'launcher_wall_seconds': time.monotonic() - started}
        path = output / ('run-' + variant)
        if (path / 'results.json').exists() and (path / 'checkpoint.json').exists():
            record['summary'] = summarize_run(path)
        records.append(record)
        save_new(output / (variant + '-launch.json'), record)
        print('END', variant, code, flush=True)
    missing = [case['variant'] for case in plan['cases'] if not all(
        (output / ('run-' + case['variant']) / filename).exists()
        for filename in ('results.json', 'checkpoint.json'))]
    if missing:
        report = {'schema': 'warehouse-layout-comparison-v1', 'pair_complete': False,
                  'status': 'incomplete-evidence', 'missing_result_or_checkpoint': missing,
                  'launch_records': records, 'plan': plan,
                  'bottleneck_eliminated_generally': False}
        save_new(output / 'comparison.json', report)
    else:
        report = analyze(plan_path)
    return 0 if report['pair_complete'] else 2


def analyze(plan_path, report_path=None):
    plan = json.loads(Path(plan_path).read_text(encoding='utf-8'))
    output = Path(plan['output'])
    old, wide = [summarize_run(output / ('run-' + variant)) for variant in ('old', 'wide')]
    comparison = compare_runs(old, wide)
    cutoff = min(old['elapsed'], wide['elapsed'])
    comparison['common_simulated_cutoff'] = {
        'second': cutoff,
        'old': summarize_common_cutoff(output / 'run-old', cutoff),
        'wide': summarize_common_cutoff(output / 'run-wide', cutoff)}
    comparison['plan'] = plan
    target = Path(report_path) if report_path else output / 'comparison.json'
    save_new(target, comparison)
    return comparison


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    subs = parser.add_subparsers(dest='command', required=True)
    p = subs.add_parser('prepare')
    for key in ('old-scene', 'wide-scene', 'source-dataset', 'output', 'run-prefix'):
        p.add_argument('--' + key, required=True)
    p.add_argument('--config-json')
    p.add_argument('--disturbances-json')
    p.add_argument('--max-wall-seconds', type=int, default=900)
    p.add_argument('--drain-seconds', type=int, default=7200)
    p.add_argument('--knowledge-backend', choices=('journal', 'neo4j'), default='journal')
    p.add_argument('--frames', action='store_true')
    for command in ('run', 'analyze'):
        p = subs.add_parser(command)
        p.add_argument('--plan', required=True)
        if command == 'analyze':
            p.add_argument('--report')
    args = parser.parse_args(argv)
    if args.command == 'prepare':
        values = vars(args).copy()
        values.pop('command')
        for key in ('config', 'disturbances'):
            path = values.pop(key + '_json')
            values[key] = json.loads(Path(path).read_text(encoding='utf-8')) if path else None
        print(json.dumps(prepare(**values), ensure_ascii=False, indent=2))
        return 0
    if args.command == 'run':
        return execute(args.plan)
    print(json.dumps(analyze(args.plan, args.report), ensure_ascii=False, indent=2))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
