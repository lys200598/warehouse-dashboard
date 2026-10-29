"""Reproducible opt-in layout/transport ablation over an unchanged peak input.

Creates a new owned directory; preserves deadlines and both original CSVs.
Coverage-remapped inputs are labelled separately from byte-identical controls.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--output', type=Path, required=True)
    p.add_argument('--dataset', type=Path, default=ROOT/'data/examples/regression/peak')
    p.add_argument('--old-scene', type=Path, default=ROOT/'data/layouts/warehouse100-wide-v2.json')
    p.add_argument('--workers', type=int, choices=(1, 2), default=2)
    p.add_argument('--wall-seconds', type=int, default=1800)
    args = p.parse_args(argv)
    if args.output.exists():
        p.error('output exists: preserve previous attempts')
    if args.wall_seconds < 1:
        p.error('positive wall budget required')
    args.output.mkdir(parents=True)
    from ops.remap_capacity_coverage import remap
    from ops.convert_logical_boundaries import convert
    from backend.wide.data import migrate_dataset
    source = {n: sha(args.dataset/n) for n in ('tasks.csv', 'initial_inventory.csv', 'manifest.json')}
    base = json.loads((ROOT/'config/free-ports-balanced-100.json').read_text())
    prepared = {}
    for layout in ('balanced', 'medium', 'large'):
        scene = ROOT/f'data/layouts/expansion-candidates/{layout}/layout.json'
        preserve = args.output/f'input-{layout}-preserve'
        migrate_dataset(args.old_scene, scene, args.dataset, preserve)
        coverage = args.output/f'input-{layout}-coverage'
        coverage_report = remap(args.old_scene, args.dataset, scene, coverage, seed=20261029)
        for kind, dataset in (('preserve', preserve), ('coverage', coverage)):
            logical = args.output/f'input-{layout}-{kind}-logical'
            convert(scene, dataset, logical)
            prepared[layout, kind] = {'dataset':str(logical.resolve()),
                'coverage':coverage_report['coverage'] if kind=='coverage' else None}
    cases = [('balanced-old', 'balanced', 'preserve', .8, 'legacy', None),
             ('balanced-wheel', 'balanced', 'preserve', 1.2, 'charge-only', None),
             ('medium-wheel-preserve', 'medium', 'preserve', 1.2, 'charge-only', None),
             ('medium-wheel-cover', 'medium', 'coverage', 1.2, 'charge-only', None),
             ('large-wheel-cover', 'large', 'coverage', 1.2, 'charge-only', None),
             ('medium-swap-cover', 'medium', 'coverage', 1.2, 'swap-first', 4),
             ('large-swap-cover', 'large', 'coverage', 1.2, 'swap-first', 4)]
    plan = []
    for name, layout, kind, speed, policy, channels in cases:
        cfg = {**base, 'speed': speed}
        if policy != 'legacy':
            cfg.update(replenishment_policy=policy)
            if channels is not None:
                cfg['swap_offline_chargers_per_station'] = channels
        path = args.output/f'{name}-config.json'
        path.write_text(json.dumps(cfg, indent=2)+'\n')
        plan.append({'name': name, 'layout': layout, 'input_kind': kind,
            'dataset': prepared[layout, kind]['dataset'], 'scene': str(ROOT/f'data/layouts/expansion-candidates/{layout}/layout.json'),
            'config': str(path.resolve()), 'speed_mps': speed, 'replenishment': policy,
            'same_original_task_times': True, 'deadline_changes': 0,
            'source_csv_sha256': source, 'coverage':prepared[layout,kind]['coverage'],
            'new_racks_coverage_claim':bool(prepared[layout,kind]['coverage'] and prepared[layout,kind]['coverage']['task_rack_coverage_complete'])})
    (args.output/'plan.json').write_text(json.dumps(plan, indent=2)+'\n')

    def execute(case):
        output = args.output/case['name']
        # Keep persistence and canonical state/vector generation identical.
        cmd = [sys.executable, '-m', 'backend.warehouse', 'run', '--mode', 'rules',
               '--mobility','legacy' if case['replenishment']=='legacy' else 'wheeled']
        cmd += ['--scene', case['scene'], '--dataset', case['dataset'], '--config-json', case['config'],
                '--output', str(output), '--run-id', 'capacity-'+case['name'], '--knowledge-backend', 'journal',
                '--commit-seconds', '300', '--frames', '--max-wall-seconds', str(args.wall_seconds),
                '--drain-seconds', '14400']
        started = time.time()
        with (args.output/f'{case["name"]}.log').open('x') as log:
            try:
                process = subprocess.run(cmd, cwd=ROOT, stdout=log, stderr=subprocess.STDOUT,
                    timeout=args.wall_seconds+180, env={**os.environ, 'OMP_NUM_THREADS':'1', 'OPENBLAS_NUM_THREADS':'1'})
                code = process.returncode
            except subprocess.TimeoutExpired:
                code = 124
        payload,read_error=None,None
        try:
            if (output/'results.json').exists():payload=json.loads((output/'results.json').read_text())
        except (OSError,ValueError) as exc:read_error=type(exc).__name__
        result = {'name': case['name'], 'exit': code, 'wall_seconds': time.time()-started,
                  'result':payload,'result_read_error':read_error}
        (args.output/f'{case["name"]}.exit').write_text(str(code)+'\n')
        return result

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        results = list(pool.map(execute, plan))
    assert source == {n: sha(args.dataset/n) for n in source}, 'source input changed'
    (args.output/'results-matrix.json').write_text(json.dumps(results, indent=2)+'\n')
    print(json.dumps([{'name': r['name'], 'exit': r['exit'], 'status': (r['result'] or {}).get('status'),
          'completed': (r['result'] or {}).get('metrics', {}).get('completed'),
          'late': (r['result'] or {}).get('metrics', {}).get('late')} for r in results]))
    return int(any(r['exit'] != 0 for r in results))


if __name__ == '__main__':
    raise SystemExit(main())
