"""Paired immutable-input regressions. Two single-thread workers; no graph writes."""
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]


def run_mode(mode):
    env = dict(os.environ, PYTHONUNBUFFERED='1', OMP_NUM_THREADS='1',
               OPENBLAS_NUM_THREADS='1', MKL_NUM_THREADS='1')
    for case in ['peak', 'day']:
        output = f'data/regression/{mode}-{case}'
        command = ['timeout', '1900', '/usr/bin/time', '-v', '-o', f'logs/{mode}-{case}.time',
                   'nice', '-n', '10', sys.executable, '-m', 'backend.scheduling_v4.runner',
                   '--scheduler', mode, '--scene', 'data/layouts/warehouse100-wide-v2.json',
                   '--dataset', f'data/inputs/{case}', '--config-json', f'data/inputs/{case}-config.json',
                   '--disturbances-json', f'data/inputs/{case}-disturbances.json',
                   '--output', output, '--run-id', f'wh-v4-{mode}-{case}-20261027',
                   '--frames', '--max-wall-seconds', '1800', '--knowledge-backend', 'journal']
        launch = {'command': command, 'start_unix': time.time(), 'mode': mode, 'case': case}
        (ROOT / f'logs/{mode}-{case}-launch.json').write_text(json.dumps(launch, indent=2))
        with (ROOT / f'logs/{mode}-{case}.log').open('wb') as log:
            result = subprocess.run(command, cwd=ROOT, env=env, stdout=log, stderr=subprocess.STDOUT)
        launch.update(returncode=result.returncode, end_unix=time.time())
        (ROOT / f'logs/{mode}-{case}-launch.json').write_text(json.dumps(launch, indent=2))
        if result.returncode:
            return {'mode': mode, 'failed_case': case, 'returncode': result.returncode}
    return {'mode': mode, 'status': 'complete'}


def main():
    os.chdir(ROOT)
    (ROOT / 'logs').mkdir(exist_ok=True)
    inputs = {}
    for p in sorted((ROOT / 'data/inputs').rglob('*')):
        if p.is_file():
            inputs[p.relative_to(ROOT).as_posix()] = hashlib.sha256(p.read_bytes()).hexdigest()
    (ROOT / 'logs/input-identity.json').write_text(json.dumps(inputs, indent=2))
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(run_mode, ['optimized', 'baseline']))
    (ROOT / 'logs/matrix-status.json').write_text(json.dumps(results, indent=2))
    return 0 if all(r.get('status') == 'complete' for r in results) else 2


if __name__ == '__main__':
    raise SystemExit(main())
