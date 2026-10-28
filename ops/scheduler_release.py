"""Select a default for NEW runs only; never convert an existing checkpoint."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from backend.scheduling_v4.sim import execution_identity

CONFIG = ROOT / 'config' / 'scheduler-mode.json'


def main():
    p = argparse.ArgumentParser(description=__doc__)
    sub = p.add_subparsers(dest='command', required=True)
    sub.add_parser('status')
    select = sub.add_parser('select')
    select.add_argument('mode', choices=['optimized', 'baseline'])
    run = sub.add_parser('run')
    run.add_argument('args', nargs=argparse.REMAINDER)
    args = p.parse_args()
    if args.command == 'select':
        CONFIG.parent.mkdir(parents=True, exist_ok=True)
        temporary = CONFIG.with_suffix('.tmp')
        with temporary.open('w', encoding='utf-8') as f:
            json.dump({'mode': args.mode}, f)
            f.write('\n')
            f.flush()
            os.fsync(f.fileno())
        temporary.replace(CONFIG)
    mode = json.loads(CONFIG.read_text(encoding='utf-8'))['mode'] if CONFIG.exists() else 'optimized'
    identity = execution_identity(mode)
    if args.command == 'run':
        remainder = args.args[1:] if args.args[:1] == ['--'] else args.args
        if any(v == '--scheduler' or v.startswith('--scheduler=') for v in remainder):
            p.error('use select or the explicit backend.scheduling_v4.runner CLI, not two mode sources')
        return subprocess.call([sys.executable, '-m', 'backend.scheduling_v4.runner',
                                '--scheduler', mode, *remainder], cwd=ROOT)
    print(json.dumps({'default_for_new_runs': mode, 'identity': identity,
                     'existing_runs_changed': False}, ensure_ascii=False))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
