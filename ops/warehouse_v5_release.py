"""Frozen selected warehouse policy; defaults affect new runs only."""
import argparse
import json
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from backend.scheduling_v5.sim import execution_identity


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command', choices=['status', 'run'])
    parser.add_argument('args', nargs=argparse.REMAINDER)
    args = parser.parse_args()
    choice = json.loads((ROOT/'config/v5-selected.json').read_text(encoding='utf-8'))
    actual = json.loads((ROOT/'config/v5-policy-parameters.json').read_text(encoding='utf-8'))
    if actual != choice['parameters']:
        raise ValueError('selected policy and parameter file disagree; refusing ambiguous launch')
    identity = execution_identity(choice['policy'], choice['parameters'], choice['planner'])
    if args.command == 'status':
        print(json.dumps({'choice': choice, 'identity': identity, 'existing_runs_changed': False}))
        return 0
    rest = args.args[1:] if args.args[:1] == ['--'] else args.args
    if any(v.split('=')[0] in {'--policy', '--planner', '--params-json', '--policy-json'} for v in rest):
        parser.error('selected launcher has one mode source; use the explicit runner for experiments')
    return subprocess.call([sys.executable, '-m', 'backend.scheduling_v5.runner',
        '--policy', choice['policy'], '--planner', choice['planner'],
        '--params-json', str(ROOT/'config/v5-policy-parameters.json'), *rest], cwd=ROOT)


if __name__ == '__main__':
    raise SystemExit(main())
