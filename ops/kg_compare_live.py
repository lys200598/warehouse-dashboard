"""Reconcile persisted checkpoints when transport cleanup interrupted the live harness."""
import argparse
import hashlib
import json
from pathlib import Path

from backend.checkpoint import read_checkpoint
from ops.kg_live_check import normalized

parser = argparse.ArgumentParser()
parser.add_argument('output', type=Path)
args = parser.parse_args()
states, files = {}, {}
for mode in ('neo4j', 'memory'):
    paths = list((args.output / mode).glob('wz-test-*/checkpoint.json'))
    if len(paths) != 1:
        raise ValueError(f'expected one {mode} checkpoint, found {len(paths)}')
    payload = read_checkpoint(paths[0])
    states[mode] = normalized(payload, payload['run_id'])
    files[mode] = str(paths[0])
differences = [key for key in states['neo4j'] if states['neo4j'].get(key) != states['memory'].get(key)]
result = {'tick': states['neo4j']['tick'], 'checkpoint_equal': not differences,
          'different_top_level_keys': differences, 'checkpoints': files,
          'excluded': ['run ID identity', 'kg_state_source', 'worker_pid', 'measured wall times'],
          'normalized_sha256': hashlib.sha256(json.dumps(states['neo4j'], sort_keys=True).encode()).hexdigest()}
(args.output / 'checkpoint-comparison.json').write_text(json.dumps(result, indent=2) + '\n')
print(json.dumps(result))
raise SystemExit(0 if result['checkpoint_equal'] else 1)
