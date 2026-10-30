"""Control only the explicitly named owned test runtime."""
import argparse
import json
from urllib.request import Request, urlopen

parser = argparse.ArgumentParser()
parser.add_argument('--run-id', required=True)
parser.add_argument('--speed', type=float, required=True)
args = parser.parse_args()
if not args.run_id.startswith('wz-test-'):
    raise ValueError('expected an owned wz-test- run')
body = {'action': 'speed', 'value': args.speed, 'expected_run_id': args.run_id}
with urlopen(Request('http://127.0.0.1:14590/api/control', data=json.dumps(body).encode(),
                     headers={'Content-Type': 'application/json'}), timeout=60) as response:
    frame = json.load(response)['snapshot']
    print(json.dumps({'run_id': frame['runId'], 'tick': frame['tick'], 'speed': frame['speed']}))
