"""Resize only the designated warehouse frontend Ray cluster."""
import argparse
import os
import subprocess
import time
import signal
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument('--cpus', type=int, choices=(4, 16, 32), required=True)
args = parser.parse_args()
owned = []
for path in Path('/proc').iterdir():
    if not path.name.isdigit():
        continue
    try:
        command = (path / 'cmdline').read_bytes().decode(errors='replace').split('\0')
    except (FileNotFoundError, PermissionError):
        continue
    if any('/tmp/wh-ray-frontend' in value for value in command) and any('/ray/' in value for value in command):
        owned.append((int(path.name), command))
for pid, _ in owned:
    try:
        os.kill(pid, signal.SIGTERM)
    except ProcessLookupError:
        pass
deadline = time.monotonic() + 10
while time.monotonic() < deadline and any(Path(f'/proc/{pid}/cmdline').exists() for pid, _ in owned):
    time.sleep(.1)
for pid, command in owned:
    try:
        current = Path(f'/proc/{pid}/cmdline').read_bytes().decode(errors='replace').split('\0')
        if current == command:
            os.kill(pid, signal.SIGKILL)
    except (FileNotFoundError, ProcessLookupError):
        pass
env = dict(os.environ, RAY_AUTH_MODE='disabled')
subprocess.run(['bash', 'run_in_lade.sh', 'ray', 'start', '--head', '--port=27380',
    '--min-worker-port=27400', '--max-worker-port=27599', '--temp-dir=/tmp/wh-ray-frontend',
    '--num-cpus=' + str(args.cpus), '--include-dashboard=false', '--dashboard-port=19265',
    '--metrics-export-port=0'], check=True, env=env)
print('frontend CPUs:', args.cpus, flush=True)
