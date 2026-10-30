"""Stop/restart the writer started by kg_start_writer.sh, and inspect this task's runs."""
import argparse
import json
import os
from pathlib import Path
import signal
import subprocess
import time
from urllib.request import Request, urlopen

parser = argparse.ArgumentParser()
parser.add_argument('action', choices=('restart', 'stop', 'list', 'cleanup'))
parser.add_argument('runs', nargs='*')
args = parser.parse_args()
root = Path('/tmp/wz-kg-evidence')
if args.action in ('restart', 'stop'):
    pid = int((root / 'writer.pid').read_text())
    path = Path(f'/proc/{pid}/cmdline')
    if path.exists():
        cmd = path.read_bytes().decode().split('\0')
        assert 'backend.global_writer_api' in cmd and '14591' in cmd, cmd
        os.kill(pid, signal.SIGTERM)
        deadline = time.monotonic() + 20
        while path.exists() and path.read_bytes() and time.monotonic() < deadline:
            time.sleep(.1)
        if path.exists() and path.read_bytes():
            raise RuntimeError('writer did not stop')
    if args.action == 'restart':
        with (root / 'writer.log').open('a') as out:
            proc = subprocess.Popen(['bash', 'ops/kg_dev_env.sh', 'python', '-m', 'backend.global_writer_api',
                                     '--port', '14591'], stdout=out, stderr=out, stdin=subprocess.DEVNULL,
                                    start_new_session=True)
        (root / 'writer.pid').write_text(str(proc.pid))
        print('writer pid:', proc.pid)
        deadline = time.monotonic() + 30
        while True:
            try:
                with urlopen('http://127.0.0.1:14591/v1/health', timeout=1) as response:
                    assert response.status == 200
                break
            except OSError:
                if proc.poll() is not None or time.monotonic() > deadline:
                    raise
                time.sleep(.1)
else:
    from neo4j import GraphDatabase
    with GraphDatabase.driver(os.environ['WAREHOUSE_NEO4J_URI'],
        auth=(os.environ['WAREHOUSE_NEO4J_USER'], os.environ['WAREHOUSE_NEO4J_PASSWORD'])) as driver:
        with driver.session() as session:
            if args.action == 'list':
                print(json.dumps(session.run('MATCH (r:Run) WHERE r.id STARTS WITH "wz-test-kg-" '
                    'RETURN r.id AS id,r.latest_step AS tick,r.started_at_ms AS began ORDER BY began').data()))
            else:
                for run in args.runs:
                    assert run.startswith('wz-test-')
                    with urlopen(Request('http://127.0.0.1:14591/v1/runs/' + run, method='DELETE',
                        data=json.dumps({'confirm_run_id': run}).encode(),
                        headers={'Content-Type': 'application/json'}), timeout=60) as response:
                        assert response.status == 200
                    remaining = session.run('MATCH (n) WHERE n.run_id=$run OR (n:Run AND n.id=$run) '
                                            'RETURN count(n) AS n', run=run).single()['n']
                    assert remaining == 0
                    print(run, 'remaining=0')
