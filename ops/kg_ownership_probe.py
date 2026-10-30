"""Exercise the service writer's ownership Cypher against the test database."""
import json
import os
from urllib.request import Request, urlopen
from backend.global_writer import GlobalWriter

run_id = 'wz-test-kg-ownership-probe'
writer = GlobalWriter(uri=os.environ['WAREHOUSE_NEO4J_URI'], user=os.environ['WAREHOUSE_NEO4J_USER'],
                      password=os.environ['WAREHOUSE_NEO4J_PASSWORD'])
record = dict(run_id=run_id, tick=1, elapsed=1, status='running', backend='test', robot_count=100,
              max_active_robots=100, counts={}, tasks={}, assists={}, metrics={}, inside=0,
              dispatch={}, events=[], ownership_changes=[dict(change_id='assigned-T-1', task_id='T',
                       tick=1, to_robot_id=1, reason='assigned')])
try:
    writer.write(record)
    print('ownership probe passed')
finally:
    writer.close()
    with urlopen(Request('http://127.0.0.1:14591/v1/runs/' + run_id, method='DELETE',
        data=json.dumps({'confirm_run_id': run_id}).encode(), headers={'Content-Type': 'application/json'})) as response:
        print(response.status)
