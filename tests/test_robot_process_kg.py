"""Opt-in real Ray + global writer + Neo4j golden acceptance.

Set WAREHOUSE_RAY_TEST_ADDRESS and WAREHOUSE_KG_TEST_URL, plus Neo4j
credentials. Each run is isolated with a wz-test- ID and removed via the API.
"""
import json
import os
from pathlib import Path
import statistics
import time
import traceback
import unittest
from urllib.request import Request, urlopen
import uuid

from tests import test_dispatch_golden as golden

ADDRESS = os.environ.get('WAREHOUSE_RAY_TEST_ADDRESS')
URL = os.environ.get('WAREHOUSE_KG_TEST_URL')


def cleanup(sim):
    close_error = None
    try:
        sim.close()
    except Exception as exc:
        close_error = exc
    request = Request(URL + '/v1/runs/' + sim.run_id, method='DELETE',
        data=json.dumps({'confirm_run_id': sim.run_id}).encode(), headers={'Content-Type': 'application/json'})
    with urlopen(request, timeout=30) as response:
        if response.status != 200:
            raise AssertionError('run cleanup failed')
    from neo4j import GraphDatabase
    cfg = sim._graph_config
    with GraphDatabase.driver(cfg['uri'], auth=(cfg['user'], cfg['password'])) as driver:
        with driver.session(database=cfg['database']) as session:
            count = session.run('MATCH (n) WHERE n.run_id=$run OR (n:Run AND n.id=$run) RETURN count(n) AS n',
                                run=sim.run_id).single()['n']
    if count:
        raise AssertionError(f'{count} nodes remain for {sim.run_id}')
    print(f'[cleanup] {sim.run_id}: remaining=0', flush=True)
    if close_error is not None:
        raise close_error


def engine_class(instances, timings):
    from backend.robot_actor.ray_actor import RayRobotProcessSplit
    from backend.wheeled.sim import WheeledSimulation
    class Engine(RayRobotProcessSplit, WheeledSimulation):
        ray_address = ADDRESS
        ray_actor_cpus = .02

        def __init__(self, scene, dataset, run_id, config):
            super().__init__(scene, dataset, 'wz-test-kg-' + uuid.uuid4().hex[:12], config,
                robot_graph=True, kg_state_source='neo4j', global_writer_url=URL)
            instances.append(self)
            print(f'[kg start] {self.run_id}', flush=True)

        def step(self):
            began = time.perf_counter()
            super().step()
            timings.append({**self.kg_timing, 'total_ms': (time.perf_counter() - began) * 1000})
            if self.tick % 200 == 0:
                print(f'[kg] {self.run_id} tick={self.tick} completed={self.metrics["completed"]}', flush=True)
    return Engine


def audit(sim):
    sim._step_client.flush(15.)
    sim._kg_reader.read(sim.run_id, sim.tick, sim.robots, sim._kg_fields)
    with sim._kg_reader.driver.session(database=sim._kg_reader.database) as session:
        step = session.run('MATCH (r:Run {id:$run}) MATCH (s:Step {run_id:$run}) '
            'RETURN r.latest_step AS latest, count(s) AS count, min(s.received_robot_count) AS receipts',
            run=sim.run_id).single()
        assert dict(step) == {'latest': sim.tick, 'count': sim.tick + 1, 'receipts': 100}, dict(step)
        owners = session.run('MATCH (h:Handoff {run_id:$run}) MATCH (e:Execution {run_id:$run, task_id:h.task_id}) '
            'RETURN h.task_id AS task, h.receiver_id AS receiver, e.current_robot_id AS owner', run=sim.run_id).data()
        assert all(row['receiver'] == row['owner'] for row in owners), owners
        payloads = session.run('MATCH (r:Robot {run_id:$run}) RETURN r.robot_id AS id,r.payload AS payload',
                               run=sim.run_id).data()
        assert {r['id']: json.loads(r['payload']) for r in payloads} == sim.robots
    return {'step': dict(step), 'owners': owners, 'robot_fields': len(sim._kg_fields)}


@unittest.skipUnless(ADDRESS and URL, 'set WAREHOUSE_RAY_TEST_ADDRESS and WAREHOUSE_KG_TEST_URL')
class KGGoldenTests(unittest.TestCase):
    def run_scenario(self, scenario):
        instances, timings = [], []
        engine = engine_class(instances, timings)
        began = time.perf_counter()
        try:
            if scenario == 'ordinary':
                actual, fixture = golden.record(sim_class=engine), golden.FIXTURE
            else:
                actual = golden.record_relay(sim_class=engine, report_at=300 if scenario == 'redo' else 250)
                fixture = golden.REDO_FIXTURE if scenario == 'redo' else golden.RELAY_FIXTURE
            expected = json.loads(fixture.read_text())
            self.assertEqual(actual, expected)
            evidence = audit(instances[0])
            result = {'scenario': scenario, 'ticks': actual['ticks'], 'wall_s': time.perf_counter() - began,
                      'mean_ms': {k: statistics.fmean(r.get(k, 0.) for r in timings[1:]) for k in timings[0]}, **evidence}
            print('[acceptance] ' + json.dumps(result), flush=True)
            if os.environ.get('WAREHOUSE_KG_RESULT_DIR'):
                path = Path(os.environ['WAREHOUSE_KG_RESULT_DIR'])
                path.mkdir(parents=True, exist_ok=True)
                (path / (scenario + '.json')).write_text(json.dumps(result, indent=2) + '\n')
        except BaseException:
            traceback.print_exc()
            raise
        finally:
            for sim in instances:
                cleanup(sim)

    def test_ordinary(self):
        self.run_scenario('ordinary')

    def test_relay(self):
        self.run_scenario('relay')

    def test_redo(self):
        self.run_scenario('redo')


if __name__ == '__main__':
    unittest.main()
