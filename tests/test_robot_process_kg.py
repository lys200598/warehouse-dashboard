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
STATE_SOURCE = os.environ.get('WAREHOUSE_KG_STATE_SOURCE', 'neo4j')
GRAPH_ASYNC = os.environ.get('WAREHOUSE_KG_GRAPH_ASYNC', '1') != '0'
GRAPH_BACKLOG = int(os.environ.get('WAREHOUSE_KG_GRAPH_BACKLOG', '8'))


def cleanup(sim):
    close_error = None
    try:
        sim.close()
    except Exception as exc:
        close_error = exc
    from backend.robot_actor.graph_async import inflight_cleanup_status
    blocked = inflight_cleanup_status(sim, close_error)
    if blocked is not None:
        print('[cleanup] ' + json.dumps({'run_id': sim.run_id, **blocked}), flush=True)
        raise RuntimeError('cleanup blocked_inflight; run and queue preserved: ' + sim.run_id) from close_error
    request = Request(URL + '/v1/runs/' + sim.run_id, method='DELETE',
        data=json.dumps({'confirm_run_id': sim.run_id}).encode(), headers={'Content-Type': 'application/json'})
    with urlopen(request, timeout=600) as response:
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
                robot_graph=True, kg_state_source=STATE_SOURCE, global_writer_url=URL,
                robot_graph_async=GRAPH_ASYNC, robot_graph_backlog=GRAPH_BACKLOG)
            if os.environ.get('WAREHOUSE_KG_RESULT_DIR'):
                logs = Path(os.environ['WAREHOUSE_KG_RESULT_DIR'])
                logs.mkdir(parents=True, exist_ok=True)
                self.context_log_path = str(logs / (self.run_id + '-context.jsonl'))
                self.graph_timing_log_path = str(logs / (self.run_id + '-graph-timing.jsonl'))
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
    sim.flush_robot_graph()
    sim._step_client.flush(15.)
    from backend.robot_actor.kg_state import KGStateReader
    own_reader = sim._kg_reader is None
    reader = sim._kg_reader or KGStateReader(**{k: sim._graph_config[k]
        for k in ('uri', 'user', 'password', 'database')})
    try:
        reader.read(sim.run_id, sim.tick, sim.robots, sim._kg_fields)
        return _audit_graph(sim, reader)
    finally:
        if own_reader:
            reader.close()


def _audit_graph(sim, reader):
    with reader.driver.session(database=reader.database) as session:
        step = session.run('MATCH (r:Run {id:$run}) MATCH (s:Step {run_id:$run}) '
            'RETURN r.latest_step AS latest, count(s) AS count, min(s.received_robot_count) AS receipts',
            run=sim.run_id).single()
        assert dict(step) == {'latest': sim.tick, 'count': sim.tick + 1, 'receipts': 100}, dict(step)
        all_steps = session.run('MATCH (s:Step {run_id:$run}) RETURN s.tick AS tick,s.complete AS complete,'
            's.expected_robot_count AS expected_robot_count,s.received_robot_ids_json AS received_robot_ids_json '
            'ORDER BY s.tick', run=sim.run_id).data()
        step_audit = audit_complete_steps(all_steps, sim.tick, sorted(sim.robots))
        owners = session.run('MATCH (h:Handoff {run_id:$run}) MATCH (e:Execution {run_id:$run, task_id:h.task_id}) '
            'RETURN h.task_id AS task, h.receiver_id AS receiver, e.current_robot_id AS owner', run=sim.run_id).data()
        assert all(row['receiver'] == row['owner'] for row in owners), owners
        payloads = session.run('MATCH (r:Robot {run_id:$run}) RETURN r.robot_id AS id,r.payload AS payload',
                               run=sim.run_id).data()
        assert {r['id']: json.loads(r['payload']) for r in payloads} == sim.robots
    # Real Cypher + HTTP coverage: mocks cannot detect importing-WITH syntax errors.
    from backend.warehouse_graph_query import Neo4jWarehouseGraph
    graph = Neo4jWarehouseGraph(**{k: sim._graph_config[k] for k in ('uri', 'user', 'password', 'database')},
                               driver=reader.driver)
    latest = graph.latest_complete_step(sim.run_id, include_states=True)
    assert latest['step']['tick'] == sim.tick
    assert sorted(row['robot_id'] for row in latest['states']) == sorted(sim.robots)
    with urlopen(sim.global_writer_url.rstrip('/') + '/v1/runs/' + sim.run_id +
                 '/steps/latest?states=true', timeout=15) as response:
        api_latest = json.load(response)
    assert api_latest['step']['tick'] == sim.tick
    assert api_latest['states'] == latest['states']
    return {'step': dict(step), 'owners': owners, 'robot_fields': len(sim._kg_fields),
            'step_audit': step_audit,
            'kg_state_source': sim.kg_state_source, 'robot_graph_async': sim.robot_graph_async,
            'graph_backpressure': sim.graph_backpressure_status()}


def audit_complete_steps(rows, final_tick, expected_ids):
    assert [row['tick'] for row in rows] == list(range(final_tick + 1)), 'missing, duplicate or unordered Step tick'
    assert len(expected_ids) == 100 and len(set(expected_ids)) == 100, 'expected robot set must contain 100 ids'
    for row in rows:
        assert row['complete'] is True, f'incomplete Step at {row["tick"]}'
        assert row['expected_robot_count'] == 100, f'wrong expected count at {row["tick"]}'
        assert json.loads(row['received_robot_ids_json']) == expected_ids, f'wrong receipt ids at {row["tick"]}'
    return {'complete_steps': len(rows), 'missing_steps': 0, 'invalid_steps': 0, 'receipt_ids_per_step': 100}


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

    def test_duplicate_history_rows_are_idempotent(self):
        from neo4j import GraphDatabase
        from backend.warehouse_runtime_graph import Neo4jRuntimeGraphWriter
        from ops.kg_long_run import graph_config
        from tests.test_warehouse_runtime_graph import snapshot
        config = graph_config()
        run_id = 'wz-test-kg-retry-' + uuid.uuid4().hex[:12]
        sample = snapshot()
        sample.update(runId=run_id, tick=4)
        sample['events'] = sample['events'] * 2
        sample['robots'] = sample['robots'] * 2
        with GraphDatabase.driver(config['uri'], auth=(config['user'], config['password'])) as driver:
            try:
                previous = None
                for _ in range(3):
                    writer = Neo4jRuntimeGraphWriter(**config)
                    try:
                        writer.record_snapshot(sample)
                    finally:
                        writer.close()
                    with driver.session(database=config['database']) as session:
                        rows = session.run('MATCH (n {run_id:$run}) OPTIONAL MATCH (n)-[r:IN_RUN]->(:Run {id:$run}) '
                            'RETURN labels(n) AS labels,count(DISTINCT n) AS nodes,count(r) AS edges ORDER BY labels',
                            run=run_id).data()
                        self.assertTrue(rows)
                        self.assertTrue(all(row['nodes'] == row['edges'] == 1 for row in rows), rows)
                        self.assertEqual(session.run('MATCH (n {run_id:$run}) WHERE n.__in_run_new IS NOT NULL '
                            'RETURN count(n) AS n',run=run_id).single()['n'], 0)
                    if previous is not None:
                        self.assertEqual(rows, previous)
                    previous = rows
                # Duplicate Execution rows must create one run edge, then remain stable on replay.
                from backend.warehouse_runtime_graph import _WRITE_EXECUTIONS
                execution = {'id': run_id + ':execution:duplicate-query', 'run_id': run_id,
                    'task_id': 'duplicate-query', 'robot_node_id': run_id + ':robot:1',
                    'robot_id': 1, 'status': 'assigned', 'updated_tick': 4}
                with driver.session(database=config['database']) as session:
                    for _ in range(3):
                        session.run(_WRITE_EXECUTIONS, run_id=run_id, dataset_id=None,
                                    executions=[execution, execution]).consume()
                    row = session.run('MATCH (e:Execution {id:$id}) OPTIONAL MATCH (e)-[r:IN_RUN]->(:Run {id:$run}) '
                        'RETURN count(DISTINCT e) AS nodes,count(r) AS edges,max(e.__in_run_new) AS marker',
                        id=execution['id'],run=run_id).single()
                    self.assertEqual(dict(row), {'nodes':1,'edges':1,'marker':None})
            finally:
                with urlopen(Request(URL + '/v1/runs/' + run_id, method='DELETE',
                    data=json.dumps({'confirm_run_id':run_id}).encode(),
                    headers={'Content-Type':'application/json'}), timeout=600) as response:
                    self.assertEqual(response.status, 200)
                with driver.session(database=config['database']) as session:
                    self.assertEqual(session.run('MATCH (n) WHERE n.run_id=$run OR (n:Run AND n.id=$run) '
                        'RETURN count(n) AS n', run=run_id).single()['n'], 0)

    def test_ordinary(self):
        self.run_scenario('ordinary')

    def test_relay(self):
        self.run_scenario('relay')

    def test_redo(self):
        self.run_scenario('redo')


if __name__ == '__main__':
    unittest.main()
