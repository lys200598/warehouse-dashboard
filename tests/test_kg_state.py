"""Fast failure-contract tests; full services are opt-in in test_robot_process_kg."""
import json
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from backend.robot_actor.kg_state import KGStateError, KGStateReader
from backend.robot_actor.local import RobotProcessSplit
from backend.robot_actor.ray_actor import RobotActor, RayRobotProcessSplit
from backend.global_writer import ownership_changes


class ReaderTests(unittest.TestCase):
    def reader(self, rows, latest=7, timeout=.02):
        session = Mock()
        session.__enter__ = Mock(return_value=session)
        session.__exit__ = Mock(return_value=False)
        session.run.side_effect = lambda query, **kw: SimpleNamespace(
            single=lambda: {'tick': latest}, data=lambda: rows)
        return KGStateReader(uri='unused', user='unused', password=None,
                             driver=Mock(session=Mock(return_value=session)), timeout=timeout)

    def rows(self):
        return [{'robot_id': i, 'payload': json.dumps({'robot_id': i, 'battery': 80.})} for i in range(1, 101)]

    def test_exact_frame_and_reject_missing_duplicate_or_malformed(self):
        ids = set(range(1, 101))
        fields = {'robot_id', 'battery'}
        self.assertEqual(len(self.reader(self.rows()).read('run', 7, ids, fields)), 100)
        for rows in (self.rows()[:-1], self.rows()[:-1] + [self.rows()[0]],
                     self.rows()[:-1] + [{'robot_id': 100, 'payload': '{}'}]):
            with self.assertRaises(KGStateError):
                self.reader(rows).read('run', 7, ids, fields)

    def test_stale_step_times_out(self):
        with self.assertRaisesRegex(KGStateError, 'timed out'):
            self.reader(self.rows(), latest=6).read('run', 7, range(1, 101), {'robot_id', 'battery'})

    def test_replacement_preserves_dictionary_identity(self):
        class Base:
            def release_and_dispatch(self):
                return self.robots[1]['battery']
        class Split(RobotProcessSplit, Base):
            kg_state_source = 'neo4j'
            def _prepare_kg_dispatch(self):
                pass
        sim = Split()
        original = {'robot_id': 1, 'battery': 12}
        sim.robots, sim.run_id, sim.tick = {1: original}, 'run', 7
        sim._kg_fields, sim.kg_timing = set(original), {}
        sim._kg_reader = SimpleNamespace(read=lambda *a: {1: {'robot_id': 1, 'battery': 80}}, timings={})
        self.assertEqual(sim.release_and_dispatch(), 80)
        self.assertIs(sim.robots[1], original)

    def test_actor_ack_requires_current_state_success(self):
        actor = object.__new__(RobotActor)
        actor.writer = Mock()
        actor.writer.record_robot_current_state.side_effect = RuntimeError('offline')
        actor.robot = {'robot_id': 1}
        self.assertIn('offline', actor.write_graph({'current_robot': {'robot_id': 1}}))

    def test_context_failure_is_observation_and_preserves_robot_and_plan(self):
        actor = object.__new__(RobotActor)
        actor.robot_id, actor.robot, actor.plan = 1, {'robot_id': 1, 'battery': 80}, {'parts': []}
        actor.context_client = Mock()
        actor.context_client.query.side_effect = RuntimeError('query offline')
        before_robot, before_plan = dict(actor.robot), dict(actor.plan)
        result = actor.query_context('run', 7)
        self.assertIn('query offline', result['error'])
        self.assertEqual(actor.robot, before_robot)
        self.assertEqual(actor.plan, before_plan)

    def test_same_run_graph_rewind_rejected_before_state_changes(self):
        sim = object.__new__(RayRobotProcessSplit)
        sim.kg_state_source, sim.tick = 'neo4j', 50
        with self.assertRaisesRegex(ValueError, 'rewind'):
            sim._validate_restore({'tick': 40})
        self.assertEqual(sim.tick, 50)

    def test_exhausted_receipts_fail_without_advancing(self):
        sim = object.__new__(RayRobotProcessSplit)
        sim.kg_state_source, sim.graph_errors = 'neo4j', []
        sim._graph_pending = [(7, {1: {}}, {1: 'ref'})]
        pool = SimpleNamespace(ray=SimpleNamespace(get=lambda _: ['offline']), write_graph=Mock(return_value='offline'))
        sim._robot_pool = lambda: pool
        with self.assertRaisesRegex(RuntimeError, 'writes failed'):
            sim._collect_robot_graph()
        self.assertEqual(pool.write_graph.call_count, 3)
        self.assertEqual(sim.graph_receipt['robots'], [])

    def test_stable_assignment_and_handoff_ids(self):
        events = [{'kind': 'task_assigned', 'task_id': 'T', 'tick': 1, 'robot_id': 2},
                  {'kind': 'cargo_handoff', 'task_id': 'T', 'tick': 7, 'donor_id': 2, 'receiver_id': 3}]
        first = ownership_changes(events)
        self.assertEqual(first, ownership_changes(events))
        self.assertEqual([c['change_id'] for c in first], ['assigned-T-1', 'handoff-T-7'])
        self.assertEqual(first[-1]['to_robot_id'], 3)

    def test_current_state_is_written_on_unsampled_tick(self):
        from backend.warehouse_runtime_graph import Neo4jRuntimeGraphWriter, _WRITE_AGENT_STATES
        from tests.test_warehouse_runtime_graph import FakeDriver, snapshot
        driver = FakeDriver()
        writer = Neo4jRuntimeGraphWriter(uri='unused', user='unused', password=None, driver=driver)
        snap = snapshot()
        snap['tick'] = 2
        robot = {'robot_id': 1, 'battery': 83.123456, 'carrying': True}
        writer.record_robot_snapshot(snap)
        writer.record_robot_current_state(snap, robot)
        calls = driver.session_instance.calls
        self.assertFalse(any(q == _WRITE_AGENT_STATES for q, _ in calls))
        row = calls[-1][1]['robots'][0]
        self.assertEqual(row['last_written_tick'], 2)
        self.assertEqual(row['payload'], json.dumps(robot, sort_keys=True))

    def test_mode_dependencies_rejected_before_runtime_starts(self):
        from tests.test_warehouse_live_runtime import MOD
        for options in ({}, {'robot_processes': 'ray'}, {'robot_processes': 'ray', 'robot_graph': True}):
            with self.assertRaisesRegex(ValueError, 'neo4j state requires'):
                MOD.WarehouseLiveRuntime(source_root='unused', output_root='unused', datasets={}, dataset_id='',
                                         kg_state_source='neo4j', **options)


if __name__ == '__main__':
    unittest.main()
