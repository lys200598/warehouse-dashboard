import unittest

from backend.global_writer import GlobalWriter, build_dispatch_state, build_global_record
from backend.global_writer_api import _validate_batch


class _Config:
    max_active_robots = 100


class _Sim:
    run_id = 'run-test'
    tick = 7
    elapsed = 70
    inside = 42
    robots = {
        1: {'stage': 'to_target', 'x': 1., 'y': 2., 'battery': 80., 'task_id': 'T-1'},
        2: {'stage': 'idle', 'x': 3., 'y': 4., 'battery': 90., 'task_id': None},
    }
    tasks = {'T-1': {'status': 'carrying'}, 'T-2': {'status': 'waiting'}}
    assists = {1: {'receiver': 2, 'status': 'active'}}
    metrics = {'completed': 1, 'traffic_wait_seconds': 3}
    config = _Config()
    run_metadata = {'backend': 'warehouse-real-scale-v1-ray'}


class GlobalWriterRecordTests(unittest.TestCase):
    def test_global_record_contains_counts_stats_and_only_global_events(self):
        events = [
            {'event_seq': 1, 'kind': 'initial_stock', 'robot_id': None, 'tick': 0},
            {'event_seq': 2, 'kind': 'assist_started', 'robot_id': None, 'tick': 7,
             'task_id': 'T-1', 'goods_id': 'G-1'},
            {'event_seq': 3, 'kind': 'task_assigned', 'robot_id': 1, 'tick': 7},
        ]
        record = build_global_record(_Sim(), events, status='running')
        self.assertEqual(record['run_id'], 'run-test')
        self.assertEqual(record['robot_count'], 2)
        self.assertEqual(record['max_active_robots'], 100)
        self.assertEqual(record['counts'], {'to_target': 1, 'idle': 1})
        self.assertEqual(record['tasks'], {'carrying': 1, 'waiting': 1})
        self.assertEqual(record['assists'], {'1': {'receiver': 2, 'status': 'active', 'donor_id': 1}})
        self.assertEqual(record['inside'], 42)
        self.assertEqual(len(record['events']), 1)
        self.assertEqual(record['events'][0]['type'], 'assist_started')

    def test_dispatch_state_is_a_bounded_summary(self):
        state = build_dispatch_state(_Sim(), commit_seq=7, identity={'dataset_id': 'medium19'})
        self.assertEqual(state['schema_version'], 1)
        self.assertEqual(state['run_id'], 'run-test')
        self.assertEqual(state['commit_seq'], 7)
        self.assertEqual(state['dataset_id'], 'medium19')
        self.assertEqual(state['robots']['count'], 2)
        self.assertEqual(state['robots']['stage_counts'], {'to_target': 1, 'idle': 1})
        self.assertEqual(state['tasks']['count'], 2)
        self.assertEqual(state['active_job_count'], 0)
        self.assertEqual(state['pending_count'], 0)

    def test_initial_stock_and_robot_events_are_not_global_rows(self):
        record = build_global_record(_Sim(), [
            {'event_seq': 1, 'kind': 'initial_stock', 'robot_id': None},
            {'event_seq': 2, 'kind': 'task_assigned', 'robot_id': 1},
        ], status='ready')
        self.assertEqual(record['events'], [])

    def test_global_record_reserves_ownership_changes_for_coordinator(self):
        record = build_global_record(_Sim(), [], status='running')
        self.assertEqual(record['ownership_changes'], [])


class _Session:
    def __init__(self):
        self.calls = []

    def run(self, query, **parameters):
        self.calls.append((query, parameters))
        return self

    def consume(self):
        return self

    def data(self):
        return []


class GlobalWriterAssistTests(unittest.TestCase):
    def test_assist_node_and_relationships_are_written(self):
        writer = object.__new__(GlobalWriter)
        writer._active_assists = {}
        session = _Session()
        writer._write_assists(session, 'run-test', 7, 1000, {
            '1': {'receiver': 2, 'task_id': 'T-1', 'reason': 'low_energy', 'handoff_at': 7},
        }, 'running')
        queries = [row[0] for row in session.calls]
        self.assertTrue(any('MERGE (a:Assist {id: row.id})' in query for query in queries))
        self.assertTrue(any('ABOUT_TASK' in query for query in queries))
        self.assertTrue(any('DONOR' in query for query in queries))
        self.assertTrue(any('RECEIVER' in query for query in queries))
        self.assertIn('run-test:assist:1:T-1:7', writer._active_assists['run-test'])

    def test_ownership_changes_write_current_owner_and_close_history(self):
        writer = object.__new__(GlobalWriter)
        session = _Session()
        writer._write_ownership_changes(session, 'run-test', [{
            'change_id': 'handoff-7', 'task_id': 'T-1', 'tick': 7,
            'from_robot_id': 1, 'to_robot_id': 2, 'reason': 'cargo_handoff',
        }], 1000)
        query = session.calls[0][0]
        self.assertIn('current_robot_id', query)
        self.assertIn('previous.until_tick', query)
        self.assertIn('OwnershipChange', query)
        self.assertIn('FROM_ROBOT', session.calls[1][0])


class GlobalWriterApiValidationTests(unittest.TestCase):
    @staticmethod
    def _record(tick):
        return {'run_id': 'run-test', 'tick': tick, 'elapsed': tick, 'status': 'running',
                'backend': 'warehouse-v5', 'robot_count': 2, 'max_active_robots': 2,
                'counts': {}, 'tasks': {}, 'assists': {}, 'metrics': {}, 'inside': 0,
                'dispatch': {}, 'events': [], 'handoffs': [], 'ownership_changes': []}

    def test_batch_requires_one_run_and_ascending_ticks(self):
        self.assertEqual(len(_validate_batch([self._record(1), self._record(2)])), 2)
        with self.assertRaisesRegex(ValueError, 'strictly increasing'):
            _validate_batch([self._record(2), self._record(2)])

    def test_ownership_change_requires_a_destination_robot(self):
        record = self._record(1)
        record['ownership_changes'] = [{'task_id': 'T-1', 'tick': 1, 'reason': 'assigned'}]
        with self.assertRaisesRegex(ValueError, 'to_robot_id'):
            _validate_batch([record])

    def test_batch_rejects_two_owner_changes_for_one_task_and_tick(self):
        record = self._record(1)
        record['ownership_changes'] = [
            {'task_id': 'T-1', 'tick': 1, 'to_robot_id': 1, 'reason': 'assigned'},
            {'task_id': 'T-1', 'tick': 1, 'to_robot_id': 2, 'reason': 'reassigned'},
        ]
        with self.assertRaisesRegex(ValueError, 'multiple ownership changes'):
            _validate_batch([record])


if __name__ == '__main__':
    unittest.main()
