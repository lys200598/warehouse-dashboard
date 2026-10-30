"""Fast queue contracts: no Ray process or graph service is required."""
from copy import deepcopy
import threading
import time
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, MagicMock
from unittest.mock import patch

from backend.robot_actor.graph_async import AsyncRobotGraphQueue, GraphQueueError, GraphWaitCancelled, inflight_cleanup_status


class RobotGraphIsolationTests(unittest.TestCase):
    def test_concurrency_groups_are_set_on_remote_decorator_not_actor_options(self):
        from backend.robot_actor.ray_actor import RayRobotPool, RobotActor
        factory = Mock(return_value=Mock())
        fake_ray = SimpleNamespace(is_initialized=lambda: True, remote=Mock(return_value=factory))
        with patch.dict('sys.modules', {'ray': fake_ray}):
            RayRobotPool({}, None, 80.)
        fake_ray.remote.assert_called_once_with(concurrency_groups={'graph': 1})
        factory.assert_called_once_with(RobotActor)

    def test_old_graph_snapshot_never_rewinds_live_robot_or_plan(self):
        from backend.robot_actor.ray_actor import RobotActor
        actor = object.__new__(RobotActor)
        actor.robot = {'robot_id': 1, 'battery': 79., 'x': 20.}
        actor.plan = {'phase_index': 4}
        actor._before = ('current motion undo boundary',)
        actor.writer = Mock()
        old = {'robot_id': 1, 'battery': 83., 'x': 10.}
        self.assertIsNone(actor.write_graph({'tick': 7, 'current_robot': old}))
        self.assertEqual(actor.robot, {'robot_id': 1, 'battery': 79., 'x': 20.})
        self.assertEqual(actor.plan, {'phase_index': 4})
        self.assertEqual(actor._before, ('current motion undo boundary',))
        self.assertEqual(actor.writer.record_robot_current_state.call_args.args[1], old)

    def test_failed_robot_retries_use_the_same_frozen_frame(self):
        from backend.robot_actor.ray_actor import RayRobotProcessSplit
        sim = object.__new__(RayRobotProcessSplit)
        sim._async_metrics_lock = threading.Lock()
        sim._async_phase_windows = {}
        sim.graph_timing_log_path = None
        snapshots = {i: {'tick': 7, 'current_robot': {'robot_id': i, 'battery': 80.}} for i in range(1, 101)}
        errors = [None] * 100
        errors[8] = 'offline'
        pool = SimpleNamespace(ray=SimpleNamespace(get=lambda _: errors),
            write_graph_all=lambda rows: {i: f'ref-{i}' for i in rows},
            write_graph=Mock(side_effect=['offline', None]))
        sim._robot_pool = lambda: pool
        sim.graph_error_count, sim.graph_errors = 0, []
        sim._process_async_graph_frame({'tick': 7, 'snapshots': snapshots, 'record': None})
        self.assertEqual(pool.write_graph.call_count, 2)
        self.assertEqual(pool.write_graph.call_args_list[0], pool.write_graph.call_args_list[1])
        self.assertEqual(pool.write_graph.call_args.args, (9, snapshots[9]))
        self.assertEqual(sim.graph_receipt, {'tick': 7, 'robots': list(range(1, 101))})

    def test_cancel_only_interrupts_capacity_wait_not_accepted_writes(self):
        entered, release, cancelled, event = (threading.Event() for _ in range(4))
        written = []
        def process(frame):
            entered.set()
            release.wait(2)
            written.append(frame['tick'])
        queue = AsyncRobotGraphQueue(process, capacity=1)
        queue.submit({'tick': 7})
        self.assertTrue(entered.wait(1))
        def reserve():
            try:
                queue.reserve(event)
            except GraphWaitCancelled:
                cancelled.set()
        waiter = threading.Thread(target=reserve)
        waiter.start()
        event.set()
        self.assertTrue(cancelled.wait(1))
        self.assertEqual(queue.snapshot()['pending'], 1)
        release.set()
        waiter.join(1)
        queue.flush(1)
        self.assertEqual(written, [7])
        self.assertEqual(queue.snapshot()['confirmed_tick'], 7)
        queue.close(1)

    def test_http_acceptance_keeps_capacity_and_step_watermark_precedes_context(self):
        from collections import deque
        from backend.robot_actor.ray_actor import RayRobotProcessSplit
        accepted, context_entered, context_release = (threading.Event() for _ in range(3))
        latest = [9]
        sim = object.__new__(RayRobotProcessSplit)
        sim._async_metrics_lock, sim._context_lock = threading.Lock(), threading.Lock()
        sim._async_phase_windows, sim._async_context_windows = {}, {}
        sim._async_latest_step = None
        sim.graph_timing_log_path = sim.context_log_path = None
        sim.context_errors, sim.context_samples, sim._context_tick = 0, deque(), None
        sim.robot_context_interval, sim.run_id = 10, 'run'
        sim._graph_config = {'database': 'neo4j'}
        sim._step_client = SimpleNamespace(submit=lambda _: accepted.set(), flush=lambda _: None)
        session = Mock()
        session.__enter__ = Mock(return_value=session)
        session.__exit__ = Mock(return_value=False)
        session.run.return_value = SimpleNamespace(single=lambda: {'tick': latest[0]})
        sim._async_step_driver = Mock(session=Mock(return_value=session))
        def query_context(run_id, tick, concurrency_group=None):
            self.assertEqual((run_id, tick, concurrency_group), ('run', 10, 'graph'))
            context_entered.set()
            context_release.wait(2)
            return []
        pool = SimpleNamespace(ray=SimpleNamespace(get=lambda _: [None] * 100),
            write_graph_all=lambda rows: {i: i for i in rows}, query_context_all=query_context)
        sim._robot_pool = lambda: pool
        exceptions = SimpleNamespace(ServiceUnavailable=type('ServiceUnavailable', (Exception,), {}),
            SessionExpired=type('SessionExpired', (Exception,), {}),
            TransientError=type('TransientError', (Exception,), {}))
        fake_neo4j = SimpleNamespace(GraphDatabase=None, Query=lambda value, **_: value)
        with patch.dict('sys.modules', {'neo4j': fake_neo4j, 'neo4j.exceptions': exceptions}):
            queue = AsyncRobotGraphQueue(sim._process_async_graph_frame, capacity=1)
            queue.submit({'tick': 10, 'snapshots': {i: {} for i in range(1, 101)}, 'record': {}})
            try:
                self.assertTrue(accepted.wait(1))
                self.assertEqual(queue.snapshot()['pending'], 1)
                self.assertIsNone(sim._async_latest_step)
                latest[0] = 10
                self.assertTrue(context_entered.wait(1))
                self.assertEqual(sim._async_latest_step, 10)
                self.assertEqual(queue.snapshot()['confirmed_tick'], -1)
            finally:
                context_release.set()
                queue.close(2)
        timing = sim.async_graph_timing_windows()[0]
        self.assertEqual(set(timing), {'write_ms', 'wait_step_ms', 'context_ms'})
        self.assertTrue(all(len(values) == 1 for values in timing.values()))


class GraphCleanupTests(unittest.TestCase):
    def test_foreign_service_watermark_cannot_confirm_this_run(self):
        from backend.robot_actor.ray_actor import RayRobotProcessSplit
        sim = object.__new__(RayRobotProcessSplit)
        sim._async_graph_queue, sim._async_write_refs, sim._graph_pending = None, [], []
        sim._async_metrics_lock = threading.Lock()
        sim._async_latest_step = 6
        sim._step_client = SimpleNamespace(health=lambda: {'last_sent_tick': 7,
            'last_flushed_tick': 100, 'buffer_length': 0, 'worker_alive': True, 'closed': False})
        self.assertTrue(sim.graph_inflight_active())
        sim._async_latest_step = 7
        self.assertFalse(sim.graph_inflight_active())

    def test_uncertain_close_failure_blocks_but_quiescent_failure_can_be_cleaned(self):
        error = TimeoutError('still draining')
        self.assertEqual(inflight_cleanup_status(SimpleNamespace(graph_inflight_active=lambda: True), error)['status'],
                         'blocked_inflight')
        self.assertEqual(inflight_cleanup_status(SimpleNamespace(), error)['status'], 'blocked_inflight')
        self.assertIsNone(inflight_cleanup_status(SimpleNamespace(graph_inflight_active=lambda: False), error))
        self.assertIsNone(inflight_cleanup_status(SimpleNamespace(), None))

    def cleanup_fixture(self, close_error, active):
        from tests import test_robot_process_kg as acceptance
        response, driver, session = MagicMock(), MagicMock(), MagicMock()
        response.__enter__.return_value.status = 200
        driver.__enter__.return_value = driver
        driver.session.return_value.__enter__.return_value = session
        session.run.return_value.single.return_value = {'n': 0}
        fake_neo4j = SimpleNamespace(GraphDatabase=SimpleNamespace(driver=Mock(return_value=driver)))
        sim = SimpleNamespace(close=Mock(side_effect=close_error), graph_inflight_active=lambda: active,
            run_id='wz-test-cleanup-unit', _graph_config={'uri': 'unused', 'user': 'unused', 'password': None,
                                                       'database': 'neo4j'})
        return acceptance, sim, response, fake_neo4j

    def test_golden_cleanup_never_deletes_while_failed_close_has_active_writes(self):
        acceptance, sim, response, fake = self.cleanup_fixture(TimeoutError('still writing'), True)
        with patch.object(acceptance, 'URL', 'http://unused'), patch.object(acceptance, 'urlopen', return_value=response) as delete:
            with self.assertRaisesRegex(RuntimeError, 'blocked_inflight'):
                acceptance.cleanup(sim)
        delete.assert_not_called()

    def test_golden_cleanup_deletes_quiescent_failed_run_but_keeps_original_error(self):
        error = RuntimeError('permanent worker already exited')
        acceptance, sim, response, fake = self.cleanup_fixture(error, False)
        with patch.dict('sys.modules', {'neo4j': fake}), patch.object(acceptance, 'URL', 'http://unused'), \
                patch.object(acceptance, 'urlopen', return_value=response) as delete:
            with self.assertRaises(RuntimeError) as caught:
                acceptance.cleanup(sim)
        self.assertIs(caught.exception, error)
        delete.assert_called_once()

    def test_golden_normal_cleanup_deletes_and_checks_zero(self):
        acceptance, sim, response, fake = self.cleanup_fixture(None, False)
        with patch.dict('sys.modules', {'neo4j': fake}), patch.object(acceptance, 'URL', 'http://unused'), \
                patch.object(acceptance, 'urlopen', return_value=response) as delete:
            acceptance.cleanup(sim)
        delete.assert_called_once()

    def test_every_step_receipt_and_complete_flag_is_audited(self):
        from tests.test_robot_process_kg import audit_complete_steps
        import json
        ids = list(range(1, 101))
        rows = [{'tick': tick, 'complete': True, 'expected_robot_count': 100,
                 'received_robot_ids_json': json.dumps(ids)} for tick in range(3)]
        self.assertEqual(audit_complete_steps(rows, 2, ids)['complete_steps'], 3)
        for bad in (rows[:-1], [rows[0], rows[0], rows[2]],
                    [rows[0], {**rows[1], 'complete': False}, rows[2]],
                    [rows[0], {**rows[1], 'received_robot_ids_json': json.dumps(ids[:-1])}, rows[2]]):
            with self.assertRaises(AssertionError):
                audit_complete_steps(bad, 2, ids)


class AsyncRobotGraphQueueTests(unittest.TestCase):
    def wait_until(self, predicate, timeout=1.):
        deadline = time.monotonic() + timeout
        while not predicate():
            if time.monotonic() >= deadline:
                self.fail('queue condition did not become true before the test deadline')
            time.sleep(.001)

    def test_capacity_includes_inflight_and_pause_waits_for_full_drain(self):
        entered = {tick: threading.Event() for tick in (1, 2)}
        release = {tick: threading.Event() for tick in (1, 2)}
        resumed = threading.Event()
        errors = []

        def process(frame):
            tick = frame['tick']
            entered[tick].set()
            if not release[tick].wait(2.):
                raise TimeoutError('test processor was not released')

        queue = AsyncRobotGraphQueue(process, capacity=2)
        waiter = None

        def cleanup():
            for event in release.values():
                event.set()
            if waiter is not None:
                waiter.join(1.)
            queue.close(timeout=1.)

        self.addCleanup(cleanup)
        queue.submit({'tick': 1})
        self.assertTrue(entered[1].wait(1.))
        queue.submit({'tick': 2})
        self.assertEqual(queue.snapshot()['pending'], 2)
        with self.assertRaises(GraphQueueError):
            queue.submit({'tick': 3})
        self.assertEqual(queue.snapshot()['pending'], 2)

        def wait_for_capacity():
            try:
                queue.wait_for_capacity()
                resumed.set()
            except BaseException as exc:
                errors.append(exc)

        waiter = threading.Thread(target=wait_for_capacity, daemon=True)
        waiter.start()
        self.wait_until(lambda: queue.snapshot()['pause_count'] == 1)
        self.assertFalse(resumed.is_set())
        self.assertTrue(queue.snapshot()['waiting'])

        release[1].set()
        self.assertTrue(entered[2].wait(1.))
        self.assertEqual(queue.snapshot()['pending'], 1)
        self.assertFalse(resumed.wait(.02), 'one freed slot must not end a full-drain pause')
        self.assertTrue(queue.snapshot()['waiting'])

        release[2].set()
        self.assertTrue(resumed.wait(1.))
        waiter.join(1.)
        queue.flush(timeout=1.)
        self.assertEqual(errors, [])
        stats = queue.snapshot()
        self.assertEqual(stats['pending'], 0)
        self.assertEqual(stats['max_pending'], 2)
        self.assertEqual(stats['limit'], 2)
        self.assertEqual(stats['confirmed_tick'], 2)
        self.assertEqual(stats['pause_count'], 1)
        self.assertGreater(stats['pause_seconds'], 0.)
        self.assertFalse(stats['waiting'])

    def test_submit_freezes_nested_robot_events_and_global_record(self):
        entered, release = threading.Event(), threading.Event()
        written = []

        def process(frame):
            entered.set()
            if not release.wait(2.):
                raise TimeoutError('test processor was not released')
            written.append(deepcopy(frame))

        queue = AsyncRobotGraphQueue(process, capacity=2)

        def cleanup():
            release.set()
            queue.close(timeout=1.)

        self.addCleanup(cleanup)
        frame = {'tick': 1, 'snapshots': {1: {'current_robot': {'battery': 80., 'plan': [1]},
                                          'events': [{'kind': 'pickup', 'items': ['A']}]}},
                 'record': {'tick': 1, 'ownership_changes': [{'change_id': 'assigned-A-1'}],
                            'dispatch': {'active_jobs': [{'task_ids': ['A']}]}}}
        expected = deepcopy(frame)
        queue.submit(frame)
        self.assertTrue(entered.wait(1.))
        frame['snapshots'][1]['current_robot']['battery'] = 3.
        frame['snapshots'][1]['current_robot']['plan'].append(2)
        frame['snapshots'][1]['events'][0]['items'].append('B')
        frame['record']['ownership_changes'][0]['change_id'] = 'changed'
        frame['record']['dispatch']['active_jobs'][0]['task_ids'].append('B')
        release.set()
        queue.flush(timeout=1.)
        self.assertEqual(written, [expected])

    def test_complete_frames_are_processed_in_strict_tick_order(self):
        written = []
        queue = AsyncRobotGraphQueue(lambda frame: written.append(frame['tick']), capacity=4)
        self.addCleanup(queue.close, timeout=1.)
        for tick in (3, 4, 5):
            queue.submit({'tick': tick})
        queue.flush(timeout=1.)
        self.assertEqual(written, [3, 4, 5])
        self.assertEqual(queue.snapshot()['confirmed_tick'], 5)
        for tick in (5, 4):
            with self.subTest(tick=tick), self.assertRaises((ValueError, GraphQueueError)):
                queue.submit({'tick': tick})
        self.assertEqual(queue.snapshot()['pending'], 0)

    def test_processor_failure_is_retained_and_never_reported_as_confirmed(self):
        entered = threading.Event()

        def process(frame):
            entered.set()
            raise RuntimeError('permanent writer failure')

        queue = AsyncRobotGraphQueue(process, capacity=2)
        queue.submit({'tick': 7, 'record': {'ownership_changes': ['stable']}})
        self.assertTrue(entered.wait(1.))
        with self.assertRaisesRegex(GraphQueueError, 'permanent writer failure'):
            queue.flush(timeout=1.)
        stats = queue.snapshot()
        self.assertEqual(stats['pending'], 1)
        self.assertNotEqual(stats['confirmed_tick'], 7)
        self.assertIn('permanent writer failure', str(stats['error']))
        with self.assertRaises(GraphQueueError):
            queue.wait_for_capacity()
        with self.assertRaises(GraphQueueError):
            queue.close(timeout=1.)

    def test_flush_timeout_keeps_inflight_frame_for_later_confirmation(self):
        entered, release = threading.Event(), threading.Event()
        written = []

        def process(frame):
            entered.set()
            if not release.wait(2.):
                raise TimeoutError('test processor was not released')
            written.append(frame['tick'])

        queue = AsyncRobotGraphQueue(process, capacity=1)

        def cleanup():
            release.set()
            queue.close(timeout=1.)

        self.addCleanup(cleanup)
        queue.submit({'tick': 9})
        self.assertTrue(entered.wait(1.))
        with self.assertRaises(TimeoutError):
            queue.flush(timeout=.01)
        self.assertEqual(queue.snapshot()['pending'], 1)
        self.assertNotEqual(queue.snapshot()['confirmed_tick'], 9)
        release.set()
        queue.flush(timeout=1.)
        self.assertEqual(written, [9])
        self.assertEqual(queue.snapshot()['pending'], 0)
        self.assertEqual(queue.snapshot()['confirmed_tick'], 9)


if __name__ == '__main__':
    unittest.main()
