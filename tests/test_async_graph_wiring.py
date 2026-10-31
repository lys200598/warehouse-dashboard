"""Validate asynchronous graph options before any runtime or external I/O starts."""
import unittest
import queue
import threading
from types import SimpleNamespace
from unittest.mock import Mock, patch
import sys
from ops.warehouse_live_runtime import WarehouseLiveRuntime


class AsyncGraphWiringTests(unittest.TestCase):
    def test_invalid_backlog_rejected_before_runtime_start(self):
        for value in (0, -1, 129, True, 1.5):
            with self.subTest(value=value), self.assertRaisesRegex(ValueError, 'robot_graph_backlog'):
                WarehouseLiveRuntime(source_root='unused', output_root='unused', datasets={},
                                     dataset_id='', robot_graph_backlog=value)

    def test_live_cli_default_and_override_reach_the_runtime(self):
        from ops import warehouse_live as cli
        from types import SimpleNamespace
        for flags, expected in (([], 120), (['--robot-graph-backlog', '8'], 8)):
            with self.subTest(flags=flags), \
                    patch.object(cli, 'load_registry', return_value=({'demo': {}}, 'demo')), \
                    patch.object(cli, 'dataset_descriptions', return_value={'datasets': [{'id': 'demo', 'available': True}]}), \
                    patch.object(cli, 'create_app'), \
                    patch('ops.warehouse_live_runtime.WarehouseLiveRuntime') as runtime, \
                    patch('backend.warehouse_graph_query.Neo4jWarehouseGraph'), \
                    patch.dict('sys.modules', {'uvicorn': SimpleNamespace(run=Mock())}), \
                    patch.object(sys, 'path', list(sys.path)):
                cli.main(flags)
            self.assertEqual(runtime.call_args.kwargs['robot_graph_backlog'], expected)

    def test_live_invalid_backlog_fails_before_registry_or_runtime_io(self):
        from ops import warehouse_live as cli
        for value in ('0', '129'):
            with self.subTest(value=value), patch.object(cli, 'load_registry') as registry, \
                    self.assertRaises(SystemExit) as error:
                cli.main(['--robot-graph-backlog', value])
            self.assertEqual(error.exception.code, 2)
            registry.assert_not_called()

    def test_day_cli_default_and_override_reach_the_runner(self):
        from ops import kg_long_run
        base = ['kg_long_run', 'run', '--scene', 'unused', '--dataset', 'unused',
                '--output', 'unused', '--run-id', 'wz-test-parse-only']
        for flags, expected in (([], 120), (['--robot-graph-backlog', '8'], 8)):
            with self.subTest(flags=flags), patch.object(sys, 'argv', base + flags), \
                    patch.object(kg_long_run, 'run') as run:
                kg_long_run.main()
            self.assertEqual(run.call_args.args[0].robot_graph_backlog, expected)

    def test_async_flag_requires_boolean(self):
        with self.assertRaisesRegex(ValueError, 'robot_graph_async'):
            WarehouseLiveRuntime(source_root='unused', output_root='unused', datasets={},
                                 dataset_id='', robot_graph_async='false')

    def test_pause_interrupts_owner_wait_without_stopping_writes(self):
        runtime = object.__new__(WarehouseLiveRuntime)
        runtime._stopping = False
        runtime._thread = SimpleNamespace(is_alive=lambda: True)
        runtime._commands = queue.Queue()
        event = threading.Event()
        runtime._sim = SimpleNamespace(graph_wait_cancel_event=event)
        future = runtime.submit('pause')
        self.assertTrue(event.is_set())
        self.assertFalse(future.done())
        self.assertEqual(runtime._commands.get()[0], 'pause')

    def test_cancelled_capacity_wait_does_not_persist_or_fail_partial_step(self):
        from backend.robot_actor.graph_async import GraphWaitCancelled
        runtime = object.__new__(WarehouseLiveRuntime)
        event = threading.Event()
        event.set()
        runtime._sim = SimpleNamespace(graph_wait_cancel_event=event,
            knowledge_boundary_due=lambda: False, step=Mock(side_effect=GraphWaitCancelled()))
        runtime._disturb = Mock()
        runtime._persist = Mock()
        runtime._advance_one()
        self.assertFalse(event.is_set())
        runtime._persist.assert_not_called()
