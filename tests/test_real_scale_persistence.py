import ast
from copy import deepcopy
import hashlib
import inspect
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from backend.checkpoint import read_checkpoint, write_checkpoint as original_write
from backend.real_scale.persistence import write_checkpoint
from backend.real_scale import runner
from backend.scheduling_v3 import runner as original_runner
from backend.real_scale.writer import RealSimulation
from backend.scheduling_v5.sim import Config
from backend.wide.scenarios import generate_scenario
from tests.test_scheduler_v4 import physical


ROOT = Path(__file__).resolve().parents[1]


class RemoveOnlyPersistenceDiagnostics(ast.NodeTransformer):
    names = {'checkpoint_file_seconds', 'checkpoint_file_writes', 'checkpoint_file_started'}

    def visit_Assign(self, node):
        if any(isinstance(target, ast.Name) and target.id in self.names for target in node.targets):
            return None
        if any(isinstance(target, ast.Subscript) and isinstance(target.value, ast.Name)
               and target.value.id == 'result' and isinstance(target.slice, ast.Constant)
               and target.slice.value == 'persistence' for target in node.targets):
            return None
        return self.generic_visit(node)

    def visit_AugAssign(self, node):
        return None if isinstance(node.target, ast.Name) and node.target.id in self.names else self.generic_visit(node)

    def visit_Nonlocal(self, node):
        node.names = [name for name in node.names if name not in self.names]
        return node if node.names else None


class AtomicPersistenceTests(unittest.TestCase):
    def setUp(self):
        (ROOT/'data/local-check').mkdir(parents=True, exist_ok=True)
        self.temp = tempfile.TemporaryDirectory(dir=ROOT/'data/local-check')
        self.folder = Path(self.temp.name)

    def tearDown(self):
        self.temp.cleanup()

    def check_same_payload_and_sha(self, payload):
        old, new = self.folder/'old.json', self.folder/'new.json'
        original_write(old, payload); write_checkpoint(new, payload)
        self.assertEqual(read_checkpoint(old), read_checkpoint(new))
        self.assertEqual(read_checkpoint(new), payload)
        a, b = [json.loads(p.read_text(encoding='utf-8')) for p in (old, new)]
        self.assertEqual(a['format'], b['format'])
        self.assertEqual(a['sha256'], b['sha256'])
        canonical = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()
        self.assertEqual(b['sha256'], hashlib.sha256(canonical).hexdigest())

    def test_unicode_floats_escaping_and_nested_payload_match_old_reader(self):
        self.check_same_payload_and_sha({'z': '中文🚚\n\t"\\', 'a': {'nul': '\x00', 'empty': [], 'none': None,
            'booleans': [True, False], 'floats': [-0.0, 0.0, 1.25, 1e-9, 1e20, -1e250]}, 'list': [{'b': 2, 'a': 1}]})

    def test_large_nested_warehouse_shape_keeps_payload_and_sha(self):
        self.check_same_payload_and_sha({'run_id': 'large-test', 'tasks': {
            f'T{i:05d}': {'task_id': f'T{i:05d}', 'source_id': 'R01-L1-C1-F1', 'robot_id': i % 100+1,
                         'nested': {'text': '货物状态'*5, 'values': [i*.01, i, None, True]}}
            for i in range(4000)}, 'robots': [{'state': {'robot_id': i, 'battery': 78.3}} for i in range(1, 101)]})

    def test_single_json_encode_not_dump(self):
        with patch('backend.real_scale.persistence.json.dumps', wraps=json.dumps) as encode, \
             patch('backend.real_scale.persistence.json.dump', side_effect=AssertionError('second encode forbidden')):
            write_checkpoint(self.folder/'once.json', {'payload': [1, 2, 3]})
        self.assertEqual(encode.call_count, 1)

    def test_replace_and_fsync_failures_preserve_previous_file_and_cleanup_temp(self):
        path = self.folder/'checkpoint.json'
        original_write(path, {'old': True})
        before = path.read_bytes()
        for target in ['os.replace', 'os.fsync']:
            with patch('backend.real_scale.persistence.'+target, side_effect=OSError('injected')):
                with self.assertRaises(OSError): write_checkpoint(path, {'new': True})
            self.assertEqual(path.read_bytes(), before)
            self.assertEqual(list(self.folder.glob('.checkpoint.json.*.tmp')), [])

    def test_nonfinite_and_bad_encoding_fail_without_leaving_temp_or_replacing_file(self):
        path = self.folder/'checkpoint.json'; original_write(path, {'old': True}); before = path.read_bytes()
        for value in [float('nan'), float('inf'), '\ud800']:
            with self.assertRaises((ValueError, UnicodeError)):
                write_checkpoint(path, {'invalid': value})
            self.assertEqual(path.read_bytes(), before)
            self.assertEqual(list(self.folder.glob('.checkpoint.json.*.tmp')), [])

    def test_tampered_valid_json_is_rejected_by_old_reader(self):
        path = self.folder/'tampered.json'; write_checkpoint(path, {'number': 1.25})
        path.write_bytes(path.read_bytes().replace(b'1.25', b'1.26'))
        with self.assertRaisesRegex(ValueError, 'SHA256'): read_checkpoint(path)

    def test_generic_run_business_ast_unchanged_except_explicit_timing(self):
        old = ast.parse(inspect.getsource(original_runner.run))
        new = RemoveOnlyPersistenceDiagnostics().visit(ast.parse(inspect.getsource(runner.run)))
        self.assertEqual(ast.dump(old, include_attributes=False), ast.dump(new, include_attributes=False))
        self.assertEqual(ast.dump(ast.parse(inspect.getsource(original_runner.build_parser)), include_attributes=False),
                         ast.dump(ast.parse(inspect.getsource(runner.build_parser)), include_attributes=False))

    def test_old_and_single_encoded_files_restore_identical_actual_future(self):
        scene = ROOT/'data/layouts/warehouse100-wide-v2.json'; data = self.folder/'input'
        generate_scenario(scene, data, active_slots=32, duration_seconds=1800, slot_interval_seconds=3600)
        sim = RealSimulation(scene, data, 'real-persistence-resume', Config())
        for _ in range(700):
            sim.step()
            if any(r['carrying'] for r in sim.robots.values()):
                break
        self.assertTrue(any(r['carrying'] for r in sim.robots.values()))
        state = sim.checkpoint()
        original_write(self.folder/'old.json', state); write_checkpoint(self.folder/'new.json', state)
        old = RealSimulation(scene, data, sim.run_id, Config(), checkpoint=read_checkpoint(self.folder/'old.json'))
        new = RealSimulation(scene, data, sim.run_id, Config(), checkpoint=read_checkpoint(self.folder/'new.json'))
        self.assertEqual(physical(old), physical(new))
        for _ in range(60): old.step(); new.step()
        self.assertEqual(physical(old), physical(new))
        self.assertEqual(old.planner.expansions, new.planner.expansions)


if __name__ == '__main__':
    unittest.main()
