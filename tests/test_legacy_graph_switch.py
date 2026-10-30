"""The C-only live path retains journal persistence and a real page snapshot."""
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from tests.test_warehouse_live_runtime import MOD, REGISTRY, ROOT, TEMP


class LegacyGraphSwitchTests(unittest.TestCase):
    def test_c_only_neo4j_mode_keeps_journal_and_does_not_construct_a_mirror(self):
        with tempfile.TemporaryDirectory(dir=TEMP) as folder:
            with patch.dict(os.environ, {'NEO4J_URI': 'bolt://unused'}), \
                 patch('backend.real_scale.writer.RealMirror.from_env', side_effect=AssertionError('A mirror used')):
                runtime = MOD.WarehouseLiveRuntime(source_root=ROOT, output_root=folder,
                    datasets=REGISTRY, dataset_id='sample19', run_id='legacy-a-switch-test',
                    knowledge_backend='neo4j')
                try:
                    frame = runtime.command('step')
                    self.assertEqual(frame['tick'], 1)
                    self.assertEqual(len(frame['robots']), 100)
                    self.assertIsNone(runtime._store.mirror)
                    self.assertFalse(frame['metadata']['legacy_a_graph'])
                    self.assertGreater(frame['knowledge']['durable_seq'], 0)
                finally:
                    runtime.shutdown()

    def test_deprecated_native_agents_rejected_before_startup(self):
        with self.assertRaisesRegex(ValueError, 'deprecated'):
            MOD.WarehouseLiveRuntime(source_root='unused', output_root='unused', datasets={},
                                     dataset_id='', robot_agents=True)


if __name__ == '__main__':
    unittest.main()
