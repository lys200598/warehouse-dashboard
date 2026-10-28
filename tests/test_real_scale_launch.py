import argparse
import hashlib
import importlib.util
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch


PATH = Path(__file__).resolve().parents[1]/'ops/run_real_query_benchmark.py'
SPEC = importlib.util.spec_from_file_location('real_query_launch', PATH)
launch = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(launch)
TEMP = Path(__file__).resolve().parents[1]/'data/local-check'


class LaunchEvidenceTests(unittest.TestCase):
    def setUp(self):
        hostname = patch.object(launch.platform, 'node', return_value='test-host')
        hostname.start()
        self.addCleanup(hostname.stop)

    def fixture(self, root, processes=1):
        folder = root/'backend/real_scale'
        folder.mkdir(parents=True)
        names = [*launch.READERS, *(['loadgen.py'] if processes > 1 else [])]
        for name in names:
            (folder/name).write_bytes(('frozen-'+name).encode())
        manifest = root/'reader-manifest.json'
        manifest.write_text(json.dumps({'overlay': launch.hashes(root, names)}), encoding='utf-8')
        return argparse.Namespace(root=root, evidence_dir=root/'new-evidence',
            reader_manifest=manifest, oracle_index=root/'oracle.sqlite', mode='ordered',
            clients=100, rounds=3, samples=10, min_states=1000000,
            stability_seconds=120, recall_samples=2, max_wall_seconds=600,
            observe_writer=True, worker_processes=processes)

    def test_wrapper_retains_actual_argv_sha_logs_and_nonzero_exit(self):
        with tempfile.TemporaryDirectory(dir=TEMP) as temp:
            args = self.fixture(Path(temp))
            with patch.object(launch.subprocess, 'run', return_value=SimpleNamespace(returncode=2)) as child:
                self.assertEqual(launch.run(args), 2)
            command = child.call_args.args[0]
            self.assertIn('--observe-writer', command)
            for flag, value in [('--clients', '100'), ('--rounds', '3'), ('--samples', '10'),
                                ('--min-states', '1000000'), ('--stability-seconds', '120')]:
                self.assertEqual(command[command.index(flag)+1], value)
            before = json.loads((args.evidence_dir/'invocation.json').read_text(encoding='utf-8'))
            after = json.loads((args.evidence_dir/'finish.json').read_text(encoding='utf-8'))
            self.assertEqual(before['reader_pre_sha256'], after['reader_post_sha256'])
            self.assertTrue(after['reader_source_unchanged'])
            self.assertEqual(after['exit_code'], 2)
            self.assertTrue((args.evidence_dir/'stdout.log').is_file())
            self.assertTrue((args.evidence_dir/'stderr.log').is_file())

    def test_all_worker_code_is_hashed_and_selected_explicitly(self):
        with tempfile.TemporaryDirectory(dir=TEMP) as temp:
            args = self.fixture(Path(temp), processes=4)
            with patch.object(launch.subprocess, 'run', return_value=SimpleNamespace(returncode=0)) as child:
                self.assertEqual(launch.run(args), 0)
            command = child.call_args.args[0]
            self.assertEqual(command[command.index('--worker-processes')+1], '4')
            evidence = json.loads((args.evidence_dir/'invocation.json').read_text(encoding='utf-8'))
            self.assertIn('loadgen.py', evidence['reader_pre_sha256'])

    def test_manifest_mismatch_never_starts_child_and_post_drift_fails(self):
        with tempfile.TemporaryDirectory(dir=TEMP) as temp:
            args = self.fixture(Path(temp))
            (args.root/'backend/real_scale/store.py').write_text('changed')
            with patch.object(launch.subprocess, 'run') as child:
                self.assertEqual(launch.run(args), 2)
                child.assert_not_called()
        with tempfile.TemporaryDirectory(dir=TEMP) as temp:
            args = self.fixture(Path(temp))
            def drift(*a, **kw):
                (args.root/'backend/real_scale/store.py').write_text('changed')
                return SimpleNamespace(returncode=0)
            with patch.object(launch.subprocess, 'run', side_effect=drift):
                self.assertEqual(launch.run(args), 3)
            self.assertFalse(json.loads((args.evidence_dir/'finish.json').read_text())['reader_source_unchanged'])


if __name__ == '__main__':
    unittest.main()
