import json
from pathlib import Path
import tempfile
import unittest

from ops.warehouse_live_runtime import WarehouseLiveRuntime

ROOT=Path(__file__).resolve().parents[1]
TEMP=ROOT/'data/local-check/activation-limit'
TEMP.mkdir(parents=True,exist_ok=True)


class LiveActivationConfigTests(unittest.TestCase):
    def runtime(self, folder, limit=None, dataset_limit=None, run='cap-first'):
        config={'charging_policy':'none'}
        if dataset_limit is not None:
            config['max_active_robots']=dataset_limit
        return WarehouseLiveRuntime(source_root=ROOT, output_root=folder,
            datasets={'sample':{'scene':str(ROOT/'data/layouts/warehouse100-wide-v2.json'),
                'dataset':str(ROOT/'data/examples/warehouse-v5'),'config':config}},
            dataset_id='sample',run_id=run,mobility='wheeled',max_active_robots=limit,
            knowledge_mode='rules',knowledge_backend='journal')

    def test_omitted_parameter_preserves_existing_default(self):
        with tempfile.TemporaryDirectory(dir=TEMP) as folder:
            runtime=self.runtime(folder)
            try:
                self.assertEqual(runtime.latest()['activation']['limit'],100)
                self.assertEqual(len(runtime.latest()['robots']),100)
            finally:
                runtime.shutdown()

    def test_cli_override_wins_over_dataset_and_applies_to_new_run(self):
        with tempfile.TemporaryDirectory(dir=TEMP) as folder:
            runtime=self.runtime(folder,limit=3,dataset_limit=8)
            try:
                self.assertEqual(runtime.latest()['activation']['limit'],3)
                self.assertEqual(runtime.latest()['metadata']['max_active_robots'],3)
                runtime.command('new_run',{'dataset_id':'sample'})
                self.assertEqual(runtime.latest()['activation']['limit'],3)
                identity=json.loads((Path(runtime.latest()['metadata']['output'])/'live-manifest.json').read_text())
                self.assertEqual(identity['config']['max_active_robots'],3)
            finally:
                runtime.shutdown()

    def test_invalid_constructor_limits_fail_before_a_run_is_created(self):
        with tempfile.TemporaryDirectory(dir=TEMP) as folder:
            for value in (True,0,101,50.0,'50'):
                with self.subTest(value=value),self.assertRaisesRegex(ValueError,'max_active_robots'):
                    self.runtime(folder,limit=value)
            self.assertEqual(list(Path(folder).iterdir()),[])


if __name__=='__main__':unittest.main()
