from pathlib import Path
import tempfile
import threading
import time
from types import SimpleNamespace
import unittest
from ops.warehouse_live_runtime import WarehouseLiveRuntime


class CleanupTests(unittest.TestCase):
    def test_retired_provider_failure_still_closes_store(self):
        calls=[]
        class Provider:
            def close(self):
                calls.append('provider')
                raise OSError('test close failure')
        owner=object.__new__(WarehouseLiveRuntime)
        owner._provider=Provider();owner._store=SimpleNamespace(close=lambda:calls.append('store'))
        owner._sim=object()
        with tempfile.TemporaryDirectory() as folder:
            def create(*args,**kwargs):
                owner._store=object();owner._provider=None;owner._sim=object();owner._output=Path(folder)
            owner._create_run_impl=create
            with self.assertRaisesRegex(RuntimeError,'resource cleanup'):
                owner._create_run('new')
            self.assertEqual(calls,['provider','store'])
            self.assertTrue((Path(folder)/'retirement-errors.json').is_file())

    def test_failed_new_run_restores_summary_point_and_published_frame(self):
        owner=object.__new__(WarehouseLiveRuntime)
        old_store=object();old_sim=object()
        owner._store=old_store;owner._provider=None;owner._sim=old_sim
        owner._query_summary_status={'published_seq':7};owner._last_query_point=('old',7)
        owner._published_scene={'old':True};owner._latest={'runId':'old'};owner._revision=8
        closed=[]
        def create(*args,**kwargs):
            owner._store=object();owner._query_summary_status={'published_seq':1}
            owner._last_query_point=('new',1);owner._published_scene={'new':True}
            owner._latest={'runId':'new'};owner._revision=9
            raise OSError('new boundary failure')
        owner._create_run_impl=create;owner._close=lambda:closed.append('new-store')
        with self.assertRaisesRegex(ValueError,'prior run preserved'):owner._create_run('new')
        self.assertIs(owner._store,old_store);self.assertIs(owner._sim,old_sim)
        self.assertEqual(owner._query_summary_status,{'published_seq':7})
        self.assertEqual(owner._last_query_point,('old',7));self.assertEqual(owner._revision,8)
        self.assertEqual(owner._published_scene,{'old':True});self.assertEqual(owner._latest,{'runId':'old'})
        self.assertEqual(closed,['new-store'])

    def test_invalid_initialization_does_not_close_old_resources(self):
        owner=object.__new__(WarehouseLiveRuntime);owner._store=object();owner._provider=None;owner._sim=object()
        owner._create_run_impl=lambda *a,**k:(_ for _ in ()).throw(ValueError('bad config'))
        owner._close=lambda:self.fail('old store must remain open')
        with self.assertRaises(ValueError):owner._create_run('bad')


if __name__=='__main__':unittest.main()
