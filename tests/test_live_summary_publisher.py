"""Derived views cannot be advertised before the durable graph commit."""
from collections import deque
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import threading
import unittest

from ops.warehouse_live_runtime import WarehouseLiveRuntime


class LiveSummaryPublisherTests(unittest.TestCase):
    def test_query_point_does_not_copy_or_expose_the_heavy_snapshot(self):
        class NoCopy:
            def __deepcopy__(self,memo):raise AssertionError('unnecessary fleet copy')
        owner=object.__new__(WarehouseLiveRuntime);owner._lock=threading.RLock()
        owner._latest={'runId':'live-run','knowledge':{'mirrored_seq':17},'robots':NoCopy()}
        point=owner.query_point();self.assertEqual(point,{'run_id':'live-run','seq':17})
        point['seq']=99;self.assertEqual(owner._latest['knowledge']['mirrored_seq'],17)

    def test_query_point_before_publication_is_not_invented(self):
        owner=object.__new__(WarehouseLiveRuntime);owner._lock=threading.RLock();owner._latest=None
        with self.assertRaisesRegex(RuntimeError,'not been published'):owner.query_point()

    def fixture(self, folder, *, mirror_error=None, summary_error=None):
        calls=[];owner=object.__new__(WarehouseLiveRuntime)
        owner._sim=SimpleNamespace(knowledge_seq=0,elapsed=12,run_id='summary-test',pending_events=[{'kind':'test'}],
            checkpoint=lambda:{'run_id':'summary-test'})
        mirror=object()
        owner._store=SimpleNamespace(mirror=mirror,last_error=mirror_error,
            append=lambda cp,events:calls.append('source-and-mirror'),
            status=lambda run:{'durable_seq':1,'mirrored_seq':0 if mirror_error else 1})
        owner._core=SimpleNamespace(persistence=SimpleNamespace(write_checkpoint=lambda path,cp:calls.append('checkpoint')))
        owner._disturbances=[];owner._applied=[];owner._events=deque();owner._output=Path(folder)
        owner._query_summary_status=None
        def catch_up(actual, run, target, *, timeout):
            self.assertIs(actual,mirror);self.assertEqual((run,target,timeout),('summary-test',1,15))
            calls.append('summary')
            if summary_error:raise RuntimeError(summary_error)
            return {'published_seq':target,'batch_checksum':'verified-by-summary-directory'}
        owner._query_summary=SimpleNamespace(catch_up_from_graph=catch_up)
        return owner,calls

    def test_summary_advances_only_after_actual_source_and_graph(self):
        with TemporaryDirectory() as folder:
            owner,calls=self.fixture(folder);owner._persist()
            self.assertEqual(calls,['source-and-mirror','checkpoint','summary'])
            self.assertEqual(owner._query_summary_status['published_seq'],1)
            self.assertEqual(owner._last_commit,12)

    def test_failed_graph_never_advances_summary(self):
        with TemporaryDirectory() as folder:
            owner,calls=self.fixture(folder,mirror_error='unavailable')
            with self.assertRaisesRegex(RuntimeError,'graph mirror failed'):owner._persist()
            self.assertNotIn('summary',calls);self.assertIsNone(owner._query_summary_status)

    def test_failed_summary_propagates_without_fabricating_publication(self):
        with TemporaryDirectory() as folder:
            owner,calls=self.fixture(folder,summary_error='invalid checksum')
            with self.assertRaisesRegex(RuntimeError,'invalid checksum'):owner._persist()
            self.assertEqual(calls[-1],'summary');self.assertIsNone(owner._query_summary_status)

    def test_disabled_path_has_no_summary_work(self):
        with TemporaryDirectory() as folder:
            owner,calls=self.fixture(folder);owner._query_summary=None;owner._persist()
            self.assertEqual(calls,['source-and-mirror','checkpoint'])


if __name__=='__main__':unittest.main()
