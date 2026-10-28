"""Configured live query admission is the real executor limit, including failures.

The event gates below are only deterministic unit-test controls. They are not
used in performance measurement and never run a remote service or database.
"""
from concurrent.futures import ThreadPoolExecutor
from contextlib import redirect_stderr
import importlib.util
import io
from pathlib import Path
import tempfile
import threading
import unittest

ROOT=Path(__file__).resolve().parents[1]
SPEC=importlib.util.spec_from_file_location('live_query_workers',ROOT/'ops/warehouse_live.py')
live=importlib.util.module_from_spec(SPEC);SPEC.loader.exec_module(live)
try:
    from fastapi.testclient import TestClient
except ImportError:TestClient=None


class Runtime:
    def __init__(self):self.closed=0
    def query_point(self):return {'run_id':'unit-run','seq':7}
    def shutdown(self):self.closed+=1


class Tracker:
    def __init__(self,expected):
        self.expected=expected;self.lock=threading.Lock();self.active=0;self.peak=0
        self.reached=threading.Event();self.release=threading.Event();self.services=[]

    def factory(self):
        tracker=self
        class Service:
            def __init__(self):self.closed=0
            def execute(self,request):
                with tracker.lock:
                    tracker.active+=1;tracker.peak=max(tracker.peak,tracker.active)
                    if tracker.active==tracker.expected:tracker.reached.set()
                try:
                    if not tracker.release.wait(5):raise RuntimeError('unit-test gate timeout')
                    if request.get('fail'):raise ValueError('unit query failure')
                    return {'request':request}
                finally:
                    with tracker.lock:tracker.active-=1
            def close(self):self.closed+=1
        service=Service()
        with self.lock:self.services.append(service)
        return service


@unittest.skipUnless(TestClient is not None,'optional FastAPI HTTP tests require requirements.txt')
class LiveQueryWorkerTests(unittest.TestCase):
    def setUp(self):
        parent=ROOT/'data/local-check/live-query-workers';parent.mkdir(parents=True,exist_ok=True)
        self.temp=tempfile.TemporaryDirectory(dir=parent);self.addCleanup(self.temp.cleanup)
        self.dist=Path(self.temp.name);(self.dist/'index.html').write_text('unit')

    def check_concurrency(self,workers,*,use_default=False):
        tracker=Tracker(workers);runtime=Runtime();kwargs={} if use_default else {'catalog_workers':workers}
        app=live.create_app(runtime,{},self.dist,catalog_factory=tracker.factory,**kwargs)
        with TestClient(app) as client:
            with ThreadPoolExecutor(max_workers=workers*2+1) as pool:
                futures=[pool.submit(client.post,'/api/knowledge/query',json={'query_id':'self','robot_id':i})
                         for i in range(1,workers*2+2)]
                try:
                    self.assertTrue(tracker.reached.wait(4),'configured workers were not all executing')
                    with tracker.lock:self.assertEqual(tracker.active,workers)
                finally:tracker.release.set()
                replies=[f.result(timeout=10) for f in futures]
        self.assertTrue(all(x.status_code==200 for x in replies))
        self.assertEqual(tracker.peak,workers)
        self.assertEqual(len(tracker.services),workers)
        self.assertTrue(all(x.closed==1 for x in tracker.services))
        self.assertEqual(runtime.closed,1)
        self.assertTrue(all(x.json()['request']['run_id']=='unit-run' and x.json()['request']['seq']==7 for x in replies))

    def test_default_stays_four(self):self.check_concurrency(4,use_default=True)

    def test_configured_real_executor_and_admission_limit(self):
        for workers in (1,3):
            with self.subTest(workers=workers):self.check_concurrency(workers)

    def test_upper_bound_constructs_and_closes_without_query_work(self):
        runtime=Runtime()
        with TestClient(live.create_app(runtime,{},self.dist,catalog_workers=24)):pass
        self.assertEqual(runtime.closed,1)

    def test_invalid_values_rejected_before_executor_creation(self):
        for value in (True,False,0,-1,25,1.0,'4',None):
            with self.subTest(value=value),self.assertRaises(ValueError):
                live.create_app(Runtime(),{},self.dist,catalog_workers=value)

    def test_error_request_still_closes_worker_catalog(self):
        tracker=Tracker(1);tracker.release.set();runtime=Runtime()
        with TestClient(live.create_app(runtime,{},self.dist,catalog_factory=tracker.factory,catalog_workers=2)) as client:
            response=client.post('/api/knowledge/query',json={'query_id':'self','fail':True})
            self.assertEqual(response.status_code,400)
            self.assertEqual(response.json()['error'],'unit query failure')
        self.assertEqual(runtime.closed,1)
        self.assertEqual(len(tracker.services),1)
        self.assertEqual(tracker.services[0].closed,1)

    def test_cli_invalid_worker_counts_rejected_before_loading_registry(self):
        for value in ('0','25','-1','True'):
            with self.subTest(value=value),redirect_stderr(io.StringIO()),self.assertRaises(SystemExit) as exc:
                live.main(['--query-workers',value])
            self.assertEqual(exc.exception.code,2)


if __name__=='__main__':unittest.main()
