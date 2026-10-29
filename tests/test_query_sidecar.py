"""Local fake-catalog contracts for the independent query-only service."""
from concurrent.futures import ThreadPoolExecutor
from contextlib import redirect_stderr,redirect_stdout
import importlib.util
import hashlib
import io
import json
import os
from pathlib import Path
import sys
import tempfile
import threading
import types
import unittest
from unittest.mock import patch

ROOT=Path(__file__).resolve().parents[1]
SPEC=importlib.util.spec_from_file_location('query_sidecar',ROOT/'ops/warehouse_query_sidecar.py')
sidecar=importlib.util.module_from_spec(SPEC);SPEC.loader.exec_module(sidecar)
try:
    from fastapi.testclient import TestClient
except ImportError:TestClient=None


class Tracker:
    def __init__(self,workers=3):
        self.workers=workers;self.lock=threading.Lock();self.active=0;self.peak=0;self.services=[]
        self.ready=threading.Event();self.release=threading.Event()
    def factory(self):
        tracker=self
        class Service:
            def __init__(self):self.closed=0
            def execute(self,body):
                with tracker.lock:
                    tracker.active+=1;tracker.peak=max(tracker.peak,tracker.active)
                    if tracker.active==tracker.workers:tracker.ready.set()
                try:
                    if not tracker.release.wait(5):raise RuntimeError('unit gate timeout')
                    if body.get('query_id')=='invalid':raise ValueError('original source error')
                    if body.get('query_id')=='broken':raise RuntimeError('original transport error')
                    return {'run_id':body['run_id'],'seq':body['seq'],'result':body,'read_only':True}
                finally:
                    with tracker.lock:tracker.active-=1
            def close(self):self.closed+=1
        value=Service()
        with self.lock:self.services.append(value)
        return value


@unittest.skipUnless(TestClient is not None,'optional local FastAPI HTTP tests require requirements.txt')
class QuerySidecarTests(unittest.TestCase):
    def test_explicit_run_cutoff_and_raw_business_result_preserved(self):
        tracker=Tracker();tracker.release.set()
        with TestClient(sidecar.create_app(catalog_factory=tracker.factory,source_identity={'frozen':'sha'})) as client:
            body={'query_id':'self','run_id':'fixture-run','seq':3,'robot_id':27}
            value=client.post('/api/knowledge/query',json=body)
            self.assertEqual(value.status_code,200)
            self.assertEqual(value.json(),{'run_id':'fixture-run','seq':3,'result':body,'read_only':True})
            self.assertGreaterEqual(float(value.headers['X-Warehouse-Service-Ms']),float(value.headers['X-Warehouse-Queue-Ms']))
            self.assertEqual(value.headers['Cache-Control'],'no-store')
            health=client.get('/api/health').json()
            self.assertEqual(health['kind'],'query_only');self.assertFalse(health['runtime_created'])
            self.assertEqual(health['max_Neo4j_query_connections'],3)
            self.assertEqual(health['source_identity'],{'frozen':'sha'})
            self.assertEqual(client.post('/api/control',json={'action':'start'}).status_code,404)
        self.assertTrue(all(x.closed==1 for x in tracker.services))

    def test_strict_json_and_missing_or_invalid_explicit_point_rejected(self):
        tracker=Tracker();tracker.release.set()
        with TestClient(sidecar.create_app(catalog_factory=tracker.factory)) as client:
            for raw in ('[]','{"seq":1,"seq":2}','{"run_id":"r","seq":NaN}','x'*65537):
                self.assertEqual(client.post('/api/knowledge/query',content=raw).status_code,400)
            for body in ({},{'run_id':'r'},{'seq':1},{'run_id':'r','seq':True},
                         {'run_id':'r','seq':0},{'run_id':'../outside','seq':1}):
                self.assertEqual(client.post('/api/knowledge/query',json=body).status_code,400)
        self.assertEqual(tracker.services,[])

    def test_actual_executor_limit_three_and_queue_in_service_timing(self):
        tracker=Tracker(3)
        with TestClient(sidecar.create_app(catalog_factory=tracker.factory)) as client:
            with ThreadPoolExecutor(max_workers=9) as pool:
                pending=[pool.submit(client.post,'/api/knowledge/query',json={
                    'query_id':'self','run_id':'fixture','seq':i}) for i in range(1,10)]
                try:self.assertTrue(tracker.ready.wait(4));self.assertEqual(tracker.peak,3)
                finally:tracker.release.set()
                results=[p.result(timeout=10) for p in pending]
        self.assertEqual(tracker.peak,3);self.assertEqual(len(tracker.services),3)
        self.assertTrue(all(x.closed==1 for x in tracker.services))
        for value in results:
            self.assertEqual(value.status_code,200)
            self.assertGreaterEqual(float(value.headers['X-Warehouse-Service-Ms']),float(value.headers['X-Warehouse-Queue-Ms']))

    def test_errors_are_original_400_or_503_without_retry_and_catalogs_close(self):
        tracker=Tracker();tracker.release.set()
        with TestClient(sidecar.create_app(catalog_factory=tracker.factory)) as client:
            original=client.post('/api/knowledge/query',json={'query_id':'invalid','run_id':'r','seq':1})
            self.assertEqual(original.status_code,400);self.assertEqual(original.json(),{'error':'original source error'})
            unexpected=client.post('/api/knowledge/query',json={'query_id':'broken','run_id':'r','seq':1})
            self.assertEqual(unexpected.status_code,503);self.assertEqual(unexpected.json(),{'error':'query failed: RuntimeError'})
        self.assertTrue(all(x.closed==1 for x in tracker.services))

    def test_worker_budget_and_expected_endpoint_rejected_before_factories(self):
        tracker=Tracker()
        for value in (True,False,0,4,-1,1.,'3',None):
            with self.subTest(value=value),self.assertRaises(ValueError):
                sidecar.create_app(catalog_factory=tracker.factory,catalog_workers=value)
        with patch.dict(os.environ,{'NEO4J_URI':'bolt://127.0.0.1:1111'}):
            with self.assertRaises(ValueError):sidecar.create_app(catalog_factory=tracker.factory,expected_neo4j_uri='bolt://127.0.0.1:2222')
        self.assertEqual(tracker.services,[])

    def test_cli_only_loopback_validated_before_any_catalog_creation(self):
        for args in (['--host','0.0.0.0'],['--query-workers','4'],['--port','0'],['--prepare-run','r']):
            with self.subTest(args=args),redirect_stderr(io.StringIO()),self.assertRaises(SystemExit) as exc:
                sidecar.main(args)
            self.assertEqual(exc.exception.code,2)

    def test_optional_startup_preparation_is_separate_and_closes_temporary_catalog(self):
        class Preparation:
            closed=0
            def prepare_run(self,run):return {'run_id':run,'head':9,'consumed':2}
            def close(self):self.closed+=1
        prepare=Preparation();called=[];server=types.ModuleType('uvicorn')
        server.run=lambda app,**kwargs:called.append((app,kwargs))
        parent=ROOT/'data/local-check/query-sidecar-tests';parent.mkdir(parents=True,exist_ok=True)
        temp=tempfile.TemporaryDirectory(dir=parent);self.addCleanup(temp.cleanup)
        manifest=Path(temp.name)/'source-manifest.json'
        files={name:hashlib.sha256((ROOT/name).read_bytes()).hexdigest()
               for name in ('ops/warehouse_query_sidecar.py','backend/query_catalog/service.py')}
        manifest.write_text(json.dumps({'files':files}));output=io.StringIO()
        with (patch('backend.query_catalog.QueryCatalogService.from_env',return_value=prepare) as factory,
              patch.dict(sys.modules,{'uvicorn':server}),redirect_stdout(output)):
            self.assertEqual(sidecar.main(['--source-root',str(ROOT),'--source-manifest',str(manifest),
                '--summary-root',str(ROOT/'data/local-check'),'--warm-run-id','unit-run','--port','14210']),0)
        self.assertEqual(prepare.closed,1);self.assertEqual(factory.call_count,1)
        self.assertEqual(factory.call_args.kwargs['pool_size'],1)
        reports=[json.loads(x) for x in output.getvalue().splitlines()]
        self.assertEqual(reports[0]['query_summary_startup']['result']['head'],9)
        self.assertEqual(reports[1]['query_sidecar_startup']['kind'],'query_only')
        self.assertEqual(reports[1]['query_sidecar_startup']['source_identity']['verified_files'],2)
        self.assertEqual(reports[1]['query_sidecar_startup']['source_identity']['source_manifest_sha256'],
                         hashlib.sha256(manifest.read_bytes()).hexdigest())
        self.assertEqual(called[0][1]['workers'],1)
        with TestClient(called[0][0]) as client:
            self.assertEqual(client.get('/api/health').json()['startup_preparation']['result']['run_id'],'unit-run')

    def test_source_manifest_all_hashes_and_path_bounds_are_enforced(self):
        parent=ROOT/'data/local-check/query-sidecar-tests';parent.mkdir(parents=True,exist_ok=True)
        temp=tempfile.TemporaryDirectory(dir=parent);self.addCleanup(temp.cleanup)
        manifest=Path(temp.name)/'manifest.json'
        own='ops/warehouse_query_sidecar.py';sha=hashlib.sha256((ROOT/own).read_bytes()).hexdigest()
        for files in ({own:sha,'../outside':'0'*64},{own:sha,'C:/outside':'0'*64},
                      {own:sha,'backend\\outside':'0'*64},{own:'0'*64},{own:sha,'missing.py':'0'*64},{}):
            manifest.write_text(json.dumps({'files':files}))
            with self.subTest(files=files),self.assertRaises(ValueError):sidecar._source_identity(ROOT,manifest)
        manifest.write_text(json.dumps({'files':{own:sha}}))
        self.assertEqual(sidecar._source_identity(ROOT,manifest)['verified_files'],1)


if __name__=='__main__':unittest.main()
