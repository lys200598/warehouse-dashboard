from concurrent.futures import ThreadPoolExecutor
import threading
import unittest

from ops.warehouse_query_sidecar import create_app

try:
    from fastapi.testclient import TestClient
except ImportError:
    TestClient=None


class Gates:
    def __init__(self):
        self.lock=threading.Lock();self.heavy_active=threading.Event();self.release=threading.Event()
        self.services=[];self.active=0;self.peak=0

    def factory(self):
        tracker=self
        class Service:
            closed=0
            def execute(self,body):
                with tracker.lock:
                    tracker.active+=1;tracker.peak=max(tracker.active,tracker.peak)
                try:
                    if body.get('query_id') not in ('self','readiness'):
                        tracker.heavy_active.set()
                        if not tracker.release.wait(5):raise RuntimeError('fixture heavy gate timeout')
                    return {'run_id':body['run_id'],'seq':body['seq'],'query_id':body.get('query_id')}
                finally:
                    with tracker.lock:tracker.active-=1
            def close(self):self.closed+=1
        value=Service()
        with self.lock:self.services.append(value)
        return value


@unittest.skipUnless(TestClient is not None,'optional FastAPI/httpx required')
class FairLaneTests(unittest.TestCase):
    def heavy(self,client):
        return client.post('/api/knowledge/query',json={'run_id':'fixture','seq':1,'query_id':'global'})

    def test_blocked_heavy_does_not_block_light_and_total_budget_is_preserved(self):
        gates=Gates()
        with TestClient(create_app(catalog_factory=gates.factory,catalog_workers=3,heavy_workers=1)) as client:
            with ThreadPoolExecutor(max_workers=4) as pool:
                heavy=pool.submit(self.heavy,client)
                try:
                    self.assertTrue(gates.heavy_active.wait(3))
                    light=client.post('/api/knowledge/query',json={'run_id':'fixture','seq':1,'query_id':'self'})
                    self.assertEqual(light.status_code,200)
                    self.assertEqual(light.headers['X-Warehouse-Lane'],'light')
                    self.assertFalse(heavy.done())
                    health=client.get('/api/health').json()
                    self.assertEqual(health['max_Neo4j_query_connections'],3)
                    self.assertEqual(health['lanes']['heavy']['workers'],1)
                    self.assertEqual(health['lanes']['light']['workers'],2)
                    self.assertLessEqual(len(gates.services),3)
                finally:gates.release.set()
                self.assertEqual(heavy.result(5).status_code,200)
        self.assertTrue(all(service.closed==1 for service in gates.services))

    def test_zero_queue_rejection_visible_and_not_disguised_as_success(self):
        gates=Gates()
        with TestClient(create_app(catalog_factory=gates.factory,heavy_workers=1,max_lane_queue=0)) as client:
            with ThreadPoolExecutor(max_workers=2) as pool:
                first=pool.submit(self.heavy,client)
                try:
                    self.assertTrue(gates.heavy_active.wait(3))
                    rejected=self.heavy(client)
                    self.assertEqual(rejected.status_code,503)
                    self.assertEqual(rejected.json()['error'],'query admission queue full')
                    self.assertEqual(rejected.headers['X-Warehouse-Admission-Rejected'],'1')
                    self.assertEqual(client.get('/api/health').json()['lanes']['heavy']['rejected'],1)
                finally:gates.release.set()
                self.assertEqual(first.result(5).status_code,200)

    def test_wait_timeout_has_queue_time_and_cleans_pending_count(self):
        gates=Gates()
        with TestClient(create_app(catalog_factory=gates.factory,heavy_workers=1,max_lane_queue=1,queue_timeout=.03)) as client:
            with ThreadPoolExecutor(max_workers=2) as pool:
                first=pool.submit(self.heavy,client)
                try:
                    self.assertTrue(gates.heavy_active.wait(3))
                    expired=self.heavy(client)
                    self.assertEqual(expired.status_code,503)
                    self.assertEqual(expired.json()['error'],'query admission queue timeout')
                    self.assertGreaterEqual(float(expired.headers['X-Warehouse-Queue-Ms']),20)
                    health=client.get('/api/health').json()
                    self.assertEqual(health['lanes']['heavy']['pending'],1)
                    self.assertEqual(health['lanes']['heavy']['timed_out'],1)
                finally:gates.release.set()
                first.result(5)
            self.assertEqual(client.get('/api/health').json()['lanes']['heavy']['pending'],0)

    def test_original_shared_default_and_flags_rejected_before_factory(self):
        gates=Gates();gates.release.set()
        with TestClient(create_app(catalog_factory=gates.factory)) as client:
            value=client.get('/api/health').json()
            self.assertFalse(value['fair_lanes_enabled'])
            self.assertIsNone(value['max_lane_queue'])
            self.assertEqual(set(value['lanes']),{'shared'})
        for kwargs in ({'heavy_workers':True},{'heavy_workers':3},{'heavy_workers':-1},
                       {'catalog_workers':1,'heavy_workers':1},{'max_lane_queue':True},
                       {'max_lane_queue':-1},{'queue_timeout':True},{'queue_timeout':float('nan')}):
            with self.subTest(kwargs=kwargs),self.assertRaises(ValueError):create_app(catalog_factory=gates.factory,**kwargs)


if __name__=='__main__':unittest.main()
