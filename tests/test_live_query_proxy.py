"""Proxy contracts; deterministic fake HTTP gates are not benchmark workloads."""
import asyncio
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from ops.warehouse_query_proxy import validate_upstreams,HttpCatalogClient
from ops.warehouse_live import create_app
try:
    import httpx
    from fastapi.testclient import TestClient
except ImportError:httpx=None;TestClient=None


class UpstreamValidationTests(unittest.TestCase):
    def test_loopback_explicit_origins_only(self):
        self.assertEqual(validate_upstreams(['http://127.0.0.1:123/','http://[::1]:124']),
                         ('http://127.0.0.1:123','http://[::1]:124'))
        for values in ([],None,'http://127.0.0.1:1',['http://localhost:1'],
                       ['https://127.0.0.1:1'],['http://127.0.0.1'],['http://127.0.0.1:0'],
                       ['http://user@127.0.0.1:1'],['http://127.0.0.1:1/query'],
                       ['http://127.0.0.1:1?x=1'],['http://127.0.0.1:1#x'],
                       ['http://127.0.0.1:1/','http://127.0.0.1:1'],['http://127.0.0.1:1']*9):
            with self.subTest(values=values),self.assertRaises(ValueError):validate_upstreams(values)


@unittest.skipUnless(httpx is not None,'optional HTTP dependencies')
class HttpCatalogClientTests(unittest.IsolatedAsyncioTestCase):
    async def test_least_inflight_distribution_and_complete_raw_response(self):
        seen=[];ready=asyncio.Event();release=asyncio.Event()
        async def handler(request):
            self.assertEqual(request.url.path,'/api/knowledge/query')
            seen.append(request.url.port)
            if len(seen)==24:ready.set()
            await release.wait()
            return httpx.Response(200,content=b'{ "complete" : [1, 2, 3] }',headers={'content-type':'application/json'})
        proxy=HttpCatalogClient([f'http://127.0.0.1:{20000+i}' for i in range(8)],transport=httpx.MockTransport(handler))
        tasks=[asyncio.create_task(proxy.execute({'run_id':'x','seq':4})) for _ in range(24)]
        try:
            await asyncio.wait_for(ready.wait(),3)
            self.assertEqual(proxy.active,[3]*8)
            self.assertEqual(sorted(seen),sorted(list(range(20000,20008))*3))
        finally:release.set()
        replies=await asyncio.gather(*tasks)
        self.assertTrue(all(r.content==b'{ "complete" : [1, 2, 3] }' for r in replies))
        self.assertEqual(proxy.active,[0]*8);await proxy.close()
        self.assertTrue(all(client.is_closed for client in proxy.clients))

    async def test_error_status_preserved_without_retry(self):
        seen=[]
        def handler(request):
            seen.append(request);return httpx.Response(400,json={'error':'catalog source point absent'})
        proxy=HttpCatalogClient(['http://127.0.0.1:20000'],transport=httpx.MockTransport(handler))
        try:
            reply=await proxy.execute({'run_id':'x','seq':4})
            self.assertEqual(reply.status_code,400);self.assertEqual(len(seen),1)
            self.assertEqual(proxy.active,[0])
        finally:await proxy.close()

    async def test_bad_transport_response_is_not_a_success_or_retry(self):
        seen=[]
        def handler(request):
            seen.append(request);return httpx.Response(302,headers={'location':'http://example.com'})
        proxy=HttpCatalogClient(['http://127.0.0.1:20000'],transport=httpx.MockTransport(handler))
        try:
            with self.assertRaisesRegex(RuntimeError,'unexpected'):await proxy.execute({'seq':4})
            self.assertEqual(len(seen),1);self.assertEqual(proxy.active,[0])
        finally:await proxy.close()


@unittest.skipUnless(TestClient is not None,'optional FastAPI tests')
class LiveProxyTests(unittest.TestCase):
    def setUp(self):
        root=Path(__file__).resolve().parents[1]/'data/local-check/live-query-proxy';root.mkdir(parents=True,exist_ok=True)
        self.temp=tempfile.TemporaryDirectory(dir=root);self.addCleanup(self.temp.cleanup)
        self.dist=Path(self.temp.name);(self.dist/'index.html').write_text('unit')

    def test_gateway_selects_exact_point_and_delivers_bytes_without_json_roundtrip(self):
        records=[]
        class Runtime:
            def query_point(self):return {'run_id':'run-a','seq':7}
            def shutdown(self):records.append('shutdown')
        class FakeProxy:
            def __init__(self,values):records.append(values)
            async def execute(self,body):
                records.append(dict(body))
                return httpx.Response(200,content=b'{ "unmodified" : [ 1, 2 ] }',headers={'content-type':'application/json',
                                                                                         'X-Warehouse-Service-Ms':'12.3'})
            async def close(self):records.append('close')
        with patch('ops.warehouse_query_proxy.HttpCatalogClient',FakeProxy):
            app=create_app(Runtime(),{},self.dist,catalog_upstreams=['http://127.0.0.1:20000'],catalog_workers=24)
        with TestClient(app) as client:
            reply=client.post('/api/knowledge/query',json={'query_id':'self','robot_id':1})
            self.assertEqual(reply.status_code,200);self.assertEqual(reply.content,b'{ "unmodified" : [ 1, 2 ] }')
            self.assertEqual(reply.headers['X-Warehouse-Upstream-Ms'],'12.3')
            self.assertGreaterEqual(float(reply.headers['X-Warehouse-Service-Ms']),0.)
            for body in ({'run_id':'wrong','seq':7},{'run_id':'run-a','seq':8}):
                self.assertEqual(client.post('/api/knowledge/query',json=body).status_code,400)
        self.assertEqual(records[1],{'query_id':'self','robot_id':1,'run_id':'run-a','seq':7})
        self.assertEqual(len([r for r in records if isinstance(r,dict)]),1)
        self.assertIn('close',records);self.assertIn('shutdown',records)

    def test_local_and_remote_factories_are_mutually_exclusive(self):
        with self.assertRaisesRegex(ValueError,'choose local'):
            create_app(object(),{},self.dist,catalog_factory=lambda:None,catalog_upstreams=['http://127.0.0.1:20000'])
