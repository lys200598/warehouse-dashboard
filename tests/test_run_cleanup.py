import unittest
from backend import warehouse_run_cleanup as cleanup


class Result:
    def __init__(self, rows): self.rows = rows
    def data(self): return self.rows
    def single(self): return self.rows[0]


class Graph:
    def __init__(self):
        self.nodes = {'hub':('Robot','ours'), 'run':('Run','ours'),
                      'foreign':('Robot','other'), 'other-run':('Run','other')}
        self.edges = {'safe':('foreign','other-run')}
        for i in range(30):
            self.nodes[str(i)] = ('AgentState','ours')
            self.edges['s'+str(i)] = (str(i),'hub')
            self.edges['r'+str(i)] = (str(i),'run')
        self.nodes['unknown'] = ('NewRuntimeLabel','ours')
        self.edges['u'] = ('unknown','run')
        # A hub attached to many foreign nodes must still delete edges in bounded chunks.
        for i in range(40):
            self.nodes['f'+str(i)] = ('Fixture','other')
            self.edges['x'+str(i)] = ('hub','f'+str(i))
        self.calls = []
        self.fail_once = False
        self.edge_deletes = 0
    def session(self, **kwargs): return self
    def __enter__(self): return self
    def __exit__(self, *args): pass
    def run(self, query, **params):
        self.calls.append((query,params))
        run = params['run_id']
        if query == cleanup._READ_EDGES:
            own = set(params['ids'])
            ids = [key for key,(a,b) in self.edges.items() if a in own or b in own]
            return Result([{'id':key} for key in ids[:params['batch']]])
        if query == cleanup._DELETE_EDGES:
            self.edge_deletes += 1
            if self.fail_once and self.edge_deletes == 2:
                self.fail_once = False
                raise RuntimeError('simulated interruption')
            ids = [key for key in params['ids'] if key in self.edges and
                   any(self.nodes[node][1] == run for node in self.edges[key])]
            for key in ids: del self.edges[key]
            return Result([{'deleted':len(ids)}])
        if query == cleanup._DELETE_NODES:
            ids = [key for key in params['ids'] if key in self.nodes and self.nodes[key][1] == run]
            assert not any(a in ids or b in ids for a,b in self.edges.values()), 'unbounded detach attempted'
            for key in ids: del self.nodes[key]
            return Result([{'deleted':len(ids)}])
        ids = [key for key,(label,owner) in self.nodes.items() if owner == run]
        if 'WHERE NOT n:Run' in query:
            ids = [key for key in ids if self.nodes[key][0] != 'Run']
        elif '(n:Run ' in query:
            ids = [key for key in ids if self.nodes[key][0] == 'Run']
        else:
            label = query.split('(n:',1)[1].split(' ',1)[0]
            ids = [key for key in ids if self.nodes[key][0] == label]
        return Result([{'id':key} for key in ids[:params['batch']]])


class CleanupTests(unittest.TestCase):
    def test_hubs_unknown_labels_and_foreign_data(self):
        graph = Graph()
        stats = cleanup.delete_runtime_run(graph,'neo4j','ours',node_batch=3,relationship_batch=7)
        self.assertEqual(stats['nodes_deleted'],33)
        self.assertEqual(graph.edges,{'safe':('foreign','other-run')})
        self.assertTrue(all(owner=='other' for label,owner in graph.nodes.values()))
        self.assertLessEqual(stats['max_node_batch'],3)
        self.assertLessEqual(stats['max_relationship_batch'],7)
        again = cleanup.delete_runtime_run(graph,'neo4j','ours')
        self.assertEqual(again['nodes_deleted'],0)
    def test_interrupted_cleanup_can_resume(self):
        graph = Graph()
        graph.fail_once = True
        with self.assertRaisesRegex(RuntimeError,'simulated interruption'):
            cleanup.delete_runtime_run(graph,'neo4j','ours')
        self.assertNotIn('0', graph.nodes)  # the first batch was already committed
        cleanup.delete_runtime_run(graph,'neo4j','ours',node_batch=2,relationship_batch=3)
        self.assertEqual(graph.edges,{'safe':('foreign','other-run')})
    def test_invalid_limits_do_not_query(self):
        graph = Graph()
        for invalid in (0,-1,True,10001):
            with self.assertRaises(ValueError):
                cleanup.delete_runtime_run(graph,'neo4j','ours',node_batch=invalid)
        self.assertFalse(graph.calls)


class CleanupHttpTests(unittest.TestCase):
    def test_cleanup_does_not_block_health_and_requires_confirmation(self):
        from concurrent.futures import ThreadPoolExecutor
        import threading
        import time
        from unittest.mock import patch
        from fastapi.testclient import TestClient
        from backend.global_writer_api import create_app
        class Writer:
            driver = object()
            database = 'neo4j'
            def close(self): pass
            def write(self, record): raise AssertionError('unexpected write')
        entered, release = threading.Event(), threading.Event()
        def slow_cleanup(*args):
            entered.set()
            release.wait(3)
            return {}
        with patch.object(cleanup, 'delete_runtime_run', slow_cleanup):
            with TestClient(create_app(Writer())) as client:
                bad = client.request('DELETE', '/v1/runs/ours', json={'confirm_run_id':'other'})
                self.assertEqual(bad.status_code, 400)
                self.assertFalse(entered.is_set())
                with ThreadPoolExecutor(max_workers=1) as pool:
                    request = pool.submit(client.request, 'DELETE', '/v1/runs/ours', json={'confirm_run_id':'ours'})
                    try:
                        self.assertTrue(entered.wait(1))
                        began = time.monotonic()
                        response = client.get('/v1/health')
                        self.assertEqual(response.status_code, 200)
                        self.assertLess(time.monotonic()-began, 1.)
                    finally:
                        release.set()
                    self.assertEqual(request.result().json(), {'deleted':True,'run_id':'ours'})
