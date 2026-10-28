"""HTTP controls only reach the single writer; reads never advance physics."""
from concurrent.futures import Future
from copy import deepcopy
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

ROOT=Path(__file__).resolve().parents[1]
SPEC=importlib.util.spec_from_file_location('warehouse_live_http',ROOT/'ops/warehouse_live.py')
live=importlib.util.module_from_spec(SPEC);SPEC.loader.exec_module(live)
try:
    from fastapi.testclient import TestClient
except ImportError:TestClient=None


class Runtime:
    def __init__(self):
        self.commands=[];self.stopped=0
        self.frame={'runId':'fixture','tick':0,'elapsed':0,'status':'paused','speed':1.,'robots':[],
            'metadata':{'dataset_id':'sample'},'knowledge':{'mirrored_seq':3,'durable_seq':3,'lag':0},'capturedAtMs':1}
    def latest(self):return deepcopy(self.frame)
    def scene(self):return {'spec':{'width':90,'depth':76}}
    def status(self):return {'single_writer':True}
    def shutdown(self):self.stopped+=1
    def submit(self,action,value=None,expected_run_id=None):
        result=Future();self.commands.append((action,value,expected_run_id))
        if expected_run_id!=self.frame['runId']:result.set_exception(RuntimeError('stale run identity'))
        elif action=='new_run' and self.frame['status']=='running':result.set_exception(RuntimeError('pause before new_run'))
        else:
            if action=='start':self.frame['status']='running'
            if action=='pause':self.frame['status']='paused'
            if action=='step':self.frame['elapsed']+=1;self.frame['tick']+=1
            if action=='speed':self.frame['speed']=value
            result.set_result(self.latest())
        return result


class Query:
    def __init__(self):self.requests=[];self.closed=False
    def execute(self,request):self.requests.append(deepcopy(request));return {'complete_source_result':request}
    def close(self):self.closed=True


@unittest.skipUnless(TestClient is not None,'optional FastAPI HTTP integration requires requirements.txt')
class LiveHTTPTests(unittest.TestCase):
    def setUp(self):
        parent=ROOT/'data/local-check/live-http-tests';parent.mkdir(parents=True,exist_ok=True)
        self.temp=tempfile.TemporaryDirectory(dir=parent);self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name);(self.root/'live.html').write_text('<h1>live</h1>')
        dataset=self.root/'sample';dataset.mkdir()
        for n in ('tasks.csv','initial_inventory.csv'):(dataset/n).write_text('test')
        (dataset/'manifest.json').write_text(json.dumps({'tasks':19,'duration_seconds':3600}))
        scene=self.root/'scene.json';scene.write_text('{}')
        self.datasets={'sample':{'dataset':str(dataset),'scene':str(scene),'label':'样例'}}
        self.runtime=Runtime();self.query=Query()
    def app(self,graph=True):
        return live.create_app(self.runtime,self.datasets,self.root,
            catalog_factory=(lambda:self.query) if graph else None,catalog_description={'queries':{'self':'state'}})
    def test_reads_and_websocket_do_not_advance_or_mutate(self):
        with TestClient(self.app()) as client:
            self.assertEqual(client.get('/').url.path,'/live.html')
            self.assertEqual(client.get('/api/scene').json()['spec']['width'],90)
            for _ in range(3):self.assertEqual(client.get('/api/snapshot').json()['elapsed'],0)
            with client.websocket_connect('/ws/simulation') as ws:
                self.assertEqual(ws.receive_json()['runId'],'fixture')
            self.assertEqual(self.runtime.commands,[])
            self.assertEqual(client.get('/api/health').json()['sourceKind'],'live_simulation_memory')
        self.assertEqual(self.runtime.stopped,1)
    def test_control_is_single_writer_future_and_stale_run_rejected(self):
        with TestClient(self.app()) as client:
            value=client.post('/api/control',json={'action':'step','expected_run_id':'fixture'})
            self.assertEqual(value.status_code,200);self.assertEqual(value.json()['snapshot']['elapsed'],1)
            self.assertEqual(self.runtime.commands[-1],('step',None,'fixture'))
            self.assertEqual(client.post('/api/control',json={'action':'pause','expected_run_id':'other'}).status_code,409)
    def test_speed_wall_only_and_invalid_values(self):
        with TestClient(self.app()) as client:
            self.assertEqual(client.post('/api/control',json={'action':'speed','value':20}).status_code,200)
            self.assertEqual(self.runtime.frame['elapsed'],0)
            for value in (True,0,121,'20'):
                self.assertEqual(client.post('/api/control',json={'action':'speed','value':value}).status_code,400)
    def test_new_run_uses_alias_only_and_random_inputs_rejected(self):
        with TestClient(self.app()) as client:
            self.assertEqual(client.post('/api/control',json={'action':'new_run','dataset_id':'sample'}).status_code,200)
            self.assertEqual(self.runtime.commands[-1][1],{'dataset_id':'sample'})
            self.assertEqual(client.post('/api/control',json={'action':'new_run','dataset_id':'../../secret'}).status_code,400)
            self.assertEqual(client.post('/api/control',json={'action':'reset'}).status_code,400)
            self.assertEqual(client.post('/api/control',json={'action':'start','dataset_path':'arbitrary'}).status_code,400)
            self.assertEqual(client.post('/api/tasks/batch',json={'count':100}).status_code,405)
    def test_graph_query_defaults_are_one_run_immutable_point(self):
        with TestClient(self.app()) as client:
            response=client.post('/api/knowledge/query',json={'query_id':'self','robot_id':1})
            self.assertEqual(response.status_code,200);self.assertIn('X-Warehouse-Service-Ms',response.headers)
            self.assertEqual(self.query.requests[-1],{'query_id':'self','robot_id':1,'run_id':'fixture','seq':3})
            for bad in ({'run_id':'foreign'},{'seq':4},{'seq':True},{'seq':0}):
                self.assertEqual(client.post('/api/knowledge/query',json={'query_id':'self',**bad}).status_code,400)
        self.assertTrue(self.query.closed)
    def test_no_graph_mode_remains_explicit(self):
        with TestClient(self.app(False)) as client:
            self.assertFalse(client.get('/api/health').json()['graph_queries_enabled'])
            self.assertEqual(client.post('/api/knowledge/query',json={'query_id':'self'}).status_code,503)
            self.assertEqual(client.get('/api/knowledge/catalog').json()['queries'],{'self':'state'})
    def test_cross_origin_controls_rejected_and_bounded_strict_json(self):
        with TestClient(self.app()) as client:
            self.assertEqual(client.post('/api/control',json={'action':'start'},headers={'Origin':'https://foreign.example'}).status_code,403)
            for raw in ('{"action":"start","action":"pause"}','{"action":"speed","value":NaN}','[]','x'*65537):
                self.assertEqual(client.post('/api/control',content=raw).status_code,400)
    def test_registry_describes_availability_and_resolves_relative(self):
        file=self.root/'registry.json'
        file.write_text(json.dumps({'version':'warehouse-live-datasets-v1','base_directory':'.','default_dataset':'s',
            'datasets':{'s':{'scene':'scene.json','dataset':'sample'}}}))
        datasets,selected=live.load_registry(file)
        self.assertEqual(selected,'s');self.assertEqual(datasets['s']['dataset'],str((self.root/'sample').resolve()))
        self.assertEqual(live.dataset_descriptions(datasets)['datasets'][0]['task_count'],19)
        (self.root/'sample/tasks.csv').unlink()
        self.assertFalse(live.dataset_descriptions(datasets)['datasets'][0]['available'])


if __name__=='__main__':unittest.main()
