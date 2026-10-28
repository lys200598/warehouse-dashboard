"""External observer-only ASGI wrapper over frozen combined S2.

No injected delays/barriers, decision changes, source edits or request oracle.
"""
from pathlib import Path
import json,os,queue,sys,threading,time,traceback,hashlib

R=Path('/data/workspace/wz/warehouse-free-ports-20261029');S=R/'source-combined-v3'
E=R/'live-joint-evidence-v4';E.mkdir(exist_ok=False)
OUT=R/'live-joint-runs-v4';sys.path.insert(0,str(S))
for line in Path('/data/workspace/wz/warehouse-meeting-candidates-20261028/graph-test/neo4j.env').read_text().splitlines():
    if line and not line.startswith('#'):
        key,value=line.split('=',1);os.environ[key]=value.strip().strip('"').strip("'")
assert os.environ['NEO4J_URI']=='bolt://127.0.0.1:17691'
from ops.warehouse_live_runtime import WarehouseLiveRuntime
from ops.warehouse_live import create_app,CatalogExecutor
from backend.query_catalog import QueryCatalogService
from backend.knowledge_runtime.provider import GraphHistoryProvider
from backend.knowledge_runtime.sim import KnowledgeSimulation

events=queue.SimpleQueue()
def sink():
    with (E/'server-observations.jsonl').open('x',encoding='utf-8',buffering=1024*1024) as stream:
        while True:
            row=events.get()
            if row is None:break
            stream.write(json.dumps(row,separators=(',',':'))+'\n')
            if events.empty():stream.flush()
observer=threading.Thread(target=sink,name='external-observation-sink',daemon=True);observer.start()
original_executor=CatalogExecutor.execute

def catalog_executor(self,request):
    started=time.perf_counter_ns();outcome='error';exception=None
    try:
        value=original_executor(self,request);outcome='ok';return value
    except Exception as exc:
        exception=type(exc).__name__+': '+str(exc);raise
    finally:
        ended=time.perf_counter_ns()
        events.put({'kind':'catalog_execution','start_ns':started,'end_ns':ended,
            'request':dict(request),'outcome':outcome,'exception':exception,'thread_id':threading.get_ident()})
CatalogExecutor.execute=catalog_executor

original_execute=QueryCatalogService.execute

def execute(self,request):
    started=time.perf_counter_ns()
    try:return original_execute(self,request)
    except Exception as exc:
        events.put({'kind':'catalog_error','start_ns':started,'end_ns':time.perf_counter_ns(),
            'request':dict(request),'exception_type':type(exc).__name__,'exception':str(exc)})
        raise
QueryCatalogService.execute=execute
original_context=GraphHistoryProvider.context
def context(self,sim,task):
    start=time.perf_counter_ns();outcome='error';seq=None
    try:
        result=original_context(self,sim,task);seq=result.get('seq');outcome='ok';return result
    finally:
        events.put({'kind':'history_context','start_ns':start,'end_ns':time.perf_counter_ns(),
            'run_id':sim.run_id,'sim_time':sim.elapsed,'task_id':task['task_id'],'seq':seq,'outcome':outcome})
GraphHistoryProvider.context=context
original_event=KnowledgeSimulation.event
def event(self,kind,robot_id=None,task_id=None,**extra):
    value=original_event(self,kind,robot_id,task_id,**extra)
    if kind=='knowledge_action_applied':
        events.put({'kind':'control_action','at_ns':time.perf_counter_ns(),'run_id':self.run_id,
            'sim_time':self.elapsed,'task_id':task_id,'event_seq':self.event_sequence,'action':extra.get('action')})
    return value
KnowledgeSimulation.event=event

scene=S/'data/layouts/expansion-candidates/balanced/layout.json'
datasets={
    'logical19':{'scene':scene,'dataset':R/'inputs/logical19','config_json':R/'inputs/combined-free-config.json',
        'disturbances_json':R/'inputs/soc250.json','label':'Logical ports / 19 tasks / required low SOC'},
    'logical1728':{'scene':scene,'dataset':R/'inputs/logical-boundaries','config_json':R/'inputs/combined-free-config.json',
        'label':'Logical ports / 1728 peak / 100 robots'},
}
runtime=WarehouseLiveRuntime(source_root=S,output_root=OUT,datasets=datasets,dataset_id='logical19',
    commit_seconds=30,knowledge_mode='required',knowledge_backend='neo4j',resource_graph=True,
    expected_neo4j_uri='bolt://127.0.0.1:17691',query_summary_root=OUT,wall_interval=1.)
factory=lambda:QueryCatalogService.from_env(timeout=15,pool_size=1,summary_root=OUT,cache_fleet=True)
warm=factory()
try:prewarm=warm.prepare_run(runtime.latest()['runId'])
finally:warm.close()
app=create_app(runtime,datasets,S/'dist',catalog_factory=factory,
    catalog_description=QueryCatalogService.describe(),own_runtime=False,catalog_workers=16)

class ObservedASGI:
    def __init__(self,application):self.application=application;self.active=0
    async def __call__(self,scope,receive,send):
        if scope['type']!='http' or scope['path']!='/api/knowledge/query':
            return await self.application(scope,receive,send)
        start=time.perf_counter_ns();self.active+=1;at_start=self.active
        request_id=dict(scope.get('headers',[])).get(b'x-probe-id',b'').decode('ascii','replace')
        status=None;complete=False
        def finished(end,body_completed):
            self.active-=1
            events.put({'kind':'api','request_id':request_id,'start_ns':start,'end_ns':end,
                'inflight_at_start':at_start,'status':status,'body_send_completed':body_completed,
                'end_semantics':'last_response_body_send_completed' if body_completed else 'application_exit_without_final_body'})
        async def observed_send(message):
            nonlocal status,complete
            if message['type']=='http.response.start':status=message['status']
            await send(message)
            if message['type']=='http.response.body' and not message.get('more_body',False):
                end=time.perf_counter_ns();complete=True;finished(end,True)
        try:return await self.application(scope,receive,observed_send)
        finally:
            if not complete:finished(time.perf_counter_ns(),False)

ready={'port':14189,'catalog_workers':16,'run_id':runtime.latest()['runId'],'source':str(S),'output_root':str(OUT),
    'initial_snapshot':runtime.latest(),'prewarm':prewarm,'wrapper_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
    'observations':'external observer wrappers delegate original context/event unchanged; no artificial wait or barrier'}
(E/'ready.json').write_text(json.dumps(ready,indent=2),encoding='utf-8')
try:
    import uvicorn
    uvicorn.run(ObservedASGI(app),host='127.0.0.1',port=14189,workers=1,log_level='warning',access_log=False,
        timeout_graceful_shutdown=120)
finally:
    runtime.shutdown();events.put(None);observer.join(30)
