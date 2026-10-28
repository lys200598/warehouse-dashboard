"""External observer-only ASGI wrapper over frozen combined S2.

No injected delays/barriers, decision changes, source edits or request oracle.
"""
from pathlib import Path
import json,os,queue,sys,threading,time,traceback,hashlib,subprocess,urllib.request,socket

R=Path('/data/workspace/wz/warehouse-free-ports-20261029');S=R/'source-combined-v5'
E=R/'live-joint-evidence-v7';E.mkdir(exist_ok=False)
OUT=R/'live-joint-runs-v7';sys.path.insert(0,str(S))
manifest_path=S/'source-s7-manifest.json';manifest=json.loads(manifest_path.read_text())
assert hashlib.sha256(manifest_path.read_bytes()).hexdigest()=='be168548f414cc585c58b11a956cb3a4672c47392a0dc2d8846ffeb21e916fb4'
assert all(hashlib.sha256((S/p).read_bytes()).hexdigest()==h for p,h in manifest['files'].items())
for line in Path('/data/workspace/wz/warehouse-meeting-candidates-20261028/graph-test/neo4j.env').read_text().splitlines():
    if line and not line.startswith('#'):
        key,value=line.split('=',1);os.environ[key]=value.strip().strip('"').strip("'")
assert os.environ['NEO4J_URI']=='bolt://127.0.0.1:17691'
from ops.warehouse_live_runtime import WarehouseLiveRuntime
from ops.warehouse_live import create_app
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
# Only the central process constructs one Runtime. Children are query-only.
ports=list(range(14200,14208));origins=[f'http://127.0.0.1:{port}' for port in ports]
listeners=subprocess.check_output(['ss','-H','-ltn'],text=True)
for port in ports:
    assert not any(line.split()[3].rsplit(':',1)[-1]==str(port) for line in listeners.splitlines()),('port already listening',port)
    with socket.socket() as check:check.bind(('127.0.0.1',port))
children=[];log_handles=[];startup=[]
for port in ports:
    log=(E/f'sidecar-{port}.stdout.log').open('xb');log_handles.append(log)
    args=[sys.executable,'-u',str(R/'deployment/free-live-query-sidecar-v7.py'),
        '--source-root',str(S),'--source-manifest',str(manifest_path),'--summary-root',str(OUT),
        '--warm-run-id',runtime.latest()['runId'],'--expected-neo4j-uri','bolt://127.0.0.1:17691',
        '--query-workers','3','--port',str(port)]
    started=time.perf_counter_ns();child=subprocess.Popen(args,stdout=log,stderr=subprocess.STDOUT,stdin=subprocess.DEVNULL,start_new_session=True)
    children.append(child);startup.append({'port':port,'pid':child.pid,'started_ns':started,'args':args})
for record,child in zip(startup,children):
    deadline=time.monotonic()+90
    while True:
        if child.poll() is not None:raise RuntimeError(f'sidecar exited: {record}')
        try:
            with urllib.request.urlopen(f"http://127.0.0.1:{record['port']}/api/health",timeout=2) as response:health=json.load(response)
            break
        except Exception:
            if time.monotonic()>deadline:raise TimeoutError(f'sidecar readiness: {record}')
            time.sleep(.1)
    assert health['runtime_created'] is False and health['catalog_workers']==health['max_Neo4j_query_connections']==3
    assert health['source_identity']['source_manifest_sha256']==hashlib.sha256(manifest_path.read_bytes()).hexdigest()
    assert health['source_identity']['verified_files']==239 and health['source_identity']['manifest_verified'] is True
    record.update(ready_ns=time.perf_counter_ns(),health=health)
app=create_app(runtime,datasets,S/'dist',catalog_factory=None,catalog_upstreams=origins,catalog_workers=24,
    catalog_description=QueryCatalogService.describe(),own_runtime=False)

class ObservedASGI:
    def __init__(self,application):self.application=application;self.active=0
    async def __call__(self,scope,receive,send):
        if scope['type']!='http' or scope['path']!='/api/knowledge/query':
            return await self.application(scope,receive,send)
        start=time.perf_counter_ns();self.active+=1;at_start=self.active
        request_id=dict(scope.get('headers',[])).get(b'x-probe-id',b'').decode('ascii','replace')
        status=None;complete=False;request_body_end=None
        async def observed_receive():
            nonlocal request_body_end
            message=await receive()
            if message['type']=='http.request' and not message.get('more_body',False) and request_body_end is None:
                request_body_end=time.perf_counter_ns()
            return message
        def finished(end,body_completed):
            self.active-=1
            events.put({'kind':'api','request_id':request_id,'start_ns':start,'end_ns':end,
                'inflight_at_start':at_start,'status':status,'body_send_completed':body_completed,'request_body_end_ns':request_body_end,
                'end_semantics':'last_response_body_send_completed' if body_completed else 'application_exit_without_final_body'})
        async def observed_send(message):
            nonlocal status,complete
            if message['type']=='http.response.start':status=message['status']
            await send(message)
            if message['type']=='http.response.body' and not message.get('more_body',False):
                end=time.perf_counter_ns();complete=True;finished(end,True)
        try:return await self.application(scope,observed_receive,observed_send)
        finally:
            if not complete:finished(time.perf_counter_ns(),False)

ready={'port':14189,'catalog_workers':24,'sidecars':startup,'source_full_manifest_sha256':hashlib.sha256(manifest_path.read_bytes()).hexdigest(),'run_id':runtime.latest()['runId'],'source':str(S),'output_root':str(OUT),
    'initial_snapshot':runtime.latest(),'prewarm':[x['health']['startup_preparation'] for x in startup],'wrapper_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
    'observations':'external observer wrappers delegate original context/event unchanged; no artificial wait or barrier'}
(E/'ready.json').write_text(json.dumps(ready,indent=2),encoding='utf-8')
try:
    import uvicorn
    uvicorn.run(ObservedASGI(app),host='127.0.0.1',port=14189,workers=1,log_level='warning',access_log=False,
        timeout_graceful_shutdown=120)
finally:
    runtime.shutdown();events.put(None);observer.join(30)
    for child in children:
        if child.poll() is None:child.terminate()
    for child in children:
        try:child.wait(120)
        except subprocess.TimeoutExpired:pass
    for log in log_handles:log.close()
