"""External observer-only ASGI wrapper over frozen combined S2.

No injected delays/barriers, decision changes, source edits or request oracle.
"""
from pathlib import Path
import json,os,queue,sys,threading,time,traceback,hashlib,subprocess,urllib.request,socket,shutil,signal

R=Path('/data/workspace/wz/warehouse-free-ports-20261029');S=R/'source-combined-v7'
E=R/'live-joint-evidence-v9b';E.mkdir(exist_ok=False)
OUT=R/'live-joint-runs-v9b';sys.path.insert(0,str(S))
manifest_path=S/'source-s9-manifest.json';manifest=json.loads(manifest_path.read_text())
assert hashlib.sha256(manifest_path.read_bytes()).hexdigest()=='d057e847ff37f1ea4d29510e5dc81d66889b3ca0e14acd4aea894500626b42d7'
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

POINT=OUT/'live-query-point.json';runtime_ref=[]
from backend.real_scale import persistence
original_checkpoint_write=persistence.write_checkpoint

def checkpoint_write(path,payload):
    if Path(path).resolve()!=POINT.resolve():return original_checkpoint_write(path,payload)
    start=time.perf_counter_ns();outcome='error';observer_error=None;envelope_sha=None;payload_sha=None;summary=None;knowledge_seq=None
    try:
        value=original_checkpoint_write(path,payload);outcome='ok';return value
    finally:
        end=time.perf_counter_ns()
        try:
            if outcome=='ok':
                raw=Path(path).read_bytes();envelope_sha=hashlib.sha256(raw).hexdigest();payload_sha=json.loads(raw)['sha256']
            if runtime_ref:
                owner=runtime_ref[0];summary=dict(owner._query_summary_status or {});knowledge_seq=owner._sim.knowledge_seq
        except Exception as exc:observer_error=type(exc).__name__+': '+str(exc)
        events.put({'kind':'published_query_point','start_ns':start,'end_ns':end,'payload':dict(payload),
            'outcome':outcome,'envelope_sha256':envelope_sha,'payload_sha256':payload_sha,
            'cached_summary':summary,'cached_knowledge_seq':knowledge_seq,'observer_error':observer_error})
persistence.write_checkpoint=checkpoint_write
scene=S/'data/layouts/expansion-candidates/balanced/layout.json' 
datasets={
    'logical19':{'scene':scene,'dataset':R/'inputs/logical19','config_json':R/'inputs/combined-free-config.json',
        'disturbances_json':R/'inputs/soc250.json','label':'Logical ports / 19 tasks / required low SOC'},
    'logical1728':{'scene':scene,'dataset':R/'inputs/logical-boundaries','config_json':R/'inputs/combined-free-config.json',
        'label':'Logical ports / 1728 peak / 100 robots'},
}
runtime=WarehouseLiveRuntime(source_root=S,output_root=OUT,datasets=datasets,dataset_id='logical19',
    commit_seconds=30,knowledge_mode='required',knowledge_backend='neo4j',resource_graph=True,
    expected_neo4j_uri='bolt://127.0.0.1:17691',query_summary_root=OUT,query_point_path=POINT,wall_interval=1.)
runtime_ref.append(runtime)
# Only the central process constructs one Runtime. Children are query-only.
ports=list(range(14200,14208));origins=[f'http://127.0.0.1:{port}' for port in ports]
listeners=subprocess.check_output(['ss','-H','-ltn'],text=True)
for port in ports+[14189,14190]:
    assert not any(line.split()[3].rsplit(':',1)[-1]==str(port) for line in listeners.splitlines()),('port already listening',port)
    with socket.socket() as check:
        check.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1);check.bind(('127.0.0.1',port))
children=[];log_handles=[];startup=[]
for port in ports:
    log=(E/f'sidecar-{port}.stdout.log').open('xb');log_handles.append(log)
    args=[sys.executable,'-u',str(R/'deployment/free-live-query-sidecar-v9b.py'),
        '--source-root',str(S),'--source-manifest',str(manifest_path),'--summary-root',str(OUT),
        '--warm-run-id',runtime.latest()['runId'],'--expected-neo4j-uri','bolt://127.0.0.1:17691',
        '--query-workers','3','--port',str(port),'--live-query-point',str(POINT),
        '--expected-runtime-instance',runtime.runtime_instance_id]
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
    assert health['source_identity']['verified_files']==242 and health['source_identity']['manifest_verified'] is True
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

# The owner remains private; the native front door carries every public query.
import uvicorn
private_server=uvicorn.Server(uvicorn.Config(ObservedASGI(app),host='127.0.0.1',port=14190,workers=1,
    log_level='warning',access_log=False,timeout_graceful_shutdown=120))
private_thread=threading.Thread(target=private_server.run,name='private-owned-http',daemon=True);private_thread.start()
deadline=time.monotonic()+45
while True:
    try:
        with urllib.request.urlopen('http://127.0.0.1:14190/api/snapshot',timeout=2) as response:private_initial=json.load(response)
        break
    except Exception:
        if time.monotonic()>deadline:raise TimeoutError('private owner readiness')
        time.sleep(.1)
assert private_initial['runId']==runtime.latest()['runId'] and private_initial['elapsed']==0
native_prefix=E/'native-nginx';(native_prefix/'logs').mkdir(parents=True)
for name in ('client-body','proxy-temp','fastcgi-temp','uwsgi-temp','scgi-temp'):(native_prefix/'logs'/name).mkdir()
config=native_prefix/'query-gateway.conf';shutil.copyfile(S/'config/query-gateway-nginx.conf.example',config)
assert hashlib.sha256(config.read_bytes()).hexdigest()=='88bf4c077a50ac152c32c170bb46862bbc8e7cbde08ceda02632fe73dc053615'
binary=R/'native-nginx-deb-v1/extracted/usr/sbin/nginx'
assert hashlib.sha256(binary.read_bytes()).hexdigest()=='40ab7ce04f0d0af77e54bc73055131a8f1330c7ec5f4763fb5e8b8bb4fe8a2d8'
args=[str(binary),'-p',str(native_prefix)+'/', '-c',str(config)]
with (native_prefix/'config-test.log').open('xb') as log:subprocess.run(args+['-t'],stdout=log,stderr=subprocess.STDOUT,check=True,timeout=30)
native_log=(native_prefix/'startup.stderr.log').open('xb')
native=subprocess.Popen(args+['-g','daemon off;'],stdout=native_log,stderr=subprocess.STDOUT,stdin=subprocess.DEVNULL,start_new_session=True)
deadline=time.monotonic()+30
while True:
    if native.poll() is not None:raise RuntimeError('owned native front door exited')
    try:
        with urllib.request.urlopen('http://127.0.0.1:14189/api/snapshot',timeout=2) as response:public_initial=json.load(response)
        break
    except Exception:
        if time.monotonic()>deadline:raise TimeoutError('native front door readiness')
        time.sleep(.1)
assert public_initial['runId']==runtime.latest()['runId'] and public_initial['elapsed']==0
native_identity={'pid':native.pid,'binary':str(binary),'binary_sha256':hashlib.sha256(binary.read_bytes()).hexdigest(),
    'config':str(config),'config_sha256':hashlib.sha256(config.read_bytes()).hexdigest(),'prefix':str(native_prefix),
    'argv':args+['-g','daemon off;'],'private_owner_port':14190,'public_port':14189,'source':'workspace extracted binary; no system installation'}
ready={'port':14189,'private_port':14190,'catalog_workers':24,'runtime_instance_id':runtime.runtime_instance_id,'point_file':str(POINT),
    'native_nginx':native_identity,'sidecars':startup,'source_full_manifest_sha256':hashlib.sha256(manifest_path.read_bytes()).hexdigest(),
    'run_id':runtime.latest()['runId'],'source':str(S),'output_root':str(OUT),'initial_snapshot':runtime.latest(),
    'prewarm':[x['health']['startup_preparation'] for x in startup],'wrapper_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
    'observations':'API ASGI intervals are on eight query-only sidecars; client full end-to-end includes Native; no artificial wait or retry'}
(E/'ready.json').write_text(json.dumps(ready,indent=2),encoding='utf-8')
stop=threading.Event()
for signum in (signal.SIGTERM,signal.SIGINT):signal.signal(signum,lambda *args:stop.set())
try:
    while not stop.wait(.5):
        if not private_thread.is_alive() or native.poll() is not None or any(child.poll() is not None for child in children):
            raise RuntimeError('owned service child stopped unexpectedly')
finally:
    if native.poll() is None:native.terminate()
    private_server.should_exit=True;private_thread.join(130)
    runtime.shutdown()
    for child in children:
        if child.poll() is None:child.terminate()
    for child in children:
        try:child.wait(120)
        except subprocess.TimeoutExpired:pass
    try:native.wait(30)
    except subprocess.TimeoutExpired:pass
    events.put(None);observer.join(30)
    for log in log_handles:log.close()
    native_log.close()
