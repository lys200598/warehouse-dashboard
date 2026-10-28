"""Single-writer interactive V5 console, separate from immutable batch runs.

The HTTP/WS layer never advances simulation. WarehouseLiveRuntime owns every
physics/control/write operation. Source root is selected before backend imports.
"""
import argparse
import asyncio
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
import hashlib
import json
import math
from pathlib import Path
import sys
import threading
import time
from urllib.parse import urlsplit


ROOT=Path(__file__).resolve().parents[1]


def load_registry(path):
    path=Path(path).resolve();data=json.loads(path.read_text(encoding='utf-8'))
    if data.get('version')!='warehouse-live-datasets-v1' or not isinstance(data.get('datasets'),dict):
        raise ValueError('versioned dataset registry required')
    base=(path.parent/data.get('base_directory','.')).resolve();datasets={}
    for ident,raw in data['datasets'].items():
        if not isinstance(ident,str) or not ident or not isinstance(raw,dict):raise ValueError('invalid dataset alias')
        entry=dict(raw)
        for name in ('scene','dataset','disturbances_json'):
            if entry.get(name) is not None:entry[name]=str((base/entry[name]).resolve())
        if not entry.get('scene') or not entry.get('dataset'):raise ValueError('dataset scene/path required')
        datasets[ident]=entry
    return datasets,data.get('default_dataset')


def dataset_descriptions(datasets,selected=None):
    result=[]
    for ident,item in datasets.items():
        directory=Path(item['dataset']);manifest=directory/'manifest.json';scene=Path(item['scene'])
        available=scene.is_file() and all((directory/n).is_file() for n in ('tasks.csv','initial_inventory.csv','manifest.json'))
        row={'id':ident,'label':item.get('label',ident),'available':available,'task_count':None,'manifest_sha256':None}
        if available:
            try:
                raw=manifest.read_bytes();data=json.loads(raw)
                row.update(task_count=data['tasks'],duration_seconds=data['duration_seconds'],
                    manifest_sha256=hashlib.sha256(raw).hexdigest(),days=data.get('days'),scenario=data.get('scenario'))
            except (KeyError,ValueError,OSError):row.update(available=False,error='manifest unreadable; validate the input first')
        else:row['error']='input absent; generate or register the dataset before creating a run'
        result.append(row)
    return {'datasets':result,'selected_dataset_id':selected}


class CatalogExecutor:
    """One catalog instance per bounded worker; no cross-thread metric mutation."""
    def __init__(self,factory):
        self.factory=factory;self.local=threading.local();self.services=[];self.lock=threading.Lock()

    def execute(self,request):
        if not hasattr(self.local,'service'):
            self.local.service=self.factory()
            with self.lock:self.services.append(self.local.service)
        return self.local.service.execute(request)

    def close(self):
        for service in self.services:service.close()


def _body(raw):
    if len(raw)>65536:raise ValueError('request exceeds 64 KiB')
    def pairs(values):
        result={}
        for key,value in values:
            if key in result:raise ValueError('duplicate JSON field')
            result[key]=value
        return result
    def invalid(value):raise ValueError('nonfinite JSON number')
    value=json.loads(raw,object_pairs_hook=pairs,parse_constant=invalid)
    if not isinstance(value,dict):raise ValueError('JSON object required')
    return value


def create_app(runtime,datasets,dist,*,catalog_factory=None,catalog_description=None,own_runtime=True):
    from fastapi import FastAPI,Request,WebSocket
    from fastapi.responses import JSONResponse,RedirectResponse
    from fastapi.staticfiles import StaticFiles
    from starlette.websockets import WebSocketDisconnect

    dist=Path(dist).resolve()
    if not (dist/'live.html').is_file():raise ValueError('live.html is absent from the selected static directory')
    executor=ThreadPoolExecutor(max_workers=4,thread_name_prefix='live-catalog')
    queries=CatalogExecutor(catalog_factory) if catalog_factory else None
    query_slots=asyncio.Semaphore(4)

    @asynccontextmanager
    async def lifespan(app):
        try:yield
        finally:
            if own_runtime:await asyncio.to_thread(runtime.shutdown)
            await asyncio.to_thread(executor.shutdown,wait=True)
            if queries:queries.close()

    app=FastAPI(title='Warehouse V5 live console',lifespan=lifespan)

    @app.middleware('http')
    async def loopback_origin(request,call_next):
        if request.method not in ('GET','HEAD','OPTIONS'):
            origin=request.headers.get('origin')
            if origin and urlsplit(origin).netloc!=request.headers.get('host'):
                return JSONResponse({'error':'cross-origin controls are not allowed'},status_code=403)
        response=await call_next(request)
        if request.url.path.startswith('/api/'):response.headers['Cache-Control']='no-store'
        return response

    @app.get('/')
    async def index():return RedirectResponse('/live.html')

    @app.get('/api/snapshot')
    async def snapshot():return runtime.latest()

    @app.get('/api/scene')
    async def scene():return runtime.scene()

    @app.get('/api/health')
    async def health():
        frame=runtime.latest();state=runtime.status()
        return {'status':'degraded' if frame.get('status')=='failed' else 'ok','runtime':state,
            'run_id':frame.get('runId'),'simulation_status':frame.get('status'),
            'knowledge':frame.get('knowledge',{}),'metadata':frame.get('metadata',{}),
            'graph_queries_enabled':queries is not None,'sourceKind':'live_simulation_memory'}

    @app.get('/api/datasets')
    async def list_datasets():
        return dataset_descriptions(datasets,runtime.latest().get('metadata',{}).get('dataset_id'))

    @app.get('/api/knowledge/status')
    async def knowledge_status():return runtime.latest().get('knowledge',{})

    @app.get('/api/knowledge/catalog')
    async def catalog():return catalog_description or {'queries':{},'error':'catalog not configured'}

    @app.post('/api/control')
    async def control(request:Request):
        try:
            body=_body(await request.body());allowed={'action','value','dataset_id','expected_run_id'}
            if set(body)-allowed:raise ValueError('unknown control field')
            action=body.get('action');value=body.get('value')
            if action not in ('start','resume','pause','step','speed','checkpoint','new_run'):
                raise ValueError('choose start, pause, step, speed, checkpoint or new_run')
            if action=='new_run':
                ident=body.get('dataset_id')
                if ident not in datasets:raise ValueError('choose a registered dataset alias')
                if not next(r['available'] for r in dataset_descriptions(datasets)['datasets'] if r['id']==ident):
                    raise ValueError('dataset input is unavailable')
                value={'dataset_id':ident}
            if action=='speed' and (type(value) not in (int,float) or not math.isfinite(value) or not .1<=value<=120):
                raise ValueError('wall-clock speed must be within 0.1..120')
            expected=body.get('expected_run_id',runtime.latest().get('runId'))
            result=await asyncio.shield(asyncio.wrap_future(runtime.submit(action,value,expected_run_id=expected)))
            return {'snapshot':runtime.latest(),'control':{'action':action,'accepted':True}}
        except (ValueError,KeyError,TypeError,json.JSONDecodeError) as exc:
            return JSONResponse({'error':str(exc)},status_code=400)
        except RuntimeError as exc:return JSONResponse({'error':str(exc)},status_code=409)

    @app.post('/api/knowledge/query')
    async def query(request:Request):
        if not queries:return JSONResponse({'error':'Neo4j queries are disabled in journal/rules mode'},status_code=503)
        started=time.perf_counter()
        try:
            body=_body(await request.body());frame=runtime.latest();knowledge=frame.get('knowledge',{})
            run=frame['runId'];cutoff=knowledge.get('mirrored_seq',0)
            body.setdefault('run_id',run);body.setdefault('seq',cutoff)
            if body['run_id']!=run:raise ValueError('query run must match the currently selected live run')
            if type(body['seq']) is not int or not 1<=body['seq']<=cutoff:
                raise ValueError('query cutoff must be within this run mirrored watermark')
            async with query_slots:
                result=await asyncio.get_running_loop().run_in_executor(executor,queries.execute,body)
            response=JSONResponse(result)
            response.headers['X-Warehouse-Service-Ms']=str((time.perf_counter()-started)*1000)
            return response
        except (ValueError,KeyError,TypeError,json.JSONDecodeError) as exc:
            return JSONResponse({'error':str(exc)},status_code=400)
        except Exception as exc:
            return JSONResponse({'error':'query failed: '+type(exc).__name__},status_code=503)

    @app.post('/api/tasks/priority')
    @app.post('/api/tasks/batch')
    async def fixed_dataset():
        return JSONResponse({'error':'fixed-dataset run: create a new registered dataset run instead of injecting random tasks'},status_code=405)

    @app.websocket('/ws/simulation')
    async def websocket(ws:WebSocket):
        origin=ws.headers.get('origin')
        if origin and urlsplit(origin).netloc!=ws.headers.get('host'):
            await ws.close(code=1008);return
        await ws.accept();last=None;sent=0.
        try:
            while True:
                frame=runtime.latest();key=(frame.get('runId'),frame.get('tick'),frame.get('status'),frame.get('speed'),
                    frame.get('knowledge',{}).get('mirrored_seq'),frame.get('capturedAtMs'))
                now=time.monotonic()
                if key!=last or now-sent>=2:
                    await ws.send_json(frame);last=key;sent=now
                await asyncio.sleep(.1)
        except (WebSocketDisconnect,RuntimeError):pass

    app.mount('/',StaticFiles(directory=str(dist),html=True),name='frontend')
    return app


def main(argv=None):
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--source-root',type=Path,default=ROOT)
    p.add_argument('--dist',type=Path,default=ROOT/'dist')
    p.add_argument('--registry',type=Path,default=ROOT/'config/live-datasets.json')
    p.add_argument('--output-root',type=Path,default=ROOT/'data/live-runs')
    p.add_argument('--dataset-id');p.add_argument('--mode',choices=('required','rules'),default='required')
    p.add_argument('--knowledge-backend',choices=('neo4j','journal'),default='neo4j')
    p.add_argument('--commit-seconds',type=int,default=5);p.add_argument('--resume-run-id')
    p.add_argument('--expected-neo4j-uri',help='refuse startup if the live database differs from this isolated endpoint')
    p.add_argument('--host',choices=('127.0.0.1','localhost','::1'),default='127.0.0.1')
    p.add_argument('--port',type=int,default=14185)
    args=p.parse_args(argv)
    source=args.source_root.resolve()
    if not (source/'backend/knowledge_runtime/sim.py').is_file():p.error('select the actual producer source tree')
    datasets,default=load_registry(args.registry);selected=args.dataset_id or default
    available={r['id']:r['available'] for r in dataset_descriptions(datasets)['datasets']}
    if not available.get(selected):p.error('selected dataset is unavailable; generate/register its input first')
    if args.mode=='required' and args.knowledge_backend!='neo4j':p.error('required mode needs Neo4j')
    sys.path.insert(0,str(ROOT));sys.path.insert(0,str(source))
    from ops.warehouse_live_runtime import WarehouseLiveRuntime
    from backend.query_catalog import QueryCatalogService
    # An optional, not-yet-generated dataset stays visible as unavailable in
    # the UI; the owner only receives existing inputs it can actually validate.
    active_datasets={key:spec for key,spec in datasets.items() if available.get(key)}
    runtime=WarehouseLiveRuntime(source_root=source,output_root=args.output_root,datasets=active_datasets,dataset_id=selected,
        commit_seconds=args.commit_seconds,knowledge_mode=args.mode,knowledge_backend=args.knowledge_backend,
        wall_interval=1.,resume_run_id=args.resume_run_id,expected_neo4j_uri=args.expected_neo4j_uri)
    factory=(lambda:QueryCatalogService.from_env(timeout=15,pool_size=1)) if args.knowledge_backend=='neo4j' else None
    try:
        app=create_app(runtime,datasets,args.dist,catalog_factory=factory,catalog_description=QueryCatalogService.describe(),own_runtime=False)
        import uvicorn
        uvicorn.run(app,host=args.host,port=args.port,workers=1,timeout_graceful_shutdown=120)
    finally:runtime.shutdown()


if __name__=='__main__':raise SystemExit(main())
