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
import os
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
        for name in ('scene','dataset','disturbances_json','config_json'):
            if entry.get(name) is not None:entry[name]=str((base/entry[name]).resolve())
        if not entry.get('scene') or not entry.get('dataset'):raise ValueError('dataset scene/path required')
        datasets[ident]=entry
    return datasets,data.get('default_dataset')


def dataset_descriptions(datasets,selected=None):
    result=[]
    for ident,item in datasets.items():
        directory=Path(item['dataset']);manifest=directory/'manifest.json';scene=Path(item['scene'])
        available=scene.is_file() and all((directory/n).is_file() for n in ('tasks.csv','initial_inventory.csv','manifest.json'))
        available=available and all(not item.get(n) or Path(item[n]).is_file() for n in ('config_json','disturbances_json'))
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


def create_app(runtime,datasets,dist,*,catalog_factory=None,catalog_description=None,own_runtime=True,
               catalog_workers=4,catalog_upstreams=None,kg_graph=None):
    if type(catalog_workers) is not int or not 1<=catalog_workers<=24:
        raise ValueError('catalog_workers must be an integer within 1..24')
    from fastapi import FastAPI,Request,WebSocket
    from fastapi.responses import JSONResponse,RedirectResponse,Response
    from fastapi.staticfiles import StaticFiles
    from starlette.websockets import WebSocketDisconnect

    dist=Path(dist).resolve()
    if not (dist/'index.html').is_file():raise ValueError('index.html is absent from the selected static directory')
    if catalog_upstreams is not None and catalog_factory is not None:
        raise ValueError('choose local catalog workers or query-only upstreams')
    proxy=None
    if catalog_upstreams is not None:
        from ops.warehouse_query_proxy import HttpCatalogClient
        proxy=HttpCatalogClient(catalog_upstreams)
    executor=ThreadPoolExecutor(max_workers=catalog_workers,thread_name_prefix='live-catalog')
    queries=CatalogExecutor(catalog_factory) if catalog_factory else None
    query_slots=asyncio.Semaphore(catalog_workers)

    @asynccontextmanager
    async def lifespan(app):
        try:yield
        finally:
            if own_runtime:await asyncio.to_thread(runtime.shutdown)
            await asyncio.to_thread(executor.shutdown,wait=True)
            if queries:queries.close()
            if proxy:await proxy.close()
            if kg_graph is not None:await asyncio.to_thread(kg_graph.close)

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
    async def index():return RedirectResponse('/index.html')

    @app.get('/api/snapshot')
    async def snapshot():return runtime.latest()

    @app.get('/api/scene')
    async def scene():return runtime.scene()

    @app.get('/api/scene-point')
    async def scene_point():return runtime.scene_point()

    @app.get('/api/health')
    async def health():
        frame=runtime.latest();state=runtime.status()
        return {'status':'degraded' if frame.get('status')=='failed' else 'ok','runtime':state,
            'run_id':frame.get('runId'),'simulation_status':frame.get('status'),
            'knowledge':frame.get('knowledge',{}),'metadata':frame.get('metadata',{}),
            'graph_queries_enabled':queries is not None or proxy is not None,'sourceKind':'live_simulation_memory'}

    @app.get('/api/datasets')
    async def list_datasets():
        return dataset_descriptions(datasets,runtime.latest().get('metadata',{}).get('dataset_id'))

    @app.get('/api/knowledge/status')
    async def knowledge_status():return runtime.latest().get('knowledge',{})

    @app.get('/api/knowledge/catalog')
    async def catalog():return catalog_description or {'queries':{},'error':'catalog not configured'}

    def kg_unavailable(exc=None):
        detail=str(exc) if exc is not None else 'Neo4j warehouse graph is not configured'
        return JSONResponse({'detail':detail,'backend':'neo4j-warehouse'},status_code=503)

    @app.get('/api/kg/warehouse/health')
    async def kg_health():
        if kg_graph is None:return kg_unavailable()
        return await asyncio.to_thread(kg_graph.health)

    @app.get('/api/kg/warehouse/dashboard')
    async def kg_dashboard(dataset_id: str | None = None):
        if kg_graph is None:return kg_unavailable()
        try:
            summary=await asyncio.to_thread(kg_graph.summary,dataset_id)
            health=await asyncio.to_thread(kg_graph.health)
            return {'source':'neo4j-knowledge-graph','health':health,'summary':summary}
        except Exception as exc:return kg_unavailable(exc)

    @app.get('/api/kg/warehouse/tasks')
    async def kg_tasks(limit: int = 8, dataset_id: str | None = None):
        if kg_graph is None:return kg_unavailable()
        try:return await asyncio.to_thread(kg_graph.task_list,limit,dataset_id)
        except Exception as exc:return kg_unavailable(exc)

    @app.get('/api/kg/warehouse/tasks/{task_id}/trace')
    async def kg_task_trace(task_id: str, dataset_id: str | None = None):
        if kg_graph is None:return kg_unavailable()
        try:return await asyncio.to_thread(kg_graph.task_execution_trace,task_id,dataset_id)
        except Exception as exc:return kg_unavailable(exc)

    @app.get('/api/kg/warehouse/query-presets')
    async def kg_presets():
        return {'presets':list(getattr(kg_graph,'QUERY_PRESETS',())) if kg_graph is not None else [],
                'readOnly':True,'maxRows':200}

    @app.post('/api/kg/warehouse/query-presets/{preset}')
    async def kg_preset(preset: str, request: Request):
        if kg_graph is None:return kg_unavailable()
        try:
            body=_body(await request.body())
            return await asyncio.to_thread(kg_graph.query_preset,preset,
                task_id=str(body.get('taskId',''))[:160],run_id=str(body.get('runId',''))[:160],
                robot_id=int(body.get('robotId',1)),tick=int(body.get('tick',0)))
        except (ValueError,TypeError,json.JSONDecodeError) as exc:
            return JSONResponse({'detail':str(exc)},status_code=400)
        except Exception as exc:return kg_unavailable(exc)

    @app.get('/api/kg/warehouse/runs/{run_id}/robots/{robot_id}/trace')
    async def kg_robot_trace(run_id: str, robot_id: int, tick: int | None = None, history_limit: int = 20):
        if kg_graph is None:return kg_unavailable()
        try:return await asyncio.to_thread(kg_graph.robot_trace_context,run_id,robot_id,tick,history_limit)
        except Exception as exc:return kg_unavailable(exc)

    @app.get('/api/kg/warehouse/runs/{run_id}/runtime-trace')
    async def kg_runtime_trace(run_id: str, tick: int | None = None, robot_id: int | None = None):
        if kg_graph is None:return kg_unavailable()
        try:return await asyncio.to_thread(kg_graph.runtime_state_trace,run_id,tick,robot_id)
        except ValueError as exc:return JSONResponse({'detail':str(exc)},status_code=400)
        except Exception as exc:return kg_unavailable(exc)

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
        if queries is None and proxy is None:
            return JSONResponse({'error':'Neo4j queries are disabled in journal/rules mode'},status_code=503)
        started=time.perf_counter()
        try:
            body=_body(await request.body())
            if hasattr(runtime,'query_point'):
                point=runtime.query_point();run,cutoff=point['run_id'],point['seq']
            else:
                # Compatibility for external read-only adapters, not the owned producer.
                frame=runtime.latest();run=frame['runId'];cutoff=frame.get('knowledge',{}).get('mirrored_seq',0)
            body.setdefault('run_id',run);body.setdefault('seq',cutoff)
            if body['run_id']!=run:raise ValueError('query run must match the currently selected live run')
            if type(body['seq']) is not int or not 1<=body['seq']<=cutoff:
                raise ValueError('query cutoff must be within this run mirrored watermark')
            async with query_slots:
                if proxy:
                    upstream=await proxy.execute(body)
                    response=Response(upstream.content,status_code=upstream.status_code,media_type='application/json')
                    if upstream.headers.get('x-warehouse-service-ms') is not None:
                        response.headers['X-Warehouse-Upstream-Ms']=upstream.headers['x-warehouse-service-ms']
                else:
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
    p.add_argument('--resource-graph',action='store_true',help='same-transaction rack resources for new runs')
    p.add_argument('--expected-neo4j-uri',help='refuse startup if the live database differs from this isolated endpoint')
    p.add_argument('--query-acceleration',choices=('baseline','summary'),default='baseline',
                   help='explicit journal-derived exact summaries and fenced immutable fleet fragments')
    p.add_argument('--query-summary-cache',type=Path,help='derived cache directory, never the original journal')
    p.add_argument('--summary-root',type=Path,help='journal root containing run_id/journal.sqlite; enables summary queries')
    p.add_argument('--query-workers',type=int,default=4,
                   help='catalog executor and admission slots, 1..24; each worker uses one Neo4j connection')
    p.add_argument('--query-upstream',action='append',help='query-only loopback HTTP origin; repeat up to eight times')
    p.add_argument('--query-point-file',type=Path,
                   help='new direct child of output-root; publish SHA-fenced points for native query routing')
    p.add_argument('--host',choices=('127.0.0.1','localhost','::1'),default='127.0.0.1')
    p.add_argument('--port',type=int,default=14185)
    p.add_argument('--max-active-robots',type=int,default=None,
                   help='optional 1..100 active execution slots; omitted keeps the dataset default (normally 100)')
    # Parse before selecting/importing a producer tree; importing backend here
    # would pin the wrong source when --source-root selects another release.
    p.add_argument('--mobility',choices=('legacy','wheeled'),default='legacy')
    p.add_argument('--ray',action='store_true',help='100 Ray motion actors; V5 dispatch/reservations remain central')
    p.add_argument('--ray-address',default=os.environ.get('RAY_ADDRESS'))
    p.add_argument('--ray-actor-cpus',type=float,default=0.1)
    p.add_argument('--robot-graph',action='store_true',help='each Ray actor writes its own robot graph subgraph')
    p.add_argument('--graph-state-interval',type=int,default=4,help='AgentState cadence per robot actor, 1..300 ticks')
    p.add_argument('--robot-agents',action='store_true')
    p.add_argument('--agent-query-seconds',type=int,default=30)
    p.add_argument('--agent-observation-seconds',type=int,default=30)
    p.add_argument('--agent-timeout',type=float,default=30.)
    p.add_argument('--agent-startup-timeout',type=float,default=120.)
    args=p.parse_args(argv)
    if args.max_active_robots is not None and not 1<=args.max_active_robots<=100:
        p.error('--max-active-robots must be within 1..100')
    if not 1<=args.query_workers<=24:p.error('--query-workers must be within 1..24')
    if args.query_upstream:
        from ops.warehouse_query_proxy import validate_upstreams
        try:validate_upstreams(args.query_upstream)
        except ValueError as error:p.error(str(error))
    if args.summary_root is not None:args.query_acceleration='summary'
    source=args.source_root.resolve()
    if not (source/'backend/knowledge_runtime/sim.py').is_file():p.error('select the actual producer source tree')
    datasets,default=load_registry(args.registry);selected=args.dataset_id or default
    available={r['id']:r['available'] for r in dataset_descriptions(datasets)['datasets']}
    if not available.get(selected):p.error('selected dataset is unavailable; generate/register its input first')
    if args.mode=='required' and args.knowledge_backend!='neo4j':p.error('required mode needs Neo4j')
    if args.query_acceleration=='summary' and args.knowledge_backend!='neo4j':p.error('summary queries need Neo4j point fences')
    if args.query_upstream and args.knowledge_backend!='neo4j':p.error('query-only upstreams need Neo4j mode')
    sys.path.insert(0,str(ROOT));sys.path.insert(0,str(source))
    from ops.warehouse_live_runtime import WarehouseLiveRuntime
    from backend.query_catalog import QueryCatalogService
    kg_graph=None
    try:
        from backend.warehouse_graph_query import Neo4jWarehouseGraph
        kg_graph=Neo4jWarehouseGraph(
            uri=os.environ.get('WAREHOUSE_NEO4J_URI') or os.environ.get('NEO4J_URI') or 'bolt://127.0.0.1:47687',
            user=os.environ.get('WAREHOUSE_NEO4J_USER') or os.environ.get('NEO4J_USER') or 'neo4j',
            password=os.environ.get('WAREHOUSE_NEO4J_PASSWORD') or os.environ.get('NEO4J_PASSWORD'),
            database=os.environ.get('WAREHOUSE_NEO4J_DATABASE') or os.environ.get('NEO4J_DATABASE') or 'neo4j',
            dataset_id=os.environ.get('WAREHOUSE_NEO4J_DATASET_ID'))
    except Exception:
        kg_graph=None
    # An optional, not-yet-generated dataset stays visible as unavailable in
    # the UI; the owner only receives existing inputs it can actually validate.
    active_datasets={key:spec for key,spec in datasets.items() if available.get(key)}
    runtime=WarehouseLiveRuntime(source_root=source,output_root=args.output_root,datasets=active_datasets,dataset_id=selected,
        commit_seconds=args.commit_seconds,knowledge_mode=args.mode,knowledge_backend=args.knowledge_backend,
        wall_interval=1.,resume_run_id=args.resume_run_id,expected_neo4j_uri=args.expected_neo4j_uri,resource_graph=args.resource_graph,
        query_summary_root=(args.summary_root or args.output_root) if args.query_acceleration=='summary' else None,
        query_summary_cache=args.query_summary_cache if args.query_acceleration=='summary' else None,
        query_point_path=args.query_point_file,mobility=args.mobility,max_active_robots=args.max_active_robots,
        ray=args.ray,ray_address=args.ray_address,ray_actor_cpus=args.ray_actor_cpus,
        robot_graph=args.robot_graph,graph_state_interval=args.graph_state_interval,
        robot_agents=args.robot_agents,agent_options=dict(query_interval=args.agent_query_seconds,
            observation_interval=args.agent_observation_seconds,timeout=args.agent_timeout,
            startup_timeout=args.agent_startup_timeout))
    if args.query_point_file is not None:
        print(json.dumps({'live_query_owner':{'runtime_instance_id':runtime.runtime_instance_id,
            'point':runtime.query_point(),'query_point_file':str(runtime.query_point_path)}}),flush=True)
    query_options=({'summary_root':args.summary_root or args.output_root,'summary_cache_root':args.query_summary_cache,
                    'cache_fleet':True} if args.query_acceleration=='summary' else {})
    factory=(lambda:QueryCatalogService.from_env(timeout=15,pool_size=1,**query_options)) if args.knowledge_backend=='neo4j' and not args.query_upstream else None
    try:
        if args.query_acceleration=='summary' and factory is not None:
            # Startup prewarm is before HTTP readiness and explicitly reported;
            # later request-side catch-up remains included in API service time.
            warm=factory()
            try:print(json.dumps({'query_summary_startup':warm.prepare_run(runtime.latest()['runId'])}),flush=True)
            finally:warm.close()
        app=create_app(runtime,datasets,args.dist,catalog_factory=factory,catalog_description=QueryCatalogService.describe(),
                       own_runtime=False,catalog_workers=args.query_workers,catalog_upstreams=args.query_upstream,
                       kg_graph=kg_graph)
        import uvicorn
        uvicorn.run(app,host=args.host,port=args.port,workers=1,timeout_graceful_shutdown=120)
    finally:runtime.shutdown()


if __name__=='__main__':raise SystemExit(main())
