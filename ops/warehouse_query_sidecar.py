"""Query-only loopback service; never constructs a simulation Runtime or writer.

Each process has at most three query threads and one Neo4j connection per
thread. Eight independent instances therefore use at most 24 query connections;
the producer's provider(4) and writer(1) fit within the agreed total budget29.
No request is retried and admission waiting remains inside service timing.
"""
import argparse
import asyncio
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
import hashlib
import json
import math
import os
from pathlib import Path,PurePosixPath
import re
import sys
import time
from urllib.parse import urlsplit


ROOT=Path(__file__).resolve().parents[1]
QUERY_POINT_VERSION='warehouse-live-query-point-v1'
MAX_QUERY_POINT_BYTES=8192


def _point_options(live_query_point,expected_runtime_instance):
    if (live_query_point is None)!=(expected_runtime_instance is None):
        raise ValueError('live_query_point and expected_runtime_instance must be supplied together')
    if live_query_point is not None:
        if (not isinstance(expected_runtime_instance,str)
                or re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}',expected_runtime_instance) is None):
            raise ValueError('invalid expected runtime instance')
        return Path(live_query_point).resolve()
    return None


def _read_live_query_point(path,expected_runtime_instance):
    """Read only the bounded atomic point; plain/unverified snapshots are refused."""
    from ops.warehouse_live import _body
    from backend.real_scale.store import identity,integer
    with Path(path).open('rb') as stream:
        raw=stream.read(MAX_QUERY_POINT_BYTES+1)
    if len(raw)>MAX_QUERY_POINT_BYTES:raise ValueError('live query point exceeds 8 KiB')
    envelope=_body(raw)
    if set(envelope)!={'format','sha256','payload'} or envelope.get('format')!='warehouse-checkpoint-v2':
        raise ValueError('live query point requires a SHA checkpoint envelope')
    payload=envelope.get('payload')
    if not isinstance(payload,dict):raise ValueError('live query point payload must be an object')
    canonical=json.dumps(payload,ensure_ascii=False,sort_keys=True,separators=(',',':'),allow_nan=False)
    if hashlib.sha256(canonical.encode('utf-8')).hexdigest()!=envelope.get('sha256'):
        raise ValueError('live query point SHA256 mismatch')
    if set(payload)!={'version','run_id','seq','runtime_instance_id','source_sha256'}:
        raise ValueError('invalid live query point fields')
    if payload['version']!=QUERY_POINT_VERSION:raise ValueError('unsupported live query point version')
    identity(payload['runtime_instance_id'],'runtime instance')
    if payload['runtime_instance_id']!=expected_runtime_instance:
        raise ValueError('live query point runtime owner mismatch')
    identity(payload['run_id'],'run');integer(payload['seq'],'seq')
    if (not isinstance(payload['source_sha256'],str)
            or re.fullmatch('[0-9a-f]{64}',payload['source_sha256']) is None):
        raise ValueError('invalid live query point source SHA256')
    return payload


def create_app(*,catalog_factory,catalog_description=None,catalog_workers=3,
               expected_neo4j_uri=None,source_identity=None,startup_preparation=None,
               live_query_point=None,expected_runtime_instance=None,
               heavy_workers=0,max_lane_queue=128,queue_timeout=15.):
    if type(catalog_workers) is not int or not 1<=catalog_workers<=3:
        raise ValueError('sidecar catalog_workers must be an integer within 1..3')
    if type(heavy_workers) is not int or not 0<=heavy_workers<catalog_workers:
        raise ValueError('heavy_workers must be an integer below catalog_workers, or zero for original shared lane')
    if type(max_lane_queue) is not int or not 0<=max_lane_queue<=10000:
        raise ValueError('max_lane_queue must be an integer within 0..10000')
    if (type(queue_timeout) not in (float,int) or not math.isfinite(queue_timeout)
            or not 0<queue_timeout<=300):raise ValueError('queue_timeout must be within (0,300]')
    if not callable(catalog_factory):raise ValueError('query catalog factory required')
    if expected_neo4j_uri is not None and os.environ.get('NEO4J_URI')!=expected_neo4j_uri:
        raise ValueError('sidecar Neo4j endpoint differs from expected_neo4j_uri')
    point_path=_point_options(live_query_point,expected_runtime_instance)
    from fastapi import FastAPI,Request
    from fastapi.responses import JSONResponse
    from ops.warehouse_live import CatalogExecutor,_body
    from backend.real_scale.store import identity,integer

    lanes={}
    allocations=({'light':catalog_workers-heavy_workers,'heavy':heavy_workers} if heavy_workers
                 else {'shared':catalog_workers})
    for name,workers in allocations.items():
        lanes[name]={'workers':workers,'executor':ThreadPoolExecutor(max_workers=workers,
            thread_name_prefix='sidecar-'+name),'queries':CatalogExecutor(catalog_factory),
            'slots':asyncio.Semaphore(workers),'pending':0,'rejected':0,'timed_out':0}

    class AdmissionError(Exception):
        def __init__(self,message,queue_ms=0.):super().__init__(message);self.queue_ms=queue_ms

    async def admitted_execute(lane,body,observation):
        # All admission mutations occur on this process's single event loop.
        # Opt-in lanes divide the original total threads/connections budget.
        if heavy_workers and lane['pending']>=lane['workers']+max_lane_queue:
            lane['rejected']+=1
            raise AdmissionError('query admission queue full')
        lane['pending']+=1;acquired=False;deferred=False;queued=time.perf_counter()
        try:
            if heavy_workers:
                try:await asyncio.wait_for(lane['slots'].acquire(),timeout=queue_timeout)
                except asyncio.TimeoutError:
                    lane['timed_out']+=1
                    raise AdmissionError('query admission queue timeout',(time.perf_counter()-queued)*1000) from None
            else:await lane['slots'].acquire()
            acquired=True;waited=(time.perf_counter()-queued)*1000
            observation['queue_ms']=waited
            future=asyncio.get_running_loop().run_in_executor(lane['executor'],lane['queries'].execute,body)
            try:return await asyncio.shield(future),waited
            except asyncio.CancelledError:
                # A disconnected requester must not free a slot while its Bolt
                # operation is still running. The original operation is not retried.
                deferred=True
                def release_when_done(done):
                    lane['slots'].release();lane['pending']-=1
                    if not done.cancelled():done.exception()
                future.add_done_callback(release_when_done)
                raise
        finally:
            if not deferred:
                if acquired:lane['slots'].release()
                lane['pending']-=1

    @asynccontextmanager
    async def lifespan(app):
        try:yield
        finally:
            for lane in lanes.values():
                await asyncio.to_thread(lane['executor'].shutdown,wait=True)
                lane['queries'].close()

    app=FastAPI(title='Warehouse query-only sidecar',lifespan=lifespan)

    @app.get('/api/health')
    async def health():
        return {'status':'ok','kind':'query_only','runtime_created':False,
                'catalog_workers':catalog_workers,'admission_slots':catalog_workers,
                'max_Neo4j_query_connections':catalog_workers,'pool_size_per_worker':1,
                'heavy_workers':heavy_workers,'fair_lanes_enabled':bool(heavy_workers),
                'max_lane_queue':max_lane_queue if heavy_workers else None,
                'queue_timeout_seconds':queue_timeout if heavy_workers else None,
                'lanes':{name:{k:value[k] for k in ('workers','pending','rejected','timed_out')}
                         for name,value in lanes.items()},
                'source_identity':source_identity,'startup_preparation':startup_preparation}

    @app.get('/api/knowledge/catalog')
    async def catalog():return catalog_description or {'queries':{}}

    @app.post('/api/knowledge/query')
    async def query(request:Request):
        started=time.perf_counter();queue_ms=0.;lane_name=None;admission_rejection=False
        if point_path is not None:
            origin=request.headers.get('origin')
            if origin and urlsplit(origin).netloc!=request.headers.get('host'):
                response=JSONResponse({'error':'cross-origin controls are not allowed'},status_code=403)
                response.headers['X-Warehouse-Service-Ms']=str((time.perf_counter()-started)*1000)
                response.headers['X-Warehouse-Queue-Ms']='0.0'
                response.headers['Cache-Control']='no-store'
                return response
        try:
            body=_body(await request.body())
            # The gateway fixes the selected producer point before forwarding.
            # A query-only process never invents a run or a newer cutoff.
            if point_path is not None:
                try:point=_read_live_query_point(point_path,expected_runtime_instance)
                except (OSError,ValueError,KeyError,TypeError) as exc:
                    raise RuntimeError('live query point unavailable') from exc
                body.setdefault('run_id',point['run_id']);body.setdefault('seq',point['seq'])
                identity(body['run_id'],'run');integer(body['seq'],'seq')
                if body['run_id']!=point['run_id']:raise ValueError('stale or different live run rejected')
                if body['seq']>point['seq']:raise ValueError('requested seq exceeds the published live point')
            else:
                identity(body.get('run_id'),'run');integer(body.get('seq'),'seq')
            lane_name=('light' if body.get('query_id') in ('self','readiness') else 'heavy') if heavy_workers else 'shared'
            admission={'queue_ms':0.}
            try:result,queue_ms=await admitted_execute(lanes[lane_name],body,admission)
            finally:queue_ms=admission['queue_ms']
            response=JSONResponse(result)
        except AdmissionError as exc:
            queue_ms=exc.queue_ms;admission_rejection=True
            response=JSONResponse({'error':str(exc)},status_code=503)
        except (ValueError,KeyError,TypeError,json.JSONDecodeError) as exc:
            response=JSONResponse({'error':str(exc)},status_code=400)
        except Exception as exc:
            response=JSONResponse({'error':'query failed: '+type(exc).__name__},status_code=503)
        response.headers['X-Warehouse-Service-Ms']=str((time.perf_counter()-started)*1000)
        response.headers['X-Warehouse-Queue-Ms']=str(queue_ms)
        if lane_name:response.headers['X-Warehouse-Lane']=lane_name
        if admission_rejection:response.headers['X-Warehouse-Admission-Rejected']='1'
        response.headers['Cache-Control']='no-store'
        return response

    return app


def _source_identity(source,manifest):
    """Validate every expected source hash before creating a driver or listener."""
    source=Path(source).resolve();manifest=Path(manifest).resolve()
    raw=manifest.read_bytes();body=json.loads(raw)
    if not isinstance(body,dict):raise ValueError('source manifest JSON object required')
    files=body.get('files')
    if not isinstance(files,dict) or not files:raise ValueError('nonempty source manifest files required')
    for name,expected in files.items():
        if (not isinstance(name,str) or not name or '\\' in name or ':' in name
                or PurePosixPath(name).is_absolute() or '..' in PurePosixPath(name).parts):
            raise ValueError('source manifest path must stay relative to source root')
        path=(source/name).resolve()
        if not path.is_relative_to(source) or not path.is_file():raise ValueError('source manifest file absent/outside root: '+name)
        if not isinstance(expected,str) or re.fullmatch('[0-9a-f]{64}',expected) is None:
            raise ValueError('invalid source manifest SHA256: '+name)
        if hashlib.sha256(path.read_bytes()).hexdigest()!=expected:
            raise ValueError('source manifest SHA256 mismatch: '+name)
    own='ops/warehouse_query_sidecar.py'
    if own not in files or hashlib.sha256(Path(__file__).read_bytes()).hexdigest()!=files[own]:
        raise ValueError('executing sidecar is not covered by the verified source manifest')
    return {'source_root':str(source),'source_manifest':str(manifest),
            'source_manifest_sha256':hashlib.sha256(raw).hexdigest(),
            'verified_files':len(files),'manifest_verified':True}


def main(argv=None):
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--source-root',type=Path,default=ROOT)
    p.add_argument('--source-manifest',type=Path,help='expected frozen files/SHA256 manifest; default source-root/source-manifest.json')
    p.add_argument('--host',choices=('127.0.0.1','localhost','::1'),default='127.0.0.1')
    p.add_argument('--port',type=int,default=14200)
    p.add_argument('--query-workers',type=int,default=3)
    p.add_argument('--heavy-workers',type=int,default=0,help='opt-in reserve heavy workers within original total budget')
    p.add_argument('--max-lane-queue',type=int,default=128)
    p.add_argument('--queue-timeout',type=float,default=15.)
    p.add_argument('--event-count-cache',action='store_true',help='opt-in exact event-count cache with fresh graph fences')
    p.add_argument('--expected-neo4j-uri')
    p.add_argument('--summary-root',type=Path,help='same producer root containing run_id/journal.sqlite')
    p.add_argument('--query-summary-cache',type=Path)
    p.add_argument('--live-query-point',type=Path,help='explicit producer-owned atomic query point; never latest.json')
    p.add_argument('--expected-runtime-instance',help='required owner identity for --live-query-point')
    p.add_argument('--warm-run-id','--prepare-run',dest='prepare_run',
                   help='explicit startup preparation before HTTP readiness; never a request-time omission')
    args=p.parse_args(argv)
    if not 1<=args.query_workers<=3:p.error('--query-workers must be within 1..3')
    if not 0<=args.heavy_workers<args.query_workers:p.error('--heavy-workers must be below --query-workers, or zero')
    if not 0<=args.max_lane_queue<=10000:p.error('--max-lane-queue must be within 0..10000')
    if not math.isfinite(args.queue_timeout) or not 0<args.queue_timeout<=300:p.error('--queue-timeout must be within (0,300]')
    if not 1<=args.port<=65535:p.error('--port must be within 1..65535')
    try:_point_options(args.live_query_point,args.expected_runtime_instance)
    except (OSError,ValueError,TypeError) as exc:p.error(str(exc))
    source=args.source_root.resolve()
    if not (source/'backend/query_catalog/service.py').is_file():p.error('select the frozen query source tree')
    if args.expected_neo4j_uri is not None and os.environ.get('NEO4J_URI')!=args.expected_neo4j_uri:
        p.error('sidecar Neo4j endpoint differs from --expected-neo4j-uri')
    if args.prepare_run and args.summary_root is None:p.error('--prepare-run requires --summary-root')
    if args.query_summary_cache is not None and args.summary_root is None:
        p.error('--query-summary-cache requires --summary-root')
    try:source_identity=_source_identity(source,args.source_manifest or source/'source-manifest.json')
    except (OSError,ValueError,TypeError) as exc:p.error('source verification failed: '+str(exc))
    sys.path.insert(0,str(ROOT));sys.path.insert(0,str(source))
    from backend.query_catalog import QueryCatalogService
    from backend.real_scale.store import identity
    options=({'summary_root':args.summary_root,'summary_cache_root':args.query_summary_cache,
              'cache_fleet':True} if args.summary_root is not None else {})
    if args.event_count_cache:options.update(cache_event_counts=True)
    factory=lambda:QueryCatalogService.from_env(timeout=15,pool_size=1,**options)
    preparation=None
    if args.prepare_run:
        identity(args.prepare_run,'prepare run');started=time.perf_counter();warm=factory()
        try:preparation=warm.prepare_run(args.prepare_run)
        finally:warm.close()
        preparation={'phase':'explicit sidecar startup preparation before HTTP readiness',
                     'total_wall_seconds':time.perf_counter()-started,'result':preparation}
        print(json.dumps({'query_summary_startup':preparation}),flush=True)
    app=create_app(catalog_factory=factory,catalog_description=QueryCatalogService.describe(),
        catalog_workers=args.query_workers,expected_neo4j_uri=args.expected_neo4j_uri,
        source_identity=source_identity,startup_preparation=preparation,
        live_query_point=args.live_query_point,expected_runtime_instance=args.expected_runtime_instance,
        heavy_workers=args.heavy_workers,max_lane_queue=args.max_lane_queue,queue_timeout=args.queue_timeout)
    # All flags are explicit; zero heavy workers keeps the original shared lane.
    print(json.dumps({'query_sidecar_startup':{'kind':'query_only','runtime_created':False,
        'source_identity':source_identity,'host':args.host,'port':args.port,
        'catalog_workers':args.query_workers,'pool_size_per_worker':1,
        'heavy_workers':args.heavy_workers,'event_count_cache':args.event_count_cache,
        'startup_preparation':preparation}}),flush=True)
    import uvicorn
    uvicorn.run(app,host=args.host,port=args.port,workers=1,timeout_graceful_shutdown=120)
    return 0


if __name__=='__main__':raise SystemExit(main())
