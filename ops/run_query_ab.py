"""Owned read-only, sealed-prefix A/B orchestration for warehouse query services.

Never changes Neo4j schema/data, existing services, the original journal/oracle
or frozen source. New derived summaries, listeners, logs and clients belong to
the selected fresh output directory. All originals and failed samples survive.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
import gzip
import hashlib
import json
import os
from pathlib import Path
import shutil
import signal
import socket
import sqlite3
import subprocess
import sys
import time
import threading
import urllib.request


def sha(path):
    digest=hashlib.sha256()
    with Path(path).open('rb') as stream:
        for part in iter(lambda:stream.read(1024*1024),b''):digest.update(part)
    return digest.hexdigest()


def write(path,value):
    with Path(path).open('x',encoding='utf-8') as stream:json.dump(value,stream,indent=2,allow_nan=False)


def source_check(source):
    path=source/'source-manifest.json';manifest=json.loads(path.read_text())
    for name,expected in manifest['files'].items():
        target=(source/name).resolve()
        if not target.is_relative_to(source.resolve()) or sha(target)!=expected:raise ValueError('frozen source mismatch: '+name)
    return {'source':str(source),'manifest_sha256':sha(path),'verified_files':len(manifest['files'])}


def load_env(path):
    for line in Path(path).read_text().splitlines():
        if line.strip() and not line.startswith('#'):
            name,value=line.split('=',1);os.environ[name]=value.strip().strip('"').strip("'")


def worker(config):
    cfg=json.loads(Path(config).read_text());source=Path(cfg['source']);sys.path.insert(0,str(source))
    identity=source_check(source)
    from backend.query_catalog import QueryCatalogService
    from ops.warehouse_query_sidecar import create_app
    from ops.load_probe import LoadProbeMiddleware
    from ops.warehouse_live import CatalogExecutor
    import uvicorn
    observed={};observed_lock=threading.Lock()
    original_execute=CatalogExecutor.execute
    def observed_execute(self,body):
        value=original_execute(self,body)
        if body.get('query_id')=='global':
            # Wrapper-only successful execution evidence, bounded by thread count
            # for this fixed sealed point. No delay/barrier or request alteration.
            with observed_lock:
                observed[threading.get_ident()]={'thread_id':threading.get_ident(),
                    'run_id':body.get('run_id'),'seq':body.get('seq'),
                    'catalog_class':type(self.local.service).__name__,
                    'event_count_cache_hit':self.local.service.latest_metrics.get('event_count_cache_hit')}
        return value
    if cfg.get('observe_workers'):CatalogExecutor.execute=observed_execute
    factory=lambda:QueryCatalogService.from_env(timeout=15,pool_size=1,summary_root=cfg['journal_root'],
        summary_cache_root=cfg['summary_cache'],cache_fleet=True,catch_up_on_read=False,
        cache_event_counts=cfg['candidate'])
    app=create_app(catalog_factory=factory,catalog_description=QueryCatalogService.describe(),catalog_workers=3,
        heavy_workers=cfg.get('heavy_workers',1) if cfg['candidate'] else 0,source_identity=identity,
        expected_neo4j_uri=os.environ['NEO4J_URI'])
    if cfg.get('observe_workers'):
        @app.get('/api/benchmark/worker-state')
        async def worker_state():
            with observed_lock:return {'pid':os.getpid(),'global_threads':list(observed.values())}
    probe=LoadProbeMiddleware(app,output=cfg['trace'],source_identity={**identity,'run_id':cfg['run_id'],
        'seq':cfg['seq'],'candidate':cfg['candidate'],'stage':'outer_query_only_sidecar'})
    # Uvicorn restores and re-raises captured signals on recent versions. A
    # benign prior handler permits the outer finally to seal observer evidence.
    for signum in (signal.SIGTERM,signal.SIGINT):signal.signal(signum,lambda *args:None)
    try:uvicorn.run(probe,host='127.0.0.1',port=cfg['port'],workers=1,access_log=False,
                    timeout_graceful_shutdown=120,log_level='warning')
    finally:probe.close()


def read_json_url(url,timeout=2):
    with urllib.request.urlopen(url,timeout=timeout) as response:return json.load(response)


def wait_health(port,child):
    deadline=time.monotonic()+90
    while time.monotonic()<deadline:
        if child.poll() is not None:raise RuntimeError('owned sidecar exited before readiness')
        try:return read_json_url(f'http://127.0.0.1:{port}/api/health')
        except Exception:time.sleep(.1)
    raise TimeoutError('owned sidecar readiness deadline')


@contextmanager
def services(args,mode,summary_seed):
    root=args.output/mode;root.mkdir();children=[];handles=[];native=None;cleanup=[]
    try:
        startups=[]
        for index in range(8):
            cache=root/f'summary-cache-{index}';shutil.copytree(summary_seed,cache)
            config=root/f'sidecar-{index}.json'
            write(config,{'source':str(args.source),'journal_root':str(args.journal_root),
                'summary_cache':str(cache),'candidate':mode=='candidate','run_id':args.run_id,'seq':args.seq,
                'heavy_workers':args.candidate_heavy_workers,'observe_workers':args.fully_warm_heavy,
                'port':args.sidecar_port+index,'trace':str(root/f'server-{index}.jsonl')})
            log=(root/f'sidecar-{index}.log').open('xb');handles.append(log)
            command=[sys.executable,str(Path(__file__).resolve()),'--worker-config',str(config)]
            child=subprocess.Popen(command,stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
            children.append(child)
        for index,child in enumerate(children):
            startups.append({'index':index,'pid':child.pid,'port':args.sidecar_port+index,
                             'health':wait_health(args.sidecar_port+index,child)})
        prefix=root/'nginx';(prefix/'logs').mkdir(parents=True)
        for name in ('client-body','proxy-temp','fastcgi-temp','uwsgi-temp','scgi-temp'):(prefix/'logs'/name).mkdir()
        servers='\n'.join(f'        server 127.0.0.1:{args.sidecar_port+i} max_fails=0;' for i in range(8))
        config=prefix/'nginx.conf'
        config.write_text('''worker_processes 1;
pid logs/nginx.pid;
error_log logs/error.log warn;
events { worker_connections 2048; }
http {
    log_format warehouse escape=json '{"request_id":"$http_x_probe_id","duration_s":"$request_time","status":$status,"upstream":"$upstream_addr"}';
    access_log logs/access.jsonl warehouse;
    client_body_temp_path logs/client-body;
    proxy_temp_path logs/proxy-temp;
    fastcgi_temp_path logs/fastcgi-temp;
    uwsgi_temp_path logs/uwsgi-temp;
    scgi_temp_path logs/scgi-temp;
    upstream warehouse_queries {
        least_conn;
'''+servers+'''
        keepalive 128;
    }
    server {
        listen 127.0.0.1:'''+str(args.gateway_port)+''';
        proxy_http_version 1.1;
        proxy_next_upstream off;
        proxy_cache off;
        proxy_buffering off;
        proxy_request_buffering off;
        proxy_connect_timeout 15s;
        proxy_read_timeout 90s;
        proxy_send_timeout 90s;
        location = /api/knowledge/query {
            proxy_set_header Connection '';
            proxy_set_header Host $http_host;
            proxy_pass http://warehouse_queries;
        }
        location / { return 404; }
    }
}
''')
        command=[str(args.nginx),'-p',str(prefix)+'/', '-c',str(config)]
        with (prefix/'config-test.log').open('xb') as log:
            subprocess.run(command+['-t'],stdout=log,stderr=subprocess.STDOUT,check=True,timeout=30)
        native_log=(prefix/'startup.log').open('xb');handles.append(native_log)
        native=subprocess.Popen(command+['-g','daemon off;'],stdout=native_log,stderr=subprocess.STDOUT,start_new_session=True)
        deadline=time.monotonic()+15
        while time.monotonic()<deadline:
            if native.poll() is not None:raise RuntimeError('owned nginx exited')
            try:
                with socket.create_connection(('127.0.0.1',args.gateway_port),timeout=.2):break
            except OSError:time.sleep(.1)
        else:raise TimeoutError('nginx startup timeout')
        write(root/'service-identity.json',{'sidecars':startups,'nginx_pid':native.pid,
            'nginx_binary_sha256':sha(args.nginx),'nginx_config_sha256':sha(config),
            'max_query_threads':24,'max_query_connections':24,'created_unix_ns':time.time_ns()})
        yield root,children,native
    finally:
        if native is not None and native.poll() is None:
            native.send_signal(signal.SIGQUIT)
            try:native.wait(20)
            except subprocess.TimeoutExpired:native.terminate();native.wait(10)
        for child in children:
            if child.poll() is None:child.terminate()
        for child in children:
            try:child.wait(135)
            except subprocess.TimeoutExpired:child.kill();child.wait(10)
            cleanup.append({'pid':child.pid,'exitcode':child.returncode})
        for handle in handles:handle.close()
        write(root/'cleanup.json',{'owned_sidecars':cleanup,'nginx_exitcode':native.returncode if native else None})


def graph_observation(args):
    # Direct READ sessions avoid schema/bootstrap helpers and never run in a
    # measured request interval. All counts belong only to this one run.
    from neo4j import GraphDatabase,READ_ACCESS
    driver=GraphDatabase.driver(os.environ['NEO4J_URI'],auth=(os.environ.get('NEO4J_USER','neo4j'),os.environ['NEO4J_PASSWORD']),
                               max_connection_pool_size=1,max_transaction_retry_time=0)
    try:
        with driver.session(database=os.environ.get('NEO4J_DATABASE','neo4j'),default_access_mode=READ_ACCESS) as session:
            head=[dict(row) for row in session.run('''MATCH (r:WHRun {run_id:$run})
                MATCH (s:WHStep {key:$key}) RETURN r.committed_seq AS head,toString(r.created_at) AS created_at,
                    r.real_source_run AS source_run,s.seq AS step_seq,s.checksum AS checksum''',run=args.run_id,key=f'{args.run_id}:{args.seq}')]
            count=[dict(row) for row in session.run('''MATCH (n) WHERE n.run_id=$run
                RETURN count(n) AS nodes,
                  sum(CASE WHEN coalesce(n.seq,0)>$seq OR coalesce(n.commit_seq,0)>$seq THEN 1 ELSE 0 END) AS future_nodes''',run=args.run_id,seq=args.seq)]
        if len(head)!=1 or len(count)!=1:raise ValueError('single actual head/count required')
        return {'at_unix_ns':time.time_ns(),'run_id':args.run_id,'seq':args.seq,**head[0],**count[0]}
    finally:driver.close()


def full_verify(args,phase):
    from backend.real_scale.oracle import RealOracle
    from ops.verify_catalog_responses import JournalTruth,ResponseVerifier
    if args.cache_truth:
        from query_ab_validation import CachedJournalTruth
        truth_class=CachedJournalTruth
    else:truth_class=JournalTruth
    oracle=RealOracle.open(args.oracle);truth=truth_class(args.journal_root/args.run_id/'journal.sqlite',oracle)
    verifier=ResponseVerifier(truth);errors=[];count=0;outcomes={};started=time.perf_counter()
    try:
        for path in sorted(phase.glob('client-*.jsonl.gz')):
            with gzip.open(path,'rt',encoding='utf-8') as source:
                for line in source:
                    row=json.loads(line);count+=1
                    outcomes[row['outcome']]=outcomes.get(row['outcome'],0)+1
                    if row['outcome']!='ok':
                        errors.append({'request_id':row['request_id'],'error':row['outcome']});continue
                    try:
                        mismatch=verifier.check(row['request'],row['response'])
                        if mismatch:errors.append({'request_id':row['request_id'],'errors':mismatch})
                    except Exception as exc:errors.append({'request_id':row['request_id'],'exception':type(exc).__name__,'message':str(exc)})
    finally:truth.close();oracle.close()
    result={'scope':'complete JSON vs original journal + existing read-only oracle; after measured load',
        'requests':count,'outcomes':outcomes,'errors':errors,'passed':count>0 and not errors,
        'elapsed_seconds':time.perf_counter()-started}
    result['truth_cache']=args.cache_truth
    write(phase/'full-source-validation.json',result);return result


def master(args):
    args.source=args.source.resolve();args.output=args.output.resolve();args.journal_root=args.journal_root.resolve()
    args.output.mkdir(parents=True,exist_ok=False)
    source_identity=source_check(args.source);load_env(args.env)
    sys.path.insert(0,str(args.source))
    for port in [args.gateway_port,*range(args.sidecar_port,args.sidecar_port+8)]:
        with socket.socket() as sock:
            try:sock.bind(('127.0.0.1',port))
            except OSError:raise ValueError('selected port occupied: '+str(port))
    before=graph_observation(args);write(args.output/'graph-before.json',before)
    if before['head']!=args.seq or before['step_seq']!=args.seq or before['source_run']!=args.run_id:
        raise ValueError('static run is not sealed at selected exact point')
    if before['nodes']<1000000 or before['future_nodes']!=0:raise ValueError('million same-run prefix qualification absent')
    identity={**source_identity,'run_id':args.run_id,'seq':args.seq,'graph_before':before,
        'orchestrator_sha256':sha(__file__),'harness_sha256':sha(args.harness),
        'oracle':str(args.oracle),'oracle_sha256':sha(args.oracle),'graph_writes':False,
        'source_journal':str(args.journal_root/args.run_id/'journal.sqlite'),
        'source_journal_sha256':sha(args.journal_root/args.run_id/'journal.sqlite'),
        'candidate_heavy_workers':args.candidate_heavy_workers,'modes':args.modes,
        'fully_warm_heavy':args.fully_warm_heavy,'cache_truth':args.cache_truth,
        'truth_helper_sha256':sha(Path(__file__).with_name('query_ab_validation.py')) if args.cache_truth else None}
    write(args.output/'identity.json',identity)
    from backend.query_catalog import QueryCatalogService
    seed=args.output/'summary-seed';started=time.perf_counter()
    existing=args.journal_root/'.query-summary'
    snapshot_sources=[]
    if existing.is_dir():
        seed.mkdir()
        for path in sorted(existing.glob('*.sqlite')):
            # Snapshot an existing derived summary through read-only SQLite;
            # copying a live .sqlite without its WAL would be unsafe.
            source_db=sqlite3.connect(path.resolve().as_uri()+'?mode=ro',uri=True)
            target_db=sqlite3.connect(seed/path.name)
            try:source_db.backup(target_db)
            finally:target_db.close();source_db.close()
            snapshot_sources.append({'source':str(path),'copied_sha256':sha(seed/path.name)})
    warm=QueryCatalogService.from_env(timeout=15,pool_size=1,summary_root=args.journal_root,
        summary_cache_root=seed,cache_fleet=True)
    try:preparation=warm.prepare_run(args.run_id)
    finally:warm.close()
    write(args.output/'preparation.json',{'elapsed_seconds':time.perf_counter()-started,'result':preparation,
        'read_only_summary_snapshots':snapshot_sources,
        'scope':'derived task summary preparation outside HTTP/API intervals; no event-count warmup'})
    results=[]
    modes=args.modes.split(',')
    if not modes or any(mode not in ('baseline','candidate') for mode in modes) or len(set(modes))!=len(modes):
        raise ValueError('modes must be unique baseline/candidate identifiers')
    for mode in modes:
        phase_outputs=[]
        with services(args,mode,seed) as (root,children,native):
            request={'run_id':args.run_id,'seq':args.seq,'robot_id':'{{client_id}}'}
            cases={
                'cold-heavy':[{'name':'global','query_class':'heavy','path':'/api/knowledge/query',
                    'body':{**request,'query_id':'global','event_limit':10000}}],
                'heavy-burst':[{'name':'global','query_class':'heavy','path':'/api/knowledge/query',
                    'body':{**request,'query_id':'global','event_limit':10000}}],
                'light-burst':[{'name':'self','query_class':'light','path':'/api/knowledge/query',
                    'body':{**request,'query_id':'self'}}],
                'mixed-periodic':[{'name':'self','query_class':'light','weight':98,'path':'/api/knowledge/query',
                    'body':{**request,'query_id':'self'}},{'name':'global','query_class':'heavy','weight':2,
                    'path':'/api/knowledge/query','body':{**request,'query_id':'global','event_limit':10000}}]}
            if args.fully_warm_heavy:
                cases={key:value for key,value in [
                    ('cold-heavy',cases['cold-heavy']),('heavy-burst',cases['heavy-burst']),
                    ('fully-warm-heavy',cases['heavy-burst']),('light-burst',cases['light-burst']),
                    ('mixed-periodic',cases['mixed-periodic'])]}
            for phase,work in cases.items():
                if phase=='fully-warm-heavy':
                    warm_dir=root/'explicit-warmup';warm_dir.mkdir()
                    body={**request,'query_id':'global','event_limit':10000,'robot_id':1}
                    rows=[];proof=[]
                    for round_id in range(5):
                        def warm_one(number):
                            port=args.sidecar_port+(number//(args.candidate_heavy_workers*2))
                            rid=f'explicit-warm-{mode}-r{round_id}-n{number}'
                            started=time.perf_counter_ns()
                            req=urllib.request.Request(f'http://127.0.0.1:{port}/api/knowledge/query',
                                data=json.dumps(body).encode(),headers={'Content-Type':'application/json','X-Probe-ID':rid})
                            with urllib.request.urlopen(req,timeout=30) as response:
                                raw=response.read();status=response.status
                            ended=time.perf_counter_ns();value=json.loads(raw)
                            return {'request_id':rid,'client_id':number+1,'outcome':'ok' if status==200 else 'http_error',
                                'request':body,'response':value,'start_ns':started,'end_ns':ended,
                                'elapsed_ms':(ended-started)/1e6,'phase':'explicit_preparation_not_measured_load'}
                        with ThreadPoolExecutor(max_workers=8*args.candidate_heavy_workers*2) as pool:
                            rows.extend(pool.map(warm_one,range(8*args.candidate_heavy_workers*2)))
                        proof=[read_json_url(f'http://127.0.0.1:{args.sidecar_port+i}/api/benchmark/worker-state') for i in range(8)]
                        if all(len([row for row in point['global_threads'] if row['run_id']==args.run_id and row['seq']==args.seq])
                               >=args.candidate_heavy_workers for point in proof):break
                    else:raise RuntimeError('cannot prove every heavy worker prewarmed; no warm qualification claimed')
                    with gzip.open(warm_dir/'client-0001.jsonl.gz','xt',encoding='utf-8') as stream:
                        for row in rows:stream.write(json.dumps(row,separators=(',',':'))+'\n')
                    write(warm_dir/'worker-proof.json',{'scope':'explicit successful same-point global preparation before fully-warm load',
                        'workers':proof,'requests':len(rows),'candidate_heavy_workers':args.candidate_heavy_workers})
                workload=root/f'{phase}-workload.json';write(workload,{'version':'warehouse-load-v1','cases':work})
                output=root/phase
                command=[sys.executable,str(args.harness),'run','--base-url',f'http://127.0.0.1:{args.gateway_port}',
                    '--workload',str(workload),'--output',str(output),'--session',f'ab-{mode}-{phase}',
                    '--run-id',args.run_id,'--identity',str(args.output/'identity.json'),
                    '--clients','1' if phase=='cold-heavy' else '100','--processes','1' if phase=='cold-heavy' else '100',
                    '--mode','periodic' if phase=='mixed-periodic' else 'closed','--interval','5',
                    '--duration','15' if phase=='mixed-periodic' else '60','--timeout','30',
                    '--requests','1' if phase=='cold-heavy' else '0' if phase=='mixed-periodic' else '2',
                    '--phase','cold' if phase=='cold-heavy' else 'warm']
                print(json.dumps({'event':'phase_start','mode':mode,'phase':phase,'at':time.time()}),flush=True)
                with (root/f'{phase}-load.log').open('xb') as log:
                    result=subprocess.run(command,cwd=args.source,stdout=log,stderr=subprocess.STDOUT,timeout=240,
                        env={**os.environ,'PYTHONPATH':str(args.source)})
                phase_outputs.append(output)
                if result.returncode!=0:raise RuntimeError('load process failed; original outputs retained: '+phase)
                if native.poll() is not None or any(child.poll() is not None for child in children):raise RuntimeError('owned service exited')
        # Server probes are sealed before any verification; all case IDs remain.
        for output in phase_outputs:
            command=[sys.executable,str(args.harness),'analyze','--input',str(output),'--output',str(output/'analysis.json'),
                '--require-run',args.run_id,'--validator','ops.load_validation:validate_catalog',
                '--server-trace',*[str(root/f'server-{index}.jsonl') for index in range(8)]]
            with (output/'analyze.log').open('xb') as log:
                analysis=subprocess.run(command,cwd=args.source,stdout=log,stderr=subprocess.STDOUT,timeout=300,
                    env={**os.environ,'PYTHONPATH':str(args.source)})
            # Return 2 explicitly means observed latency/error target not met;
            # it is retained and not mistaken for tool/infrastructure success.
            if analysis.returncode not in (0,2):raise RuntimeError('offline analysis failed')
            proof=full_verify(args,output)
            report=json.loads((output/'analysis.json').read_text())
            results.append({'mode':mode,'phase':output.name,'analysis_exit':analysis.returncode,
                'all_requests_under_500ms':report['all_requests_under_500ms'],
                'actual_server_api_peak':report['actual_server_api_peak'],
                'server_peak_qualified':report['server_peak_qualified'],
                'planned_arrivals':report['planned_arrivals'],'outcomes':report['outcomes'],
                'groups':report['groups'],'full_source_validation':proof['passed']})
            write(args.output/f'{mode}-{output.name}-complete.json',results[-1])
            print(json.dumps({'event':'phase_complete',**results[-1]}),flush=True)
    after=graph_observation(args);write(args.output/'graph-after.json',after)
    stable=all(before.get(key)==after.get(key) for key in ('head','step_seq','source_run','created_at','checksum','nodes','future_nodes'))
    source_stable=(sha(args.oracle)==identity['oracle_sha256']
                   and sha(args.journal_root/args.run_id/'journal.sqlite')==identity['source_journal_sha256'])
    report={'scope':'read-only static sealed-prefix A/B; dynamic writer and live control qualification remain false',
        'source_identity':identity,'results':results,'graph_stable':stable,
        'static_million_prefix_qualified':stable and before['nodes']>=1000000 and before['future_nodes']==0,
        'write_read_coexistence_qualified':False,'hundred_inflight_every_phase':False,
        'source_artifact_sha_stable':source_stable,
        'complete':stable and source_stable and len(results)==len(modes)*(5 if args.fully_warm_heavy else 4) and all(r['full_source_validation'] for r in results)}
    if args.fully_warm_heavy:
        report['explicit_preparation_source_validation']={mode:full_verify(args,args.output/mode/'explicit-warmup')['passed'] for mode in modes}
        report['complete']=report['complete'] and all(report['explicit_preparation_source_validation'].values())
    write(args.output/'final.json',report)
    files={str(path.relative_to(args.output)):sha(path) for path in sorted(args.output.rglob('*'))
           if path.is_file() and not path.name.endswith(('.sqlite-wal','.sqlite-shm'))}
    write(args.output/'artifact-sha256.json',files)
    return 0 if report['complete'] else 2


def main(argv=None):
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--worker-config',type=Path)
    parser.add_argument('--source',type=Path)
    parser.add_argument('--output',type=Path)
    parser.add_argument('--env',type=Path)
    parser.add_argument('--journal-root',type=Path)
    parser.add_argument('--oracle',type=Path)
    parser.add_argument('--harness',type=Path)
    parser.add_argument('--nginx',type=Path)
    parser.add_argument('--run-id',default='wh-density120-week-required-20261028')
    parser.add_argument('--seq',type=int,default=5073)
    parser.add_argument('--gateway-port',type=int,default=14319)
    parser.add_argument('--sidecar-port',type=int,default=14320)
    parser.add_argument('--modes',default='baseline,candidate')
    parser.add_argument('--candidate-heavy-workers',type=int,choices=(1,2),default=1)
    parser.add_argument('--fully-warm-heavy',action='store_true')
    parser.add_argument('--cache-truth',action='store_true')
    args=parser.parse_args(argv)
    if args.worker_config:return worker(args.worker_config)
    for name in ('source','output','env','journal_root','oracle','harness','nginx'):
        if getattr(args,name) is None:parser.error('--'+name.replace('_','-')+' required')
    def stop_master(signum,frame):
        raise KeyboardInterrupt('owned A/B interrupted by signal '+str(signum))
    signal.signal(signal.SIGTERM,stop_master)
    try:return master(args)
    except BaseException as exc:
        if args.output and args.output.exists():
            path=args.output/'failure.json'
            if not path.exists():write(path,{'type':type(exc).__name__,'message':str(exc),'at_unix_ns':time.time_ns()})
        raise


if __name__=='__main__':raise SystemExit(main())
