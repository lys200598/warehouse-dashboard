"""Diagnostic-only read profiler; never an acceptance run or an environment edit.

Profiles one selected worker (main-thread cProfile would miss worker work).
All concurrent workers retain phase/CPU measurements; one profiler avoids the
Python 3.12 monitoring-tool-slot conflict between concurrent cProfile objects.
Use on a frozen copy; no dataset/index mutation or package installation.
"""
import argparse
import cProfile
from concurrent.futures import ThreadPoolExecutor
import importlib.metadata
import io
import json
import math
import os
from pathlib import Path
import pstats
import sys
import threading
import time

from .cli import save,source_contract,task_oracles,summary
from .model import (VECTOR_INDEX,DIMENSIONS,source_sequence,feature_vector,
    validate_business,validate_history,validate_vectors,fingerprint)
from .store import ScaleStore,BATCHED_QUERY


def connect_read_only():
    # Do not use from_env(): its normal application bootstrap calls ensure_schema.
    from neo4j import GraphDatabase
    from backend.knowledge_graph import Neo4jMirror
    driver=GraphDatabase.driver(os.environ['NEO4J_URI'],
        auth=(os.environ.get('NEO4J_USER','neo4j'),os.environ['NEO4J_PASSWORD']),
        max_connection_pool_size=120,connection_timeout=10,connection_acquisition_timeout=30,
        max_transaction_retry_time=0)
    driver.verify_connectivity()
    return ScaleStore(Neo4jMirror(driver,os.environ.get('NEO4J_DATABASE','neo4j')))


def inspect_server(store,args):
    """PROFILE executes one bounded read. It does not modify schema/data."""
    from neo4j import Query,READ_ACCESS
    params={'business':args.business_run,'business_seq':args.profile_business_seq,
        'business_key':f'{args.business_run}:{args.profile_business_seq}:28',
        'point_key':f'{args.archive_run}:511:28','run':args.archive_run,'robot':28,
        'seq':511,'index':VECTOR_INDEX}
    started=time.perf_counter()
    with store.mirror.driver.session(database=store.mirror.database,default_access_mode=READ_ACCESS) as session:
        result=session.run(Query('PROFILE '+BATCHED_QUERY,timeout=15),params)
        rows=[dict(r) for r in result]
        info=result.consume()
    return {'wall_seconds':time.perf_counter()-started,'records':len(rows),
        'result_available_after_ms':info.result_available_after,
        'result_consumed_after_ms':info.result_consumed_after,
        'profile':info.profile,'note':'PROFILE adds overhead; not an acceptance latency'}


def run(args,store):
    metadata,available,templates,source_hash=source_contract(store,args.business_run)
    tasks,oracle=task_oracles(args,store,available,templates)
    archive=store.head(args.archive_run)
    if not archive or archive.get('source_hash')!=source_hash or archive['committed_seq']!=archive['archive_steps']:
        raise ValueError('archive/source contract mismatch or incomplete archive')
    steps=int(archive['archive_steps'])
    if steps<511:raise ValueError('diagnostic expects the existing >=511-step archive')
    args.profile_business_seq=source_sequence(511,steps,available)
    server_profile=inspect_server(store,args) if args.server_profile else None
    if server_profile:save(args.output_dir/'server-profile.json',server_profile)
    barrier=threading.Barrier(args.clients)
    started=time.perf_counter();process_started=time.process_time()
    deadline=started+args.max_wall_seconds
    def client(robot):
        selected=getattr(args,'profile_client',min(28,args.clients))
        profiler=cProfile.Profile() if robot==selected else None
        records=[]
        barrier.wait(timeout=30)
        if profiler:profiler.enable()
        try:
            for sample in range(args.samples):
                if time.perf_counter()>deadline:break
                seq=1+((robot*997+1297+sample*499)%steps)
                source=source_sequence(seq,steps,available)
                begin=time.perf_counter();cpu=time.thread_time()
                row={'client_id':robot,'sample':sample,'archive_seq':seq,'business_seq':source,'ok':False}
                try:
                    result=store.full_query(args.business_run,source,args.archive_run,robot,seq,query_mode='batched')
                    fetched=time.perf_counter()
                    expected=json.loads(templates[(source,robot)])
                    validate_business(result['business'],args.business_run,source,expected,tasks[(source,robot)])
                    validate_history(result['history'],args.archive_run,robot,seq,args.business_run,available,steps,templates)
                    vector=feature_vector(templates[(source,robot)],seq,robot)
                    if (len(result['query_vector'])!=DIMENSIONS
                            or not all(math.isfinite(v) for v in result['query_vector'])
                            or any(abs(a-b)>1e-10 for a,b in zip(vector,result['query_vector']))):
                        raise ValueError('point vector source oracle mismatch')
                    validate_vectors(result['similar'],vector,args.archive_run,seq,args.business_run,available,steps,templates)
                    validated=time.perf_counter()
                    checksum=fingerprint(result)
                    hashed=time.perf_counter()
                    row.update(ok=True,fetch_materialize_ms=(fetched-begin)*1000,
                        oracle_ms=(validated-fetched)*1000,result_fingerprint_ms=(hashed-validated)*1000,
                        retrieval_path=result.get('retrieval_path'),result_sha256=checksum)
                except Exception as exc:
                    row['error']=type(exc).__name__+': '+str(exc)[:400]
                row.update(ms=(time.perf_counter()-begin)*1000,thread_cpu_ms=(time.thread_time()-cpu)*1000)
                records.append(row)
        finally:
            if profiler:profiler.disable()
        return records,profiler
    with ThreadPoolExecutor(max_workers=args.clients) as pool:
        returned=list(pool.map(client,range(1,args.clients+1)))
    elapsed=time.perf_counter()-started;cpu=time.process_time()-process_started
    rows=[r for records,_ in returned for r in records]
    profilers=[profiler for _,profiler in returned if profiler is not None]
    stats=pstats.Stats(profilers[0])
    stats.dump_stats(str(args.output_dir/'workers.pstats'))
    output=io.StringIO();stats.stream=output
    stats.strip_dirs().sort_stats('cumulative').print_stats(100)
    with (args.output_dir/'workers-top100.txt').open('x',encoding='utf-8') as stream:stream.write(output.getvalue())
    with (args.output_dir/'requests.jsonl').open('x',encoding='utf-8') as stream:
        for row in rows:stream.write(json.dumps(row)+'\n')
    packages={}
    for name in ('neo4j','neo4j-rust-ext','orjson','numpy'):
        try:packages[name]=importlib.metadata.version(name)
        except importlib.metadata.PackageNotFoundError:packages[name]=None
    report={'scope':'diagnostic-only, cProfile adds overhead, not acceptance, no schema/data/environment writes',
        'clients':args.clients,'samples_per_client':args.samples,'actual_requests':len(rows),
        'deadline_reached':len(rows)<args.clients*args.samples,'source_hash':source_hash,'business_oracle':oracle,
        'client_process_cpu_seconds':cpu,'wall_seconds':elapsed,'client_cpu_core_equivalent':cpu/elapsed,
        'packages':packages,'requests_summary':summary(rows,elapsed) if rows else None,
        'loaded_driver_modules':{name:getattr(module,'__file__',None) for name,module in list(sys.modules.items())
            if name=='neo4j' or name.startswith('neo4j') and ('rust' in name.lower() or '_codec' in name)},
        'mean_stage_ms':{key:sum(r[key] for r in rows if key in r)/sum(key in r for r in rows)
            for key in ['fetch_materialize_ms','oracle_ms','result_fingerprint_ms','thread_cpu_ms']
            if any(key in r for r in rows)},
        'profile_client':getattr(args,'profile_client',min(28,args.clients)),
        'profile_note':'only selected worker cProfiled; every worker phase/thread-CPU measured; diagnostic overhead not acceptance',
        'server_profile':server_profile is not None}
    save(args.output_dir/'report.json',report)
    print(json.dumps(report),flush=True)


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--business-run',required=True)
    p.add_argument('--archive-run',required=True)
    p.add_argument('--business-output',type=Path)
    p.add_argument('--output-dir',required=True,type=Path)
    p.add_argument('--clients',type=int,default=100)
    p.add_argument('--samples',type=int,default=3)
    p.add_argument('--max-wall-seconds',type=int,default=60)
    p.add_argument('--profile-client',type=int,default=28)
    p.add_argument('--server-profile',action='store_true')
    args=p.parse_args()
    if (not 1<=args.clients<=100 or not 1<=args.samples<=10 or not 10<=args.max_wall_seconds<=120
            or not 1<=args.profile_client<=args.clients):
        p.error('clients 1..100, samples 1..10, max-wall-seconds 10..120, profile-client 1..clients')
    args.output_dir.mkdir(parents=True,exist_ok=False)
    store=None
    try:
        store=connect_read_only();run(args,store)
    except Exception as exc:
        save(args.output_dir/'failure.json',{'type':type(exc).__name__,'error':str(exc)[:1000]})
        raise
    finally:
        if store:store.close()


if __name__=='__main__':main()
