"""CLI for additive preload and separate-run million-history combined reads."""
import argparse
import bisect
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import statistics
import sqlite3
import threading
import time
import zlib

from .model import (VERSION, DIMENSIONS, VECTOR_INDEX, archive_state, canonical,
                    fingerprint, identity, source_sequence, feature_vector,
                    validate_history, validate_vectors, validate_business)
from .store import ScaleStore


def save(path, value):
    path=Path(path)
    with path.open('x',encoding='utf-8') as output:
        json.dump(value,output,indent=2,ensure_ascii=False,default=str)


def summary(rows, seconds):
    values=sorted(row['ms'] for row in rows)
    def percentile(p):
        pos=(len(values)-1)*p
        lo=int(pos);hi=min(lo+1,len(values)-1)
        return values[lo]+(values[hi]-values[lo])*(pos-lo)
    return {'requests':len(rows),'successes':sum(r['ok'] for r in rows),
            'failures':sum(not r['ok'] for r in rows),
            'mean_ms_all_requests':statistics.mean(values),'p95_ms_all_requests':percentile(.95),
            'p99_ms_all_requests':percentile(.99),'max_ms_all_requests':max(values),
            'over_500ms_all_requests':sum(v>500 for v in values),
            'self_retrieval_hits':sum(r.get('self_retrieved',False) for r in rows),
            'throughput_successes_s':sum(r['ok'] for r in rows)/seconds,
            'wall_seconds':seconds}


def source_contract(store,business):
    metadata=store.head(business)
    if not metadata or metadata.get('scale_version'):
        raise ValueError('source must be a real committed business run, not another scale archive')
    available,templates=store.templates(business)
    source_hash=fingerprint([[seq,robot,json.loads(payload)] for (seq,robot),payload in sorted(templates.items())])
    return metadata,available,templates,source_hash


def task_oracles(args,store,available,templates):
    output=getattr(args,'business_output',None)
    if output:
        from backend.wide.verify import load_source
        from backend.knowledge import digest
        source=load_source(output)
        if source['run_id']!=args.business_run or sorted(source['batches'])!=available:
            raise ValueError('journal oracle belongs to a different/incomplete business source')
        expected={};tasks={}
        # wide.verify materializes task maps for only its two smoke cutoffs.
        # Replay every immutable delta here for our arbitrarily sampled cutoffs.
        journal=(Path(output)/'journal.sqlite').resolve()
        db=sqlite3.connect(f'file:{journal.as_posix()}?mode=ro',uri=True)
        try:
            for seq,checksum,blob in db.execute('SELECT seq,checksum,batch FROM commits WHERE run_id=? ORDER BY seq',(args.business_run,)):
                batch=json.loads(zlib.decompress(blob))
                if digest(batch)!=checksum:
                    raise ValueError('journal changed or corrupt during task oracle construction')
                tasks.update({t['task_id']:t for t in batch['tasks']})
                for robot in range(1,101):
                    state=json.loads(templates[(seq,robot)])
                    expected[(seq,robot)]=tasks.get(state.get('task_id'))
        finally:
            db.close()
        for (seq,robot),payload in templates.items():
            batch=source['batches'][seq]
            state=json.loads(payload)
            if state!=batch['robots'][robot]:
                raise ValueError('graph robot source differs from independently verified journal')
        return expected,{'kind':'SHA-validated checkpoint + complete journal replay','identity':source['identity']}
    versions={}
    for row in store.task_templates(args.business_run):
        versions.setdefault(row['task_id'],[]).append((row['seq'],json.loads(row['payload'])))
    expected={}
    for (seq,robot),payload in templates.items():
        choices=versions.get(json.loads(payload).get('task_id'),[])
        index=bisect.bisect_right([s for s,_ in choices],seq)-1
        expected[(seq,robot)]=choices[index][1] if index>=0 else None
    return expected,{'kind':'fixed source graph historical task versions; NOT independent journal validation'}


def preload(args,store):
    started=time.monotonic()
    metadata,available,templates,source_hash=source_contract(store,args.business_run)
    source_head=int(metadata['committed_seq'])
    if source_head!=max(available):
        raise ValueError('source changed or incomplete commit snapshots')
    seq=store.create_archive(args.archive_run,args.business_run,args.steps,source_hash,args.resume)
    contract={'version':VERSION,'business_run':args.business_run,'business_head':source_head,
              'archive_run':args.archive_run,'steps':args.steps,'expected_agent_states':args.steps*100,
              'source_state_templates':len(templates),'source_sequences':available,'source_hash':source_hash,
              'dimensions':DIMENSIONS,'vectors':'structured features, NOT text/image semantic embeddings',
              'scope':'separate-run synthetic resampling; duplicated source snapshots retain provenance',
              'resumed_after_seq':seq}
    save(args.output_dir/'contract.json',contract)
    log=args.output_dir/'preload.jsonl'
    with log.open('x',encoding='utf-8') as output:
        while seq<args.steps:
            if time.monotonic()-started>=args.max_wall_seconds:
                raise TimeoutError('preload wall limit reached; archive retained for explicit resume')
            first=seq+1;last=min(args.steps,seq+args.chunk_steps)
            rows=[]
            for new_seq in range(first,last+1):
                source=source_sequence(new_seq,args.steps,available)
                for robot in range(1,101):
                    rows.append(archive_state(args.archive_run,new_seq,robot,args.business_run,
                        source,templates[(source,robot)]))
            before=time.monotonic()
            store.append(args.archive_run,rows,first,last)
            seq=last
            entry={'first':first,'last':last,'states':len(rows),'batch_seconds':time.monotonic()-before,
                   'elapsed_seconds':time.monotonic()-started}
            output.write(json.dumps(entry)+'\n');output.flush()
            if seq%1000==0 or seq==args.steps:
                print(json.dumps(entry),flush=True)
    indexes=store.ensure_vector_index()
    counts=store.counts(args.archive_run)
    passed=(counts['states']==args.steps*100 and counts['vectors']==args.steps*100
            and counts['source_links']==args.steps*100 and counts['robots']==100
            and counts['first_seq']==1 and counts['last_seq']==args.steps)
    report={'status':'complete' if passed else 'failed','contract':contract,'counts':counts,
            'indexes':indexes,'elapsed_seconds':time.monotonic()-started,
            'business_head_unchanged':store.head(args.business_run)['committed_seq']==source_head}
    save(args.output_dir/'report.json',report)
    if not passed or not report['business_head_unchanged']:
        raise ValueError('preload count/provenance/business-head verification failed')
    print(json.dumps({'status':report['status'],'counts':counts}),flush=True)


def benchmark(args,store):
    metadata,available,templates,source_hash=source_contract(store,args.business_run)
    expected_tasks,oracle_identity=task_oracles(args,store,available,templates)
    archive=store.head(args.archive_run)
    if not archive or archive.get('source_run')!=args.business_run or archive.get('source_hash')!=source_hash:
        raise ValueError('archive does not derive from this unchanged business source')
    steps=int(archive['archive_steps'])
    if int(archive['committed_seq'])!=steps:
        raise ValueError('preload incomplete')
    counts=store.counts(args.archive_run)
    if counts['states']<args.min_states or counts['vectors']!=counts['states'] or counts['source_links']!=counts['states']:
        raise ValueError('million-state/vector/provenance count requirement failed')
    indexes=store.ensure_vector_index()
    if not indexes or indexes[0]['state']!='ONLINE':
        raise ValueError('vector index not ONLINE')
    raw=args.output_dir/'queries.jsonl'
    rounds=[]
    lock=threading.Lock()
    active=0;peak=0
    observations=[];stop=threading.Event()
    def observe():
        while not stop.is_set():
            stamp=time.time_ns()
            try:point=store.watermark(args.observe_writer_run)
            except Exception as exc:point={'error':type(exc).__name__}
            observations.append({'start_ns':stamp,'end_ns':time.time_ns(),**point})
            stop.wait(.2)
    watcher=None
    if args.observe_writer_run:
        watcher=threading.Thread(target=observe,name='scale-writer-watermark',daemon=True)
        watcher.start()
    try:
      with raw.open('x',encoding='utf-8') as output:
        for round_id in range(1,args.rounds+1):
            barrier=threading.Barrier(args.clients)
            peak=0
            def client(robot):
                nonlocal active,peak
                barrier.wait(timeout=60)
                records=[]
                for sample in range(args.samples):
                    seq=1+((robot*997+round_id*1297+sample*499)%steps)
                    source=source_sequence(seq,steps,available)
                    with lock:
                        active+=1;peak=max(peak,active)
                    begin=time.perf_counter();start_ns=time.time_ns()
                    row={'round':round_id,'client_id':robot,'sample':sample,'archive_seq':seq,
                         'business_seq':source,'start_ns':start_ns,'ok':False,'query_mode':args.query_mode,
                         'retrieval_path':'error_before_result','bolt_statements':None}
                    try:
                        result=store.full_query(args.business_run,source,args.archive_run,robot,seq,query_mode=args.query_mode)
                        row.update(retrieval_path=result.get('retrieval_path','baseline_ann100'),
                            bolt_statements=result.get('bolt_statements',5))
                        expected=json.loads(templates[(source,robot)])
                        validate_business(result['business'],args.business_run,source,expected,expected_tasks[(source,robot)])
                        validate_history(result['history'],args.archive_run,robot,seq,args.business_run,
                            available,steps,templates)
                        vector=feature_vector(templates[(source,robot)],seq,robot)
                        if (len(result['query_vector'])!=DIMENSIONS or not all(math.isfinite(v) for v in result['query_vector'])
                                or any(abs(a-b)>1e-10 for a,b in zip(vector,result['query_vector']))):
                            raise ValueError('query vector differs from deterministic source oracle')
                        validate_vectors(result['similar'],vector,args.archive_run,seq,
                            args.business_run,available,steps,templates)
                        row.update(ok=True,history_rows=len(result['history']),vector_rows=len(result['similar']),
                            self_retrieved=any(r['key']==f'{args.archive_run}:{seq}:{robot}' for r in result['similar']),
                            result_sha256=fingerprint(result),business_task_present=result['business']['task'] is not None,
                            similar_keys=[r['key'] for r in result['similar']],
                            retrieval_path=result.get('retrieval_path','baseline_ann100'),
                            bolt_statements=result.get('bolt_statements',5))
                    except Exception as exc:
                        row['error']=type(exc).__name__+': '+str(exc)[:512]
                        if hasattr(exc,'scale_retrieval_path'):
                            row.update(retrieval_path=exc.scale_retrieval_path,bolt_statements=exc.scale_bolt_statements)
                    finally:
                        row.update(ms=(time.perf_counter()-begin)*1000,end_ns=time.time_ns())
                        with lock:active-=1
                    records.append(row)
                return records
            round_start_ns=time.time_ns()
            writer_before=store.watermark(args.observe_writer_run) if args.observe_writer_run else None
            started=time.perf_counter()
            with ThreadPoolExecutor(max_workers=args.clients) as pool:
                records=[r for batch in pool.map(client,range(1,args.clients+1)) for r in batch]
            seconds=time.perf_counter()-started
            writer_after=store.watermark(args.observe_writer_run) if args.observe_writer_run else None
            round_end_ns=time.time_ns()
            inside=[o for o in observations if round_start_ns<=o['start_ns'] and o['end_ns']<=round_end_ns and 'seq' in o]
            report={'round':round_id,'peak_active_full_api_requests':peak,**summary(records,seconds),
                'measurement_start_ns':round_start_ns,'measurement_end_ns':round_end_ns,
                'writer_before':writer_before,'writer_after':writer_after,
                'writer_samples_inside_round':len(inside),
                'writer_commits_observed_inside_round':bool(inside) and max(o['seq'] for o in inside)>min(o['seq'] for o in inside)}
            report['retrieval_paths']={path:sum(r.get('retrieval_path')==path for r in records)
                for path in sorted({r.get('retrieval_path') for r in records if r.get('retrieval_path')})}
            rounds.append(report)
            for row in records:output.write(json.dumps(row)+'\n')
            output.flush()
            print(json.dumps(report),flush=True)
    finally:
        stop.set()
        if watcher:watcher.join(timeout=20)
        save(args.output_dir/'writer-observations.json',observations)
    report={'version':VERSION,'scope':'same database, distinct real business and synthetic million-history runs',
            'business_run':args.business_run,'archive_run':args.archive_run,'counts':counts,'indexes':indexes,
            'clients':args.clients,'samples_per_client':args.samples,'source_hash':source_hash,
            'business_oracle':oracle_identity,
            'query_mode':args.query_mode,
            'measurement':'complete materialized Python/Bolt API: business robot/task context + million-history range + vector fetch + time-filtered ANN + JSON + correctness checks',
            'source':'deterministically resampled actual business state snapshots; archive sequences are synthetic, not new runtime commits',
            'warmup':'no discarded measurement round; preprocessing/count/index checks may warm database',
            'vector_scope':('32-dimensional structured features; approximate top100 then time-filtered top5; '
                'batched mode expands to ANN1000 only if empty, then exact eligible cosine top5 only if still empty; '
                'normal ANN path NOT guaranteed exact temporal top-k'),
            'qualification':{'million_states':counts['states']>=1000000,
                'hundred_clients':args.clients>=100,'three_repetitions':args.rounds>=3,
                'all_correct':all(r['failures']==0 for r in rounds),
                'all_requests_under_500ms':all(r['over_500ms_all_requests']==0 for r in rounds),
                'same_run_actual_million_history':False,
                'concurrent_observed_writer_commits':bool(args.observe_writer_run) and all(r['writer_commits_observed_inside_round'] for r in rounds),
                'semantic_multimodal_embeddings':False,'specified_edge_device':False,'full_task2_acceptance':False},
            'observe_writer_run':args.observe_writer_run,'writer_observation_errors':sum('error' in o for o in observations),
            'writer_scope':'watermark observation only; correlate actual runtime process/log evidence separately',
            'rounds':rounds,'environment':{'hostname':platform.node(),'platform':platform.platform(),
                'python':platform.python_version(),'logical_cpus':os.cpu_count()},
            'database_components':store.mirror.query('CALL dbms.components() YIELD name,versions,edition RETURN name,versions,edition',{}),
            'raw_sha256':hashlib.sha256(raw.read_bytes()).hexdigest()}
    save(args.output_dir/'report.json',report)
    if not report['qualification']['all_correct']:
        raise ValueError('benchmark has errors; full raw evidence retained')


def parser():
    p=argparse.ArgumentParser(description=__doc__)
    sub=p.add_subparsers(dest='command',required=True)
    for command in ('preload','benchmark'):
        c=sub.add_parser(command)
        c.add_argument('--business-run',required=True,type=identity)
        c.add_argument('--archive-run',required=True,type=identity)
        c.add_argument('--output-dir',required=True,type=Path)
        if command=='preload':
            c.add_argument('--steps',type=int,default=10000)
            c.add_argument('--chunk-steps',type=int,default=20)
            c.add_argument('--max-wall-seconds',type=int,default=1200)
            c.add_argument('--resume',action='store_true')
        else:
            c.add_argument('--clients',type=int,default=100)
            c.add_argument('--rounds',type=int,default=3)
            c.add_argument('--samples',type=int,default=5)
            c.add_argument('--min-states',type=int,default=1000000)
            c.add_argument('--observe-writer-run',type=identity)
            c.add_argument('--business-output',type=Path,help='optional original finalized checkpoint/journal directory for independent robot/task oracle')
            c.add_argument('--query-mode',choices=['baseline','batched'],default='baseline')
    return p


def main():
    p=parser();args=p.parse_args()
    if args.business_run==args.archive_run:p.error('business and archive runs must differ')
    if args.command=='preload':
        if not 2<=args.steps<=20000 or not 1<=args.chunk_steps<=100 or not 10<=args.max_wall_seconds<=7200:
            p.error('steps 2..20000, chunk-steps 1..100, max-wall-seconds 10..7200')
    elif not 1<=args.clients<=100 or not 1<=args.rounds<=10 or not 1<=args.samples<=100 or not 1<=args.min_states<=2000000:
        p.error('clients 1..100, rounds 1..10, samples 1..100, min-states 1..2000000')
    args.output_dir.mkdir(parents=True,exist_ok=False)
    store=None
    try:
        store=ScaleStore.from_env()
        (preload if args.command=='preload' else benchmark)(args,store)
    except Exception as exc:
        save(args.output_dir/'failure.json',{'type':type(exc).__name__,'message':str(exc)[:2000]})
        raise
    finally:
        if store:store.close()
