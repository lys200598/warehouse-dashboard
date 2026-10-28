"""100 HTTP clients querying live + million-state historical contexts during Ray writes.

The formal mode --same-run --inject-batch --resume continues a million-state run
with Ray and tests it during new work. Historical Local preload and live Ray
execution are explicitly separate phases, not separate live runs. Without
--same-run the report rejects the stronger same-run criterion. Neither mode tests
100 physical devices or autonomous Actor-initiated knowledge queries.
"""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
import hashlib
import http.client
import json
from pathlib import Path
import threading
import time
from urllib.parse import urlsplit
from urllib.request import urlopen

from .benchmark import summarize
from .knowledge import source_fingerprint
from .knowledge_graph import Neo4jMirror


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--base-url',required=True)
    parser.add_argument('--archive-run-id',default='warehouse-kg-scale-million')
    parser.add_argument('--output-dir',type=Path,required=True)
    parser.add_argument('--samples',type=int,default=10)
    parser.add_argument('--rounds',type=int,default=3)
    parser.add_argument('--threshold-ms',type=float,default=500.)
    parser.add_argument('--resume',action='store_true')
    parser.add_argument('--same-run',action='store_true')
    parser.add_argument('--inject-batch',action='store_true')
    args=parser.parse_args()
    if not 2<=args.samples<=100 or not 1<=args.rounds<=10:parser.error('samples 2..100, rounds 1..10')
    args.output_dir.mkdir(parents=True,exist_ok=False)
    def snapshot():
        with urlopen(args.base_url+'/api/snapshot',timeout=30) as r:return json.load(r)
    mirror=Neo4jMirror.from_env()
    first=snapshot()
    if args.same_run:args.archive_run_id=first['runId']
    archive_count=mirror.query('MATCH (a:WHAgentState {run_id:$run}) RETURN count(a) AS n',{'run':args.archive_run_id})[0]['n']
    if archive_count<1_000_000:raise RuntimeError('archive is not million scale')
    archive_head=mirror.query('MATCH (r:WHRun {run_id:$run}) RETURN r.committed_seq AS seq',{'run':args.archive_run_id})[0]['seq']
    # Wait past initial route compilation; otherwise a query burst can finish
    # before the first live writer commit and falsely look like an online test.
    deadline=time.monotonic()+180
    first=snapshot()
    wait_until_tick=first['tick']+20
    batch=None
    if args.inject_batch:
        if first['running']:raise RuntimeError('inject acceptance batch only while paused')
        from urllib.request import Request
        with urlopen(Request(args.base_url+'/api/tasks/batch',data=b'{"count":100}',headers={'Content-Type':'application/json'}),timeout=60) as r:batch=json.load(r)
        if batch['assigned']!=100:raise RuntimeError('insufficient ready robots/compatible free slots for 100 tasks: '+str(batch))
    if args.resume and not first['running']:
        from urllib.request import Request
        with urlopen(Request(args.base_url+'/api/control',data=b'{"action":"resume"}',headers={'Content-Type':'application/json'}),timeout=30) as r:json.load(r)
    while first['tick']<wait_until_tick:
        if time.monotonic()>deadline:raise RuntimeError('live simulation did not reach tick20')
        time.sleep(.2);first=snapshot()
    if first['runtime']['workerProcessCount']!=100 or first['maxActiveRobots']!=100:
        raise RuntimeError('live service is not configured with 100 real workers and task capacity')
    if first['metrics']['assignedRobots']!=100:
        raise RuntimeError('100 tasks are not simultaneously assigned at measurement start')
    run=first['runId']
    root=urlsplit(args.base_url)
    connections={i:http.client.HTTPConnection(root.hostname,root.port,timeout=15) for i in range(1,101)}
    def request(robot,kind,sample):
        payload={'query_id':'context','robot_id':robot,'run_id':run if kind=='live' else args.archive_run_id}
        if kind=='archive':payload['as_of_seq']=1+((robot*997+sample*499)%archive_head)
        start=time.perf_counter();wall=time.time_ns();error=None;result={}
        try:
            c=connections[robot];c.request('POST','/api/knowledge/query',json.dumps(payload),{'Content-Type':'application/json'})
            response=c.getresponse();raw=response.read()
            if response.status!=200:raise RuntimeError('HTTP '+str(response.status)+': '+raw.decode(errors='replace')[:256])
            result=json.loads(raw)
            if len(result['rows'])!=1 or result['rows'][0]['data']['robot_id']!=robot:raise RuntimeError('wrong identity')
            if result['run_id']!=payload['run_id']:raise RuntimeError('wrong run')
            if any(e['tick']>result['as_of_tick'] for e in result['events']):raise RuntimeError('future event')
        except Exception as exc:error=str(exc)
        return {'client_id':robot,'kind':kind,'sample':sample,'run_id':payload['run_id'],
                'start_ns':wall,'end_ns':time.time_ns(),'ms':(time.perf_counter()-start)*1000,
                'ok':error is None,'error':error,'as_of_seq':result.get('as_of_seq'),'as_of_tick':result.get('as_of_tick')}
    stop=threading.Event();monitor=[]
    def watch():
        while not stop.is_set():
            try:
                s=snapshot()
                monitor.append({'wall_ns':time.time_ns(),'tick':s['tick'],'completed':s['completed'],
                    'assigned':s['metrics']['assignedRobots'],'knowledge':s['knowledge'],
                    'workers':s['runtime']['workerProcessCount'],
                    'robots':[{k:r[k] for k in ('id','x','y','stage','actionTimer','taskId','blocked')} for r in s['robots']]})
            except Exception as exc:monitor.append({'wall_ns':time.time_ns(),'error':str(exc)})
            stop.wait(.1)
    thread=threading.Thread(target=watch,name='joint-state-monitor',daemon=True);thread.start()
    all_rows=[];rounds=[];lock=threading.Lock();active=0;peak=0
    try:
        with ThreadPoolExecutor(max_workers=100) as pool:
            warm_barrier=threading.Barrier(100)
            def warm(robot):
                warm_barrier.wait(timeout=60)
                return request(robot,'archive',0)
            warmup=list(pool.map(warm,range(1,101)))
            (args.output_dir/'warmup.json').write_text(json.dumps(warmup,indent=2),encoding='utf-8')
            measurement_start=time.time_ns()
            before=snapshot()
            for round_id in range(1,args.rounds+1):
                barrier=threading.Barrier(100);peak=0
                def client(robot):
                    nonlocal active,peak
                    barrier.wait(timeout=60);rows=[]
                    for sample in range(args.samples):
                        with lock:active+=1;peak=max(peak,active)
                        row=request(robot,'live' if sample%2==0 else 'archive',round_id*args.samples+sample)
                        row['round']=round_id;rows.append(row)
                        with lock:active-=1
                    return rows
                started=time.perf_counter()
                rows=[r for batch in pool.map(client,range(1,101)) for r in batch]
                rounds.append({'round':round_id,'peak_active_requests':peak,**summarize(rows,time.perf_counter()-started)})
                all_rows.extend(rows)
                print(json.dumps(rounds[-1]),flush=True)
            measurement_end=time.time_ns();after=snapshot()
    finally:
        stop.set();thread.join(timeout=35)
        for c in connections.values():c.close()
    within=[m for m in monitor if measurement_start<=m['wall_ns']<=measurement_end and 'error' not in m]
    progressed=set()
    for left,right in zip(within,within[1:]):
        old={r['id']:r for r in left['robots']}
        for r in right['robots']:
            prev=old[r['id']]
            if r['stage'] not in {'idle','charging'} and any(r[k]!=prev[k] for k in ('x','y','stage','actionTimer')):progressed.add(r['id'])
    live_count=mirror.query('MATCH (a:WHAgentState {run_id:$run}) RETURN count(a) AS n',{'run':run})[0]['n']
    mirror.close()
    for name,rows in (('queries.jsonl',all_rows),('monitor.jsonl',monitor)):
        (args.output_dir/name).write_text(''.join(json.dumps(r)+'\n' for r in rows),encoding='utf-8')
    over=sum(r['ms']>args.threshold_ms for r in all_rows)
    checks={'archive_million':archive_count>=1_000_000,'same_run_million':args.archive_run_id==run and archive_count>=1_000_000,
        '100_workers':all(m['workers']==100 for m in within) and bool(within),
        '100_simultaneous_tasks':bool(within) and all(m['assigned']==100 for m in within),
        '100_robots_actually_progressed':len(progressed)==100,
        'ticks_advanced':after['tick']>before['tick'],
        'graph_advanced':after['knowledge']['mirrored_seq']>before['knowledge']['mirrored_seq'],
        'all_requests_ok':all(r['ok'] for r in all_rows),'all_requests_within_threshold':over==0}
    report={'source_sha256':source_fingerprint(),'base_url':args.base_url,'live_run_id':run,'scene':first['scene'],
        'scope':'same run with historical Local preload + concurrent live Ray writes' if args.same_run else 'shared database under concurrent live Ray writes; separate runs',
        'injected_task_batch':batch,
        'archive_run_id':args.archive_run_id,'archive_agent_states':archive_count,'live_agent_states':live_count,
        'threshold_ms':args.threshold_ms,'over_threshold':over,'rounds':rounds,'checks':checks,
        'before':{k:before[k] for k in ('tick','completed','metrics','knowledge')},
        'after':{k:after[k] for k in ('tick','completed','metrics','knowledge')},
        'progressed_robot_ids':sorted(progressed),'monitor_samples':len(within),
        'measurement_start_ns':measurement_start,'measurement_end_ns':measurement_end,
        'raw_sha256':{name:hashlib.sha256((args.output_dir/name).read_bytes()).hexdigest() for name in ('queries.jsonl','monitor.jsonl')}}
    (args.output_dir/'report.json').write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding='utf-8')
    print(json.dumps({'checks':checks,'over_threshold':over,'progressed':len(progressed)}),flush=True)
    if not all(checks.values()):raise SystemExit('joint acceptance failed; retained raw evidence')


if __name__=='__main__':main()
