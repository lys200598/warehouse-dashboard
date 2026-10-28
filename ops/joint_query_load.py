"""Latest-observed, 100-client catalog reads during the ORIGINAL live writer.

Every request keeps its true interval and complete response. Think time is an
explicit workload parameter, never included as service time or used to pad a
request's concurrency interval. Source validation happens after the whole run.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
import gzip,json,multiprocessing as mp
from pathlib import Path
import threading,time,traceback

from backend.knowledge import digest
from backend.query_catalog import QueryCatalogService


def worker(number,clients,run,round_id,folder,head,lock,barrier,baseline,think,max_seconds,pool_size):
    root=Path(folder);service=QueryCatalogService.from_env(timeout=15,pool_size=pool_size)
    def client(owner):
        records=[];values=[]
        barrier.wait(timeout=90);started=time.monotonic();sample=0
        while True:
            with lock:
                cutoff=int(head.value);start=time.time_ns()
            request={'query_id':'self' if (owner+sample)%2 else 'readiness','run_id':run,'seq':cutoff,'robot_id':owner}
            rid=f'r{round_id}-c{owner}-s{sample}'
            error=None;value=None
            try:value=service.execute(request)
            except Exception as exc:error=type(exc).__name__+': '+str(exc)
            end=time.time_ns()
            records.append({'request_id':rid,'round':round_id,'client_id':owner,'run_id':run,'seq':cutoff,
                'point_kind':'latest','start_ns':start,'end_ns':end,'ok':error is None,
                'response_sha256':digest(value),'request':request,'error':error})
            values.append({'request_id':rid,'result':value})
            sample+=1
            with lock:advanced=head.value>=baseline+2
            if sample>=20 and advanced or time.monotonic()-started>=max_seconds:break
            time.sleep(think)
        return records,values
    try:
        with ThreadPoolExecutor(max_workers=len(clients)) as pool:
            results=list(pool.map(client,clients))
        with (root/f'worker-{number}-requests.jsonl').open('x') as out, gzip.open(root/f'worker-{number}-responses.jsonl.gz','wt',encoding='utf-8') as responses:
            for records,values in results:
                for r in records:out.write(json.dumps(r,ensure_ascii=False,allow_nan=False)+'\n')
                for r in values:responses.write(json.dumps(r,ensure_ascii=False,allow_nan=False)+'\n')
    finally:service.close()


def main(argv=None):
    p=argparse.ArgumentParser();p.add_argument('--run-id',required=True);p.add_argument('--output',type=Path,required=True)
    p.add_argument('--milestones',default='12000,180000,540000');p.add_argument('--think-seconds',type=float,default=1.)
    p.add_argument('--max-round-seconds',type=float,default=120);p.add_argument('--max-wall-seconds',type=float,default=23000)
    p.add_argument('--pool-size',type=int,default=3,help='per-worker Bolt connection budget; API waiting remains timed')
    args=p.parse_args(argv)
    args.output.mkdir(parents=True,exist_ok=False)
    ctx=mp.get_context('spawn');head=ctx.Value('q',0,lock=False);lock=ctx.Lock();stop=threading.Event()
    monitor_errors=[];service=QueryCatalogService.from_env(timeout=15,pool_size=1);started=time.monotonic()
    def monitor():
        with (args.output/'writer-head-observations.jsonl').open('x') as out:
            while not stop.is_set():
                try:
                    rows=service.store.mirror.query('''MATCH (r:WHRun {run_id:$run})
                        MATCH (s:WHStep {key:$run+':'+toString(r.committed_seq)})
                        RETURN r.committed_seq AS seq,s.sim_time AS sim_time''',{'run':args.run_id},timeout=15)
                    if rows:
                        with lock:
                            head.value=int(rows[0]['seq']);clock=time.time_ns()
                            record={'observed_at_ns':clock,'run_id':args.run_id,'committed_seq':head.value,
                                    'sim_time':int(rows[0]['sim_time'])}
                            out.write(json.dumps(record)+'\n');out.flush()
                        monitor.latest_time=record['sim_time']
                except Exception as e:monitor_errors.append({'at':time.time_ns(),'error':repr(e)})
                stop.wait(.2)
    monitor.latest_time=-1
    thread=threading.Thread(target=monitor,daemon=True);thread.start();rounds=[]
    try:
        for round_id,milestone in enumerate(map(int,args.milestones.split(',')),1):
            while monitor.latest_time<milestone:
                if time.monotonic()-started>args.max_wall_seconds:raise TimeoutError('writer milestone not reached')
                time.sleep(1)
            folder=args.output/f'round-{round_id}';folder.mkdir()
            barrier=ctx.Barrier(100)
            with lock:baseline=head.value
            processes=[ctx.Process(target=worker,args=(n,list(range(n+1,101,8)),args.run_id,round_id,str(folder),head,lock,barrier,
                baseline,args.think_seconds,args.max_round_seconds,args.pool_size)) for n in range(8)]
            beginning=time.time_ns()
            for process in processes:process.start()
            for process in processes:process.join(timeout=args.max_round_seconds+120)
            codes=[process.exitcode for process in processes]
            if any(c!=0 for c in codes):raise RuntimeError('worker failed or exceeded limit: '+repr(codes))
            rounds.append({'round':round_id,'milestone':milestone,'start_ns':beginning,'end_ns':time.time_ns(),
                           'head_before':baseline,'head_after':head.value,'worker_exits':codes})
            print(json.dumps(rounds[-1]),flush=True)
        # Functional packs use actual first task, not a fabricated fixture ID.
        with lock:task_cutoff=head.value
        task_rows=service.store.mirror.query('''MATCH (e:WHEvent {run_id:$run})
            WHERE e.commit_seq<=$seq AND e.task_id IS NOT NULL
            RETURN e.task_id AS task ORDER BY e.event_seq DESC LIMIT 1''',{'run':args.run_id,'seq':task_cutoff})
        task=task_rows[0]['task']
        functional=[];complete=[]
        for number,kind in enumerate(('task','goods','energy','assistance','progress','memory','local_awareness','global','similar_history')):
            with lock:seq=head.value;beginning=time.time_ns()
            request={'query_id':kind,'run_id':args.run_id,'seq':seq,'robot_id':1,'task_id':task,
                'start_seq':max(1,seq-3),'event_limit':10000,'history_limit':1000,'window_steps':60,'max_candidates':10000,'limit':5}
            value=service.execute(request);ending=time.time_ns();rid=f'functional-{kind}'
            functional.append({'request_id':rid,'round':4,'client_id':1,'run_id':args.run_id,'seq':seq,
                'point_kind':'latest','start_ns':beginning,'end_ns':ending,'ok':True,'response_sha256':digest(value),'request':request})
            complete.append({'request_id':rid,'result':value})
        with (args.output/'requests.jsonl').open('x') as out,gzip.open(args.output/'responses.jsonl.gz','wt',encoding='utf-8') as responses:
            for folder in sorted(args.output.glob('round-*')):
                for n in range(8):
                    out.write((folder/f'worker-{n}-requests.jsonl').read_text())
                    with gzip.open(folder/f'worker-{n}-responses.jsonl.gz','rt',encoding='utf-8') as source:
                        for line in source:responses.write(line)
        with (args.output/'functional-requests.jsonl').open('x') as out,gzip.open(args.output/'functional-responses.jsonl.gz','wt',encoding='utf-8') as responses:
            for r in functional:out.write(json.dumps(r,ensure_ascii=False,allow_nan=False)+'\n')
            for r in complete:responses.write(json.dumps(r,ensure_ascii=False,allow_nan=False)+'\n')
        (args.output/'load-result.json').write_text(json.dumps({'rounds':rounds,'monitor_errors':monitor_errors,
            'think_seconds':args.think_seconds,'max_round_seconds':args.max_round_seconds,'per_worker_pool':args.pool_size,
            'validation_during_query':False,'cpu_processes':8,'clients':100},indent=2)+'\n')
    finally:
        stop.set();thread.join(timeout=20);service.close()


if __name__=='__main__':main()
