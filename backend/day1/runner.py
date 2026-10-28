from __future__ import annotations
import argparse
from collections import Counter
import json
from pathlib import Path
import sqlite3
import time
import traceback
import zlib
from ..checkpoint import write_checkpoint,read_checkpoint
from ..knowledge import JournalKnowledgeStore,canonical
from .sim import Simulation,Config

def quantile(values,p):
    a=sorted(values)
    return a[min(len(a)-1,int((len(a)-1)*p))] if a else 0

def audit_graph(store,sim):
    if not store.mirror:return None
    run=sim.run_id;m=store.mirror
    labels=['WHRobot','WHAgentState','WHTask','WHTaskState','WHSlot','WHEvent','WHGoodsState','WHChargerState','WHLocation']
    counts={label:m.query(f'MATCH (n:{label} {{run_id:$run}}) RETURN count(n) AS n',{'run':run})[0]['n'] for label in labels}
    counts['cargo_locations']=m.query('MATCH (g:WHCargo {run_id:$run}) RETURN g.current_location_id AS location,count(*) AS n',{'run':run})
    counts['inside']=m.query("MATCH (g:WHCargo {run_id:$run}) WHERE g.current_location_id IS NOT NULL AND g.current_location_id<>'SHIPPED' RETURN count(g) AS n",{'run':run})[0]['n']
    if counts['inside']!=sim.inside or counts['WHRobot']!=100 or counts['WHTask']!=sim.cursor:raise AssertionError('graph/source count mismatch')
    return counts

def run(args):
    root=Path(args.output);root.mkdir(parents=True,exist_ok=True);cp=root/'checkpoint.json'
    if cp.exists() and not args.resume:raise FileExistsError('run exists; use --resume or a fresh output/run id')
    mirror=None
    if args.knowledge_backend=='neo4j':
        from .graph import DayOneMirror
        mirror=DayOneMirror.from_env()
    store=JournalKnowledgeStore(root/'journal.sqlite',mirror)
    state=read_checkpoint(cp) if args.resume else None
    if state:state=store.recovery_payload(state)
    sim=Simulation(args.scene,args.dataset,args.run_id,Config(chargers=args.chargers,charging_policy=args.charging_policy,max_batch=args.max_batch),checkpoint=state)
    frames=sqlite3.connect(root/'frames.sqlite');frames.execute('PRAGMA journal_mode=WAL')
    frames.execute('CREATE TABLE IF NOT EXISTS frames (second INTEGER PRIMARY KEY,payload BLOB NOT NULL)')
    started=time.monotonic();last_commit=sim.elapsed;last_progress=started;stop_reason='complete';failure=None;graph_failures=0
    def frame():
        f=sim.frame();f['knowledge']=store.status(sim.run_id)
        with frames:frames.execute('INSERT OR REPLACE INTO frames VALUES (?,?)',(sim.elapsed,zlib.compress(canonical(f),3)))
        write_checkpoint(root/'latest.json',f)
    def persist():
        nonlocal graph_failures,last_commit
        sim.knowledge_seq+=1;state=sim.checkpoint();store.append(state,sim.pending_events);sim.pending_events=[]
        write_checkpoint(cp,state);last_commit=sim.elapsed
        graph_failures=graph_failures+1 if store.last_error else 0
        if graph_failures>=3:raise RuntimeError('graph mirror failed three consecutive commits; stopping with durable journal')
    try:
        if not args.resume:persist();frame()
        while True:
            if time.monotonic()-started>args.max_wall_seconds:stop_reason='wall-time-limit';break
            if sim.elapsed>=args.until:
                if args.until<86400 or sim.metrics['completed']==len(sim.tasks) and not any(j['task_id'] for j in sim.jobs.values()):break
                if sim.elapsed>=args.until+3600:stop_reason='unfinished-after-drain';break
            sim.step()
            if sim.elapsed%args.commit_seconds==0:persist()
            if sim.elapsed%10==0:frame()
            if time.monotonic()-last_progress>=20:
                print(json.dumps({'elapsed':sim.elapsed,'completed':sim.metrics['completed'],'pending':len(sim.pending),'inside':sim.inside,'minSOC':round(sim.metrics['min_soc'],2),'jobs':len(sim.jobs),'wall':round(time.monotonic()-started,1)},ensure_ascii=False),flush=True);last_progress=time.monotonic()
    except Exception as e:
        failure=f'{type(e).__name__}: {e}';stop_reason='failed';traceback.print_exc()
    finally:
        sim.running=False
        try:persist();frame()
        except Exception as e:failure=failure or f'final-commit: {e}';stop_reason='failed';traceback.print_exc()
    results={'run_id':sim.run_id,'status':stop_reason,'error':failure,'simulated_seconds':sim.elapsed,
        'wall_seconds':round(time.monotonic()-started,3),'backend':'local-central-reserved-trajectories','robot_processes':1,
        'registered_robots':100,'robots_completed_tasks':sum(r['completed']>0 for r in sim.robots.values()),
        'tasks_total':len(sim.tasks),'task_statuses':dict(Counter(t['status'] for t in sim.tasks.values())),
        'inside':sim.inside,'metrics':sim.metrics,'counts':sim.counts(),'knowledge':store.status(sim.run_id),
        'charging':{'configured_docks':args.chargers,'queue_p95_seconds':quantile(sim.metrics['charge_wait_seconds'],.95),
            'queue_max_seconds':max(sim.metrics['charge_wait_seconds'],default=0),
            'utilization':sum(d['charge_seconds'] for d in sim.docks.values())/max(1,args.chargers*min(sim.elapsed,86400))},
        'traffic':{'swept_pair_checks':sim.reservations.checks,'delayed_missions':sim.reservations.delayed,'max_departure_delay':sim.reservations.max_delay},
        'acceptance':{'day_completed':sim.elapsed>=86400 and sim.metrics['completed']==len(sim.tasks),
            'zero_capacity_overflow':sim.metrics['max_inside']<=1728,'minimum_soc_above_emergency':sim.metrics['min_soc']>=10,
            'no_graph_lag':store.status(sim.run_id)['lag']==0,'ray_100_workers':False,'million_node_requirement':False,'latency_joint_requirement':False}}
    if not failure and not store.last_error:
        try:results['graph_audit']=audit_graph(store,sim)
        except Exception as e:results['graph_audit_error']=str(e);results['status']='failed'
    (root/'results.json').write_text(json.dumps(results,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
    print(json.dumps({k:v for k,v in results.items() if k not in {'metrics','graph_audit'}},ensure_ascii=False,indent=2),flush=True)
    frames.close();store.close()
    return 0 if results['status']=='complete' else 2

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--scene',required=True);p.add_argument('--dataset',required=True);p.add_argument('--output',required=True);p.add_argument('--run-id',required=True)
    p.add_argument('--chargers',type=int,default=24);p.add_argument('--until',type=int,default=86400);p.add_argument('--commit-seconds',type=int,default=300)
    p.add_argument('--charging-policy',choices=['threshold','staggered'],default='staggered');p.add_argument('--max-batch',type=int,choices=[1,2],default=2)
    p.add_argument('--max-wall-seconds',type=int,default=1200);p.add_argument('--knowledge-backend',choices=['journal','neo4j'],default='neo4j');p.add_argument('--resume',action='store_true')
    raise SystemExit(run(p.parse_args()))
