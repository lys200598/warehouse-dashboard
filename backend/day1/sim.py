from __future__ import annotations
from collections import Counter
from copy import deepcopy
from dataclasses import dataclass,asdict
import math
import os
import random
import hashlib
import json
from pathlib import Path
from .data import load_scene,read_dataset,sha
from .nav import Navigator,Reservations,leg,hold,position,audit_actual_motion

@dataclass
class Config:
    robot_count:int=100
    flow_rate:float=0
    speed:float=.8
    pick_seconds:int=10
    place_seconds:int=12
    active_soc_per_second:float=60/14400
    idle_soc_per_second:float=.00025
    charge_soc_per_second:float=60/3600
    charge_request_soc:float=35
    charge_target_soc:float=80
    task_reserve_soc:float=20
    emergency_reserve_soc:float=10
    max_departure_wait:int=900
    max_charge_wait:int=7200
    chargers:int=24
    seed:int=20261027
    charging_policy:str='staggered'
    opportunity_charge_soc:float=55
    max_batch:int=2
    desired_working:int=70
    standby_headroom:int=10
    standard_box_kg:float=2
    max_payload_kg:float=5

class Simulation:
    """Deterministic central coordinator with pre-reserved continuous trajectories.

    The first pilot is local, not Ray. Travel/handling events are executed, not
    assigned made-up completion times. Each route is collision-reserved against
    all previously committed routes before departure; private homes/docks stay
    excluded from other robots' paths even when their owners are idle.
    """
    def __init__(self,scene_path,dataset,run_id,config=None,checkpoint=None):
        self.config=config or Config()
        if self.config.robot_count!=100 or not 1<=self.config.chargers<=24:raise ValueError('pilot requires 100 robots and 1..24 existing docks')
        self.scene,self.slot_spec=load_scene(scene_path)
        self.manifest,input_tasks,self.initial=read_dataset(scene_path,dataset)
        self.nav=Navigator(self.scene,self.slot_spec);self.reservations=Reservations()
        self.run_id=run_id;self.tick=0;self.elapsed=0;self.knowledge_seq=0;self.event_sequence=0;self.pending_events=[];self.dispatch_dirty=True
        self.run_metadata={'domain':'warehouse-inbound-outbound-day1','coordinate_unit':'m','time_unit':'seconds',
            'backend':'local-central-trajectory-reservations','dataset_origin':'synthetic-uniform-day1',
            'scene_sha256':sha(scene_path),'tasks_sha256':self.manifest['files']['tasks.csv'],'scene':self.scene,
            'sampling':'all business events; 10s playback frames; 300s complete agent-state graph commits',
            'not_joint_acceptance':True,'energy_parameters':'assumed, not hardware-calibrated'}
        self.run_metadata['initial_inventory_sha256']=self.manifest['files']['initial_inventory.csv']
        self.run_metadata['manifest_sha256']=sha(Path(dataset)/'manifest.json')
        if self.config.standard_box_kg>self.config.max_payload_kg:raise ValueError('standard goods exceed payload')
        if self.scene['spec']['robotRadius']<math.hypot(.35,.35):raise ValueError('planning envelope does not contain loaded 0.7m square')
        sources=[Path(__file__).parent/name for name in ['data.py','nav.py','sim.py','graph.py','runner.py']]
        sources += [Path(__file__).parent.parent/name for name in ['knowledge.py','knowledge_graph.py','knowledge_queries.py','checkpoint.py','routing.py','traffic.py']]
        self.run_metadata['code_sha256']=hashlib.sha256(json.dumps({p.name:sha(p) for p in sources},sort_keys=True).encode()).hexdigest()
        self.tasks={}
        for row in input_tasks:
            t=dict(row);t.update(status='scheduled',robot_id=None,cargo_id=t['goods_id'],
                slot_id=t['target_id'] if t['task_type']=='INBOUND' else t['source_id'])
            self.tasks[t['task_id']]=t
        self.release_order=sorted(self.tasks,key=lambda k:(self.tasks[k]['release_second'],k));self.cursor=0;self.pending=[]
        self.slots=deepcopy(self.slot_spec);self.goods={};self.inside=0;self.ports=Counter();self.jobs={};self.running=True
        rng=random.Random(self.config.seed)
        self.robots={i+1:{'robot_id':i+1,'robot_type_key':'X3','x':h['point']['x'],'y':h['point']['y'],
            'home_id':h['id'],'battery':40+rng.random()*40,'stage':'idle','carrying':False,'cargo_id':None,
            'task_id':None,'dock_id':None,'missions':0,'completed':0,'distance':0.,'active_seconds':0,'charge_seconds':0,
            'queue_seconds':0,'last_completed':0} for i,h in enumerate(self.scene['homes'])}
        # Balanced selection retains both groups when comparing fewer existing docks.
        all_docks=self.scene['docks'];selected=[]
        for a,b in zip(all_docks[:12],all_docks[12:]):selected.extend([a,b])
        self.docks={d['id']:{**d,'available_at':0,'charge_seconds':0,'sessions':0} for d in selected[:self.config.chargers]}
        self.metrics={'received':0,'shipped':0,'completed':0,'late':0,'max_inside':0,'min_inside':864,
            'min_soc':100.,'max_charging':0,'max_moving':0,'max_working':0,'max_queued':0,'schedule_retries':0,
            'charge_sessions':0,'charge_wait_seconds':[],'departure_wait_seconds':[],'charged_but_held_seconds':0,
            'reserved_motion_segments':0,'motion_audit_pairs':0,'min_separation_m':1000.,'unsafe_events':0}
        for row in self.initial:
            g,s=row['goods_id'],row['location_id'];self.slots[s].update(occupied=True,cargo_id=g);self.goods[g]=s;self.inside+=1
            self.event('initial_stock',goods_id=g,location_id=s)
        self.metrics['max_inside']=self.inside
        if checkpoint:self.restore(checkpoint)

    def event(self,kind,robot_id=None,task_id=None,**extra):
        self.dispatch_dirty=True
        self.event_sequence+=1
        self.pending_events.append({'event_id':f'{self.run_id}:event:{self.event_sequence}','event_seq':self.event_sequence,
            'tick':self.tick,'sim_time':self.elapsed,'kind':kind,'robot_id':robot_id,'task_id':task_id,**extra})

    def task_parts(self,robot,tasks):
        parts=[];at=0;home=robot['home_id']
        current=home
        for t in tasks:
            for source,target,phase,action,duration,marker in [(current,t['source_id'],'to_source','picking',self.config.pick_seconds,'picked'),
                (t['source_id'],t['target_id'],'to_target','placing',self.config.place_seconds,'delivered')]:
                more,at=leg(self.nav.route(source,target),phase,at,self.config.speed)
                for p in more:p['task_id']=t['task_id']
                parts+=more
                h=hold(self.nav.locations[target],action,at,duration,marker);h['task_id']=t['task_id'];parts.append(h);at=h['end']
            current=t['target_id']
        more,at=leg(self.nav.route(current,home),'returning',at,self.config.speed)
        for p in more:p['task_id']=tasks[-1]['task_id']
        parts+=more
        return parts

    def charge_parts(self,robot,dock):
        parts,at=leg(self.nav.route(robot['home_id'],dock['id']),'to_charge',0,self.config.speed)
        to_dock=at
        budget=at*self.config.active_soc_per_second+self.config.max_charge_wait*self.config.idle_soc_per_second
        seconds=math.ceil((self.config.charge_target_soc-robot['battery']+budget)/self.config.charge_soc_per_second)
        h=hold(self.nav.locations[dock['id']],'charging',at,max(1,seconds),'charge_complete');parts.append(h);at=h['end']
        more,at=leg(self.nav.route(dock['id'],robot['home_id']),'returning',at,self.config.speed);parts+=more
        return parts,to_dock,h['end']

    def schedule_charge(self,r):
        choices=sorted(self.docks.values(),key=lambda d:(max(self.elapsed,d['available_at'])+abs(d['point']['x']-r['x'])+abs(d['point']['y']-r['y']),d['id']))
        for dock in choices[:4]:
            parts,arrival,leave=self.charge_parts(r,dock)
            earliest=max(self.elapsed,dock['available_at']-arrival)
            horizon=min(900,self.config.max_charge_wait-(earliest-self.elapsed))
            if horizon<0:continue
            start=self.reservations.choose(parts,earliest,horizon)
            if start is None:continue
            needed=(start-self.elapsed)*self.config.idle_soc_per_second+arrival*self.config.active_soc_per_second+self.config.emergency_reserve_soc
            if r['battery']<needed:continue
            self.reservations.reserve(parts,start);dock['available_at']=start+leave+3;dock['sessions']+=1
            self.metrics['reserved_motion_segments']+=sum(p['end']-p['start'] for p in parts if p['phase']!='charging')
            self.jobs[r['robot_id']]={'parts':parts,'start':start,'dock_id':dock['id'],'task_id':None,'phase_index':0,'charge_started':False,'request_time':self.elapsed,'arrival':arrival}
            r.update(stage='charge_queue',dock_id=dock['id']);self.metrics['charge_sessions']+=1
            self.metrics['charge_wait_seconds'].append(start-self.elapsed)
            self.event('charge_reserved',r['robot_id'],dock_id=dock['id'],departure_at=start,estimated_charge_at=start+arrival)
            return True
        return False

    def schedule_task(self,r,t,extra=None):
        batch=[t]+([extra] if extra is not None else [])
        parts=self.task_parts(r,batch)
        # Complete mission, conservative departure waiting, then a further charging trip allowance.
        needed=(parts[-1]['end']+200)*self.config.active_soc_per_second+self.config.max_departure_wait*self.config.idle_soc_per_second+self.config.task_reserve_soc
        if r['battery']<needed:return False
        start=self.reservations.choose(parts,self.elapsed,self.config.max_departure_wait)
        if start is None:self.metrics['schedule_retries']+=1;return False
        self.reservations.reserve(parts,start)
        self.metrics['reserved_motion_segments']+=sum(p['end']-p['start'] for p in parts)
        self.jobs[r['robot_id']]={'parts':parts,'start':start,'task_id':t['task_id'],'task_ids':[v['task_id'] for v in batch],'dock_id':None,'phase_index':0}
        r.update(task_id=t['task_id'],stage='mission_queue',missions=r['missions']+len(batch))
        for item in batch:
            item.update(status='assigned',robot_id=r['robot_id'],assigned_at=self.elapsed,departure_at=start)
            if item['task_type']=='OUTBOUND':self.slots[item['source_id']]['reservedBy']=item['task_id']
            self.event('task_assigned',r['robot_id'],item['task_id'],departure_at=start,batch_size=len(batch))
        self.metrics['departure_wait_seconds'].append(start-self.elapsed)
        return True

    def release_and_dispatch(self):
        while self.cursor<len(self.release_order):
            key=self.release_order[self.cursor];t=self.tasks[key]
            if t['release_second']>self.elapsed:break
            self.cursor+=1;t['status']='waiting';self.pending.append(key);self.event('task_released',task_id=key)
        for key in self.pending:
            t=self.tasks[key]
            if t['task_type']!='INBOUND' or t['status']!='waiting':continue
            slot=self.slots[t['target_id']]
            if self.inside>=1728 or slot['occupied'] or slot['reservedBy'] or self.ports[t['source_id']]>=6:continue
            t['status']='accepted';t['received_at']=self.elapsed;slot['reservedBy']=key
            self.goods[t['goods_id']]=t['source_id'];self.inside+=1;self.ports[t['source_id']]+=1;self.metrics['received']+=1
            self.event('received',task_id=key,goods_id=t['goods_id'],location_id=t['source_id'])
        idle=sorted((r for i,r in self.robots.items() if i not in self.jobs),key=lambda r:(r['missions'],r['last_completed'],r['robot_id']))
        for r in sorted(idle,key=lambda r:(r['battery'],r['robot_id'])):
            charging_jobs=sum(not j['task_id'] for j in self.jobs.values())
            critical=r['battery']<=self.config.charge_request_soc
            opportunity_cap=max(0,self.config.robot_count-self.config.desired_working-self.config.standby_headroom)
            opportunistic=self.config.charging_policy=='staggered' and r['battery']<self.config.opportunity_charge_soc and charging_jobs<min(opportunity_cap,len(self.docks))
            if critical or opportunistic:self.schedule_charge(r)
        idle=[r for r in idle if r['robot_id'] not in self.jobs and r['battery']>self.config.charge_request_soc]
        if not idle:return
        ready=[]
        for key in self.pending:
            t=self.tasks[key]
            if t['status']=='accepted':ready.append(t)
            elif t['status']=='waiting' and t['task_type']=='OUTBOUND':
                slot=self.slots[t['source_id']]
                if slot['cargo_id']==t['goods_id'] and not slot['reservedBy']:ready.append(t)
        # Bound expensive scheduling, without discarding pending demand.
        assigned=0
        for t in sorted(ready,key=lambda t:(t['due_second'],t['task_id']))[:4]:
            if t['status'] not in {'waiting','accepted'}:continue
            other=[v for v in ready if v['task_id']!=t['task_id'] and v['status'] in {'waiting','accepted'}]
            partner=min(other,key=lambda v:abs(self.nav.locations[t['target_id']]['x']-self.nav.locations[v['source_id']]['x'])+abs(self.nav.locations[t['target_id']]['y']-self.nav.locations[v['source_id']]['y'])) if other and self.config.max_batch>=2 else None
            for r in idle[:3]:
                if self.schedule_task(r,t,partner) or partner is not None and self.schedule_task(r,t):idle.remove(r);assigned+=1;break
            if assigned>=2:break
        self.pending=[k for k in self.pending if self.tasks[k]['status'] in {'waiting','accepted'}]

    def marker(self,r,job,marker,task_id=None):
        if marker=='charge_complete':
            self.event('charge_complete',r['robot_id'],dock_id=job['dock_id'],battery=r['battery']);return
        t=self.tasks[task_id or job['task_id']];g=t['goods_id'];slot=self.slots[t['slot_id']]
        if marker=='picked':
            expected=t['source_id']
            if self.goods.get(g)!=expected:raise AssertionError('cargo has wrong holder at pick')
            if t['task_type']=='INBOUND':self.ports[t['source_id']]-=1
            else:slot.update(occupied=False,cargo_id=None,reservedBy=None)
            self.goods[g]=f"ROBOT-{r['robot_id']}";r.update(carrying=True,cargo_id=g);t.update(status='carrying',picked_at=self.elapsed)
            self.event('picked',r['robot_id'],t['task_id'],goods_id=g,location_id=self.goods[g])
        elif marker=='delivered':
            if self.goods.get(g)!=f"ROBOT-{r['robot_id']}":raise AssertionError('cargo duplicated or wrong delivery owner')
            if t['task_type']=='INBOUND':
                if slot['occupied'] or slot['reservedBy']!=t['task_id']:raise AssertionError('slot overbooked')
                slot.update(occupied=True,cargo_id=g,reservedBy=None);self.goods[g]=t['target_id']
            else:self.goods[g]='SHIPPED';self.inside-=1;self.metrics['shipped']+=1
            r.update(carrying=False,cargo_id=None,completed=r['completed']+1,last_completed=self.elapsed)
            t.update(status='completed',completed_at=self.elapsed,late=self.elapsed>t['due_second'])
            self.metrics['completed']+=1;self.metrics['late']+=int(t['late'])
            self.event('placed' if t['task_type']=='INBOUND' else 'shipped',r['robot_id'],t['task_id'],goods_id=g,location_id=self.goods[g])

    def step(self):
        release_due=self.cursor<len(self.release_order) and self.tasks[self.release_order[self.cursor]]['release_second']<=self.elapsed
        if self.dispatch_dirty or release_due or self.pending and len(self.jobs)<100:
            self.release_and_dispatch();self.dispatch_dirty=False
        now=self.elapsed
        before={i:{'x':r['x'],'y':r['y']} for i,r in self.robots.items()}
        for i,r in self.robots.items():
            job=self.jobs.get(i)
            if not job or now<job['start']:
                old_soc=r['battery']
                r['battery']-=self.config.idle_soc_per_second
                if any(old_soc>threshold>=r['battery'] for threshold in [self.config.charge_request_soc,self.config.opportunity_charge_soc]):self.dispatch_dirty=True
                if job:r['queue_seconds']+=1
                continue
            at=now-job['start'];parts=job['parts'];idx=job['phase_index']
            while idx<len(parts) and at>=parts[idx]['end']:idx+=1
            if idx==len(parts):raise AssertionError('unprocessed mission completion')
            job['phase_index']=idx;p=parts[idx];a=position(p,at);b=position(p,at+1)
            if math.hypot(r['x']-a['x'],r['y']-a['y'])>1e-6:raise AssertionError('motion discontinuity')
            r.update(x=b['x'],y=b['y'],stage=p['phase'])
            if p.get('task_id'):r['task_id']=p['task_id']
            r['distance']+=math.hypot(a['x']-b['x'],a['y']-b['y'])
            if p['phase']=='charging':
                if not job['charge_started']:
                    job['charge_started']=True;self.event('charge_started',i,dock_id=job['dock_id'],battery=r['battery'])
                if r['battery']>=self.config.charge_target_soc-1e-8:self.metrics['charged_but_held_seconds']+=1
                r['battery']=min(self.config.charge_target_soc,r['battery']+self.config.charge_soc_per_second)
                r['charge_seconds']+=1;self.docks[job['dock_id']]['charge_seconds']+=1
            else:r['battery']-=self.config.active_soc_per_second;r['active_seconds']+=1
            if at+1==p['end'] and p.get('marker'):
                self.elapsed=now+1;self.tick=now+1;self.marker(r,job,p['marker'],p.get('task_id'));self.elapsed=now;self.tick=now
            if at+1==parts[-1]['end']:
                r.update(stage='idle',task_id=None,dock_id=None);del self.jobs[i];self.dispatch_dirty=True
        self.elapsed+=1;self.tick=self.elapsed
        pairs,separation=audit_actual_motion(before,{i:{'x':r['x'],'y':r['y']} for i,r in self.robots.items()})
        self.metrics['motion_audit_pairs']+=pairs;self.metrics['min_separation_m']=min(self.metrics['min_separation_m'],separation)
        self.metrics['max_inside']=max(self.metrics['max_inside'],self.inside);self.metrics['min_inside']=min(self.metrics['min_inside'],self.inside)
        self.metrics['min_soc']=min(self.metrics['min_soc'],min(r['battery'] for r in self.robots.values()))
        counts=self.counts()
        for name,number in [('charging',counts['charging']),('moving',counts['moving']),('working',counts['working']),('queued',counts['queued'])]:self.metrics['max_'+name]=max(self.metrics['max_'+name],number)
        if not 0<=self.inside<=1728 or any(v<0 or v>6 for v in self.ports.values()):raise AssertionError('capacity invariant violated')
        if self.metrics['min_soc']<5:raise AssertionError('battery emergency threshold crossed')
        if now%60==0:self.reservations.prune(self.elapsed)

    def counts(self):
        c=Counter(r['stage'] for r in self.robots.values())
        return {'moving':sum(c[k] for k in ['to_source','to_target','to_charge','returning']),
            'handling':c['picking']+c['placing'],'charging':c['charging'],'queued':c['mission_queue']+c['charge_queue'],
            'standby':c['idle'],'working':sum(1 for j in self.jobs.values() if j['task_id'])}

    def assert_inventory(self):
        assert self.inside==sum(v!='SHIPPED' for v in self.goods.values())
        present=[]
        for s in self.slots.values():
            if s['occupied']:assert self.goods.get(s['cargo_id'])==s['slot_id'];present.append(s['cargo_id'])
            assert not(s['occupied'] and s['reservedBy'] and self.tasks[s['reservedBy']]['task_type']=='INBOUND')
        assert len(present)==len(set(present))
        held=[r['cargo_id'] for r in self.robots.values() if r['carrying']]
        assert len(held)==len(set(held)) and not(set(held)&set(present))

    def checkpoint(self):
        self.assert_inventory()
        return {'run_id':self.run_id,'tick':self.tick,'elapsed':self.elapsed,'knowledge_seq':self.knowledge_seq,
            'completed':self.metrics['completed'],'running':self.running,'speed':1,'config':asdict(self.config),
            'run_metadata':self.run_metadata,'event_sequence':self.event_sequence,
            'robots':[{'state':deepcopy(r),'worker_pid':os.getpid()} for r in self.robots.values()],
            # Unreleased input is immutable in the hash-checked dataset, not a giant initial graph transaction.
            'tasks':{k:deepcopy(t) for k,t in self.tasks.items() if t['status']!='scheduled'},'racks':[{**r,'slots':[deepcopy(s) for s in self.slots.values() if s['rack_id']==r['id']]} for r in self.scene['racks']],
            'jobs':deepcopy(self.jobs),'goods':dict(self.goods),'inside':self.inside,'ports':dict(self.ports),'docks':deepcopy(self.docks),
            'cursor':self.cursor,'pending':list(self.pending),'dispatch_dirty':self.dispatch_dirty,'metrics':deepcopy(self.metrics)}

    def restore(self,c):
        if c['run_id']!=self.run_id or c['config']!=asdict(self.config) or c['run_metadata']!=self.run_metadata:raise ValueError('recovery identity mismatch')
        for k in ['tick','elapsed','knowledge_seq','event_sequence','goods','inside','docks','cursor','pending','dispatch_dirty','metrics','running']:setattr(self,k,deepcopy(c[k]))
        self.tasks.update(deepcopy(c['tasks']))
        self.ports=Counter(c['ports']);self.robots={p['state']['robot_id']:deepcopy(p['state']) for p in c['robots']}
        self.slots={s['slot_id']:deepcopy(s) for r in c['racks'] for s in r['slots']};self.jobs={int(k):deepcopy(v) for k,v in c['jobs'].items()}
        for job in self.jobs.values():self.reservations.reserve(job['parts'],job['start'])
        self.reservations.prune(self.elapsed);self.pending_events=[];self.assert_inventory()

    def frame(self):
        return {'runId':self.run_id,'elapsed':self.elapsed,'tick':self.tick,'running':self.running,'backend':'local-central-reserved-trajectories',
            'robots':[deepcopy(r) for r in self.robots.values()], 'counts':self.counts(),
            'metrics':{k:v for k,v in self.metrics.items() if not isinstance(v,list)},'inside':self.inside,
            'pending':len(self.pending),'unreleased':len(self.release_order)-self.cursor,
            'occupiedSlots':[s['slot_id'] for s in self.slots.values() if s['occupied']],
            'bufferCounts':{'inbound':sum(self.ports.values()),'outbound':0},
            'totalTasks':len(self.tasks),'chargerCount':len(self.docks),'reservedChargers':sum(d['available_at']>self.elapsed for d in self.docks.values()),
            'reservationChecks':self.reservations.checks,'specVersion':self.scene['spec']['version'] if 'version' in self.scene['spec'] else self.scene['version']}
