from __future__ import annotations
import argparse
import csv
from datetime import datetime, timedelta
import hashlib
import json
from pathlib import Path
import random

FIELDS = ['task_id','task_type','goods_id','release_time','due_time','source_id','target_id']

def sha(path): return hashlib.sha256(Path(path).read_bytes()).hexdigest()

def load_scene(path):
    scene=json.loads(Path(path).read_text(encoding='utf-8'))
    assert scene['spec']['unit']=='m' and len(scene['homes'])==100 and len(scene['docks'])==24
    slots={}
    for r in scene['racks']:
        for level in range(1,4):
            for face in range(2):
                for bay in range(8):
                    key=f"{r['id']}-L{level}-C{bay+1}-F{face+1}"
                    slots[key]={'slot_id':key,'rack_id':r['id'],'level':level,'bay':bay+1,'face':face,
                        'x':r['x']+(bay+.5)*.6,'y':r['y']-.7 if face==0 else r['y']+r['d']+.7,
                        'occupied':False,'cargo_id':None,'reservedBy':None}
    assert len(slots)==1728
    return scene,slots

def generate(scene_path, output, *, seed=20261027, date='2026-10-27',slot_interval_seconds=7200):
    if slot_interval_seconds not in [7200,14400,21600]:raise ValueError('unsupported controlled turnover interval')
    output=Path(output)
    if (output/'manifest.json').exists(): raise FileExistsError('dataset exists; use a new output directory')
    output.mkdir(parents=True,exist_ok=True)
    scene,slots=load_scene(scene_path);rng=random.Random(seed)
    start=datetime.fromisoformat(date+'T00:00:00+08:00')
    keys=list(slots);rng.shuffle(keys);initial_set=set(keys[:864]);initial=[];tasks=[];serial=0
    points={s['id']:s for s in scene['stations']}
    for index,(key,slot) in enumerate(slots.items()):
        occupied=key in initial_set
        goods=f'INITIAL-{index+1:05d}' if occupied else None
        if occupied: initial.append({'goods_id':goods,'location_id':key})
        phase=rng.randrange(1,slot_interval_seconds)
        for n in range(86400//slot_interval_seconds):
            serial+=1;t=phase+n*slot_interval_seconds
            kind='OUTBOUND' if occupied else 'INBOUND'
            p=points[f"{'OUT' if occupied else 'IN'}-{rng.randrange(1,9)}"]
            if not occupied: goods=f'G-{index+1:04d}-{n//2+1:02d}'
            source,target=(key,p['id']) if occupied else (p['id'],key)
            # Axis-aligned reference estimate + deadhead allowance + slack, not a feasibility proof.
            bound=(abs(slot['x']-p['x'])+abs(slot['y']-p['y'])+108)/.8+22
            due=t+int(bound+900)
            tasks.append({'task_id':f'T-{serial:06d}','task_type':kind,'goods_id':goods,
                'release_time':(start+timedelta(seconds=t)).isoformat(),'due_time':(start+timedelta(seconds=due)).isoformat(),
                'source_id':source,'target_id':target})
            occupied=not occupied
    tasks.sort(key=lambda t:(t['release_time'],t['task_id']))
    for filename,rows,fields in [('tasks.csv',tasks,FIELDS),('initial_inventory.csv',initial,['goods_id','location_id'])]:
        with (output/filename).open('w',encoding='utf-8',newline='') as f:
            writer=csv.DictWriter(f,fieldnames=fields);writer.writeheader();writer.writerows(rows)
    manifest={'version':'warehouse-uniform-day1-v1','scenario':'uniform','seed':seed,'start':start.isoformat(),
        'duration_seconds':86400,'time_unit':'ISO8601 +08:00','scene_sha256':sha(scene_path),
        'tasks':len(tasks),'inbound':sum(t['task_type']=='INBOUND' for t in tasks),'outbound':sum(t['task_type']=='OUTBOUND' for t in tasks),
        'initial_inventory':len(initial),'capacity':1728,'source':'synthetic; LaDe-like seven-field task structure, NOT real Jilin warehouse records',
        'slot_turnover_seconds':slot_interval_seconds,
        'assumptions':{'uniformity':f'independent uniform phase per slot; alternate in/out every {slot_interval_seconds/3600:g}h; controlled synthetic periodicity',
            'handling':'robot itself picks/places; no external processing device','goods_per_task':1,
            'buffer':'8 boundary pickup points, 6 abstract boxes each; internal stack/contact geometry not simulated',
            'shipment':'outbound drop confirmation crosses warehouse boundary immediately',
            'deadline':'Manhattan reference estimate + 108m deadhead allowance at 0.8m/s +22s handling +900s slack; not a strict lower bound for diagonal routes'},
        'files':{f:sha(output/f) for f in ['tasks.csv','initial_inventory.csv']}}
    (output/'manifest.json').write_text(json.dumps(manifest,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
    report=validate(scene_path,output)
    (output/'validation.json').write_text(json.dumps(report,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
    return manifest,report

def read_dataset(scene_path,directory):
    directory=Path(directory);m=json.loads((directory/'manifest.json').read_text(encoding='utf-8'))
    if m['scene_sha256']!=sha(scene_path): raise ValueError('scene hash mismatch')
    for f,h in m['files'].items():
        if sha(directory/f)!=h: raise ValueError('dataset hash mismatch: '+f)
    with (directory/'tasks.csv').open(encoding='utf-8',newline='') as f:
        reader=csv.DictReader(f)
        if reader.fieldnames!=FIELDS: raise ValueError('task columns changed')
        tasks=list(reader)
    with (directory/'initial_inventory.csv').open(encoding='utf-8',newline='') as f: initial=list(csv.DictReader(f))
    start=datetime.fromisoformat(m['start'])
    for t in tasks:
        t['release_second']=int((datetime.fromisoformat(t['release_time'])-start).total_seconds())
        t['due_second']=int((datetime.fromisoformat(t['due_time'])-start).total_seconds())
    return m,tasks,initial

def validate(scene_path,directory):
    scene,slots=load_scene(scene_path);m,tasks,initial=read_dataset(scene_path,directory)
    errors=[];stock={};where={};task_ids=set();goods_seen=set();hourly=[0]*24
    for row in initial:
        g,s=row['goods_id'],row['location_id']
        if s not in slots or s in stock or g in where: errors.append('invalid initial inventory')
        stock[s]=g;where[g]=s;goods_seen.add(g)
    lo=hi=len(stock);ports={s['id']:s['kind'] for s in scene['stations']}
    for t in sorted(tasks,key=lambda x:(x['release_second'],x['task_id'])):
        if t['task_id'] in task_ids: errors.append('duplicate task')
        task_ids.add(t['task_id'])
        if not 0<=t['release_second']<86400 or t['due_second']<=t['release_second']: errors.append('invalid time')
        hourly[t['release_second']//3600]+=1
        g=t['goods_id']
        if t['task_type']=='INBOUND':
            s=t['target_id']
            if s not in slots or s in stock or g in goods_seen or ports.get(t['source_id'])!='inbound': errors.append('invalid inbound')
            stock[s]=g;where[g]=s;goods_seen.add(g)
        elif t['task_type']=='OUTBOUND':
            s=t['source_id']
            if stock.get(s)!=g or where.get(g)!=s or ports.get(t['target_id'])!='outbound': errors.append('invalid outbound dependency')
            stock.pop(s,None);where.pop(g,None)
        else: errors.append('unknown task type')
        lo=min(lo,len(stock));hi=max(hi,len(stock))
        if not 0<=len(stock)<=1728: errors.append('planned capacity exceeded')
    if errors: raise ValueError(f'dataset validation failed: {errors[:10]}')
    return {'valid':True,'tasks':len(tasks),'initial':len(initial),'planned_min_inventory':lo,'planned_max_inventory':hi,
        'planned_final_inventory':len(stock),'hourly_tasks':hourly,'note':'reference-plan feasibility; runtime must independently enforce real capacity and dependencies'}

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--scene',required=True);p.add_argument('--output',required=True);p.add_argument('--slot-interval-seconds',type=int,default=7200);a=p.parse_args()
    print(json.dumps(generate(a.scene,a.output,slot_interval_seconds=a.slot_interval_seconds),ensure_ascii=False,indent=2))
