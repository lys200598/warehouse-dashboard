"""Optimistic energy/queue sensitivity, NOT a warehouse traffic acceptance test."""
import argparse
import json
import math
from pathlib import Path
import random

def probe(docks,work_hours=4,charge_hours=1,hours=72,target=70,policy='staggered'):
    rng=random.Random(20261027);soc=[40+rng.random()*40 for _ in range(100)]
    worked=[0]*100;charging=set();waiting={};waits=[];shortfall=0;lost=0;peak_queue=0;minimum=min(soc);daily=[];charge_minutes=0
    for minute in range(hours*60):
        for i in list(charging):
            if soc[i]>=80-1e-8:charging.remove(i)
        for i in range(100):
            if soc[i]<=35 and i not in charging and i not in waiting:waiting[i]=minute
        for i in sorted(waiting,key=lambda i:(soc[i],waiting[i],i))[:max(0,docks-len(charging))]:
            waits.append(minute-waiting.pop(i));charging.add(i)
        if policy=='staggered':
            opportunities=[i for i in range(100) if i not in charging and i not in waiting and soc[i]<55]
            for i in sorted(opportunities,key=lambda i:(soc[i],i))[:max(0,min(20,docks)-len(charging))]:
                charging.add(i);waits.append(0)
        eligible=[i for i in range(100) if i not in charging and i not in waiting]
        active=set(sorted(eligible,key=lambda i:(worked[i],i))[:target])
        if len(active)<target:shortfall+=1;lost+=target-len(active)
        for i in range(100):
            if i in charging:soc[i]=min(80,soc[i]+60/(charge_hours*60));charge_minutes+=1
            elif i in active:soc[i]-=60/(work_hours*60);worked[i]+=1
            else:soc[i]-=.00025*60
        minimum=min(minimum,min(soc));peak_queue=max(peak_queue,len(waiting))
        if (minute+1)%1440==0:daily.append({'hour':(minute+1)//60,'mean_soc':sum(soc)/100,'queued':len(waiting),'charging':len(charging)})
    ordered=sorted(waits)
    return {'docks':docks,'policy':policy,'work_hours':work_hours,'charge_hours':charge_hours,'target_working':target,'hours':hours,
        'ideal_average_dock_demand':target*charge_hours/work_hours,
        'shortfall_minutes':shortfall,'lost_working_robot_minutes':lost,'peak_queue':peak_queue,
        'queue_p95_minutes':ordered[int(.95*(len(ordered)-1))] if ordered else 0,'queue_max_minutes':max(waits,default=0),
        'min_soc':minimum,'dock_utilization':charge_minutes/(docks*hours*60),'daily':daily}

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--output',required=True);a=p.parse_args()
    result={'scope':'optimistic 1-minute energy/queue model; no travel, no handling geometry, no faults; NOT physical warehouse sizing certification',
        'cases':[probe(24,policy='threshold')]+[probe(n) for n in [16,20,24]]+[probe(24,work_hours=3),probe(24,charge_hours=1.5)]}
    Path(a.output).parent.mkdir(parents=True,exist_ok=True);Path(a.output).write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
    print(json.dumps(result,ensure_ascii=False,indent=2))
