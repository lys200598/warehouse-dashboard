"""Independent small time-expanded oracle versus both SIPP search orders."""
import heapq
import importlib.util
import math
from pathlib import Path
import random
import sys
import unittest
from backend.scheduling_v4.planner import GoalWindowPlanner

if (Path.cwd()/'warehouse-dashboard').is_dir():sys.path.insert(0,str(Path.cwd()/'warehouse-dashboard'))
from backend.scheduling_v2.motion import SafeIntervalPlanner,MotionTable
from backend.day1.nav import hold
class Map:
    def __init__(self):
        self.nodes={y*4+x:{'x':x*2.,'y':y*2.} for y in range(4) for x in range(4)}
        self.edges={k:[n for n in self.nodes if abs(n//4-k//4)+abs(n%4-k%4)==1] for k in self.nodes}
        self.locations={'A':self.nodes[0],'B':self.nodes[15]};self.base=self
    def clear(self,*args):return False
    def connections(self,name):return [0 if name=='A' else 15]
    def distances(self,target,speed):
        return {k:2*(abs(k//4-3)+abs(k%4-3))+1 for k in self.nodes}


def oracle(m,t,earliest,dwell,horizon):
    points={**m.nodes,-1:m.locations['A'],-2:m.locations['B']}
    intervals={k:t.intervals(v,0,horizon+dwell,1) for k,v in points.items()}
    valid=lambda k,at:any(lo<=at<=hi for lo,hi in intervals[k])
    if not valid(-1,0):return None
    seen=set();queue=[(0,-1)]
    while queue:
        at,node=heapq.heappop(queue)
        if (node,at) in seen:continue
        seen.add((node,at))
        if node==-2 and at>=earliest and any(lo<=at and at+dwell<=hi for lo,hi in intervals[node]):return at+dwell
        neighbors=([0] if node==-1 else [] if node==-2 else m.edges[node]+([-2] if node==15 else []))
        for nxt in [node]+neighbors:
            a,b=points[node],points[nxt]
            delta=max(1,math.ceil(math.hypot(a['x']-b['x'],a['y']-b['y'])-1e-9))
            end=at+delta
            if end>horizon or not valid(nxt,end) or not valid(node,at):continue
            if nxt==-2 and end<earliest:continue
            if not t.clear_motion(a,b,at,end,1):continue
            if nxt==node and not any(lo<=at and end<=hi for lo,hi in intervals[node]):continue
            heapq.heappush(queue,(end,nxt))
    return None


class PlannerOracleTests(unittest.TestCase):
    def test_120_time_expanded_cases_and_continuous_route_audits(self):
        self.check_cases()

    def check_cases(self):
        rng=random.Random(271027);m=Map()
        for case in range(120):
            table=MotionTable()
            for owner in range(2,5):
                node=rng.randrange(1,16);start=rng.randrange(0,25);end=start+rng.randrange(1,30)
                table.reserve([hold(m.nodes[node],'blocked',start,end-start)],owner=owner)
            earliest=rng.randrange(0,40);dwell=rng.randrange(0,5);expected=oracle(m,table,earliest,dwell,60)
            for variant in [False,True]:
                planner=(GoalWindowPlanner if variant else SafeIntervalPlanner)(m,table,1,60,200000)
                value=planner.plan('A','B',0,1,'moving',dwell,'placing','done',earliest_goal=earliest)
                actual=value[1] if value else None
                assert actual==expected,(case,variant,actual,expected)
                if value:
                    parts=value[0]
                    assert all(table.clear_motion(p['a'],p['b'],p['start'],p['end'],1) for p in parts)
                    assert all(a['end']==b['start'] and a['b']==b['a'] for a,b in zip(parts,parts[1:]))
