"""Conservative static planning plus exact swept reservations of scheduled motion.

One-second linear segments, including all public pick/place holds, are booked by
the central coordinator before departure. Idle homes and charging pads remain
private static exclusions for every other robot. No teleportation or stopped
robot is silently removed to make another route pass.
"""
from __future__ import annotations
from collections import defaultdict
from functools import lru_cache
import heapq
import math
from ..routing import segment_hits_box
from ..traffic import swept_distance

class Navigator:
    def __init__(self,scene,slots):
        self.scene=scene;self.slots=slots;self.radius=.55;self.step=.2;self.nx=301;self.ny=241
        self.fixed=[(r['x'],r['y'],r['x']+r['w'],r['y']+r['d']) for r in scene['racks']+scene['buffers']]
        self.private={r['id']:(r['x'],r['y'],r['x']+r['w'],r['y']+r['d']) for r in scene['homes']+scene['docks']}
        self.locations={k:{'x':s['x'],'y':s['y']} for k,s in slots.items()}
        self.locations.update({r['id']:r['point'] for r in scene['homes']+scene['docks']})
        self.locations.update({r['id']:{'x':r['x'],'y':r['y']} for r in scene['stations']})
        self.blocked=bytearray(self.nx*self.ny);self.owners=defaultdict(set)
        self.geometry_cache={}
        for box in self.fixed:self._raster(box,None)
        for name,box in self.private.items():self._raster(box,name)
    def _raster(self,b,name):
        s,r=self.step,self.radius
        for y in range(max(0,math.ceil((b[1]-r)/s)),min(self.ny,math.floor((b[3]+r)/s)+1)):
            for x in range(max(0,math.ceil((b[0]-r)/s)),min(self.nx,math.floor((b[2]+r)/s)+1)):
                key=y*self.nx+x
                if name:self.owners[key].add(name)
                else:self.blocked[key]=1
    def clear(self,a,b,allowed):
        return not any(segment_hits_box((a['x'],a['y']),(b['x'],b['y']),box,self.radius-1e-7) for box in self.fixed+[v for k,v in self.private.items() if k not in allowed])
    @lru_cache(maxsize=60000)
    def route(self,source,target):
        a,b=self.locations[source],self.locations[target];allowed={source,target}
        signature=(a['x'],a['y'],b['x'],b['y'],source if source in self.private else '',target if target in self.private else '')
        if signature in self.geometry_cache:return self.geometry_cache[signature]
        if self.clear(a,b,allowed):self.geometry_cache[signature]=[a,b];return [a,b]
        def key(p):return round(p['y']/self.step)*self.nx+round(p['x']/self.step)
        def point(k):return {'x':(k%self.nx)*self.step,'y':(k//self.nx)*self.step}
        def opened(k):
            x,y=k%self.nx,k//self.nx
            return 3<=x<self.nx-3 and 3<=y<self.ny-3 and not self.blocked[k] and self.owners.get(k,set())<=allowed
        start,goal=key(a),key(b)
        if not opened(start) or not opened(goal):raise ValueError(f'inaccessible endpoint {source}/{target}')
        def heuristic(k):return abs(k%self.nx-goal%self.nx)+abs(k//self.nx-goal//self.nx)
        best={start:0};parents={};queue=[(heuristic(start),0,start)]
        while queue:
            _,negative_cost,k=heapq.heappop(queue);cost=-negative_cost
            if cost!=best[k]:continue
            if k==goal:
                path=[]
                while k!=start:path.append(point(k));k=parents[k]
                path.append(point(start));path.reverse();path=[a]+path+[b]
                out=[path[0]]
                for i in range(1,len(path)-1):
                    u,v,w=path[i-1:i+2]
                    if abs((v['x']-u['x'])*(w['y']-v['y'])-(v['y']-u['y'])*(w['x']-v['x']))>1e-8:out.append(v)
                out.append(b)
                if not all(self.clear(u,v,allowed) for u,v in zip(out,out[1:])):
                    # Keep individually checked lattice segments when connector compression bends.
                    out=path
                    if not all(self.clear(u,v,allowed) for u,v in zip(out,out[1:])):raise ValueError('unsafe route connector')
                self.geometry_cache[signature]=out
                return out
            for n in (k-1,k+1,k-self.nx,k+self.nx):
                if n<0 or n>=self.nx*self.ny or not opened(n):continue
                g=cost+1
                if g<best.get(n,10**9):best[n]=g;parents[n]=k;heapq.heappush(queue,(g+heuristic(n),-g,n))
        raise ValueError(f'no static route {source}/{target}')

def leg(path,phase,start=0,speed=.8):
    parts=[];at=start
    for a,b in zip(path,path[1:]):
        length=math.hypot(a['x']-b['x'],a['y']-b['y'])
        if length<1e-8:continue
        end=at+math.ceil(length/speed)
        parts.append({'a':a,'b':b,'start':at,'end':end,'phase':phase});at=end
    return parts,at

def hold(point,phase,start,duration,marker=None):
    return {'a':point,'b':point,'start':start,'end':start+duration,'phase':phase,'marker':marker}

def position(part,second):
    t=max(0.,min(1.,(second-part['start'])/(part['end']-part['start'])))
    return {'x':part['a']['x']+(part['b']['x']-part['a']['x'])*t,'y':part['a']['y']+(part['b']['y']-part['a']['y'])*t}

class Reservations:
    def __init__(self):self.book={};self.checks=0;self.delayed=0;self.max_delay=0
    @staticmethod
    def cells(a,b,padding=1.1):
        for x in range(math.floor((min(a['x'],b['x'])-padding)/2.2),math.floor((max(a['x'],b['x'])+padding)/2.2)+1):
            for y in range(math.floor((min(a['y'],b['y'])-padding)/2.2),math.floor((max(a['y'],b['y'])+padding)/2.2)+1):yield x,y
    @staticmethod
    def motions(parts):
        for p in parts:
            if p['phase']=='charging':continue  # Exclusive dock calendar + private geometry protect this hold.
            for t in range(p['start'],p['end']):yield t,position(p,t),position(p,t+1),p
    def choose(self,parts,earliest,horizon=900):
        motion=list(self.motions(parts))
        offset=earliest;limit=earliest+horizon
        while offset<=limit:
            valid=True;jump=offset+1
            for t,a,b,_ in motion:
                grid=self.book.get(t+offset)
                if not grid:continue
                seen=set()
                for cell in self.cells(a,b):
                    for index,other in grid.get(cell,()):
                        if index in seen:continue
                        seen.add(index);self.checks+=1
                        if swept_distance(a,b,other[0],other[1])<1.1-1e-7:
                            valid=False;span=other[2]
                            # During one other constant-velocity segment, collision offsets form
                            # an interval (projection of a convex swept-distance sublevel set).
                            # Skip only this proven colliding interval, never a potentially safe gap.
                            last=min(limit,span['end']-t-1)
                            def collides(candidate):
                                self.checks+=1
                                return swept_distance(a,b,position(span,candidate+t),position(span,candidate+t+1))<1.1-1e-7
                            if last>offset:
                                if collides(last):jump=last+1
                                else:
                                    lo,hi=offset,last
                                    while hi-lo>1:
                                        mid=(lo+hi)//2
                                        if collides(mid):lo=mid
                                        else:hi=mid
                                    jump=hi
                            break
                    if not valid:break
                if not valid:break
            if valid:
                delay=offset-earliest
                if delay:self.delayed+=1;self.max_delay=max(self.max_delay,delay)
                return offset
            offset=jump
        return None
    def reserve(self,parts,offset):
        spans={}
        for t,a,b,p in self.motions(parts):
            grid=self.book.setdefault(t+offset,defaultdict(list));index=sum(len(v) for v in grid.values())
            if id(p) not in spans:spans[id(p)]={**p,'start':p['start']+offset,'end':p['end']+offset}
            for cell in self.cells(a,b,0):grid[cell].append((index,(a,b,spans[id(p)])))
    def prune(self,now):
        for k in list(self.book):
            if k<now-1:del self.book[k]

def audit_actual_motion(before,after,*,cells=None):
    """Independent swept audit, including every idle/charging body, each real tick."""
    cells=cells or Reservations.cells
    grid=defaultdict(list);checks=0;minimum=1000.
    for robot,b in after.items():
        a=before[robot];seen=set()
        for cell in cells(a,b):
            for other,c,d in grid[cell]:
                if other in seen:continue
                seen.add(other);distance=swept_distance(a,b,c,d);checks+=1;minimum=min(minimum,distance)
                if distance<1.1-1e-6:raise AssertionError(f'actual swept collision {robot}/{other}: {distance}')
        for cell in cells(a,b,0):grid[cell].append((robot,a,b))
    return checks,minimum
