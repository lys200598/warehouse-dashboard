"""Bounded 2-D detours in the existing abstract warehouse coordinate model.

No teleportation: routes always begin at the committed position. This is not a
rigid-body or shelf-geometry planner; traffic reservations still arbitrate every
actual movement. Other active robots are temporary circular obstacles.
"""
from __future__ import annotations

import heapq
import math


def segment_distance(start, end, point):
    dx, dy = end[0]-start[0], end[1]-start[1]
    length = dx*dx+dy*dy
    t = max(0., min(1., ((point[0]-start[0])*dx+(point[1]-start[1])*dy)/length)) if length else 0.
    return math.hypot(start[0]+t*dx-point[0], start[1]+t*dy-point[1])


def segment_hits_box(a, b, box, margin=0.):
    lower,upper = 0.,1.
    for origin,delta,lo,hi in ((a[0],b[0]-a[0],box[0]-margin,box[2]+margin),(a[1],b[1]-a[1],box[1]-margin,box[3]+margin)):
        if abs(delta) < 1e-12:
            if origin < lo or origin > hi:
                return False
        else:
            x,y = (lo-origin)/delta,(hi-origin)/delta
            lower,upper = max(lower,min(x,y)),min(upper,max(x,y))
            if lower > upper:
                return False
    return True


def detour(start, goal, obstacles, radius, *, boxes=(), box_margin=.12, spacing=.3, max_expansions=6000):
    start, goal = tuple(start), tuple(goal)
    # A robot cannot plan into an occupied loading/placing station. Wait until it
    # clears instead of removing the obstacle or marking arrival early.
    obstacles = [tuple(p) for p in obstacles]
    def clear(a, b):
        return (not any(segment_hits_box(a,b,box,box_margin) for box in boxes)
                and all(segment_distance(a,b,p) >= radius for p in obstacles))
    if any(math.dist(goal,p) < radius for p in obstacles):
        return None
    if clear(start,goal):
        return [{"x":start[0],"y":start[1]}, {"x":goal[0],"y":goal[1]}]
    # The lattice is anchored at the actual position, so snapping never moves an
    # actor. Bounds match the original dashboard's abstract 32 x 22 floor.
    def position(cell):
        return start[0]+cell[0]*spacing, start[1]+cell[1]*spacing
    root = (0,0)
    queue = [(math.dist(start,goal),0.,root)]
    best, parents = {root:0.}, {}
    directions = ((1,0),(-1,0),(0,1),(0,-1),(1,1),(1,-1),(-1,1),(-1,-1))
    expansions = 0
    while queue and expansions < max_expansions:
        _,cost,cell = heapq.heappop(queue)
        if cost > best[cell]+1e-8:
            continue
        expansions += 1
        point = position(cell)
        if math.dist(point,goal) <= spacing*1.5 and clear(point,goal):
            cells = [cell]
            while cells[-1] != root:
                cells.append(parents[cells[-1]])
            points = [position(c) for c in reversed(cells)]+[goal]
            # String-pull only checked segments; keeps Actor waypoint payloads small.
            simplified = [points[0]]
            at = 0
            while at < len(points)-1:
                end = len(points)-1
                while end > at+1 and not clear(points[at],points[end]):
                    end -= 1
                simplified.append(points[end])
                at = end
            return [{"x":p[0],"y":p[1]} for p in simplified]
        for dx,dy in directions:
            neighbor = cell[0]+dx,cell[1]+dy
            target = position(neighbor)
            if not (.25 <= target[0] <= 31.75 and .25 <= target[1] <= 21.75) or not clear(point,target):
                continue
            value = cost+spacing*math.hypot(dx,dy)
            if value+1e-8 < best.get(neighbor,float('inf')):
                best[neighbor],parents[neighbor] = value,cell
                heapq.heappush(queue,(value+math.dist(target,goal),value,neighbor))
    return None
