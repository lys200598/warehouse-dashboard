from __future__ import annotations
import math
from dataclasses import dataclass
from typing import Any

@dataclass
class TrafficDecision:
    accepted: dict[int,bool]
    blocked_robot_ids: list[int]

def swept_distance(a0,a1,b0,b1):
    """Minimum separation during simultaneous linear movements, not just endpoints."""
    x,y = a0['x']-b0['x'],a0['y']-b0['y']
    dx,dy = (a1['x']-a0['x'])-(b1['x']-b0['x']),(a1['y']-a0['y'])-(b1['y']-b0['y'])
    d = dx*dx+dy*dy
    t = max(0.,min(1.,-(x*dx+y*dy)/d)) if d else 0.
    return math.hypot(x+t*dx,y+t*dy)

class TrafficManager:
    """Swept reservations. Rejected movers keep occupying their start positions.

    All visible robots occupy space, including idle/charging robots.
    """
    def __init__(self, cell_size=.38):
        self.cell_size = cell_size

    def resolve(self, proposals: list[dict[str,Any]]) -> TrafficDecision:
        accepted = {int(p['robot_id']):True for p in proposals}
        priority = {'high':0,'medium':1,'normal':2}
        movers = sorted((p for p in proposals if p['moving']),key=lambda p:(
            priority.get(p.get('priority','normal'),2),-p.get('blocked_steps',0),p['robot_id']))
        present = proposals
        def collide(left,right,left_moves=True,right_moves=True):
            return swept_distance(left['from'],left['to'] if left_moves else left['from'],
                                  right['from'],right['to'] if right_moves else right['from']) < self.cell_size-1e-9
        for i,left in enumerate(movers):
            if not accepted[left['robot_id']]:
                continue
            for right in movers[i+1:]:
                if not accepted[right['robot_id']] or not collide(left,right):
                    continue
                left_alone = not collide(left,right,True,False)
                right_alone = not collide(left,right,False,True)
                if left_alone:
                    accepted[right['robot_id']] = False
                elif right_alone:
                    accepted[left['robot_id']] = False
                    break
                else:
                    accepted[left['robot_id']] = accepted[right['robot_id']] = False
                    break
        changed = True
        while changed:
            changed = False
            stopped = [p for p in present if not p['moving'] or not accepted[p['robot_id']]]
            for p in movers:
                robot = p['robot_id']
                if accepted[robot] and any(q['robot_id'] != robot and collide(p,q,True,False) for q in stopped):
                    accepted[robot] = False
                    changed = True
        blocked = sorted(p['robot_id'] for p in movers if not accepted[p['robot_id']])
        return TrafficDecision(accepted,blocked)
