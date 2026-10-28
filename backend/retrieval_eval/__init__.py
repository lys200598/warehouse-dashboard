"""Explainable historical-state retrieval, separate from legacy 32D indexing."""
import math
from numbers import Real

VERSION = 'warehouse-business-state-v1'
FAMILIES = ('idle','transport','handling','charging','returning','assistance','fault','unknown')
DIMENSIONS = 17


def family(stage):
    if stage in {'idle','standby','mission_queue','queued'}: return 'idle'
    if stage in {'to_source','to_target','moving'}: return 'transport'
    if stage in {'picking','placing','docking','undocking'}: return 'handling'
    if stage in {'charging','charge_queue'}: return 'charging'
    if stage in {'returning','to_charge'}: return 'returning'
    if stage in {'to_handoff','handoff','handoff_receive','handoff_give'}: return 'assistance'
    if stage in {'fault_wait','faulted','recovery_wait','yielding','blocked'}: return 'fault'
    return 'unknown'


def number(value, name, *, positive=False):
    if type(value) not in (int,float) or not math.isfinite(value) or positive and value <= 0:
        raise ValueError('invalid '+name)
    return float(value)


def business_vector(state, width=90., depth=76.):
    """17D: bias, complemented xy/SOC/load, and 8 categorical phase families.

    Robot ID, task/cargo ID, timestamp, commit number and salted identity are
    deliberately absent. This is a declared structural similarity, not text
    semantics, a trained metric, or a prediction of successful assistance.
    """
    width,depth=number(width,'width',positive=True),number(depth,'depth',positive=True)
    x,y=number(state.get('x'),'x'),number(state.get('y'),'y')
    battery=number(state.get('battery'),'battery')
    if not 0<=x<=width or not 0<=y<=depth or not 0<=battery<=100:
        raise ValueError('physical feature outside declared geometry/SOC')
    if type(state.get('carrying')) is not bool or not isinstance(state.get('stage'),str) or not state['stage']:
        raise ValueError('typed carrying/stage required')
    x,y,soc,loaded=x/width,y/depth,battery/100,float(state['carrying'])
    phase=family(state['stage'])
    values=[1.,x,1-x,y,1-y,soc,1-soc,loaded,1-loaded]
    values += [float(phase==name) for name in FAMILIES]
    norm=math.sqrt(sum(v*v for v in values))
    return [v/norm for v in values]


def cosine(left,right):
    if len(left)!=len(right) or len(left)==0: raise ValueError('vector dimension mismatch')
    for v in (*left,*right):
        if isinstance(v,bool) or not isinstance(v,Real) or not math.isfinite(v):
            raise ValueError('finite numeric vector required')
    a=[float(v) for v in left]; b=[float(v) for v in right]
    norm=math.sqrt(math.fsum(v*v for v in a)*math.fsum(v*v for v in b))
    if norm==0: raise ValueError('zero vector')
    return math.fsum(x*y for x,y in zip(a,b))/norm


def business_relevant(query,candidate):
    """Independent declared relevance label, NOT defined by cosine rank.

    Same carrying and phase family, SOC difference <=10 percentage points,
    and Euclidean separation <=15m. Does not label task success or causality.
    """
    return (query['carrying']==candidate['carrying'] and family(query['stage'])==family(candidate['stage'])
        and abs(query['battery']-candidate['battery'])<=10
        and math.hypot(query['x']-candidate['x'],query['y']-candidate['y'])<=15)
