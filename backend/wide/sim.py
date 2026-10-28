"""Parameterised wide layout; unchanged v3 assignment/routing/assistance policy.

The only new traffic contract is explicit terminal occupancy: an empty physical
bay is traversable, but a parked robot cannot disappear after its trip ends.
P and C names map to one bay. There is no new joint traffic optimiser here.
"""
from collections import Counter
from contextlib import contextmanager
from copy import deepcopy
from dataclasses import dataclass
import hashlib
import json
import math
import random
from pathlib import Path

from ..day1.data import sha
from ..scheduling_v2.motion import SafeIntervalPlanner
from ..scheduling_v2.motion import continuous_clear
from ..scheduling_v3.sim import Simulation as V3Simulation, Config as V3Config
from .data import load_scene, read_dataset, validate_dataset
from .motion import WideNavigator, WideRoadmap, WideMotionTable, idle_guard, terminal_tail
from .capabilities import fleet_assignment, slot_requirement, access_reason, spare_cost, profile, VERSION as CAPABILITY_VERSION


@dataclass
class Config(V3Config):
    chargers: int = 100
    fleet_mode: str = 'all-x3'
    fleet_counts: dict | None = None
    capability_spare_cost_seconds: float = 15.
    inbound_admission_policy: str = 'fifo'
    port_relief_policy: str = 'off'

    def __post_init__(self):
        super().__post_init__()
        fleet_assignment(self.fleet_mode, self.fleet_counts, self.robot_count)
        if self.inbound_admission_policy not in ('fifo', 'deadline'):
            raise ValueError('unknown inbound admission policy')
        if self.port_relief_policy not in ('off', 'deadline'):
            raise ValueError('unknown port relief policy')
        if (isinstance(self.capability_spare_cost_seconds, bool) or
                not isinstance(self.capability_spare_cost_seconds, (int, float)) or
                not math.isfinite(self.capability_spare_cost_seconds) or
                not 0 <= self.capability_spare_cost_seconds <= 300):
            raise ValueError('capability spare cost must be finite in [0,300] seconds')


class Simulation(V3Simulation):
    reservation_type = WideMotionTable
    roadmap_type = WideRoadmap
    planner_type = SafeIntervalPlanner

    def __init__(self, scene_path, dataset, run_id, config=None, checkpoint=None):
        super().__init__(scene_path,dataset,run_id,config or Config(),checkpoint=None)
        self.run_metadata.update(backend='wide-scene-v3-policy-knowledge-baseline',
            physical_bays=100, home_aliases=100,
            occupancy='empty hardware traversable; full trip plus persistent terminal body validated before commit')
        sources = sorted(Path(__file__).parent.glob('*.py'))
        self.run_metadata['wide_code_sha256'] = hashlib.sha256(json.dumps(
            {p.name:sha(p) for p in sources},sort_keys=True).encode()).hexdigest()
        backend=Path(__file__).parents[1]
        self.run_metadata['code_sha256']=hashlib.sha256(json.dumps(
            {p.relative_to(backend).as_posix():sha(p) for p in sorted(backend.rglob('*.py'))},sort_keys=True).encode()).hexdigest()
        for owner, robot in self.robots.items():
            self.reservations.reserve([idle_guard(self._point(robot),self.elapsed)],owner=owner)
        if checkpoint:
            self.restore(checkpoint)

    def _initialize_base(self, scene_path, dataset, run_id, config):
        validate_dataset(scene_path,dataset)
        self.config=config
        self.scene,self.slot_spec=load_scene(scene_path)
        if config.robot_count != 100 or config.chargers != len(self.scene['docks']):
            raise ValueError('simple wide model requires 100 robots and all 100 assigned charging/standby bays')
        if config.standard_box_kg > config.max_payload_kg:
            raise ValueError('goods exceed payload')
        if config.dispatch_seconds < 1 or not 1 <= config.batch_tasks <= 100 or config.assignment_cost not in {'roadmap','euclidean'}:
            raise ValueError('invalid bounded dispatch configuration')
        self.manifest,input_tasks,self.initial=read_dataset(scene_path,dataset)
        self.nav=WideNavigator(self.scene,self.slot_spec)
        self.roadmap=self.roadmap_type(self.nav)
        self.reservations=self.reservation_type()
        self.planner=self.planner_type(self.roadmap,self.reservations,config.speed,config.route_horizon,config.route_expansions)
        self.run_id=run_id
        self.tick=self.elapsed=self.knowledge_seq=self.event_sequence=0
        self.pending_events=[]
        self.dispatch_dirty=True
        self.next_dispatch=0
        self.handoff_ready=set()
        sources = [Path(__file__),Path(__file__).with_name('data.py'),Path(__file__).with_name('motion.py')]
        self.run_metadata={'domain':'warehouse-wide-inbound-outbound','coordinate_unit':'m','time_unit':'seconds',
            'scene':self.scene,'scene_sha256':sha(scene_path),'tasks_sha256':self.manifest['files']['tasks.csv'],
            'initial_inventory_sha256':self.manifest['files']['initial_inventory.csv'],
            'manifest_sha256':sha(Path(dataset)/'manifest.json'),
            'code_sha256':hashlib.sha256(json.dumps({p.name:sha(p) for p in sources},sort_keys=True).encode()).hexdigest(),
            'scheduler_code_sha256':sha(Path(__file__).parents[1]/'scheduling_v2/motion.py'),
            'sampling':'all business events; 10s playback; configurable graph state commit interval',
            'not_joint_acceptance':True}
        self.tasks={}
        for row in input_tasks:
            t=dict(row)
            t.update(status='scheduled',robot_id=None,cargo_id=t['goods_id'],
                     slot_id=t['target_id'] if t['task_type']=='INBOUND' else t['source_id'])
            self.tasks[t['task_id']]=t
        self.release_order=sorted(self.tasks,key=lambda k:(self.tasks[k]['release_second'],k))
        self.cursor=0
        self.pending=[]
        self.slots=deepcopy(self.slot_spec)
        self.goods={}
        self.inside=0
        self.ports=Counter()
        self.jobs={}
        self.running=True
        rng=random.Random(config.seed)
        self.fleet_type_by_id = fleet_assignment(config.fleet_mode, config.fleet_counts, config.robot_count)
        self._fleet_is_mixed = config.fleet_mode == 'mixed'
        self.capability_requirements = {key: slot_requirement(slot, self.scene) for key, slot in self.slot_spec.items()}
        if self._fleet_is_mixed:
            self.run_metadata['fleet_capabilities'] = {'version': CAPABILITY_VERSION, 'mode': config.fleet_mode,
                'counts': dict(Counter(self.fleet_type_by_id.values())),
                'profiles': {key: profile(key) for key in sorted(set(self.fleet_type_by_id.values()))},
                'extra_extension_contract': 'explicit slot/rack required_extension_m; two-sided approach requires no undeclared extra reach',
                'spare_cost_seconds': config.capability_spare_cost_seconds}
        self.robots={i+1:{'robot_id':i+1,'robot_type_key':self.fleet_type_by_id[i+1],'x':h['point']['x'],'y':h['point']['y'],
            'home_id':h['id'],'location_id':h['id'],'battery':40+rng.random()*40,
            'stage':'idle','carrying':False,'cargo_id':None,'task_id':None,'dock_id':None,
            'missions':0,'completed':0,'distance':0.,'active_seconds':0,'charge_seconds':0,
            'queue_seconds':0,'last_completed':0} for i,h in enumerate(self.scene['homes'])}
        self.docks={d['id']:{**d,'available_at':0,'charge_seconds':0,'sessions':0}
                    for d in self.scene['docks'][:config.chargers]}
        self.metrics={'received':0,'shipped':0,'completed':0,'late':0,'max_inside':0,'min_inside':len(self.initial),
            'min_soc':100.,'max_charging':0,'max_moving':0,'max_working':0,'max_queued':0,'schedule_retries':0,
            'charge_sessions':0,'charge_wait_seconds':[],'departure_wait_seconds':[],
            'charged_but_held_seconds':0,'reserved_motion_segments':0,'motion_audit_pairs':0,
            'min_separation_m':1000.,'unsafe_events':0,
            'planning_calls':0,'planning_failures':0,'planning_seconds':0.,'assignment_batches':0,
            'task_chains':0,'escape_fallbacks':0,'traffic_wait_seconds':0,'productive_robot_seconds':0,
            'empty_distance_m':0.,'loaded_distance_m':0.,'task_flow_seconds':[],'dispatch_wait_seconds':[]}
        for row in self.initial:
            g,s=row['goods_id'],row['location_id']
            self.slots[s].update(occupied=True,cargo_id=g)
            self.goods[g]=s
            self.inside+=1
            self.event('initial_stock',goods_id=g,location_id=s)
        self.metrics['max_inside']=self.inside

    def task_capability_requirements(self, task, *, remaining=False):
        keys = [task['target_id']] if remaining else [task['source_id'], task['target_id']]
        return [self.capability_requirements[key] for key in keys if key in self.capability_requirements]

    def task_capability_reason(self, robot, task, *, remaining=False):
        return access_reason(robot, self.task_capability_requirements(task, remaining=remaining))

    def capability_spare_cost(self, robot, task):
        if not self._fleet_is_mixed:
            return 0.
        return spare_cost(robot, self.task_capability_requirements(task), self.config.capability_spare_cost_seconds)

    def _task_parts(self, robot, task, start=None):
        # Recovery/explicit fault clearing can bypass schedule_task. A collected
        # parcel only needs the remaining target, while an unpicked one still
        # requires its source shelf capability.
        reason = self.task_capability_reason(robot, task, remaining=robot['carrying'])
        if reason:
            self._last_block = {'reason': reason, 'owners': [], 'at': self.elapsed}
            return None
        return super()._task_parts(robot, task, start)

    def schedule_task(self, robot, task, extra=None):
        # Last admission guard also protects callers outside the batch matcher.
        # Both legs matter: outbound pickup may require a higher shelf even
        # though its final warehouse boundary is on the ground.
        for candidate in [task] + ([extra] if extra is not None else []):
            reason = self.task_capability_reason(robot, candidate)
            if reason:
                self._last_block = {'reason': reason, 'owners': [], 'at': self.elapsed}
                return False
        return super().schedule_task(robot, task, extra)

    def _coordinated_replan(self, required, preferred=()):
        # Multi-owner recovery commits supplied plans directly, so enforce the
        # same capability gate before considering any reservation replacement.
        for owner, (_, job) in required.items():
            task = self.tasks.get(job.get('task_id'))
            if task is None or task['status'] == 'completed':
                continue
            robot = self.robots[owner]
            remaining = robot['carrying'] or 'assist_donor' in job
            reason = self.task_capability_reason(robot, task, remaining=remaining)
            if reason:
                self._last_block = {'reason': reason, 'owners': [], 'at': self.elapsed}
                return False
        return super()._coordinated_replan(required, preferred)

    def _assistance_helpers(self, donor, task):
        candidates = super()._assistance_helpers(donor, task)
        # Pickup already happened: the receiver needs the remaining target
        # capability, not the donor's completed high-shelf pickup capability.
        candidates = [r for r in candidates if self.task_capability_reason(r, task, remaining=True) is None]
        return candidates

    def marker(self, robot, job, marker, task_id=None):
        if marker in {'picked', 'delivered', 'handoff_received'}:
            task = self.tasks[task_id or job['task_id']]
            keys = [task['source_id']] if marker == 'picked' else [task['target_id']]
            requirements = [self.capability_requirements[key] for key in keys if key in self.capability_requirements]
            reason = access_reason(robot, requirements)
            if reason:
                raise AssertionError(f'robot capability rejected before {marker}: {reason}')
        return super().marker(robot, job, marker, task_id)

    def _reservation_parts(self, owner, parts):
        return terminal_tail(parts)

    def _route(self, source, target, clock, owner, phase, dwell=0, action=None, marker=None, **kw):
        if phase=='returning' and target==self.robots[owner]['home_id']:
            # Unlike a permanently-private home, an empty wide bay may have an
            # already-booked crossing. Arrive after its last conflicting span
            # before claiming indefinite parked-body occupancy.
            point=self.nav.locations[target]
            earliest=kw.get('earliest_goal',clock)
            for other,part in self.reservations.nearby(point,point,owner,clock,10**12):
                if not continuous_clear(point,point,clock,10**12,part):
                    earliest=max(earliest,part['end']+1)
            kw['earliest_goal']=earliest
        return super()._route(source,target,clock,owner,phase,dwell,action,marker,**kw)

    def _commit(self, robot, parts, job):
        owner=robot['robot_id']
        reserved=self._reservation_parts(owner,parts)
        if not parts or parts[0]['start']!=self.elapsed or math.dist(
            (parts[0]['a']['x'],parts[0]['a']['y']),(robot['x'],robot['y']))>1e-6:
            raise AssertionError('wide candidate does not start at actual body')
        for previous,current in zip(reserved,reserved[1:]):
            if previous['end']!=current['start'] or math.dist(
                (previous['b']['x'],previous['b']['y']),(current['a']['x'],current['a']['y']))>1e-6:
                raise AssertionError('wide candidate is not continuous')
        # Same serialized single-owner commit contract as v2. All checks finish
        # before reserve erases anything; multi-owner changes still use the
        # all-or-nothing transaction. Do not clone a full fleet for each parcel.
        for p in reserved:
            if not self.reservations.clear_motion(p['a'],p['b'],p['start'],p['end'],owner):
                raise AssertionError('wide candidate or terminal occupancy collision')
        self.reservations.reserve(reserved,owner=owner)
        self.jobs[owner]={**job,'parts':[{**p,'start':p['start']-self.elapsed,'end':p['end']-self.elapsed}
                                      for p in parts],'start':self.elapsed,'phase_index':0}
        self.metrics['reserved_motion_segments']+=sum(p['end']-p['start'] for p in parts if p['phase']!='charging')

    @contextmanager
    def _charger_view(self, robot):
        """Use one physical resource; ignore own body, not other parked bodies.

        Future parked bodies conservatively reserve their assigned terminal.
        This baseline does not optimise borrowing another robot's home bay.
        Other active trajectories/charging holds remain exact table constraints.
        """
        original=self.docks
        choices={}
        owner=robot['robot_id']
        own=self.nav.canonical_location(robot['home_id'])
        for key,dock in original.items():
            # The simple knowledge baseline assigns each robot its colocated
            # charging/standby bay. Empty bays remain traversable; temporary
            # borrowing/parking swaps are intentionally not another algorithm.
            if key!=own:
                continue
            point=dock['point']
            blocked=any(i!=owner and math.hypot(p['a']['x']-point['x'],p['a']['y']-point['y']) < 1.1-1e-7
                        for i,spans in self.reservations.fixed.items() for p in spans)
            if not blocked:
                choices[key]={**dock,'available_at':self.elapsed if key==own else dock['available_at']}
        self.docks=choices
        try:
            yield
        finally:
            self.docks=original

    def _charge_tail(self, robot, clock, source=None, battery=None):
        with self._charger_view(robot):
            return super()._charge_tail(robot,clock,source,battery)

    def _energy_admission(self, robot, parts):
        with self._charger_view(robot):
            return super()._energy_admission(robot,parts)

    def step(self):
        super().step()
        # The already-validated terminal guard remains when the execution job
        # ends. This check never invents a late guard after a crossing was booked.
        for owner,robot in self.robots.items():
            if owner not in self.jobs and owner not in self.reservations.fixed:
                raise AssertionError('parked robot lost persistent occupancy')

    def restore(self, state):
        # V3 restores actual jobs first. Reconstruct the same terminal tails and
        # idle bodies, never silently restore an empty parking aisle.
        super().restore(state)
        if any(r.get('robot_type_key') != self.fleet_type_by_id[i] for i, r in self.robots.items()):
            raise ValueError('checkpoint robot type differs from configured fleet identity')
        if self._fleet_is_mixed:
            for owner, job in self.jobs.items():
                for task_id in job.get('task_ids', []):
                    task = self.tasks[task_id]
                    remaining = task['status'] == 'carrying' or 'assist_donor' in job
                    if task['status'] != 'completed' and self.task_capability_reason(self.robots[owner], task, remaining=remaining):
                        raise ValueError('checkpoint has a capability-infeasible active job')
        table=self.reservation_type()
        for owner,robot in self.robots.items():
            if owner in self.jobs:
                job=self.jobs[owner]
                parts=[{**p,'start':p['start']+job['start'],'end':p['end']+job['start']} for p in job['parts']]
                table.reserve(self._reservation_parts(owner,parts),owner=owner)
            else:
                guards=state.get('wide_occupancy',{}).get(str(owner))
                table.reserve(guards or [idle_guard(self._point(robot),self.elapsed)],owner=owner)
        table.prune(self.elapsed)
        self.reservations=table
        self.planner.table=table

    def checkpoint(self):
        state=super().checkpoint()
        state['wide_occupancy']={str(owner):deepcopy(parts) for owner,parts in self.reservations.fixed.items()}
        return state

    def frame(self):
        value=super().frame()
        value['backend']='wide-scene-v3-policy-knowledge-baseline'
        value['physicalBayCount']=len(self.scene['docks'])
        return value
