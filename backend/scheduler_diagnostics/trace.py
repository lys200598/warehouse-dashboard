"""Trace unchanged wide/v3 policy, including task gates and actual matching."""
from collections import Counter, defaultdict
from copy import deepcopy
import cProfile
import io
import json
import math
from pathlib import Path
import pstats
import time

from ..checkpoint import write_checkpoint
from ..wide.sim import Simulation as WideSimulation
from ..scheduling_v3 import sim as v3_module


def pool_order(sim, ready):
    return sorted(ready, key=lambda task: (
        sim.blocked.get(task['task_id'], {}).get('retry_after', 0) > sim.elapsed,
        task['due_second'] - .5 * (sim.elapsed - task['ready_at']), task['task_id']))


def target_gates(sim, task):
    slot = sim.slots[task['target_id']]
    return {'capacity_full': sim.inside >= 1728, 'target_occupied': bool(slot['occupied']),
            'target_reserved': bool(slot['reservedBy']), 'port_full': sim.ports[task['source_id']] >= 6,
            'port_count': sim.ports[task['source_id']], 'inside': sim.inside,
            'status': task['status']}


class TraceSimulation(WideSimulation):
    options = None
    instance = None

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        type(self).instance = self
        self.options = dict(type(self).options)
        self.directory = Path(self.options['directory'])
        self.directory.mkdir(parents=True, exist_ok=False)
        self.log = (self.directory/'dispatch.jsonl').open('x', encoding='utf-8')
        self.routes = (self.directory/'routes.jsonl').open('x', encoding='utf-8')
        self.gates = []
        self.dispatch_record = None
        self.match_pool = None
        self.route_context = None
        self.route_stats = defaultdict(lambda: Counter(calls=0, failures=0, expansions=0, seconds=0.))
        self.distance_stats = defaultdict(lambda: Counter(calls=0, misses=0, seconds=0.))
        self.distance_targets = defaultdict(lambda: Counter(calls=0, misses=0, seconds=0.))
        self.profiler = cProfile.Profile()
        self.profiling = False
        self.profile_finished = False
        self.snapshots = set(self.options.get('snapshots', []))
        original_distances = self.roadmap.distances
        self.original_distances = original_distances
        def distances(target, speed):
            before = original_distances.cache_info()
            start = time.perf_counter()
            result = original_distances(target, speed)
            duration = time.perf_counter()-start
            miss = original_distances.cache_info().misses-before.misses
            kind = self.route_context[2] if self.route_context else 'assignment_estimate'
            for bucket in (self.distance_stats[kind], self.distance_targets[target]):
                bucket.update(calls=1, misses=miss, seconds=duration)
            return result
        self.roadmap.distances = distances
        original_plan = self.planner.plan
        def plan(source, target, now, owner, phase, *more, **kw):
            previous = self.route_context
            self.route_context = (source, target, phase)
            expansions = self.planner.expansions
            start = time.perf_counter()
            try:
                value = original_plan(source, target, now, owner, phase, *more, **kw)
            finally:
                duration = time.perf_counter()-start
                self.route_context = previous
            expanded = self.planner.expansions-expansions
            self.route_stats[phase].update(calls=1, failures=int(value is None),
                                           expansions=expanded, seconds=duration)
            self.routes.write(json.dumps({'sim_second':self.elapsed,'plan_clock':now,'owner':owner,
                'source':source,'target':target,'phase':phase,'ms':duration*1000,
                'expansions':expanded,'failure':self.planner.last_failure if value is None else None})+'\n')
            return value
        self.planner.plan = plan
        original_matching = v3_module.minimum_assignment
        self.original_matching = original_matching
        def matching(costs):
            value = original_matching(costs)
            if self.match_pool is not None:
                if len(costs) != len(self.match_pool):
                    raise AssertionError('instrumentation lost actual pool order')
                n_candidates = len(costs[0])-len(costs)
                target = self.options['task']
                row = next((i for i,t in enumerate(self.match_pool) if t['task_id']==target),None)
                chosen = dict(value)
                observation = {'pool':len(costs),'candidates':n_candidates,
                    'target_row':row,'selected_real_rows':[i for i,j in value if j<n_candidates and costs[i][j]<1e8]}
                if row is not None:
                    col = chosen[row]
                    valid = [cost for cost in costs[row][:n_candidates] if cost<1e8]
                    observation.update(target_selected=col<n_candidates and costs[row][col]<1e8,
                        target_min_real_cost=min(valid,default=None),target_matched_cost=costs[row][col],
                        target_forbidden_pairs=sum(cost>=1e8 for cost in costs[row][:n_candidates]))
                self.dispatch_record['matching'].append(observation)
            return value
        self.matching_wrapper = matching
        v3_module.minimum_assignment = matching

    def _release(self):
        task = self.tasks[self.options['task']]
        inspect = task['release_second'] <= self.elapsed and task['status'] in {'scheduled','waiting'}
        before = target_gates(self,task) if inspect else None
        super()._release()
        if inspect:
            self.gates.append({'second':self.elapsed,'before':before,'after':target_gates(self,task)})

    def _ready(self):
        ready = super()._ready()
        task = self.tasks[self.options['task']]
        ordered = pool_order(self,ready)
        self.match_pool = ordered[:self.config.batch_tasks]
        target = self.options['task']
        eligible = [r for owner,r in self.robots.items() if owner not in self.faults and
            (owner not in self.jobs or self.config.task_chaining and owner in self.handoff_ready)]
        rank = next((i for i,t in enumerate(ordered) if t['task_id']==target),None)
        self.dispatch_record = {'second':self.elapsed,'target_status':task['status'],
            'target_ready_at':task.get('ready_at'),'ready':len(ready),'target_rank':rank,
            'target_in_pool':rank is not None and rank<self.config.batch_tasks,
            'eligible_before_charge':len(eligible),
            'eligible_above_charge_threshold':sum(r['battery']>self.config.charge_request_soc for r in eligible),
            'robot_stages':dict(Counter(r['stage'] for r in self.robots.values())),
            'target_block':deepcopy(self.blocked.get(target)),
            'pool_ids':[t['task_id'] for t in self.match_pool],
            'matching':[],'task_attempts':[]}
        return ready

    def release_and_dispatch(self):
        self.dispatch_record = None
        self.match_pool = None
        try:
            return super().release_and_dispatch()
        finally:
            if self.dispatch_record is not None:
                task = self.tasks[self.options['task']]
                self.dispatch_record['target_after']=task['status']
                self.dispatch_record['blocked_after']=deepcopy(self.blocked.get(task['task_id']))
                self.log.write(json.dumps(self.dispatch_record)+'\n')

    def schedule_task(self, robot, task, extra=None):
        result = super().schedule_task(robot,task,extra)
        if self.dispatch_record is not None:
            self.dispatch_record['task_attempts'].append({'task':task['task_id'],'owner':robot['robot_id'],
                'success':bool(result),'failure':None if result else deepcopy(self._last_block)})
        if result and self.match_pool is not None and task in self.match_pool:
            self.match_pool.remove(task)
        return result

    def step(self):
        if (not self.profiling and not self.profile_finished and
                self.options['profile_from'] <= self.elapsed < self.options['profile_to']):
            self.profiler.enable()
            self.profiling = True
        value = super().step()
        if self.profiling and self.elapsed >= self.options['profile_to']:
            self.profiler.disable()
            self.profiling = False
            self.profile_finished = True
        if self.elapsed in self.snapshots:
            state = self.checkpoint()
            state['diagnostic_snapshot'] = 'read-only physical-state replay; NOT a durable journal commit'
            write_checkpoint(self.directory/f'state-{self.elapsed}.json',state)
        return value

    def close_diagnostics(self):
        if self.profiling:
            self.profiler.disable()
        self.profiler.dump_stats(str(self.directory/'planner.pstats'))
        text = io.StringIO()
        stats = pstats.Stats(self.profiler,stream=text)
        stats.sort_stats('cumulative').print_stats(55)
        stats.sort_stats('tottime').print_stats(40)
        (self.directory/'planner-top.txt').write_text(text.getvalue(),encoding='utf-8')
        self.log.close();self.routes.close()
        if v3_module.minimum_assignment is self.matching_wrapper:
            v3_module.minimum_assignment = self.original_matching
        report={'target':self.tasks[self.options['task']],
            'admission_gates':self.gates,'route_phases':dict(self.route_stats),
            'static_distance_phases':dict(self.distance_stats),
            'static_distance_targets':dict(self.distance_targets),
            'distance_cache_info':self.original_distances.cache_info()._asdict(),
            'travel_time_cache_info':self.roadmap.travel_time.cache_info()._asdict(),
            'roadmap_nodes':len(self.roadmap.nodes),'roadmap_edges':sum(map(len,self.roadmap.edges.values())),
            'profile_window':[self.options['profile_from'],self.options['profile_to']],
            'profile_completed':self.profile_finished,
            'policy_changed':False,'snapshot_scope':'read-only clones, not production resume'}
        (self.directory/'summary.json').write_text(json.dumps(report,indent=2),encoding='utf-8')
