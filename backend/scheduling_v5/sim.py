"""Explicit bounded dispatch candidates over the frozen v4 motion engine.

The dispatch body is a static copy of v3's coordinator. The only policy seam
is _select_assignment; charging, failed-pair backoff, dynamic safe planning,
energy admission, reservation transactions and task completion stay inherited.
"""
from collections import Counter
from copy import deepcopy
import hashlib
import json
import math
import time

from ..scheduling_v2.matching import minimum_assignment
from ..scheduling_v2.motion import SafeIntervalPlanner
from ..scheduling_v4.sim import Simulation as EngineSimulation, Config
from ..scheduling_v4.sim import execution_identity as engine_identity
from . import RELEASE
from .invariants import assert_warehouse_inventory
from .matching import guarded_assignment, FORBIDDEN


DEFAULTS = {
    'cost': {},
    'guard': {'wait_limit': 300, 'deadline_guard': 120.},
    'port-guard': {'wait_limit': 300, 'deadline_guard': 120.,
                   'port_pressure_weight': 1., 'port_queue_cap': 6},
    'pressure': {'wait_limit': 300, 'deadline_guard': 0., 'age_weight': .5,
                 'deadline_window': 120., 'risk_weight': 4.},
}
PLANNERS = ('goal-window', 'original-sipp')


def resolve_parameters(policy, parameters=None):
    if policy not in DEFAULTS:
        raise ValueError('unknown dispatch policy')
    if parameters is None:
        parameters = {}
    if not isinstance(parameters, dict) or set(parameters) - set(DEFAULTS[policy]):
        raise ValueError('unknown policy parameters')
    value = {**DEFAULTS[policy], **parameters}
    for name, number in value.items():
        if isinstance(number, bool) or not isinstance(number, (int, float)) or abs(number) > 10000 or not math.isfinite(number):
            raise ValueError('finite policy parameters required')
        if name == 'wait_limit':
            if not isinstance(number, int) or not 1 <= number <= 3600:
                raise ValueError('wait_limit must be an integer in [1, 3600]')
        elif name == 'port_queue_cap':
            if not isinstance(number, int) or not 1 <= number <= 6:
                raise ValueError('port_queue_cap must be an integer in [1, 6]')
        elif not 0 <= number <= 10000:
            raise ValueError('bounded nonnegative policy parameters required')
    return value


def execution_identity(policy, parameters=None, planner='goal-window'):
    if planner not in PLANNERS:
        raise ValueError('unknown motion planner')
    value = resolve_parameters(policy, parameters)
    engine = engine_identity('optimized')
    identity = {'release': RELEASE, 'policy': policy, 'parameters': value, 'planner': planner,
                'source_sha256': engine['source_sha256'],
                'engine_release': engine['release'],
                'features': ['v4-motion-engine', 'strong-inventory-invariant',
                             'max-cardinality-guarded-cost' if policy != 'cost' else 'original-cost-control']}
    identity['execution_sha256'] = hashlib.sha256(json.dumps(identity, sort_keys=True,
        separators=(',', ':'), allow_nan=False).encode()).hexdigest()
    return identity


class CandidateSimulation(EngineSimulation):
    def __init__(self, scene_path, dataset, run_id, config=None, checkpoint=None,
                 *, policy='guard', policy_parameters=None, planner='goal-window'):
        self.policy = policy
        self.planner_name = planner
        self.policy_parameters = resolve_parameters(policy, policy_parameters)
        self.v5_identity = execution_identity(policy, self.policy_parameters, planner)
        # Let the base finish building its v4 identity/engine before any restore.
        super().__init__(scene_path, dataset, run_id, config or Config(), checkpoint=None,
                         scheduler='optimized')
        if planner == 'original-sipp':
            # The v4 checkpoint header still identifies the reused engine, but
            # must not advertise goal-window search when this explicit override
            # is active. Composite v5 identity independently locks the planner.
            self.execution_identity = deepcopy(self.execution_identity)
            self.execution_identity['features'].remove('goal-window-sipp')
            self.execution_identity['planner_override'] = planner
            self.execution_identity.pop('execution_sha256')
            self.execution_identity['execution_sha256'] = hashlib.sha256(json.dumps(
                self.execution_identity, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
            self.run_metadata['scheduler_identity'] = deepcopy(self.execution_identity)
        self.run_metadata.update(backend=RELEASE + '-' + policy,
            scheduler_v5_identity=deepcopy(self.v5_identity),
            execution_code_sha256=self.v5_identity['execution_sha256'])
        self.metrics.update(v5_matching_calls=0, v5_guard_rows=0,
                            v5_guard_rows_selected=0, v5_rewarded_rows=0,
                            v5_port_rewarded_rows=0)
        if checkpoint is not None:
            self.restore(checkpoint)

    def assert_inventory(self):
        super().assert_inventory()
        return assert_warehouse_inventory(self)

    def _initialize_base(self, scene_path, dataset, run_id, config):
        # V4 has already selected its exact cache-backed table/roadmap types.
        # Only the documented planner class is changed before Wide constructs it.
        if self.planner_name == 'original-sipp':
            self.planner_type = SafeIntervalPlanner
        return super()._initialize_base(scene_path, dataset, run_id, config)

    def checkpoint(self):
        state = super().checkpoint()
        state['scheduler_v5'] = {'identity': deepcopy(self.v5_identity)}
        return state

    def restore(self, state):
        header = state.get('scheduler_v5')
        if not isinstance(header, dict) or header.get('identity') != self.v5_identity:
            raise ValueError('v5 policy/parameters/release/source identity mismatch; no implicit migration')
        super().restore(state)

    def execution_status(self):
        result = super().execution_status()
        result['engine_identity'] = result['identity']
        result['identity'] = deepcopy(self.v5_identity)
        return result

    def frame(self):
        value = super().frame()
        value['scheduler'] = {'release': RELEASE, 'policy': self.policy,
            'parameters': dict(self.policy_parameters),
            'planner': self.planner_name,
            'execution_sha256': self.v5_identity['execution_sha256']}
        return value

    def _select_assignment(self, costs, pool, pair_times, candidates=None):
        if getattr(self, '_fleet_is_mixed', False):
            if candidates is None:
                raise ValueError('mixed fleet matcher requires actual dispatch column identities')
            # Filter before cardinality/priority matching, using the actual
            # candidate columns retained after every dynamic retry round.
            costs = [list(row) for row in costs]
            for row, task in enumerate(pool):
                for col, robot in enumerate(candidates):
                    if self.task_capability_reason(robot, task):
                        costs[row][col] = 1e12
                    elif costs[row][col] < FORBIDDEN:
                        costs[row][col] += self.capability_spare_cost(robot, task)
        self.metrics['v5_matching_calls'] += 1
        if self.policy == 'cost':
            return minimum_assignment(costs)
        p = self.policy_parameters
        port_waiting = Counter()
        if self.policy == 'port-guard' and self.inside < len(self.slots):
            # Count released IN demand which the full source buffer alone blocks.
            # A box waiting for an occupied/reserved target slot must not create
            # a false incentive to prioritize IN instead of the OUT freeing it.
            for task_id in self.pending:
                task = self.tasks[task_id]
                if task['task_type'] != 'INBOUND' or task['status'] != 'waiting':
                    continue
                slot = self.slots[task['target_id']]
                if not slot['occupied'] and not slot['reservedBy']:
                    port_waiting[task['source_id']] += 1
        protected, rewards = [], []
        for row, task in enumerate(pool):
            age = self.elapsed - task['ready_at']
            feasible = [seconds for col, seconds in enumerate(pair_times[row])
                        if costs[row][col] < FORBIDDEN]
            slack = task['due_second'] - self.elapsed - min(feasible) if feasible else None
            if feasible and (age >= p['wait_limit'] or slack <= p['deadline_guard']):
                protected.append(row)
            reward = 0.
            if self.policy == 'pressure':
                reward = p['age_weight'] * age + (p['risk_weight'] * max(0., p['deadline_window'] - slack)
                                                  if slack is not None else 0.)
            elif self.policy == 'port-guard' and task['task_type'] == 'INBOUND' and task['status'] == 'accepted' and self.ports.get(task['source_id'], 0) == 6:
                reward = self.config.dispatch_seconds * min(port_waiting[task['source_id']], p['port_queue_cap']) * p['port_pressure_weight']
                self.metrics['v5_port_rewarded_rows'] += int(reward > 0)
            rewards.append(reward)
        result = guarded_assignment(costs, protected, rewards)
        self.metrics['v5_guard_rows'] += len(protected)
        self.metrics['v5_guard_rows_selected'] += sum(row in protected and col < len(pair_times[row])
                                                    for row, col in result)
        self.metrics['v5_rewarded_rows'] += sum(reward > 0 for reward in rewards)
        return result

    def release_and_dispatch(self):
        self._release()
        if self.elapsed < self.next_dispatch and not self.handoff_ready:
            return
        started = time.perf_counter()
        self._update_docks()
        self.next_dispatch = self.elapsed + self.config.dispatch_seconds
        self.failed_pairs = {key: expiry for key, expiry in self.failed_pairs.items() if expiry > self.elapsed}
        ready = self._ready()
        eligible = [r for i, r in sorted(self.robots.items()) if i not in self.faults and
                    (i not in self.jobs or self.config.task_chaining and i in self.handoff_ready)]
        candidates = []
        # Emergency margins outrank arbitrary robot IDs (unchanged v3 path).
        for r in sorted(eligible, key=lambda r: (r['battery'], r['robot_id'])):
            if r['battery'] <= self.config.charge_request_soc:
                self.schedule_charge(r)
            else:
                candidates.append(r)
        pool = sorted(ready, key=lambda t: (self.blocked.get(t['task_id'], {}).get('retry_after', 0) > self.elapsed,
            t['due_second'] - .5 * (self.elapsed - t['ready_at']), t['task_id']))[:self.config.batch_tasks]
        for attempt in range(self.config.matching_rounds):
            if not pool or not candidates:
                break
            costs, pair_times = [], []
            for task in pool:
                delivery = self.roadmap.travel_time(task['source_id'], task['target_id'], self.config.speed) + self.config.pick_seconds + self.config.place_seconds
                row, travel_row = [], []
                for r in candidates:
                    eta = self.roadmap.travel_time(r['location_id'], task['source_id'], self.config.speed)
                    late = max(0, self.elapsed + eta + delivery - task['due_second'])
                    row.append(1e12 if (r['robot_id'], task['task_id']) in self.failed_pairs else eta + delivery + late * 4 + r['missions'] * .05)
                    travel_row.append(eta + delivery)
                row.extend([1e8] * len(pool))
                costs.append(row)
                pair_times.append(travel_row)
            self.metrics['assignment_batches'] += 1
            successes = []
            failures = 0
            for row, col in self._select_assignment(costs, pool, pair_times, candidates):
                if col >= len(candidates) or costs[row][col] >= 1e8:
                    continue
                r, task = candidates[col], pool[row]
                if self.schedule_task(r, task):
                    successes.append((r, task))
                    self.blocked.pop(task['task_id'], None)
                else:
                    failures += 1
                    key = (r['robot_id'], task['task_id'])
                    self.failed_pairs[key] = self.elapsed + self.config.pair_retry_seconds
                    self.blocked[task['task_id']] = dict(self._last_block, robot_id=r['robot_id'],
                                                        retry_after=self.elapsed + self.config.pair_retry_seconds)
                    self.progress.record_block(r['robot_id'], self._last_block['owners'], self.elapsed,
                        reason=self._last_block['reason'], task_id=task['task_id'])
            for r, task in successes:
                candidates.remove(r)
                pool.remove(task)
            if not failures:
                break
            self.metrics['matching_retries'] += failures
        self.metrics['escape_fallbacks'] += sum(i in self.jobs and bool(self.jobs[i].get('task_id')) and
            self.tasks[self.jobs[i]['task_id']]['status'] == 'completed' for i in self.handoff_ready)
        self.handoff_ready.clear()
        charging = sum(job.get('dock_id') is not None for job in self.jobs.values())
        cap = max(0, self.config.robot_count - self.config.desired_working - self.config.standby_headroom)
        for r in eligible:
            if charging >= min(cap, len(self.docks)):
                break
            if r['robot_id'] not in self.jobs and r['battery'] < self.config.opportunity_charge_soc:
                if self.config.charging_policy == 'staggered' and self.schedule_charge(r):
                    charging += 1
        self.pending = [k for k in self.pending if self.tasks[k]['status'] in {'waiting', 'accepted'}]
        self.metrics['dispatch_wall_seconds'].append(time.perf_counter() - started)


Simulation = CandidateSimulation
