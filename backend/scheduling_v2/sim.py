"""Fleet scheduler v2: batch assignment, SIPP routing, safe task continuation.

All new solver code is independently written. No proprietary patent code or
USC RHCR solver is incorporated. Existing day1 domain/knowledge contracts are
reused; the original simulator and its frozen server runs are not modified.
"""
from collections import Counter
from copy import deepcopy
from dataclasses import asdict, dataclass
import hashlib
import json
import math
from pathlib import Path
import time

from ..day1.sim import Simulation as BaselineSimulation, Config as BaselineConfig
from ..day1.data import sha
from .matching import minimum_assignment
from .motion import MotionTable, Roadmap, SafeIntervalPlanner


@dataclass
class Config(BaselineConfig):
    max_batch: int = 1
    dispatch_seconds: int = 10
    batch_tasks: int = 32
    task_chaining: bool = True
    route_horizon: int = 900
    route_expansions: int = 12000
    assignment_cost: str = 'roadmap'


class Simulation(BaselineSimulation):
    def __init__(self, scene_path, dataset, run_id, config=None, checkpoint=None):
        super().__init__(scene_path, dataset, run_id, config or Config(), checkpoint=None)
        if self.config.dispatch_seconds < 1 or not 1 <= self.config.batch_tasks <= 100:
            raise ValueError('invalid bounded dispatch configuration')
        if self.config.assignment_cost not in {'roadmap', 'euclidean'}:
            raise ValueError('unknown assignment cost model')
        self.roadmap = Roadmap(self.nav)
        self.reservations = MotionTable()
        self.planner = SafeIntervalPlanner(self.roadmap, self.reservations, self.config.speed,
                                          self.config.route_horizon, self.config.route_expansions)
        self.next_dispatch = 0
        self.handoff_ready = set()
        self.run_metadata.update(backend='local-central-batch-sipp-continuation-v2',
                                 scheduling_sources='docs/scheduler_v2_sources.md',
                                 not_rhcr_implementation=True,
                                 roadmap_step_m=.8)
        sources = [Path(__file__).parent / n for n in ['sim.py', 'motion.py', 'matching.py', 'runner.py', 'audit.py']]
        self.run_metadata['scheduler_code_sha256'] = hashlib.sha256(
            json.dumps({p.name: sha(p) for p in sources}, sort_keys=True).encode()).hexdigest()
        self.metrics.update(planning_calls=0, planning_failures=0, planning_seconds=0.,
                            assignment_batches=0, task_chains=0, escape_fallbacks=0,
                            traffic_wait_seconds=0, productive_robot_seconds=0,
                            empty_distance_m=0., loaded_distance_m=0.,
                            task_flow_seconds=[], dispatch_wait_seconds=[])
        for r in self.robots.values():
            r['location_id'] = r['home_id']
        if checkpoint:
            self.restore(checkpoint)

    def _plan(self, *args, **kwargs):
        started = time.perf_counter()
        result = self.planner.plan(*args, **kwargs)
        self.metrics['planning_calls'] += 1
        self.metrics['planning_seconds'] += time.perf_counter() - started
        if result is None:
            self.metrics['planning_failures'] += 1
            key = 'planning_fail_' + (self.planner.last_failure or 'unknown')
            self.metrics[key] = self.metrics.get(key, 0) + 1
        return result

    def _drain(self, parts):
        return sum((p['end'] - p['start']) *
                   (self.config.idle_soc_per_second if p['phase'] == 'traffic_wait' else self.config.active_soc_per_second)
                   for p in parts if p['phase'] != 'charging')

    def _commit(self, robot, parts, job):
        owner = robot['robot_id']
        # Check all movement and holds again before replacing an old safe escape.
        for p in parts:
            if not self.reservations.clear_motion(p['a'], p['b'], p['start'], p['end'], owner):
                raise AssertionError('candidate failed final continuous reservation check')
        relative = [{**p, 'start': p['start'] - self.elapsed, 'end': p['end'] - self.elapsed} for p in parts]
        if not relative or relative[0]['start'] != 0:
            raise AssertionError('new plan must start at current committed position/time')
        if math.hypot(parts[0]['a']['x'] - robot['x'], parts[0]['a']['y'] - robot['y']) > 1e-6:
            raise AssertionError('continuation would teleport')
        for previous, current in zip(parts, parts[1:]):
            if previous['end'] != current['start'] or math.hypot(previous['b']['x'] - current['a']['x'],
                                                                  previous['b']['y'] - current['a']['y']) > 1e-6:
                raise AssertionError('discontinuous candidate trajectory')
        self.reservations.reserve(parts, owner=owner)
        self.jobs[owner] = {**job, 'parts': relative, 'start': self.elapsed, 'phase_index': 0}
        self.metrics['reserved_motion_segments'] += sum(p['end'] - p['start'] for p in parts if p['phase'] != 'charging')

    def schedule_task(self, robot, task, extra=None):
        """One parcel at a time, with a committed escape to the real private home."""
        owner = robot['robot_id']
        parts, clock = [], self.elapsed
        current = robot['location_id']
        for target, phase, action, dwell, marker in [
            (task['source_id'], 'to_source', 'picking', self.config.pick_seconds, 'picked'),
            (task['target_id'], 'to_target', 'placing', self.config.place_seconds, 'delivered'),
            (robot['home_id'], 'returning', None, 0, None),
        ]:
            result = self._plan(current, target, clock, owner, phase, dwell, action, marker)
            if result is None:
                self.metrics['schedule_retries'] += 1
                return False
            added, clock = result
            parts.extend(added)
            current = target
        # Preserve the old task reserve and extra trip/wait allowances; no relaxed energy gate.
        needed = self._drain(parts) + 200 * self.config.active_soc_per_second + \
                 self.config.max_departure_wait * self.config.idle_soc_per_second + self.config.task_reserve_soc
        if robot['battery'] < needed:
            return False
        for p in parts:
            p['task_id'] = task['task_id']
        continued = owner in self.jobs
        self._commit(robot, parts, {'task_id': task['task_id'], 'task_ids': [task['task_id']], 'dock_id': None})
        departure = next(p['start'] for p in parts if p['phase'] != 'traffic_wait')
        delay = departure - self.elapsed
        if delay:
            self.reservations.delayed += 1
            self.reservations.max_delay = max(self.reservations.max_delay, delay)
        self.metrics['departure_wait_seconds'].append(delay)
        self.metrics['dispatch_wait_seconds'].append(self.elapsed - task.get('ready_at', task['release_second']))
        self.metrics['task_chains'] += int(continued)
        robot.update(task_id=task['task_id'], missions=robot['missions'] + 1, stage='mission_queue', dock_id=None)
        task.update(status='assigned', robot_id=owner, assigned_at=self.elapsed, departure_at=departure)
        if task['task_type'] == 'OUTBOUND':
            self.slots[task['source_id']]['reservedBy'] = task['task_id']
        self.event('task_assigned', owner, task['task_id'], departure_at=departure,
                   batch_size=1, continuation=continued, scheduler='batch-sipp-v2')
        return True

    def schedule_charge(self, robot):
        owner, source = robot['robot_id'], robot['location_id']
        candidates = sorted(self.docks.values(), key=lambda d: (
            max(self.elapsed, d['available_at']) +
            math.hypot(d['point']['x'] - robot['x'], d['point']['y'] - robot['y']) / self.config.speed,
            d['id']))
        for dock in candidates[:4]:
            # Conservative upper hold, then shorten using the actual planned arrival SOC.
            upper = math.ceil((self.config.charge_target_soc - robot['battery'] +
                               self.config.route_horizon * (self.config.active_soc_per_second + self.config.idle_soc_per_second)) /
                              self.config.charge_soc_per_second)
            upper = max(1, upper)
            out = self._plan(source, dock['id'], self.elapsed, owner, 'to_charge', upper, 'charging', 'charge_complete',
                             earliest_goal=max(self.elapsed, dock['available_at']))
            if out is None:
                continue
            parts, _ = out
            charge_hold = parts[-1]
            drain = self._drain(parts[:-1])
            if robot['battery'] < drain + self.config.emergency_reserve_soc:
                continue
            duration = max(1, math.ceil((self.config.charge_target_soc - robot['battery'] + drain) /
                                      self.config.charge_soc_per_second))
            if duration > upper:
                continue
            charge_hold['end'] = charge_hold['start'] + duration
            back = self._plan(dock['id'], robot['home_id'], charge_hold['end'], owner, 'returning')
            if back is None or self._drain(back[0]) + self.config.emergency_reserve_soc > self.config.charge_target_soc:
                continue
            parts.extend(back[0])
            arrival, charged_at = charge_hold['start'], charge_hold['end']
            leave = next((p['start'] for p in back[0] if p['phase'] != 'traffic_wait'), charged_at)
            self._commit(robot, parts, {'dock_id': dock['id'], 'task_id': None,
                                      'charge_started': False, 'request_time': self.elapsed,
                                      'arrival': arrival - self.elapsed})
            dock['available_at'] = leave + 3
            dock['sessions'] += 1
            robot.update(stage='charge_queue', dock_id=dock['id'], task_id=None)
            self.metrics['charge_sessions'] += 1
            self.metrics['charge_wait_seconds'].append(max(0, arrival - self.elapsed -
                sum(p['end'] - p['start'] for p in parts if p['phase'] == 'to_charge')))
            self.event('charge_reserved', owner, dock_id=dock['id'], departure_at=self.elapsed,
                       estimated_charge_at=arrival, scheduler='batch-sipp-v2')
            return True
        return False

    def _release(self):
        while self.cursor < len(self.release_order):
            key = self.release_order[self.cursor]
            task = self.tasks[key]
            if task['release_second'] > self.elapsed:
                break
            self.cursor += 1
            task['status'] = 'waiting'
            self.pending.append(key)
            self.event('task_released', task_id=key)
        for key in self.pending:
            task = self.tasks[key]
            if task['task_type'] != 'INBOUND' or task['status'] != 'waiting':
                continue
            slot = self.slots[task['target_id']]
            if self.inside >= 1728 or slot['occupied'] or slot['reservedBy'] or self.ports[task['source_id']] >= 6:
                continue
            task.update(status='accepted', received_at=self.elapsed)
            slot['reservedBy'] = key
            self.goods[task['goods_id']] = task['source_id']
            self.inside += 1
            self.ports[task['source_id']] += 1
            self.metrics['received'] += 1
            self.event('received', task_id=key, goods_id=task['goods_id'], location_id=task['source_id'])

    def _ready(self):
        ready = []
        for key in self.pending:
            task = self.tasks[key]
            if task['status'] == 'accepted' or (
                task['status'] == 'waiting' and task['task_type'] == 'OUTBOUND' and
                self.slots[task['source_id']]['cargo_id'] == task['goods_id'] and
                not self.slots[task['source_id']]['reservedBy']):
                task.setdefault('ready_at', self.elapsed)
                ready.append(task)
        return ready

    def release_and_dispatch(self):
        self._release()  # External release/admission times do not depend on the matching period.
        if self.elapsed < self.next_dispatch and not self.handoff_ready:
            return
        self.next_dispatch = self.elapsed + self.config.dispatch_seconds
        ready = self._ready()
        eligible = [r for i, r in sorted(self.robots.items())
                    if i not in self.jobs or (self.config.task_chaining and i in self.handoff_ready)]
        for robot in eligible:
            if robot['battery'] <= self.config.charge_request_soc:
                self.schedule_charge(robot)
        candidates = [r for r in eligible if r['battery'] > self.config.charge_request_soc]
        # Critical charging may have replaced a continuation. Never reassign those jobs.
        candidates = [r for r in candidates if not (r['robot_id'] in self.jobs and
                       self.jobs[r['robot_id']]['task_id'] is None and r['robot_id'] not in self.handoff_ready)]
        pool = sorted(ready, key=lambda t: (t['due_second'] - .2 * (self.elapsed - t['ready_at']), t['task_id']))[:self.config.batch_tasks]
        if pool and candidates:
            costs, impossible = [], 1e12
            for task in pool:
                p, q = self.nav.locations[task['source_id']], self.nav.locations[task['target_id']]
                travel = (self.roadmap.travel_time(task['source_id'], task['target_id'], self.config.speed)
                          if self.config.assignment_cost == 'roadmap' else
                          math.hypot(p['x'] - q['x'], p['y'] - q['y']) / self.config.speed)
                delivery = travel + self.config.pick_seconds + self.config.place_seconds
                row = []
                for robot in candidates:
                    pickup = (self.roadmap.travel_time(robot['location_id'], task['source_id'], self.config.speed)
                              if self.config.assignment_cost == 'roadmap' else
                              math.hypot(robot['x'] - p['x'], robot['y'] - p['y']) / self.config.speed)
                    lower_energy = (pickup + delivery) * self.config.active_soc_per_second + self.config.task_reserve_soc
                    lateness = max(0, self.elapsed + pickup + delivery - task['due_second'])
                    row.append(impossible if not math.isfinite(pickup + delivery) or lower_energy > robot['battery'] else
                               pickup + delivery + lateness * 4 + robot['missions'] * .05)
                # One dummy per task permits all tasks to remain unassigned safely.
                row.extend([1e8 + (self.elapsed - task['ready_at']) * 10] * len(pool))
                costs.append(row)
            self.metrics['assignment_batches'] += 1
            for row, column in minimum_assignment(costs):
                if column < len(candidates) and costs[row][column] < 1e8:
                    self.schedule_task(candidates[column], pool[row])
        # An unmatched continuation executes its already-reserved escape, never idles on-road.
        self.metrics['escape_fallbacks'] += sum(i in self.jobs and self.jobs[i]['task_id'] is not None and
                                              self.tasks[self.jobs[i]['task_id']]['status'] == 'completed'
                                              for i in self.handoff_ready)
        self.handoff_ready.clear()
        charging = sum(job['task_id'] is None for job in self.jobs.values())
        cap = max(0, self.config.robot_count - self.config.desired_working - self.config.standby_headroom)
        # Optional charging is lower priority than feasible business work, unlike critical return.
        for robot in eligible:
            if charging >= min(cap, len(self.docks)):
                break
            if robot['robot_id'] not in self.jobs and robot['battery'] < self.config.opportunity_charge_soc:
                if self.config.charging_policy == 'staggered' and self.schedule_charge(robot):
                    charging += 1
        self.pending = [k for k in self.pending if self.tasks[k]['status'] in {'waiting', 'accepted'}]

    def marker(self, robot, job, marker, task_id=None):
        super().marker(robot, job, marker, task_id)
        if marker == 'picked':
            robot['location_id'] = self.tasks[task_id or job['task_id']]['source_id']
        elif marker == 'delivered':
            task = self.tasks[task_id or job['task_id']]
            robot['location_id'] = task['target_id']
            self.metrics['task_flow_seconds'].append(self.elapsed - task['release_second'])
            if self.config.task_chaining:
                self.handoff_ready.add(robot['robot_id'])
        elif marker == 'charge_complete':
            if robot['battery'] < self.config.charge_target_soc - 1e-6:
                raise AssertionError('charging plan did not reach target SOC')
            robot['location_id'] = job['dock_id']
            if self.config.task_chaining:
                self.handoff_ready.add(robot['robot_id'])

    def step(self):
        before = {i: (r['x'], r['y'], r['carrying']) for i, r in self.robots.items()}
        prior_jobs = set(self.jobs)
        super().step()  # Includes unchanged independent continuous swept audit and hard capacity gates.
        for i, r in self.robots.items():
            x, y, loaded = before[i]
            distance = math.hypot(r['x'] - x, r['y'] - y)
            self.metrics['loaded_distance_m' if loaded else 'empty_distance_m'] += distance
            if r['stage'] == 'traffic_wait':
                # The old simulator charges active consumption for any non-charging part.
                # Apply the SAME idle rate used by its home-departure waits, not free energy.
                r['battery'] += self.config.active_soc_per_second - self.config.idle_soc_per_second
                r['active_seconds'] -= 1
                r['queue_seconds'] += 1
                self.metrics['traffic_wait_seconds'] += 1
            if r['stage'] in {'to_source', 'to_target', 'picking', 'placing'}:
                self.metrics['productive_robot_seconds'] += 1
            if i in prior_jobs and i not in self.jobs:
                r['location_id'] = r['home_id']
        counts = self.counts()
        if sum(counts[k] for k in ['moving', 'handling', 'charging', 'queued', 'standby']) != 100:
            raise AssertionError('robot stage accounting mismatch')

    def counts(self):
        values = super().counts()
        values['queued'] += sum(r['stage'] == 'traffic_wait' for r in self.robots.values())
        return values

    def checkpoint(self):
        state = super().checkpoint()
        # JSON object keys are strings. Normalize BEFORE hashing/journaling so a
        # disk read sorts identically even when robot IDs include 2 and 10.
        state['jobs'] = {str(k): v for k, v in state['jobs'].items()}
        state['scheduler_v2'] = {'next_dispatch': self.next_dispatch, 'handoff_ready': sorted(self.handoff_ready)}
        return state

    def restore(self, c):
        if c['run_id'] != self.run_id or c['config'] != asdict(self.config) or c['run_metadata'] != self.run_metadata:
            raise ValueError('recovery identity mismatch')
        for key in ['tick', 'elapsed', 'knowledge_seq', 'event_sequence', 'goods', 'inside', 'docks',
                    'cursor', 'pending', 'dispatch_dirty', 'metrics', 'running']:
            setattr(self, key, deepcopy(c[key]))
        self.tasks.update(deepcopy(c['tasks']))
        self.ports = Counter(c['ports'])
        self.robots = {p['state']['robot_id']: deepcopy(p['state']) for p in c['robots']}
        self.slots = {s['slot_id']: deepcopy(s) for r in c['racks'] for s in r['slots']}
        self.jobs = {int(k): deepcopy(v) for k, v in c['jobs'].items()}
        self.next_dispatch = c['scheduler_v2']['next_dispatch']
        self.handoff_ready = set(c['scheduler_v2']['handoff_ready'])
        for owner, job in self.jobs.items():
            self.reservations.reserve(job['parts'], job['start'], owner)
        self.reservations.prune(self.elapsed)
        self.pending_events = []
        self.assert_inventory()
