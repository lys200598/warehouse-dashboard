"""Prevention-first, feedback-aware extension of the independent v2 baseline.

This is a 2-D, circular-envelope transport model, not a hardware controller.
Disturbances occur at simulated second boundaries. Local retiming preserves
full safe escapes; a bounded affected-group barrier is the conservative
fallback. No robot/cargo is removed when a candidate is infeasible.
"""
from collections import Counter
from copy import deepcopy
from dataclasses import dataclass
import hashlib
import json
import math
from pathlib import Path
import time

from ..day1.nav import hold, position, audit_actual_motion
from ..day1.data import sha
from ..scheduling_v2.sim import Config as V2Config, Simulation as V2Simulation
from ..scheduling_v2.matching import minimum_assignment
from ..scheduling_v2.motion import MotionTable, SafeIntervalPlanner, continuous_clear
from .energy import EnergyProfile, assess_admission, transfer_custody
from .transactions import try_replace_many
from .progress import ProgressMonitor
from .layout import handover_candidates
from .obstacles import MotionTable as PersistentMotionTable
from .data import validate_dataset


@dataclass
class Config(V2Config):
    energy_profile: str = 'transport'
    charge_queue_margin: int = 300
    matching_rounds: int = 3
    pair_retry_seconds: int = 30
    progress_warning_seconds: int = 600
    fault_guard_seconds: int = 10800
    handoff_seconds: int = 10
    assistance_soc: float = 25.

    def __post_init__(self):
        profiles = {'transport': EnergyProfile, 'legacy': EnergyProfile.legacy,
                    'conservative': EnergyProfile.conservative}
        if self.energy_profile not in profiles:
            raise ValueError('unknown energy profile')
        p = profiles[self.energy_profile]()
        self.active_soc_per_second = p.empty_soc_per_second
        self.idle_soc_per_second = p.idle_soc_per_second
        self.charge_soc_per_second = p.charge_soc_per_second
        if not 1 <= self.matching_rounds <= 5 or self.pair_retry_seconds < 1:
            raise ValueError('invalid retry bounds')
        if min(self.charge_queue_margin, self.handoff_seconds, self.fault_guard_seconds) <= 0:
            raise ValueError('positive service/guard bounds required')
        if not 0 < self.emergency_reserve_soc < self.task_reserve_soc < self.charge_request_soc < self.charge_target_soc <= 100:
            raise ValueError('inconsistent SOC safety thresholds')
        if self.speed <= 0 or min(self.pick_seconds, self.place_seconds, self.route_horizon, self.route_expansions) <= 0:
            raise ValueError('positive motion/service/search parameters required')


class Simulation(V2Simulation):
    reservation_type = PersistentMotionTable

    def _initialize_base(self, scene_path, dataset, run_id, config):
        validate_dataset(scene_path, dataset)
        super().__init__(scene_path, dataset, run_id, config, checkpoint=None)

    def _reservation_parts(self, owner, parts):
        """World-model hook; original private-bay model needs no extra tail."""
        return parts

    def _try_replace(self, proposals, **kwargs):
        return try_replace_many(self.reservations,
            {owner:self._reservation_parts(owner,parts) for owner,parts in proposals.items()}, **kwargs)

    def _assignment(self, costs):
        """Default remains the frozen v3 policy; newer versions may override."""
        return minimum_assignment(costs)

    def __init__(self, scene_path, dataset, run_id, config=None, checkpoint=None):
        self._initialize_base(scene_path, dataset, run_id, config or Config())
        self.reservations = self.reservation_type()
        self.planner.table = self.reservations
        self.energy = {'transport': EnergyProfile, 'legacy': EnergyProfile.legacy,
                       'conservative': EnergyProfile.conservative}[self.config.energy_profile]()
        self.run_metadata.update(backend='local-central-prevention-feedback-v3',
                                 dataset_origin='synthetic-' + self.manifest.get('scenario', 'unspecified'),
                                 energy_parameters=self.energy.as_dict(),
                                 disturbance_contract='integer-second boundary; circular 2-D envelope; bounded stop/retime')
        self.run_metadata['scheduler_v3_code_sha256'] = hashlib.sha256(json.dumps(
            {p.name: sha(p) for p in sorted(Path(__file__).parent.glob('*.py'))}, sort_keys=True).encode()).hexdigest()
        self.failed_pairs = {}
        self.blocked = {}
        self.notices = set()
        self.faults = {}
        self.assists = {}
        self.dynamic_locations = {}
        self.recovery_sequence = 0
        self.energy_unresolved = None
        self.progress = ProgressMonitor(stall_seconds=60, dependency_ttl=35)
        self.metrics.update(matching_retries=0, energy_rejections=0, delay_events=0,
                            recovery_components=0, recovery_robots_total=0, recovery_group_max=0,
                            reroute_events=0, handoffs=0, assistance_requests=0,
                            assistance_failures=0, task_age_max=0, progress_warnings=0,
                            recovery_rejections=0, max_active_business=0,
                            dispatch_wall_seconds=[], recovery_wall_seconds=[])
        if checkpoint:
            self.restore(checkpoint)

    def _drain(self, parts):
        return self.energy.consume(parts)

    def schedule_charge(self, robot):
        result = self._charge_tail(robot, self.elapsed)
        if not result:
            return False
        parts, job = result
        self._commit(robot, parts, job)
        self._update_docks()
        self.docks[job['dock_id']]['sessions'] += 1
        robot.update(stage='charge_queue', dock_id=job['dock_id'], task_id=None)
        self.metrics['charge_sessions'] += 1
        self.metrics['charge_wait_seconds'].append(sum(p['end']-p['start'] for p in parts
            if p['phase'] == 'traffic_wait'))
        self.event('charge_reserved', robot['robot_id'], dock_id=job['dock_id'],
                   departure_at=self.elapsed, scheduler='prevention-feedback-v3')
        return True

    def _update_docks(self):
        """Availability means actual scheduled departure from the pad, not SOC completion."""
        for dock in self.docks.values():
            releases = [self.elapsed]
            for owner, job in self.jobs.items():
                if job.get('dock_id') != dock['id']:
                    continue
                charged = False
                for p in job['parts']:
                    if p['phase'] == 'charging':
                        charged = True
                    elif charged and math.dist((p['a']['x'], p['a']['y']), (p['b']['x'], p['b']['y'])) > 1e-8:
                        releases.append(job['start'] + p['start'] + 3)
                        break
            dock['available_at'] = max(releases)

    def _plan(self, *args, **kwargs):
        value = super()._plan(*args, **kwargs)
        if value is None:
            self._last_block = {'reason': self.planner.last_failure or 'no_path',
                                'owners': sorted(self.planner.last_conflicts), 'at': self.elapsed}
        return value

    def _point(self, robot):
        return {'x': robot['x'], 'y': robot['y']}

    def _location(self, robot):
        point = self._point(robot)
        known = robot.get('location_id')
        if known in self.nav.locations and math.dist(point.values(), self.nav.locations[known].values()) < 1e-6:
            return known
        # While inside a private bay, only its own ID authorizes the connector.
        for key in [robot['home_id'], robot.get('dock_id')]:
            if key in self.nav.private:
                x0, y0, x1, y1 = self.nav.private[key]
                if x0 - .55 <= point['x'] <= x1 + .55 and y0 - .55 <= point['y'] <= y1 + .55:
                    # A private alias retains the exemption without changing its real endpoint.
                    # Navigator excludes the original, so aliases cannot authorize that box.
                    # Instead use a per-planner view below for this rare recovery connector.
                    return self._dynamic(point, private_parent=key)
        return self._dynamic(point)

    def _dynamic(self, point, private_parent=None):
        self.recovery_sequence += 1
        name = f'@v3-{self.recovery_sequence}'
        self.nav.locations[name] = dict(point)
        self.dynamic_locations[name] = {'point': dict(point), 'private_parent': private_parent}
        return name

    def _route(self, source, target, clock, owner, phase, dwell=0, action=None, marker=None, **kw):
        # Temporarily exempt ONLY the current bay for an actual in-bay start.
        parent = self.dynamic_locations.get(source, {}).get('private_parent')
        if not parent:
            return self._plan(source, target, clock, owner, phase, dwell, action, marker, **kw)
        box = self.nav.private.pop(parent)
        try:
            return self._plan(source, target, clock, owner, phase, dwell, action, marker, **kw)
        finally:
            self.nav.private[parent] = box

    def _task_parts(self, robot, task, start=None):
        clock = self.elapsed if start is None else start
        current = self._location(robot)
        parts = []
        carrying = task['status'] == 'carrying' and task.get('robot_id') == robot['robot_id']
        steps = [] if carrying else [(task['source_id'], 'to_source', 'picking', self.config.pick_seconds, 'picked')]
        steps += [(task['target_id'], 'to_target', 'placing', self.config.place_seconds, 'delivered'),
                  (robot['home_id'], 'returning', None, 0, None)]
        for target, phase, action, dwell, marker in steps:
            # If a service is interrupted, preserve completed work instead of picking twice.
            old = self.jobs.get(robot['robot_id'])
            if old and old.get('task_id') == task['task_id']:
                for p in self._suffix(robot['robot_id']):
                    if p['start'] == self.elapsed and p.get('marker') == marker and marker and p['a'] == p['b']:
                        dwell = p['end'] - p['start']
                        break
            result = self._route(current, target, clock, robot['robot_id'], phase, dwell, action, marker)
            if result is None:
                return None
            more, clock = result
            for p in more:
                p['task_id'] = task['task_id']
                p['loaded'] = carrying
            parts.extend(more)
            carrying = marker == 'picked' or carrying and marker != 'delivered'
            current = target
        return parts

    def _energy_admission(self, robot, parts):
        end = parts[-1]['end']
        options = []
        choices = sorted(self.docks.values(), key=lambda d: (
            max(end, d['available_at']) + self.roadmap.travel_time(robot['home_id'], d['id'], self.config.speed), d['id']))
        for dock in choices[:4]:
            eta = self.roadmap.travel_time(robot['home_id'], dock['id'], self.config.speed)
            depart = max(end, dock['available_at'] - eta)
            if depart - end > self.config.max_charge_wait:
                continue
            result = self._route(robot['home_id'], dock['id'], depart, robot['robot_id'], 'to_charge',
                                 earliest_goal=max(depart, dock['available_at']))
            if result:
                wait = [hold(self.nav.locations[robot['home_id']], 'traffic_wait', end, depart - end)] if depart > end else []
                options.append({'dock_id': dock['id'], 'parts': wait + result[0],
                                'available_at': dock['available_at'],
                                'queue_seconds': self.config.charge_queue_margin})
                admission = assess_admission(robot['battery'], parts, options, self.energy,
                    reserve_soc=self.config.task_reserve_soc,
                    max_queue_seconds=self.config.max_charge_wait,
                    initial_loaded=robot['carrying'])
                if admission.admitted:
                    return admission
        return assess_admission(robot['battery'], parts, options, self.energy,
            reserve_soc=self.config.task_reserve_soc, max_queue_seconds=self.config.max_charge_wait,
            initial_loaded=robot['carrying'])

    def schedule_task(self, robot, task, extra=None):
        if robot['robot_id'] in self.faults or robot['carrying']:
            return False
        self._last_block = {'reason': 'unclassified', 'owners': [], 'at': self.elapsed}
        parts = self._task_parts(robot, task)
        if not parts:
            return False
        admission = self._energy_admission(robot, parts)
        if not admission.admitted:
            self.metrics['energy_rejections'] += 1
            self._last_block = {'reason': admission.reason, 'owners': [], 'at': self.elapsed}
            return False
        owner = robot['robot_id']
        continued = owner in self.jobs
        self._commit(robot, parts, {'task_id': task['task_id'], 'task_ids': [task['task_id']], 'dock_id': None})
        departure = next(p['start'] for p in parts if p['phase'] != 'traffic_wait')
        self.metrics['departure_wait_seconds'].append(departure - self.elapsed)
        self.metrics['dispatch_wait_seconds'].append(self.elapsed - task.get('ready_at', task['release_second']))
        self.metrics['task_chains'] += int(continued)
        robot.update(task_id=task['task_id'], missions=robot['missions'] + 1, stage='mission_queue', dock_id=None)
        task.update(status='assigned', robot_id=owner, assigned_at=self.elapsed,
                    departure_at=departure, energy_admission=admission.as_dict())
        if task['task_type'] == 'OUTBOUND':
            self.slots[task['source_id']]['reservedBy'] = task['task_id']
        self.event('task_assigned', owner, task['task_id'], departure_at=departure,
                   batch_size=1, continuation=continued, scheduler='prevention-feedback-v3',
                   energy_required=admission.required_soc, energy_dock=admission.dock_id)
        return True

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
        # Emergency margins outrank arbitrary robot IDs.
        for r in sorted(eligible, key=lambda r: (r['battery'], r['robot_id'])):
            if r['battery'] <= self.config.charge_request_soc:
                self.schedule_charge(r)
            else:
                candidates.append(r)
        # Back off failed TASKS as well as pairs so a blocked first batch cannot
        # prevent later independent work from being considered.
        pool = sorted(ready, key=lambda t: (self.blocked.get(t['task_id'], {}).get('retry_after', 0) > self.elapsed,
            t['due_second'] - .5 * (self.elapsed - t['ready_at']), t['task_id']))[:self.config.batch_tasks]
        for attempt in range(self.config.matching_rounds):
            if not pool or not candidates:
                break
            costs = []
            for task in pool:
                delivery = self.roadmap.travel_time(task['source_id'], task['target_id'], self.config.speed) + self.config.pick_seconds + self.config.place_seconds
                row = []
                for r in candidates:
                    eta = self.roadmap.travel_time(r['location_id'], task['source_id'], self.config.speed)
                    late = max(0, self.elapsed + eta + delivery - task['due_second'])
                    row.append(1e12 if (r['robot_id'], task['task_id']) in self.failed_pairs else eta + delivery + late * 4 + r['missions'] * .05)
                row.extend([1e8] * len(pool))
                costs.append(row)
            self.metrics['assignment_batches'] += 1
            successes = []
            failures = 0
            for row, col in self._assignment(costs):
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

    def _suffix(self, owner):
        job = self.jobs.get(owner)
        if not job:
            return []
        now, result = self.elapsed, []
        for p in job['parts']:
            q = {**deepcopy(p), 'start': p['start'] + job['start'], 'end': p['end'] + job['start']}
            if q['end'] <= now:
                continue
            if q['start'] < now:
                q['a'] = position(q, now)
                q['start'] = now
            result.append(q)
        return result

    def _set_job(self, owner, parts, old):
        self.jobs[owner] = {**deepcopy(old), 'parts': [{**p, 'start': p['start'] - self.elapsed,
            'end': p['end'] - self.elapsed} for p in parts], 'start': self.elapsed, 'phase_index': 0}
        self._update_docks()
        task = self.tasks.get(old.get('task_id'))
        if task and task['status'] == 'assigned':
            task.setdefault('original_departure_at', task.get('departure_at'))
            task['revised_departure_at'] = next((p['start'] for p in parts
                if p['phase'] not in {'traffic_wait', 'recovery_wait', 'fault_wait'}), parts[-1]['end'])

    def inject_delay(self, owner, seconds, reason='temporary_stop'):
        """Stop now, keep actual bodies, expand only a continuously conflicting group."""
        if not isinstance(seconds, int) or not 0 < seconds <= 600:
            raise ValueError('delay must be 1..600 integer seconds')
        if owner not in self.jobs or owner in self.faults:
            raise ValueError('delay requires an active, non-faulted robot')
        started = time.perf_counter()
        group = {owner}
        while len(group) <= len(self.robots):
            for donor, assist in self.assists.items():
                pair = {donor, assist['receiver']}
                if group & pair:
                    group.update(pair)
            proposals = {}
            for i in group:
                tail = self._suffix(i)
                if not tail:
                    self.metrics['recovery_rejections'] += 1
                    return False
                # A stationary traffic barrier does not disconnect a plugged-in
                # charger. Continue charging, conservatively retaining its tail.
                wait_phase = 'charging' if tail[0]['phase'] == 'charging' else 'recovery_wait'
                wait = hold(self._point(self.robots[i]), wait_phase, self.elapsed, seconds)
                proposals[i] = [wait] + [{**p, 'start': p['start'] + seconds, 'end': p['end'] + seconds} for p in tail]
                prefix = []
                for part in proposals[i]:
                    if part['phase'] == 'charging' or part.get('permanent_obstacle'):
                        break
                    prefix.append(part)
                if self.energy.consume(prefix, self.robots[i]['carrying']) + self.config.emergency_reserve_soc > self.robots[i]['battery']:
                    self.metrics['recovery_rejections'] += 1
                    self.event('traffic_recovery_rejected', owner, reason='insufficient_recovery_energy', affected_robot=i)
                    return False
            # Every changed body is explicitly present at the same actual start position.
            result = self._try_replace(proposals, now=self.elapsed,
                                      current_positions={i: self._point(self.robots[i]) for i in group})
            if result:
                for i, parts in proposals.items():
                    self._set_job(i, parts, self.jobs[i])
                for donor, assist in self.assists.items():
                    if donor in group:
                        assist['handoff_at'] += seconds
                for i in group & set(self.faults):
                    if self.faults[i]['until'] is not None:
                        self.faults[i]['until'] += seconds
                self.metrics['delay_events'] += 1
                self.metrics['recovery_components'] += 1
                self.metrics['recovery_robots_total'] += len(group)
                self.metrics['recovery_group_max'] = max(self.metrics['recovery_group_max'], len(group))
                self.metrics['recovery_wall_seconds'].append(time.perf_counter() - started)
                self.event('traffic_retimed', owner, self.jobs[owner].get('task_id'), seconds=seconds,
                           reason=reason, affected_robots=sorted(group), mode='bounded_component_barrier')
                return True
            added = set(result.conflicts) - group
            if not added or any(i not in self.jobs or i in self.faults for i in added):
                self.metrics['recovery_rejections'] += 1
                self.event('traffic_recovery_rejected', owner, reason=result.reason,
                           affected_robots=sorted(group), conflicts=list(result.conflicts))
                return False
            group.update(added)
        return False

    def inject_stop(self, owner, seconds, reason='temporary_drive_fault'):
        """A finite observed drive stop; reroute other impacted vehicles first.

        Returns False when no verified recovery transaction exists, so the
        runner must record a failed fault-injection test, never claim recovery.
        The finite fault duration is explicit, not a simulated mechanical repair.
        """
        if seconds is not None and (not isinstance(seconds, int) or not 1 <= seconds <= 3600):
            raise ValueError('stop must be 1..3600 integer seconds')
        if owner not in self.jobs or owner in self.faults:
            return False
        if any(owner in {donor, a['receiver']} for donor, a in self.assists.items()):
            if seconds is None or seconds > 600 or not self.inject_delay(owner, seconds, reason=reason):
                return False
            self.jobs[owner]['parts'][0]['marker'] = 'stop_cleared'
            self.faults[owner] = {'until': self.elapsed + seconds, 'reason': reason}
            self.event('robot_stopped', owner, self.jobs[owner].get('task_id'), reason=reason,
                       until=self.elapsed + seconds, affected_robot_ids=[owner], mode='coupled_handoff_barrier')
            return True
        tail = self._suffix(owner)
        if not tail or tail[0]['phase'] == 'charging':
            return False
        if seconds is None:
            parts = [self._permanent_hold(self._point(self.robots[owner]), self.elapsed)]
            metadata = {'task_id': None, 'dock_id': None, 'persistent_fault': True}
        else:
            parts = [hold(self._point(self.robots[owner]), 'fault_wait', self.elapsed, seconds, 'stop_cleared')]
            parts += [{**p, 'start': p['start'] + seconds, 'end': p['end'] + seconds} for p in tail]
            metadata = self.jobs[owner]
        prefix = []
        for p in parts:
            if p['phase'] == 'charging':
                break
            prefix.append(p)
        if seconds is not None and self.energy.consume(prefix, self.robots[owner]['carrying']) + self.config.emergency_reserve_soc > self.robots[owner]['battery']:
            return False
        if not self._coordinated_replan({owner: (parts, metadata)}):
            # A coupled handoff cannot be independently rerouted: synchronize
            # its full component instead. The fallback is explicit, not hidden.
            if seconds is None or seconds > 600 or not self.inject_delay(owner, seconds, reason=reason):
                self.event('fault_injection_unresolved', owner, reason=reason, seconds=seconds)
                return False
            self.jobs[owner]['parts'][0]['marker'] = 'stop_cleared'
        self.faults[owner] = {'until': self.elapsed + seconds if seconds is not None else None,
                              'reason': reason, 'persistent': seconds is None}
        if seconds is None and not self.robots[owner]['carrying']:
            self._requeue_unpicked(self.robots[owner], reason='persistent_drive_fault')
        self.event('robot_stopped', owner, self.jobs[owner].get('task_id'), reason=reason,
                   until=self.faults[owner]['until'], persistent=seconds is None,
                   point=self._point(self.robots[owner]), affected_robots=[owner])
        return True

    def _permanent_hold(self, point, clock):
        # Finite numeric sentinel far beyond the enforced seven-day input
        # contract. PersistentMotionTable never unrolls its time buckets.
        return {**hold(point, 'fault_wait', clock, 10**12-clock), 'permanent_obstacle': True}

    def _requeue_unpicked(self, robot, reason):
        task = self.tasks.get(robot.get('task_id'))
        if task and task['status'] == 'assigned':
            task.update(status='accepted' if task['task_type']=='INBOUND' else 'waiting', robot_id=None)
            if task['task_type'] == 'OUTBOUND':
                self.slots[task['source_id']]['reservedBy'] = None
            if task['task_id'] not in self.pending:
                self.pending.append(task['task_id'])
            self.event('task_requeued', robot['robot_id'], task['task_id'], reason=reason)
        robot['task_id'] = None

    def clear_fault(self, owner):
        """Explicit external restoration, never an implicit simulated repair."""
        if owner not in self.faults or any(owner in {d,a['receiver']} for d,a in self.assists.items()):
            return False
        robot = self.robots[owner]
        if robot['carrying']:
            parts = self._task_parts(robot, self.tasks[robot['task_id']])
            result = (parts, {'task_id':robot['task_id'], 'dock_id':None}) if parts and self._energy_admission(robot,parts).admitted else None
        else:
            result = self._charge_tail(robot, self.elapsed)
        if not result or not self._coordinated_replan({owner:result}):
            return False
        self.faults.pop(owner)
        robot['power_isolated'] = False
        self.event('robot_reenabled', owner, reason='explicit_external_fault_clear')
        return True

    def report_soc(self, owner, measured_soc):
        """Explicit sensor/model correction, not silent free depletion/recharge."""
        if not math.isfinite(measured_soc) or not self.config.emergency_reserve_soc < measured_soc <= 100:
            raise ValueError('SOC correction must leave a recoverable emergency margin')
        r = self.robots[owner]
        old = r['battery']
        r['battery'] = measured_soc
        self.event('battery_measurement_corrected', owner, r.get('task_id'),
                   old_soc=old, measured_soc=measured_soc, external_disturbance=True)
        if any(owner in {d,a['receiver']} for d,a in self.assists.items()):
            prefix = []
            for p in self._suffix(owner):
                if p['phase']=='charging' or p.get('permanent_obstacle'):
                    break
                prefix.append(p)
            ok = self.energy.consume(prefix,r['carrying']) + self.config.emergency_reserve_soc <= measured_soc
            if not ok:
                self.energy_unresolved = {'owner':owner,'at':self.elapsed,'reason':'coupled_handoff_energy_changed'}
            return ok
        if self.faults.get(owner,{}).get('persistent') and not r['carrying']:
            return True
        needs_help = r['carrying'] and (measured_soc <= self.config.assistance_soc or
            not self._energy_admission(r,self._suffix(owner)).admitted)
        if needs_help:
            recovered = self.request_assistance(owner, reason='measured_low_energy')
            if not recovered:
                self.energy_unresolved = {'owner': owner, 'at': self.elapsed, 'measured_soc': measured_soc}
            return recovered
        if owner in self.jobs and not r['carrying']:
            prefix = []
            for part in self._suffix(owner):
                if part['phase'] == 'charging':
                    break
                prefix.append(part)
            if self.energy.consume(prefix) + self.config.emergency_reserve_soc > measured_soc:
                charge = self._charge_tail(r, self.elapsed)
                if charge and self._coordinated_replan({owner: charge}):
                    # A not-yet-picked parcel remains at its source and is
                    # released back to dispatch instead of silently cancelled.
                    task = self.tasks.get(r.get('task_id'))
                    if task and task['status'] == 'assigned':
                        task.update(status='accepted' if task['task_type'] == 'INBOUND' else 'waiting', robot_id=None)
                        if task['task_type'] == 'OUTBOUND':
                            self.slots[task['source_id']]['reservedBy'] = None
                        if task['task_id'] not in self.pending:
                            self.pending.append(task['task_id'])
                        self.event('task_requeued', owner, task['task_id'], reason='measured_low_energy')
                    r.update(task_id=None, dock_id=charge[1]['dock_id'], stage='charge_queue')
                    return True
                self.event('energy_recovery_unresolved', owner, measured_soc=measured_soc)
                self.energy_unresolved = {'owner': owner, 'at': self.elapsed, 'measured_soc': measured_soc}
                return False
        return True

    def _charge_tail(self, robot, clock, source=None, battery=None):
        source = source or self._location(robot)
        battery = robot['battery'] if battery is None else battery
        choices = sorted(self.docks.values(), key=lambda d: (max(clock, d['available_at']),
            math.hypot(d['point']['x'] - robot['x'], d['point']['y'] - robot['y']), d['id']))
        for dock in choices:
            bound = self.config.route_horizon * max(self.energy.empty_soc_per_second, self.energy.idle_soc_per_second)
            upper = max(1, self.energy.charge_seconds(max(0., battery - bound), self.config.charge_target_soc))
            approach, at, current = [], clock, source
            eta = self.roadmap.travel_time(robot['home_id'], dock['id'], self.config.speed)
            if dock['available_at'] > clock + self.config.route_horizon:
                home = self._route(source, robot['home_id'], clock, robot['robot_id'], 'returning')
                if not home:
                    continue
                approach, at = home
                depart = max(at, dock['available_at'] - eta)
                if depart - at > self.config.max_charge_wait:
                    continue
                if depart > at:
                    approach.append(hold(self.nav.locations[robot['home_id']], 'traffic_wait', at, depart-at))
                at, current = depart, robot['home_id']
                bound += self.energy.consume(approach)
                upper = max(1, self.energy.charge_seconds(max(0., battery - bound), self.config.charge_target_soc))
            out = self._route(current, dock['id'], at, robot['robot_id'], 'to_charge',
                upper, 'charging', 'charge_complete', earliest_goal=max(clock, dock['available_at']))
            if not out:
                continue
            parts, _ = out
            parts = approach + parts
            drain = self.energy.consume(parts[:-1], False)
            if battery < drain + self.config.emergency_reserve_soc:
                continue
            duration = max(1, self.energy.charge_seconds(battery - drain, self.config.charge_target_soc))
            if duration > upper:
                continue
            parts[-1]['end'] = parts[-1]['start'] + duration
            back = self._route(dock['id'], robot['home_id'], parts[-1]['end'], robot['robot_id'], 'returning')
            if back and self.energy.consume(back[0]) + self.config.emergency_reserve_soc <= self.config.charge_target_soc:
                parts.extend(back[0])
                return parts, {'task_id': None, 'dock_id': dock['id'], 'charge_started': False,
                               'request_time': self.elapsed, 'arrival': parts[-len(back[0])-1]['start'] - self.elapsed}
        return None

    def _remaining(self, owner):
        robot, old = self.robots[owner], self.jobs[owner]
        task = self.tasks.get(old.get('task_id'))
        if task and task['status'] != 'completed':
            parts = self._task_parts(robot, task)
            if parts and self._energy_admission(robot, parts).admitted:
                return parts, old
            return None
        if old.get('dock_id') and robot['battery'] < self.config.charge_target_soc - 1e-6:
            # Ignore this robot's obsolete availability promise, not other
            # robots' continuous occupancy of the same charging pad.
            dock = self.docks[old['dock_id']]
            available = dock['available_at']
            dock['available_at'] = self.elapsed
            try:
                return self._charge_tail(robot, self.elapsed)
            finally:
                dock['available_at'] = available
        result = self._route(self._location(robot), robot['home_id'], self.elapsed, owner, 'returning')
        return (result[0], old) if result else None

    def _coordinated_replan(self, required, preferred=()):
        """Replan impacted suffixes, retain unaffected traffic and actual bodies.

        Required maps owner -> (full absolute plan, job metadata). Other group
        members are routed in several bounded priority orders while all not-yet
        planned members remain stationary obstacles. No candidate mutates jobs.
        """
        group = set(required) | set(preferred)
        for owner, job in self.jobs.items():
            if owner in group:
                continue
            if any(not continuous_clear(p['a'], p['b'], p['start'], p['end'], q)
                   for p in self._suffix(owner) for parts, _ in required.values() for q in parts):
                group.add(owner)
        # Do not lose a pre-transfer handoff marker or independently move a
        # declared stopped robot. Caller can use the synchronized barrier path.
        coupled = {i for d, a in self.assists.items() for i in (d, a['receiver'])}
        if any(set(required) & {d, a['receiver']} and not {d, a['receiver']} <= set(required)
               for d, a in self.assists.items()):
            return False
        if (group & coupled) - set(required) or (group & set(self.faults)) - set(required):
            return False
        for expansion in range(4):
            if (group & coupled) - set(required) or (group & set(self.faults)) - set(required):
                return False
            mobile = sorted(group - set(required))
            orders = [mobile, list(reversed(mobile)), sorted(mobile, key=lambda i: (
                self.robots[i]['battery'], self.robots[i]['robot_id']))]
            external = set()
            for order in orders:
                trial = deepcopy(self.reservations)
                for owner in group:
                    trial._erase(owner)
                proposals = {i: deepcopy(parts) for i, (parts, _) in required.items()}
                jobs = {i: deepcopy(job) for i, (_, job) in required.items()}
                for i, parts in proposals.items():
                    trial.reserve(self._reservation_parts(i,parts), owner=i)
                guard_end = self.elapsed + self.config.fault_guard_seconds
                for i in mobile:
                    trial.reserve([hold(self._point(self.robots[i]), 'recovery_wait', self.elapsed, guard_end-self.elapsed)], owner=i)
                original_table, original_planner = self.reservations, self.planner.table
                self.reservations = self.planner.table = trial
                failed = False
                try:
                    for i in order:
                        result = self._remaining(i)
                        if not result:
                            external.update(self.planner.last_conflicts)
                            failed = True
                            break
                        parts, job = result
                        proposals[i], jobs[i] = parts, deepcopy(job)
                        trial.reserve(self._reservation_parts(i,parts), owner=i)
                finally:
                    self.reservations, self.planner.table = original_table, original_planner
                if failed:
                    continue
                result = self._try_replace(proposals, now=self.elapsed,
                    current_positions={i: self._point(self.robots[i]) for i in group})
                if result:
                    for i, parts in proposals.items():
                        self._set_job(i, parts, jobs[i])
                    self.metrics['reroute_events'] += 1
                    self.event('traffic_rerouted', affected_robots=sorted(group),
                               reason='changed_actual_occupancy', priority_attempt=orders.index(order))
                    return True
                external.update(result.conflicts)
            new = {i for i in external - group if i in self.jobs and i not in self.faults}
            if not new:
                break
            group.update(new)
        self.metrics['recovery_rejections'] += 1
        return False

    def _assistance_helpers(self, donor, task):
        """Overridable candidate source; live feasibility stays authoritative."""
        return sorted((r for i, r in self.robots.items() if i != donor['robot_id'] and i not in self.jobs and
            i not in self.faults and not r['carrying'] and r['battery'] > self.config.charge_request_soc),
            key=lambda r: (math.hypot(r['x']-donor['x'], r['y']-donor['y']), r['robot_id']))

    def request_assistance(self, donor_id, reason='low_energy'):
        """Physically adjacent, reserved handoff; donor then recharges safely.

        This is preemptive help for a mobile low-energy robot, not a claim to
        tow or repair a mechanically failed robot. Failed candidates leave the
        original safe mission/custody unchanged.
        """
        donor = self.robots[donor_id]
        if not donor['carrying'] or donor_id in self.assists:
            return False
        task = self.tasks[donor['task_id']]
        helpers = self._assistance_helpers(donor, task)
        self.metrics['assistance_requests'] += 1
        failures = Counter()
        self.event('assistance_requested', donor_id, task['task_id'], reason=reason, battery=donor['battery'])
        for candidate in handover_candidates(self.roadmap, self._point(donor), limit=8):
            handoff_id = self._dynamic(candidate['point'])
            # Restrict connectors to those whose full segment clears the donor.
            signature = (candidate['point']['x'], candidate['point']['y'], '')
            self.roadmap.connections_cache[signature] = candidate['connections']
            for receiver in helpers[:8]:
                trial = deepcopy(self.reservations)
                trial.reserve([hold(self._point(donor), 'fault_wait', self.elapsed,
                                    self.config.fault_guard_seconds)], owner=donor_id)
                original_table, original_planner = self.reservations, self.planner.table
                self.reservations = self.planner.table = trial
                required = None
                try:
                    result = self._route(receiver['location_id'], handoff_id, self.elapsed,
                        receiver['robot_id'], 'to_handoff', self.config.handoff_seconds,
                        'handoff_receive', 'handoff_received')
                    if not result:
                        failures['helper_approach'] += 1
                        continue
                    received, clock = result
                    service_start = clock - self.config.handoff_seconds
                    for p in received:
                        p['task_id'] = task['task_id']
                    # Plan the donor's post-transfer departure BEFORE the
                    # receiver's delivery. Otherwise a donor already at the
                    # target incorrectly remains a permanent goal obstacle.
                    trial.reserve(received, owner=receiver['robot_id'])
                    end = clock
                    given = []
                    if service_start > self.elapsed:
                        given.append(hold(self._point(donor), 'fault_wait', self.elapsed, service_start-self.elapsed))
                    given.append(hold(self._point(donor), 'handoff_give', service_start, self.config.handoff_seconds))
                    if self.faults.get(donor_id, {}).get('persistent'):
                        given.append(self._permanent_hold(self._point(donor), end))
                        charge_job = {'task_id':None, 'dock_id':None, 'persistent_fault':True}
                    else:
                        charge_at = max(end, self.faults.get(donor_id, {}).get('until', end))
                        if charge_at > end:
                            given.append(hold(self._point(donor), 'fault_wait', end, charge_at-end, 'stop_cleared'))
                        elif donor_id in self.faults:
                            given[-1]['marker'] = 'stop_cleared'
                        battery = donor['battery'] - self.energy.consume(given, True)
                        charge = self._charge_tail(donor, charge_at, battery=battery)
                        if not charge:
                            failures['donor_charge'] += 1
                            continue
                        parts, charge_job = charge
                        given.extend(parts)
                    trial.reserve(self._reservation_parts(donor_id,given), owner=donor_id)
                    for target, phase, dwell, action, marker in [
                        (task['target_id'], 'to_target', self.config.place_seconds, 'placing', 'delivered'),
                        (receiver['home_id'], 'returning', 0, None, None)]:
                        result = self._route(handoff_id if phase == 'to_target' else task['target_id'], target,
                            clock, receiver['robot_id'], phase, dwell, action, marker)
                        if not result:
                            failures['helper_delivery_or_escape'] += 1
                            break
                        more, clock = result
                        for p in more:
                            p['task_id'] = task['task_id']
                            p['loaded'] = phase == 'to_target'
                        received.extend(more)
                    else:
                        if not self._energy_admission(receiver, received).admitted:
                            failures['helper_energy'] += 1
                            continue
                        trial.reserve(self._reservation_parts(receiver['robot_id'],received), owner=receiver['robot_id'])
                        required = {donor_id: (given, charge_job), receiver['robot_id']: (received,
                            {'task_id': task['task_id'], 'task_ids': [task['task_id']], 'dock_id': None,
                             'assist_donor': donor_id, 'handoff_id': handoff_id})}
                finally:
                    self.reservations, self.planner.table = original_table, original_planner
                if required and self._coordinated_replan(required):
                    receiver.update(task_id=task['task_id'], dock_id=None, stage='to_handoff', missions=receiver['missions']+1)
                    self.assists[donor_id] = {'receiver': receiver['robot_id'], 'task_id': task['task_id'],
                                             'reason': reason, 'handoff_at': end}
                    self.event('handoff_reserved', donor_id, task['task_id'], receiver_robot_id=receiver['robot_id'],
                               handoff_at=end, donor_point=self._point(donor), receiver_point=candidate['point'])
                    return True
                if required:
                    failures['coordination'] += 1
        self.metrics['assistance_failures'] += 1
        self.event('assistance_deferred', donor_id, task['task_id'], reason='no_safe_energy_feasible_pair',
                   candidate_failures=dict(failures), helpers_available=len(helpers))
        return False

    def marker(self, robot, job, marker, task_id=None):
        if marker == 'stop_cleared':
            self.faults.pop(robot['robot_id'], None)
            self.event('robot_stop_cleared', robot['robot_id'], reason='declared_finite_fault_ended')
        elif marker == 'handoff_received':
            donor_id = job['assist_donor']
            donor = self.robots[donor_id]
            task = self.tasks[task_id or job['task_id']]
            d, r, t, goods, event = transfer_custody(donor, robot, task, self.goods,
                now=self.elapsed, reason=self.assists[donor_id]['reason'])
            donor.update(d)
            robot.update(r)
            task.update(t)
            self.goods = goods
            if self.faults.get(donor_id,{}).get('persistent'):
                donor['power_isolated'] = True
                self.event('robot_power_isolated', donor_id, reason='persistent_drive_fault_after_cargo_transfer')
            robot['location_id'] = job['handoff_id']
            self.assists.pop(donor_id)
            self.metrics['handoffs'] += 1
            self.event('cargo_handoff', robot['robot_id'], task['task_id'],
                goods_id=task['goods_id'], location_id=goods[task['goods_id']],
                donor_id=donor_id, receiver_id=robot['robot_id'], donor_robot_id=donor_id,
                receiver_robot_id=robot['robot_id'], reason=event['reason'])
            self.assert_inventory()
        else:
            super().marker(robot, job, marker, task_id)
        self.progress.record_progress(robot['robot_id'], self.elapsed,
                                      point=self._point(robot), token=(marker, self.event_sequence))

    def _diagnostics(self):
        for task in self.tasks.values():
            if task['status'] in {'scheduled', 'completed'}:
                continue
            age = self.elapsed - task['release_second']
            self.metrics['task_age_max'] = max(self.metrics['task_age_max'], age)
            if age >= self.config.progress_warning_seconds and task['task_id'] not in self.notices:
                self.notices.add(task['task_id'])
                self.metrics['progress_warnings'] += 1
                reason = self.blocked.get(task['task_id'], {}).get('reason')
                if not reason:
                    reason = 'executing_reserved_route' if task['status'] in {'assigned', 'carrying'} else 'admission_or_dispatch_wait'
                self.event('task_progress_warning', task_id=task['task_id'], age_seconds=age, reason=reason)
        observed = self.progress.observe(self.elapsed)
        for cycle in observed.get('persistent_cycles', []):
            members = [i for i in cycle if i in self.jobs and i not in self.faults]
            if members:
                self.event('persistent_wait_cycle', affected_robots=members)
                self._coordinated_replan({}, preferred=members)

    def step(self):
        # Execution uses the same model as admission. Unlike v2 there is no
        # second, inconsistent correction after a hard-SOC assertion.
        if self.energy_unresolved:
            # No physical step after an unhandled measured-energy fault. The
            # experiment fails with actual bodies/custody still preserved.
            raise RuntimeError('unresolved measured energy; coordinator safely paused')
        self.release_and_dispatch()
        now = self.elapsed
        before = {i: self._point(r) for i, r in self.robots.items()}
        # Optional execution seam: a Ray pool returns (a,b) for every active job.
        # Default None keeps the exact local V3/V5 behavior unchanged.
        motion = self.motion_executor(now) if getattr(self, 'motion_executor', None) is not None else None
        for owner, robot in self.robots.items():
            job = self.jobs.get(owner)
            if not job:
                robot['battery'] -= self.energy.rate('idle')
                robot['stage'] = 'idle'
                continue
            at = now - job['start']
            idx = job['phase_index']
            while idx < len(job['parts']) and at >= job['parts'][idx]['end']:
                idx += 1
            if idx == len(job['parts']):
                raise AssertionError('unprocessed v3 mission completion')
            job['phase_index'] = idx
            part = job['parts'][idx]
            if motion is not None:
                a, b = motion[owner]
            else:
                a, b = position(part, at), position(part, at + 1)
            if math.dist((robot['x'], robot['y']), (a['x'], a['y'])) > 1e-6:
                raise AssertionError('v3 motion discontinuity')
            loaded = robot['carrying']
            robot.update(x=b['x'], y=b['y'], stage=part['phase'])
            distance = math.dist((a['x'], a['y']), (b['x'], b['y']))
            robot['distance'] += distance
            self.metrics['loaded_distance_m' if loaded else 'empty_distance_m'] += distance
            if part['phase'] == 'charging':
                if not job.get('charge_started'):
                    job['charge_started'] = True
                    self.event('charge_started', owner, dock_id=job['dock_id'], battery=robot['battery'])
                robot['battery'] = min(self.config.charge_target_soc, robot['battery'] + self.energy.charge_gain(robot['battery'], 1))
                robot['charge_seconds'] += 1
                self.docks[job['dock_id']]['charge_seconds'] += 1
            else:
                # After the parcel is transferred, a persistent failed vehicle
                # is powered down. Its solid body/parking blockage remains.
                isolated = self.faults.get(owner, {}).get('persistent') and not loaded and part['phase']=='fault_wait'
                if not isolated:
                    robot['battery'] -= self.energy.rate(part['phase'], loaded)
                if part['phase'] in {'traffic_wait', 'recovery_wait', 'fault_wait'}:
                    robot['queue_seconds'] += 1
                    self.metrics['traffic_wait_seconds'] += 1
                else:
                    robot['active_seconds'] += 1
            if part['phase'] in {'to_source', 'to_target', 'picking', 'placing'}:
                self.metrics['productive_robot_seconds'] += 1
            if at + 1 == part['end'] and part.get('marker'):
                self.elapsed = self.tick = now + 1
                self.marker(robot, job, part['marker'], part.get('task_id'))
                self.elapsed = self.tick = now
            if self.jobs.get(owner) is job and at + 1 == job['parts'][-1]['end']:
                robot.update(stage='idle', task_id=None, dock_id=None, location_id=robot['home_id'])
                del self.jobs[owner]
                self.dispatch_dirty = True
            self.progress.record_progress(owner, now+1, point=self._point(robot),
                                          task_id=robot.get('task_id'), token=robot['completed'])
        self.elapsed += 1
        self.tick = self.elapsed
        pairs, separation = audit_actual_motion(before, {i: self._point(r) for i, r in self.robots.items()},
                                                cells=self.reservations.cells)
        self.metrics['motion_audit_pairs'] += pairs
        self.metrics['min_separation_m'] = min(self.metrics['min_separation_m'], separation)
        self.metrics['max_inside'] = max(self.metrics['max_inside'], self.inside)
        self.metrics['min_inside'] = min(self.metrics['min_inside'], self.inside)
        self.metrics['min_soc'] = min(self.metrics['min_soc'], min(r['battery'] for r in self.robots.values()))
        for key, value in self.counts().items():
            if key in {'moving', 'working', 'queued', 'charging'}:
                self.metrics['max_' + key] = max(self.metrics['max_' + key], value)
        self.metrics['max_active_business'] = max(self.metrics['max_active_business'],
            sum(r['stage'] in {'to_source','to_target','picking','placing','to_handoff','handoff_receive'}
                for r in self.robots.values()))
        if self.metrics['min_soc'] < self.config.emergency_reserve_soc - 1e-6:
            raise AssertionError('v3 hard emergency SOC reserve violated')
        if not 0 <= self.inside <= len(self.slots) or any(not 0 <= n <= 6 for n in self.ports.values()):
            raise AssertionError('v3 capacity invariant violated')
        if sum(self.counts()[k] for k in ['moving', 'handling', 'queued', 'charging', 'standby']) != 100:
            raise AssertionError('v3 robot accounting mismatch')
        if self.elapsed % 60 == 0:
            self.reservations.prune(self.elapsed)
            self.assert_inventory()
            self._diagnostics()

    def counts(self):
        c = Counter(r['stage'] for r in self.robots.values())
        moving = sum(c[k] for k in ['to_source', 'to_target', 'to_charge', 'returning', 'to_handoff', 'yielding'])
        handling = sum(c[k] for k in ['picking', 'placing', 'handoff', 'handoff_receive', 'handoff_give'])
        charging, standby = c['charging'], c['idle']
        return {'moving': moving, 'handling': handling, 'charging': charging, 'standby': standby,
                'queued': len(self.robots) - moving - handling - charging - standby,
                'working': sum(bool(j.get('task_id')) for j in self.jobs.values())}

    def checkpoint(self):
        state = super().checkpoint()
        state['scheduler_v3'] = {'failed_pairs': [[i, t, expiry] for (i, t), expiry in self.failed_pairs.items()],
            'blocked': deepcopy(self.blocked), 'notices': sorted(self.notices),
            'faults': {str(k): deepcopy(v) for k, v in self.faults.items()},
            'assists': {str(k): deepcopy(v) for k, v in self.assists.items()},
            'dynamic_locations': deepcopy(self.dynamic_locations), 'recovery_sequence': self.recovery_sequence}
        state['scheduler_v3']['progress'] = self.progress.export_state()
        state['scheduler_v3']['energy_unresolved'] = deepcopy(self.energy_unresolved)
        return state

    def restore(self, state):
        extra = state['scheduler_v3']
        self.dynamic_locations = deepcopy(extra['dynamic_locations'])
        for key, value in self.dynamic_locations.items():
            self.nav.locations[key] = deepcopy(value['point'])
        super().restore(state)
        self.failed_pairs = {(i, t): expiry for i, t, expiry in extra['failed_pairs']}
        self.blocked, self.notices = deepcopy(extra['blocked']), set(extra['notices'])
        self.faults = {int(k): deepcopy(v) for k, v in extra['faults'].items()}
        self.assists = {int(k): deepcopy(v) for k, v in extra['assists'].items()}
        self.recovery_sequence = extra['recovery_sequence']
        self.progress = ProgressMonitor.from_state(extra['progress'])
        self.energy_unresolved = deepcopy(extra.get('energy_unresolved'))
