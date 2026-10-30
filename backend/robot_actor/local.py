"""Robot processes, local version (design doc phase 1).

Each robot owns its state and a copy of its plan and advances one second along
that plan. The main process keeps dispatch, marker handling (inventory, handoff),
completion, progress and audits. No Ray here: the same interface is meant to be
backed by one Ray actor per robot later, so everything crossing the boundary is
copied, never shared.

Result equivalence with the sequential engine is pinned by
tests/test_dispatch_golden.py and tests/test_robot_process_split.py.
"""
from copy import deepcopy
import math

from ..scheduling_v3.sim import position

QUEUE_PHASES = {'traffic_wait', 'recovery_wait', 'fault_wait'}
PRODUCTIVE_PHASES = {'to_source', 'to_target', 'picking', 'placing'}


def plan_copy(job):
    """What a robot needs from a job to move: the timed parts and its progress."""
    return {'parts': deepcopy(job['parts']), 'start': job['start'], 'phase_index': job['phase_index'],
            'charge_started': bool(job.get('charge_started'))}


class RobotProcess:
    """One robot: its own state and plan; advances one second at a time."""

    def __init__(self, robot_id, robot, energy, charge_target_soc):
        self.robot_id = robot_id
        self.robot = dict(robot)
        self.plan = None
        self.energy, self.charge_target_soc = energy, charge_target_soc
        self._before = None

    def sync(self, robot=None, plan=None, clear_plan=False):
        """Apply the main process's changes (dispatch, markers, disturbances)."""
        if robot is not None:
            self.robot = dict(robot)
        if clear_plan:
            self.plan = None
        elif plan is not None:
            self.plan = plan

    def advance(self, now, persistent_fault):
        """One second along the plan. Same arithmetic, in the same order, as the
        sequential engine, so battery and distance floats are bit-identical."""
        robot, plan = self.robot, self.plan
        self._before = (dict(robot), None if plan is None else (plan['phase_index'], plan['charge_started']))
        out = {'robot_id': self.robot_id, 'idle': plan is None, 'marker': None, 'last_done': False,
               'charge_first': False, 'battery_before_charge': None, 'charging': False, 'distance': 0.0,
               'loaded': False, 'queue': False, 'productive': False, 'phase_index': None}
        if plan is None:
            robot['battery'] -= self.energy.rate('idle')
            robot['stage'] = 'idle'
            out['robot'] = dict(robot)
            return out
        parts, at, idx = plan['parts'], now - plan['start'], plan['phase_index']
        while idx < len(parts) and at >= parts[idx]['end']:
            idx += 1
        if idx == len(parts):
            raise AssertionError('unprocessed v3 mission completion')
        plan['phase_index'] = idx
        part = parts[idx]
        a, b = position(part, at), position(part, at + 1)
        if math.dist((robot['x'], robot['y']), (a['x'], a['y'])) > 1e-6:
            raise AssertionError('v3 motion discontinuity')
        loaded = robot['carrying']
        robot.update(x=b['x'], y=b['y'], stage=part['phase'])
        distance = math.dist((a['x'], a['y']), (b['x'], b['y']))
        robot['distance'] += distance
        out.update(distance=distance, loaded=loaded, phase_index=idx)
        if part['phase'] == 'charging':
            if not plan['charge_started']:
                plan['charge_started'] = True
                out.update(charge_first=True, battery_before_charge=robot['battery'])
            robot['battery'] = min(self.charge_target_soc, robot['battery'] + self.energy.charge_gain(robot['battery'], 1))
            robot['charge_seconds'] += 1
            out['charging'] = True
        else:
            isolated = persistent_fault and not loaded and part['phase'] == 'fault_wait'
            if not isolated:
                robot['battery'] -= self.energy.rate(part['phase'], loaded)
            if part['phase'] in QUEUE_PHASES:
                robot['queue_seconds'] += 1
                out['queue'] = True
            else:
                robot['active_seconds'] += 1
        out['productive'] = part['phase'] in PRODUCTIVE_PHASES
        if at + 1 == part['end'] and part.get('marker'):
            out['marker'] = (part['marker'], part.get('task_id'))
        out['last_done'] = at + 1 == parts[-1]['end']
        out['robot'] = dict(robot)
        return out

    def redo(self, now, robot, plan, clear_plan, persistent_fault):
        """Undo this second and advance again from the state the main process now
        holds for this robot (used when an earlier robot's marker changed it)."""
        saved_robot, saved_plan = self._before
        self.robot = saved_robot
        if self.plan is not None and saved_plan is not None:
            self.plan['phase_index'], self.plan['charge_started'] = saved_plan
        self.sync(robot=robot, plan=plan, clear_plan=clear_plan)
        return self.advance(now, persistent_fault)


class RobotProcessSplit:
    """Mixin over the V3-based engine: robots advance in RobotProcess objects.

    Per tick: the main process dispatches (unchanged), syncs what it changed to
    the robots, every robot advances one second, then the main process merges
    the results in the original robot order and handles markers, completion and
    progress exactly as before. If a marker changes a robot that comes later in
    that order (handoff: the donor), that robot redoes its second from the state
    the main process now holds, which is what the sequential engine would have
    used.
    """

    def _robot_processes(self):
        procs = getattr(self, '_procs', None)
        if procs is None or set(procs) != set(self.robots):
            procs = self._procs = {i: RobotProcess(i, r, self.energy, self.config.charge_target_soc)
                                   for i, r in self.robots.items()}
            self._mirror = {i: dict(r) for i, r in self.robots.items()}  # state each process holds
            self._sent_job = {i: None for i in self.robots}               # main job whose plan it holds
            self.split_stats = {'ticks': 0, 'robot_syncs': 0, 'plans_sent': 0, 'redo': 0}
        return procs

    def _changes_for(self, owner):
        robot, job = self.robots[owner], self.jobs.get(owner)
        robot_arg = None if self._mirror[owner] == robot else robot
        if robot_arg is not None:
            self._mirror[owner] = dict(robot)
        plan, clear = None, False
        if job is not self._sent_job[owner]:
            if job is None:
                clear = True
            else:
                plan = plan_copy(job)
                self.split_stats['plans_sent'] += 1
            self._sent_job[owner] = job
        if robot_arg is not None:
            self.split_stats['robot_syncs'] += 1
        return robot_arg, plan, clear

    def _persistent_fault(self, owner):
        return bool(self.faults.get(owner, {}).get('persistent'))

    def _advance_robots(self, now):
        procs = self._robot_processes()
        order = list(self.robots)  # the sequential engine's iteration order
        for owner in order:
            robot_arg, plan, clear = self._changes_for(owner)
            if robot_arg is not None or plan is not None or clear:
                procs[owner].sync(robot=robot_arg, plan=plan, clear_plan=clear)
        # The parallel section: every robot advances from its own state.
        results = {owner: procs[owner].advance(now, self._persistent_fault(owner)) for owner in order}
        position_of = {owner: i for i, owner in enumerate(order)}
        redo = set()
        for owner in order:
            robot = self.robots[owner]
            if owner in redo:
                _, plan, clear = self._changes_for(owner)
                results[owner] = procs[owner].redo(now, robot, plan, clear, self._persistent_fault(owner))
                self.split_stats['redo'] += 1
            r = results[owner]
            robot.clear()
            robot.update(r['robot'])
            self._mirror[owner] = dict(r['robot'])
            if r['idle']:
                continue
            job = self.jobs[owner]
            job['phase_index'] = r['phase_index']
            self.metrics['loaded_distance_m' if r['loaded'] else 'empty_distance_m'] += r['distance']
            if r['charging']:
                if r['charge_first']:
                    job['charge_started'] = True
                    self.event('charge_started', owner, dock_id=job['dock_id'], battery=r['battery_before_charge'])
                self.docks[job['dock_id']]['charge_seconds'] += 1
            elif r['queue']:
                self.metrics['traffic_wait_seconds'] += 1
            if r['productive']:
                self.metrics['productive_robot_seconds'] += 1
            if r['marker']:
                kind, task_id = r['marker']
                later = order[position_of[owner] + 1:]
                robots_before = {i: dict(self.robots[i]) for i in later}
                jobs_before = {i: self.jobs.get(i) for i in later}
                self.elapsed = self.tick = now + 1
                self.marker(robot, job, kind, task_id)
                self.elapsed = self.tick = now
                redo.update(i for i in later
                            if self.robots[i] != robots_before[i] or self.jobs.get(i) is not jobs_before[i])
            if self.jobs.get(owner) is job and r['last_done']:
                robot.update(stage='idle', task_id=None, dock_id=None, location_id=robot['home_id'])
                del self.jobs[owner]
                self.dispatch_dirty = True
            self.progress.record_progress(owner, now + 1, point=self._point(robot),
                                          task_id=robot.get('task_id'), token=robot['completed'])
        self.split_stats['ticks'] += 1

    def restore(self, state):
        super().restore(state)
        self._procs = None  # rebuild robot processes from the restored state
