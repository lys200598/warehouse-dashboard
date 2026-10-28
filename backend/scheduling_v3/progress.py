"""Temporal wait dependencies and progress diagnostics, not a deadlock solver.

A circular dependency is only a candidate. Escalation additionally requires
fresh repeated observations, persistent unchanged dependencies, no measured
progress and no still-valid promised release. Safety and recovery decisions
remain with the coordinator and the atomic reservation transaction.
"""
from copy import deepcopy
import math


def _ordered(values):
    return sorted(values, key=lambda value: (type(value).__name__, repr(value)))


class ProgressMonitor:
    def __init__(self, stall_seconds=60, dependency_ttl=30, release_grace=2,
                 movement_epsilon=1e-5):
        if stall_seconds <= 0 or dependency_ttl <= 0 or release_grace < 0 or movement_epsilon < 0:
            raise ValueError('invalid progress monitor parameters')
        self.stall_seconds = stall_seconds
        self.dependency_ttl = dependency_ttl
        self.release_grace = release_grace
        self.movement_epsilon = movement_epsilon
        self.robots = {}
        self.blocks = {}

    @staticmethod
    def _time(now):
        if not isinstance(now, (int, float)) or not math.isfinite(now):
            raise ValueError('invalid observation time')

    def record_progress(self, owner, now, point=None, task_id=None, token=None):
        """Observe actual position or a business marker, not planned activity.

        An explicit call with neither point nor token denotes an actual marker
        supplied by the coordinator. Repeated unchanged positions/tokens do
        not reset the no-progress timer. Merely assigning a new task is not
        physical or business progress.
        """
        self._time(now)
        if point is not None:
            if not all(isinstance(point.get(k), (int, float)) and math.isfinite(point[k]) for k in ('x', 'y')):
                raise ValueError('invalid observed point')
            point = {'x': point['x'], 'y': point['y']}
        previous = self.robots.get(owner)
        if previous is not None and now < previous['observed_at']:
            raise ValueError('observation time moved backwards')
        changed = previous is None or (point is None and token is None)
        if previous is not None:
            if point is not None and previous['point'] is not None:
                changed |= math.hypot(point['x'] - previous['point']['x'], point['y'] - previous['point']['y']) > self.movement_epsilon
            if token is not None and previous['token'] is not None:
                changed |= token != previous['token']
        self.robots[owner] = {
            'observed_at': now, 'last_progress_at': now if changed else previous['last_progress_at'],
            'point': point if point is not None else previous.get('point') if previous else None,
            'token': deepcopy(token) if token is not None else previous.get('token') if previous else None,
            'task_id': task_id,
        }
        if changed:
            self.blocks.pop(owner, None)
        return bool(changed)

    def record_block(self, owner, blockers, now, reason='traffic', task_id=None, expected_release=None):
        """Record a current failed attempt, including a promised release time.

        ``expected_release`` is an absolute simulated time, not a duration. It
        suppresses escalation through that time plus the configured grace.
        Resubmitting the same observation does not extend that promise.
        """
        self._time(now)
        if expected_release is not None:
            self._time(expected_release)
        blockers = _ordered(set(blockers) - {owner})
        if owner not in self.robots:
            self.record_progress(owner, now)
        old = self.blocks.get(owner)
        if old is not None and now < old['last_seen']:
            raise ValueError('block observation time moved backwards')
        same = old is not None and old['blockers'] == blockers and old['reason'] == reason and old['task_id'] == task_id
        # A dependency that disappeared longer than TTL is a new observation.
        same = same and now - old['last_seen'] <= self.dependency_ttl
        if same and old['expected_release'] is not None:
            expected_release = old['expected_release'] if expected_release is None else min(expected_release, old['expected_release'])
        self.blocks[owner] = {
            'blockers': blockers, 'reason': reason, 'task_id': task_id,
            'since': old['since'] if same else now, 'last_seen': now,
            'expected_release': expected_release,
        }

    def clear_block(self, owner):
        self.blocks.pop(owner, None)

    @staticmethod
    def _components(graph):
        index, low, stack, on_stack, result = {}, {}, [], set(), []

        def visit(node):
            index[node] = low[node] = len(index)
            stack.append(node)
            on_stack.add(node)
            for nxt in graph.get(node, []):
                if nxt not in index:
                    visit(nxt)
                    low[node] = min(low[node], low[nxt])
                elif nxt in on_stack:
                    low[node] = min(low[node], index[nxt])
            if low[node] == index[node]:
                component = []
                while True:
                    current = stack.pop()
                    on_stack.remove(current)
                    component.append(current)
                    if current == node:
                        break
                if len(component) > 1:
                    result.append(_ordered(component))

        for node in _ordered(set(graph) | {v for values in graph.values() for v in values}):
            if node not in index:
                visit(node)
        return sorted(result, key=repr)

    def snapshot(self, now, tasks=None):
        self._time(now)
        active = {owner: block for owner, block in self.blocks.items()
                  if 0 <= now - block['last_seen'] <= self.dependency_ttl}
        stalled = []
        for owner, block in active.items():
            last_progress = self.robots[owner]['last_progress_at']
            release = block['expected_release']
            if now - block['since'] >= self.stall_seconds and now - last_progress >= self.stall_seconds and \
                    (release is None or now > release + self.release_grace):
                stalled.append(owner)
        candidates = self._components({owner: block['blockers'] for owner, block in active.items()})
        persistent = [members for members in candidates if all(owner in stalled for owner in members)]
        task_ages = []
        rows = tasks.values() if isinstance(tasks, dict) else tasks or []
        for task in rows:
            if task.get('status') in {'completed', 'scheduled', 'cancelled'}:
                continue
            released = task.get('release_second', task.get('released_at'))
            if released is None or released > now:
                continue
            task_ages.append({'task_id': task['task_id'], 'status': task.get('status'),
                              'age_seconds': now - released,
                              'ready_age_seconds': now - task.get('ready_at', released),
                              'overdue': task.get('due_second', math.inf) < now})
        task_ages.sort(key=lambda row: (-row['age_seconds'], row['task_id']))
        return {
            'sim_time': now, 'dependencies': [{'owner': owner, **deepcopy(active[owner])} for owner in _ordered(active)],
            'stalled_owners': _ordered(stalled), 'cycle_candidates': candidates,
            'persistent_cycles': persistent, 'unfinished_tasks': task_ages,
            'max_task_age_seconds': task_ages[0]['age_seconds'] if task_ages else 0,
            'overdue_tasks': sum(row['overdue'] for row in task_ages),
        }

    def observe(self, now, robots=None, tasks=None):
        if robots is not None:
            rows = robots.values() if isinstance(robots, dict) else robots
            for robot in rows:
                # Stage changes by themselves can oscillate without motion.
                token = [robot.get('completed', 0), robot.get('cargo_id')]
                self.record_progress(robot['robot_id'], now, robot, robot.get('task_id'), token)
        return self.snapshot(now, tasks)

    def export_state(self):
        return {'config': {'stall_seconds': self.stall_seconds, 'dependency_ttl': self.dependency_ttl,
                           'release_grace': self.release_grace, 'movement_epsilon': self.movement_epsilon},
                'robots': [{'owner': owner, **deepcopy(value)} for owner, value in self.robots.items()],
                'blocks': [{'owner': owner, **deepcopy(value)} for owner, value in self.blocks.items()]}

    @classmethod
    def from_state(cls, state):
        monitor = cls(**state['config'])
        monitor.robots = {row['owner']: {k: deepcopy(v) for k, v in row.items() if k != 'owner'} for row in state['robots']}
        monitor.blocks = {row['owner']: {k: deepcopy(v) for k, v in row.items() if k != 'owner'} for row in state['blocks']}
        return monitor
