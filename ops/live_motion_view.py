"""UI-only projections of already committed simulator jobs.

No planner, graph query, robot mutation or reconstructed destination shortcut.
The simulator replaces a job/parts list when it replans. Only execution cursors
and marker flags mutate within a committed job; the geometry cache is bounded
to the currently published fleet and is discarded when jobs change.
"""
from bisect import bisect_right
import hashlib
import json
import math

EPS = 1e-6


def _finite(value):
    return type(value) in (int, float) and math.isfinite(value)


def _same(a, b):
    return math.hypot(a[0]-b[0], a[1]-b[1]) <= EPS


def _at(part, clock):
    start, end, ax, ay, bx, by = part[:6]
    ratio = min(1., max(0., (clock-start)/(end-start)))
    return (ax+(bx-ax)*ratio, ay+(by-ay)*ratio)


def _path(points):
    """Drop stationary duplicates and forward-collinear vertices, not corners."""
    out = []
    for point in points:
        if out and _same(out[-1], point):
            continue
        while len(out) >= 2:
            a, b = out[-2:]
            u, v = (b[0]-a[0], b[1]-a[1]), (point[0]-b[0], point[1]-b[1])
            norm = math.hypot(*u)*math.hypot(*v)
            if u[0]*v[0]+u[1]*v[1] < 0 or abs(u[0]*v[1]-u[1]*v[0]) > 1e-9*max(1., norm):
                break
            out.pop()
        out.append(point)
    return [{'x': x, 'y': y} for x, y in out]


class LiveMotionView:
    def __init__(self):
        self._cache = {}

    def _plan(self, owner, job):
        if not job or not job.get('parts'):
            self._cache.pop(owner, None)
            return None
        parts = job['parts']
        try:
            first, last = parts[0], parts[-1]
            signature = (job.get('start'), job.get('task_id'), job.get('dock_id'), len(parts),
                         first['start'], first['end'], first['a']['x'], first['a']['y'],
                         first['b']['x'], first['b']['y'], last['start'], last['end'],
                         last['b']['x'], last['b']['y'])
            cached = self._cache.get(owner)
            if cached and cached[0] is job and cached[1] is parts and cached[2] == signature:
                return cached[3]
            if not _finite(job['start']):
                return None
            plan = []
            for part in parts:
                values = (part['start'], part['end'], part['a']['x'], part['a']['y'], part['b']['x'], part['b']['y'])
                if not all(_finite(n) for n in values):
                    return None
                start, end, ax, ay, bx, by = values
                if end == start and _same((ax, ay), (bx, by)):
                    continue
                if end <= start:
                    return None
                row = (job['start']+start, job['start']+end, ax, ay, bx, by, part.get('phase'),
                       bool(part.get('permanent_obstacle')))
                if plan and (abs(row[0]-plan[-1][1]) > EPS or not _same((ax, ay), plan[-1][4:6])):
                    return None
                plan.append(row)
            if not plan:
                return None
            identity = json.dumps([job.get('task_id'), job.get('dock_id'), job['start'], plan], separators=(',', ':'), allow_nan=False)
            value = (hashlib.sha256(identity.encode()).hexdigest(), tuple(plan), tuple(p[1] for p in plan))
            self._cache[owner] = (job, parts, signature, value)
            return value
        except (KeyError, TypeError, ValueError):
            self._cache.pop(owner, None)
            return None

    @staticmethod
    def _execution(plan, ends, now, actual):
        begin = now-1
        if begin < 0 or plan[0][0] > begin+EPS or plan[-1][1] < now-EPS:
            return None, None
        index = bisect_right(ends, begin)
        points = []
        cursor = begin
        heading = None
        while index < len(plan) and cursor < now-EPS:
            part = plan[index]
            if part[0] > cursor+EPS:
                return None, None
            end = min(now, part[1])
            a, b = _at(part, cursor), _at(part, end)
            if not points:
                points.append({'x': a[0], 'y': a[1], 't': begin})
            elif not _same((points[-1]['x'], points[-1]['y']), a):
                return None, None
            points.append({'x': b[0], 'y': b[1], 't': end})
            if not _same(a, b):
                heading = math.atan2(b[1]-a[1], b[0]-a[0])
            cursor, index = end, index+1
        if cursor < now-EPS or not points or not _same((points[-1]['x'], points[-1]['y']), actual):
            return None, None
        # Keep the source simulator's actual endpoint (not a rounded UI copy).
        points[-1].update(x=actual[0], y=actual[1], t=now)
        return {'from_tick': begin, 'to_tick': now, 'points': points}, heading

    @staticmethod
    def _work(robot, job, tasks, slots, now, plan):
        task = tasks.get((job or {}).get('task_id') or robot.get('task_id'))
        if task is None:
            return None
        slot = task.get('slot_id')
        if slot not in slots:
            slot = next((task.get(k) for k in ('source_id', 'target_id') if task.get(k) in slots), None)
        phase = robot.get('stage')
        value = {'task_id': task.get('task_id'), 'task_type': task.get('task_type'), 'goods_id': task.get('goods_id'),
                 'status': task.get('status'), 'source_id': task.get('source_id'),
                 'target_id': task.get('target_id'), 'slot_id': slot, 'phase': phase,
                 'phase_progress': None, 'action_elapsed': None, 'action_duration': None,
                 'action_start': None, 'action_end': None}
        if not plan:
            return value
        matching = [p for p in plan if p[6] == phase]
        moving = [p for p in matching if not _same(p[2:4], p[4:6])]
        if moving:
            total = sum(math.hypot(p[4]-p[2], p[5]-p[3]) for p in moving)
            done = sum(math.hypot(p[4]-p[2], p[5]-p[3])*min(1., max(0., (now-p[0])/(p[1]-p[0]))) for p in moving)
            value['phase_progress'] = min(100., max(0., 100.*done/total))
        else:
            current = next((p for p in matching if p[0] < now <= p[1]+EPS), None)
            if current and not current[7] and _same(current[2:4], current[4:6]):
                elapsed, duration = min(current[1]-current[0], max(0., now-current[0])), current[1]-current[0]
                value.update(action_start=current[0], action_end=current[1], action_elapsed=elapsed,
                             action_duration=duration, phase_progress=100.*elapsed/duration)
        return value

    def annotate(self, robots, jobs, now, *, tasks=None, slots=None):
        """Mutate only caller-owned UI snapshot dictionaries, never sim state."""
        tasks, slots = tasks or {}, slots or {}
        owners = set()
        for robot in robots:
            owner = robot['robot_id']
            owners.add(owner)
            job = jobs.get(owner)
            compiled = self._plan(owner, job)
            actual = (robot['x'], robot['y'])
            motion = {'tick': now, 'plan_id': None, 'path': [{'x': actual[0], 'y': actual[1]}],
                      'executed': None, 'heading': None}
            if compiled:
                identity, plan, ends = compiled
                motion['plan_id'] = identity
                index = bisect_right(ends, now)
                if index < len(plan) and plan[index][0] <= now+EPS and _same(_at(plan[index], now), actual):
                    motion['path'] = _path([actual]+[p[4:6] for p in plan[index:]])
                motion['executed'], motion['heading'] = self._execution(plan, ends, now, actual)
            else:
                plan = None
            robot['motion'] = motion
            robot['work'] = self._work(robot, job, tasks, slots, now, plan)
        for owner in set(self._cache)-owners:
            del self._cache[owner]
