"""Geometry and persistent body occupancy for the wide knowledge-test layout.

Empty charging hardware is not an obstacle. Every actual robot, including an
idle or fully charged robot, must have an explicit reservation. The runtime
must validate a terminal tail before committing a plan; this module never
automatically invents an unvalidated tail or removes a parked body.
"""
from collections import defaultdict
from copy import copy, deepcopy
import math

from ..day1.nav import Navigator, Reservations, hold
from ..scheduling_v2.motion import MotionTable as TrajectoryTable, Roadmap
from ..scheduling_v3.obstacles import MotionTable as PersistentTable


OCCUPANCY_END = 10**12  # Far beyond the enforced seven-day input contract.


def _number(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)


def _point(point):
    if not isinstance(point, dict) or not all(_number(point.get(axis)) for axis in ('x', 'y')):
        raise ValueError('invalid occupancy point')
    return {'x': point['x'], 'y': point['y']}


def idle_guard(point, start=0, until=OCCUPANCY_END, *, phase='idle'):
    """Explicit stationary physical body; not a second P/C resource."""
    point = _point(point)
    if not _number(start) or not _number(until) or until <= start:
        raise ValueError('invalid occupancy interval')
    return {**hold(point, phase, start, until-start), 'permanent_obstacle': True,
            'terminal_occupancy': True}


def terminal_tail(parts, point=None, until=OCCUPANCY_END):
    """Return copied absolute parts plus their explicitly validated idle tail.

    Do not execute the returned terminal guard as a trillion-second business
    job: store ``execution_parts(result)`` in the execution state while keeping
    the entire result in the reservation table/transaction. Existing persistent
    fault guards are retained unchanged rather than given another tail.
    """
    result = deepcopy(list(parts))
    if not result:
        raise ValueError('terminal tail needs an existing execution plan')
    last = result[-1]
    target = _point(last['b'])
    if point is not None:
        expected = _point(point)
        if math.hypot(expected['x']-target['x'], expected['y']-target['y']) > 1e-6:
            raise ValueError('terminal point would teleport the robot')
    if any(p.get('terminal_occupancy') for p in result[:-1]):
        raise ValueError('terminal occupancy must be the last part')
    if last.get('terminal_occupancy'):
        return result
    if last.get('permanent_obstacle'):
        return result
    result.append(idle_guard(target, last['end'], until))
    return result


def execution_parts(parts):
    """Copy real execution parts without a trailing idle occupancy guard.

    A permanent *fault_wait* is execution state and is not stripped. Only the
    specifically labelled terminal-occupancy guard is removed.
    """
    result = deepcopy(list(parts))
    if any(p.get('terminal_occupancy') for p in result[:-1]):
        raise ValueError('terminal occupancy must be the last part')
    if result and result[-1].get('terminal_occupancy'):
        result.pop()
    return result


class WideNavigator(Navigator):
    """Existing static navigator interface with scene-sized raster geometry."""
    def __init__(self, scene, slots):
        spec = scene['spec']
        self.scene, self.slots = scene, slots
        self.radius = float(spec.get('robotRadius', .55))
        self.step = float(spec.get('gridStep', .2))
        self.width, self.depth = float(spec['width']), float(spec['depth'])
        if not all(math.isfinite(x) and x > 0 for x in (self.radius, self.step, self.width, self.depth)):
            raise ValueError('invalid world geometry')
        self.nx = math.ceil(self.width/self.step-1e-9) + 1
        self.ny = math.ceil(self.depth/self.step-1e-9) + 1
        self.fixed, seen = [], set()
        for obstacle in scene.get('racks', []) + scene.get('buffers', []) + scene.get('obstacles', []):
            if obstacle.get('staticCollision') is False:
                continue
            try:
                box = tuple(float(value) for value in (obstacle['x'], obstacle['y'],
                            obstacle['x']+obstacle['w'], obstacle['y']+obstacle['d']))
            except (KeyError, TypeError, ValueError) as error:
                raise ValueError('invalid fixed obstacle') from error
            if not all(math.isfinite(v) for v in box) or box[2] <= box[0] or box[3] <= box[1]:
                raise ValueError('invalid fixed obstacle extent')
            if box not in seen:
                self.fixed.append(box)
                seen.add(box)
        # There are no static home/dock rectangles in the wide model. Keeping
        # those rectangles would double-block the co-located P/C aliases.
        self.private = {}
        self.locations = {key: {'x': value['x'], 'y': value['y']} for key, value in slots.items()}
        self.locations.update({p['id']: dict(p['point']) for p in scene['homes'] + scene['docks']})
        self.locations.update({p['id']: {'x': p['x'], 'y': p['y']} for p in scene['stations']})
        self.aliases = {p['id']: p['id'] for p in scene['docks']}
        self.aliases.update({p['id']: p.get('alias_of', p['id']) for p in scene['homes']})
        self.blocked = bytearray(self.nx*self.ny)
        self.owners = defaultdict(set)
        self.geometry_cache = {}
        for box in self.fixed:
            self._raster(box, None)

    def canonical_location(self, location):
        return self.aliases.get(location, location)

    def clear(self, a, b, allowed):
        try:
            a, b = _point(a), _point(b)
        except ValueError:
            return False
        for p in (a, b):
            if not (self.radius-1e-7 <= p['x'] <= self.width-self.radius+1e-7 and
                    self.radius-1e-7 <= p['y'] <= self.depth-self.radius+1e-7):
                return False
        return super().clear(a, b, allowed)


class WideRoadmap(Roadmap):
    """Existing SIPP roadmap, one actual portal per canonical physical bay."""
    def __init__(self, navigator):
        view = copy(navigator)
        view.scene = {**navigator.scene, 'homes': [], 'docks': navigator.scene['docks']}
        super().__init__(view)
        self.base = navigator
        self.locations = navigator.locations
        for home in navigator.scene['homes']:
            canonical = navigator.canonical_location(home['id'])
            if canonical not in self.portals:
                raise ValueError('home alias has no canonical charging portal: '+home['id'])
            self.portals[home['id']] = self.portals[canonical]


class WideMotionTable(PersistentTable):
    """Persistent bodies indexed by space, never one full fixed scan per node.

    Dynamic spans retain the v2 exact continuous-motion kernel/time index.
    Stationary entities use the same 2.2m spatial grid and 1.1m query padding;
    the exact kernel still decides clearance. A failed external transaction
    deep-copies this entire structure and cannot partially change the live one.
    """
    def __init__(self):
        self.fixed_grid = defaultdict(dict)
        self.fixed_cells = {}
        super().__init__()

    def _erase(self, owner):
        for cell, index in self.fixed_cells.pop(owner, ()):
            self.fixed_grid[cell].pop((owner, index), None)
            if not self.fixed_grid[cell]:
                del self.fixed_grid[cell]
        super()._erase(owner)

    def _index_owner(self, owner):
        references = []
        for index, part in enumerate(self.fixed.get(owner, [])):
            for cell in self.cells(part['a'], part['b'], 0):
                self.fixed_grid[cell][owner, index] = (owner, part)
                references.append((cell, index))
        if references:
            self.fixed_cells[owner] = tuple(references)

    def reserve(self, parts, offset=0, owner=None):
        parts = list(parts)
        if not _number(offset):
            raise ValueError('invalid reservation offset')
        # Validate before touching any old reservation/index, including direct
        # idle guard installation outside a try_replace_many transaction.
        for part in parts:
            _point(part['a'])
            _point(part['b'])
            if not _number(part['start']) or not _number(part['end']) or part['end'] <= part['start']:
                raise ValueError('invalid reservation interval')
        super().reserve(parts, offset, owner)
        self._index_owner(owner)

    def nearby(self, a, b, ignore, start=None, end=None):
        if start is not None and end-start > 3600:
            result = TrajectoryTable.nearby(self, a, b, ignore)
        else:
            result = TrajectoryTable.nearby(self, a, b, ignore, start, end)
        fixed = {}
        for cell in self.cells(a, b):
            fixed.update(self.fixed_grid.get(cell, {}))
        result.extend((owner, part) for owner, part in fixed.values() if owner != ignore and
                      (start is None or part['end'] >= start and part['start'] <= end))
        return result

    def prune(self, now):
        super().prune(now)
        # Parent prune retains persistent entities while rebuilding ordinary
        # spans; reconstruct their derived spatial references after that pass.
        self.fixed_grid = defaultdict(dict)
        self.fixed_cells = {}
        for owner in self.fixed:
            self._index_owner(owner)
