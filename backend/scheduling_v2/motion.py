"""Own roadmap + safe-interval search, retaining continuous body collision tests.

Algorithmic reference: Phillips & Likhachev, SIPP, ICRA 2011. This is NOT their
source code, not Open-RMF/openTCS, and not the USC RHCR implementation. Priority
planning is bounded and incomplete for the joint fleet; failures retain the
previously committed safe return trajectory. See docs/scheduler_v2_sources.md.
"""
from collections import defaultdict
from functools import lru_cache
import heapq
import math

from ..day1.nav import position, hold, Reservations
from ..traffic import swept_distance


RADIUS_SUM = 1.1


def continuous_clear(a, b, start, end, other):
    """Scalar relative-motion minimum distance; no sampled collision approximation."""
    left, right = max(start, other['start']), min(end, other['end'])
    if right < left:
        return True
    duration = end - start
    vx = (b['x'] - a['x']) / duration if duration else 0.
    vy = (b['y'] - a['y']) / duration if duration else 0.
    od = other['end'] - other['start']
    ox = (other['b']['x'] - other['a']['x']) / od
    oy = (other['b']['y'] - other['a']['y']) / od
    rx = a['x'] + vx * (left - start) - other['a']['x'] - ox * (left - other['start'])
    ry = a['y'] + vy * (left - start) - other['a']['y'] - oy * (left - other['start'])
    dx, dy = vx - ox, vy - oy
    speed2 = dx * dx + dy * dy
    closest = max(0., min(right - left, -(rx * dx + ry * dy) / speed2)) if speed2 > 1e-30 else 0.
    return (rx + dx * closest) ** 2 + (ry + dy * closest) ** 2 >= (RADIUS_SUM - 1e-7) ** 2


def coalesce(parts):
    """Merge only adjacent equal-velocity reservation spans, not execution events."""
    result = []
    for p in parts:
        if result:
            old = result[-1]
            dt, old_dt = p['end'] - p['start'], old['end'] - old['start']
            same = old['end'] == p['start'] and old.get('phase') == p.get('phase') and old['b'] == p['a']
            same = same and all(abs((p['b'][axis] - p['a'][axis]) / dt -
                                   (old['b'][axis] - old['a'][axis]) / old_dt) < 1e-12 for axis in ['x', 'y'])
            if same:
                old.update(b=p['b'], end=p['end'])
                continue
        result.append(dict(p))
    return result


class MotionTable:
    """Absolute-time, per-owner reservations. Replacement is committed atomically."""
    def cells(self, a, b, padding=1.1):
        return Reservations.cells(a, b, padding)

    def __init__(self):
        self.owners = {}
        self.grid = defaultdict(dict)
        self.timegrid = defaultdict(dict)
        self.time_bucket = 8
        self.serial = 0
        self.checks = self.delayed = self.max_delay = 0

    def _erase(self, owner):
        for token, part, cells, timecells in self.owners.pop(owner, []):
            for cell in cells:
                self.grid[cell].pop(token, None)
                if not self.grid[cell]:
                    del self.grid[cell]
            for cell in timecells:
                self.timegrid[cell].pop(token, None)
                if not self.timegrid[cell]:
                    del self.timegrid[cell]

    def reserve(self, parts, offset=0, owner=None):
        if owner is None:
            raise ValueError('owner required for replaceable traffic reservations')
        absolute = [{**p, 'start': p['start'] + offset, 'end': p['end'] + offset} for p in parts]
        if any(p['end'] <= p['start'] for p in absolute):
            raise ValueError('nonpositive reservation duration')
        absolute = coalesce(absolute)
        self._erase(owner)
        entries = []
        for part in absolute:
            self.serial += 1
            cells = tuple(self.cells(part['a'], part['b'], 0))
            entry = (owner, part)
            for cell in cells:
                self.grid[cell][self.serial] = entry
            timecells = []
            for bucket in range(math.floor(part['start'] / self.time_bucket), math.floor(part['end'] / self.time_bucket) + 1):
                left, right = max(part['start'], bucket * self.time_bucket), min(part['end'], (bucket + 1) * self.time_bucket)
                # Index the geometry within this time bucket, not its whole long corridor.
                a, b = position(part, left), position(part, right)
                for x, y in self.cells(a, b, 0):
                    cell = (x, y, bucket)
                    self.timegrid[cell][self.serial] = entry
                    timecells.append(cell)
            entries.append((self.serial, part, cells, timecells))
        self.owners[owner] = entries

    def nearby(self, a, b, ignore, start=None, end=None):
        found = {}
        for cell in self.cells(a, b):
            if start is None:
                found.update(self.grid.get(cell, {}))
            else:
                for bucket in range(math.floor(start / self.time_bucket), math.floor(end / self.time_bucket) + 1):
                    found.update(self.timegrid.get((*cell, bucket), {}))
        return [(owner, part) for owner, part in found.values() if owner != ignore]

    def clear_motion(self, a, b, start, end, ignore, conflicts=None):
        for owner, other in self.nearby(a, b, ignore, start, end):
            self.checks += 1
            if not continuous_clear(a, b, start, end, other):
                if conflicts is not None:
                    conflicts.add(owner)
                return False
        return True

    def intervals(self, point, start, end, ignore):
        """Conservative integer safe intervals, including stationary occupancy."""
        blocked = []
        for _, p in self.nearby(point, point, ignore):
            left, right = max(start, p['start']), min(end, p['end'])
            if right < left:
                continue
            self.checks += 1
            duration = p['end'] - p['start']
            vx = (p['b']['x'] - p['a']['x']) / duration
            vy = (p['b']['y'] - p['a']['y']) / duration
            dx, dy = p['a']['x'] - point['x'], p['a']['y'] - point['y']
            speed2 = vx * vx + vy * vy
            if speed2 < 1e-15:
                if dx * dx + dy * dy < (RADIUS_SUM - 1e-7) ** 2:
                    blocked.append((left, right))
                continue
            middle = -(dx * vx + dy * vy) / speed2
            closest2 = dx * dx + dy * dy - middle * middle * speed2
            remaining = (RADIUS_SUM - 1e-7) ** 2 - closest2
            if remaining <= 0:
                continue
            half = math.sqrt(remaining / speed2)
            lo, hi = max(left, p['start'] + middle - half), min(right, p['start'] + middle + half)
            if hi >= lo:
                blocked.append((lo, hi))
        safe, cursor = [], start
        for lo, hi in sorted(blocked):
            last = min(end, math.ceil(lo - 1e-8) - 1)
            if cursor <= last:
                safe.append((cursor, last))
            cursor = max(cursor, math.floor(hi + 1e-8) + 1)
        if cursor <= end:
            safe.append((cursor, end))
        return safe

    def prune(self, now):
        for owner in list(self.owners):
            entries = self.owners[owner]
            if not any(p['end'] < now - 1 for _, p, _, _ in entries):
                continue
            keep = [p for _, p, _, _ in entries if p['end'] >= now - 1]
            self.reserve(keep, owner=owner)


class Roadmap:
    """0.8m lane skeleton over the existing 0.2m raster, exact endpoint links.

    It is a planning discretization, NOT a larger robot clearance. Every edge
    and connector is checked against the original static obstacle geometry.
    """
    def __init__(self, navigator):
        self.base = navigator
        self.locations = navigator.locations
        self.nodes, self.edges, self.connections_cache = {}, {}, {}
        self.portals = {}
        self.stride = 4
        for y in range(4, navigator.ny - 3, self.stride):
            for x in range(4, navigator.nx - 3, self.stride):
                key = y * navigator.nx + x
                if not navigator.blocked[key] and not navigator.owners.get(key):
                    self.nodes[key] = {'x': x * navigator.step, 'y': y * navigator.step}
        for key, a in self.nodes.items():
            links = []
            for other in (key - self.stride, key + self.stride,
                          key - navigator.nx * self.stride, key + navigator.nx * self.stride):
                if other in self.nodes and navigator.clear(a, self.nodes[other], set()):
                    links.append(other)
            self.edges[key] = links
        # Exact private-bay exits are essential: a coarse lattice alone misses
        # the narrow straight departure corridor of some real parking bays.
        for index, bay in enumerate(navigator.scene['homes'] + navigator.scene['docks']):
            portal = ({'x': bay['point']['x'], 'y': bay['approachY']}
                      if bay['kind'] == 'home' else dict(bay['gate']))
            if not navigator.clear(bay['point'], portal, {bay['id']}):
                raise ValueError('invalid private bay exit: ' + bay['id'])
            node = navigator.nx * navigator.ny + index
            nearby = sorted(self.nodes, key=lambda k: (self.nodes[k]['x'] - portal['x']) ** 2 +
                            (self.nodes[k]['y'] - portal['y']) ** 2)
            links = []
            for k in nearby[:120]:
                if navigator.clear(portal, self.nodes[k], set()):
                    links.append(k)
                    if len(links) == 4:
                        break
            if not links:
                raise ValueError('private bay has no public-road connector: ' + bay['id'])
            self.nodes[node], self.edges[node], self.portals[bay['id']] = portal, links, node
            for k in links:
                self.edges[k].append(node)

    def connections(self, location):
        if location in self.portals:
            return [self.portals[location]]
        point = self.locations[location]
        signature = (point['x'], point['y'], location if location in self.base.private else '')
        if signature in self.connections_cache:
            return self.connections_cache[signature]
        candidates = sorted(self.nodes, key=lambda k: (self.nodes[k]['x'] - point['x']) ** 2 + (self.nodes[k]['y'] - point['y']) ** 2)
        links = []
        for k in candidates[:120]:
            if self.base.clear(point, self.nodes[k], {location}):
                links.append(k)
                if len(links) == 4:
                    break
        self.connections_cache[signature] = links
        return links

    @lru_cache(maxsize=128)
    def distances(self, target, speed):
        """Exact static lower bound; avoids repeatedly exploring equal-cost grid paths."""
        return self._compute_distances(target, speed)

    def _compute_distances(self, target, speed):
        """Uncached static computation; layout-specific caches keep this kernel."""
        b = self.locations[target]
        distance, queue = {}, []
        for node in self.connections(target):
            p = self.nodes[node]
            cost = max(1, math.ceil((math.hypot(p['x'] - b['x'], p['y'] - b['y']) - 1e-9) / speed))
            distance[node] = cost
            heapq.heappush(queue, (cost, node))
        while queue:
            cost, node = heapq.heappop(queue)
            if cost != distance[node]:
                continue
            for neighbor, more in self.weighted_edges(speed)[node]:
                proposed = cost + more
                if proposed < distance.get(neighbor, math.inf):
                    distance[neighbor] = proposed
                    heapq.heappush(queue, (proposed, neighbor))
        return distance

    @lru_cache(maxsize=4)
    def weighted_edges(self, speed):
        return {node: [(other, max(1, math.ceil((math.hypot(self.nodes[node]['x'] - self.nodes[other]['x'],
                                                          self.nodes[node]['y'] - self.nodes[other]['y']) - 1e-9) / speed)))
                       for other in neighbors] for node, neighbors in self.edges.items()}

    @lru_cache(maxsize=30000)
    def travel_time(self, source, target, speed):
        """Static graph arrival estimate for assignment, not a dynamic guarantee."""
        a, b = self.locations[source], self.locations[target]
        remaining = self.distances(target, speed)
        choices = [max(1, math.ceil((math.hypot(a['x'] - self.nodes[k]['x'], a['y'] - self.nodes[k]['y']) - 1e-9) / speed)) +
                   remaining.get(k, math.inf) for k in self.connections(source)]
        if self.base.clear(a, b, {source, target}):
            choices.append(max(1, math.ceil((math.hypot(a['x'] - b['x'], a['y'] - b['y']) - 1e-9) / speed)))
        return min(choices, default=math.inf)


class SafeIntervalPlanner:
    def __init__(self, roadmap, table, speed=.8, horizon=900, max_expansions=12000):
        self.map, self.table = roadmap, table
        self.speed, self.horizon, self.max_expansions = speed, horizon, max_expansions
        self.expansions = self.calls = self.failures = 0
        self.last_conflicts = set()
        self.last_failure = None

    def plan(self, source, target, now, owner, phase, dwell=0, action=None, marker=None, earliest_goal=None):
        self.calls += 1
        self.last_conflicts = set()
        self.last_failure = None
        travel_end = now + self.horizon
        # A charging service can last longer than the *travel* planning horizon.
        # Its complete occupancy still has to be checked/reserved.
        end = travel_end + dwell
        points = self.map.nodes
        a, b = self.map.locations[source], self.map.locations[target]
        endpoints = {-1: a, -2: b}
        start_links, goal_links = self.map.connections(source), set(self.map.connections(target))
        direct = self.map.base.clear(a, b, {source, target})
        static_distance = self.map.distances(target, self.speed)
        def point(k):
            return endpoints[k] if k < 0 else points[k]
        def neighbors(k):
            if k == -1:
                return start_links + ([-2] if direct else [])
            if k == -2:
                return []
            return self.map.edges[k] + ([-2] if k in goal_links else [])
        cache = {}
        def intervals(k):
            if k not in cache:
                cache[k] = self.table.intervals(point(k), now, end, owner)
            return cache[k]
        goal_earliest = max(now, earliest_goal if earliest_goal is not None else now)
        if not any(max(lo, goal_earliest) + dwell <= hi and max(lo, goal_earliest) <= travel_end
                   for lo, hi in intervals(-2)):
            self.failures += 1
            self.last_failure = 'goal_hold_unavailable'
            return None
        first = next((i for i, interval in enumerate(intervals(-1)) if interval[0] <= now <= interval[1]), None)
        if first is None:
            self.failures += 1
            self.last_failure = 'source_reserved'
            return None
        root = (-1, first)
        best, parents = {root: now}, {}
        serial = 0
        def heuristic(k):
            if k == -2:
                return 0
            if k >= 0:
                return static_distance.get(k, math.inf)
            options = [max(1, math.ceil((math.hypot(a['x'] - points[n]['x'], a['y'] - points[n]['y']) - 1e-9) / self.speed)) +
                       static_distance.get(n, math.inf) for n in start_links]
            if direct:
                options.append(max(1, math.ceil((math.hypot(a['x'] - b['x'], a['y'] - b['y']) - 1e-9) / self.speed)))
            return min(options, default=math.inf)
        queue = [(now + heuristic(-1), -now, serial, root)]
        if not math.isfinite(heuristic(-1)) or now + heuristic(-1) > travel_end:
            self.failures += 1
            self.last_failure = 'static_disconnected_or_horizon'
            return None
        expanded = 0
        while queue and expanded < self.max_expansions:
            _, negative_arrival, _, state = heapq.heappop(queue)
            arrival = -negative_arrival
            if best.get(state) != arrival:
                continue
            node, interval_index = state
            available = intervals(node)[interval_index]
            expanded += 1
            self.expansions += 1
            if node == -2 and arrival + dwell <= available[1] and (earliest_goal is None or arrival >= earliest_goal):
                chain, at = [], state
                while at != root:
                    previous, departed = parents[at]
                    chain.append((previous, at, departed, best[at]))
                    at = previous
                parts = []
                for previous, current, departed, reached in reversed(chain):
                    pa, pb = point(previous[0]), point(current[0])
                    if departed > best[previous]:
                        parts.append(hold(pa, 'traffic_wait', best[previous], departed - best[previous]))
                    if reached > departed:
                        parts.append({'a': pa, 'b': pb, 'start': departed, 'end': reached, 'phase': phase})
                if dwell:
                    parts.append(hold(b, action, arrival, dwell, marker))
                return parts, arrival + dwell
            for nxt in neighbors(node):
                pa, pb = point(node), point(nxt)
                seconds = max(1, math.ceil((math.hypot(pa['x'] - pb['x'], pa['y'] - pb['y']) - 1e-9) / self.speed))
                for number, (lo, hi) in enumerate(intervals(nxt)):
                    required = dwell if nxt == -2 else 0
                    departed = max(arrival, lo - seconds)
                    if nxt == -2 and earliest_goal is not None:
                        departed = max(departed, earliest_goal - seconds)
                    latest = min(available[1], hi - seconds - required, travel_end - seconds)
                    key = (nxt, number)
                    while departed <= latest:
                        # Lazy dominance: an earlier arrival in this SAME safe
                        # interval can wait until any later arrival. A candidate
                        # that cannot improve it needs no collision query at all.
                        if departed + seconds >= best.get(key, math.inf):
                            break
                        if self.table.clear_motion(pa, pb, departed, departed + seconds, owner, self.last_conflicts):
                            reached = departed + seconds
                            if reached < best.get(key, math.inf):
                                best[key], parents[key] = reached, (state, departed)
                                serial += 1
                                heapq.heappush(queue, (reached + heuristic(nxt), -reached, serial, key))
                            break
                        departed += 1
        self.failures += 1
        self.last_failure = 'expansion_budget' if expanded >= self.max_expansions else 'no_path'
        return None
