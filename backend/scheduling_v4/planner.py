"""Explicit goal-window SIPP, retaining v2's continuous collision contract.

Static graph travel is a lower bound. Snap that lower bound to the next goal
safe interval which fits the entire service; impossible earlier arrivals need
not dominate the frontier. Search remains bounded/incomplete for the fleet.
"""
import heapq
import math

from ..day1.nav import hold
from ..scheduling_v2.motion import SafeIntervalPlanner


class GoalWindowPlanner(SafeIntervalPlanner):
    def plan(self, source, target, now, owner, phase, dwell=0, action=None, marker=None, earliest_goal=None):
        self.calls += 1
        self.last_conflicts = set()
        self.last_failure = None
        travel_end = now + self.horizon
        end = travel_end + dwell
        points = self.map.nodes
        a, b = self.map.locations[source], self.map.locations[target]
        endpoints = {-1: a, -2: b}
        start_links, goal_links = self.map.connections(source), set(self.map.connections(target))
        direct = self.map.base.clear(a, b, {source, target})
        static_distance = self.map.distances(target, self.speed)

        def point(key):
            return endpoints[key] if key < 0 else points[key]

        def neighbors(key):
            if key == -1:
                return start_links + ([-2] if direct else [])
            if key == -2:
                return []
            return self.map.edges[key] + ([-2] if key in goal_links else [])

        cache = {}
        def intervals(key):
            if key not in cache:
                cache[key] = self.table.intervals(point(key), now, end, owner)
            return cache[key]

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

        def heuristic(key):
            if key == -2:
                return 0
            if key >= 0:
                return static_distance.get(key, math.inf)
            options = [max(1, math.ceil((math.hypot(a['x'] - points[n]['x'], a['y'] - points[n]['y']) - 1e-9) / self.speed)) +
                       static_distance.get(n, math.inf) for n in start_links]
            if direct:
                options.append(max(1, math.ceil((math.hypot(a['x'] - b['x'], a['y'] - b['y']) - 1e-9) / self.speed)))
            return min(options, default=math.inf)

        goal_windows = [(max(lo, goal_earliest), min(hi - dwell, travel_end))
                        for lo, hi in intervals(-2) if max(lo, goal_earliest) + dwell <= hi
                        and max(lo, goal_earliest) <= travel_end]

        def priority(key, arrival):
            h = heuristic(key)
            lower = arrival + h
            bound = next((max(lower, lo) for lo, hi in goal_windows if max(lower, lo) <= hi), math.inf)
            return bound, h, -arrival

        queue = [(*priority(-1, now), serial, root)]
        if not math.isfinite(priority(-1, now)[0]) or priority(-1, now)[0] > travel_end:
            self.failures += 1
            self.last_failure = 'static_disconnected_or_horizon'
            return None
        expanded = 0
        while queue and expanded < self.max_expansions:
            _, _, negative_arrival, _, state = heapq.heappop(queue)
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
                        if departed + seconds >= best.get(key, math.inf):
                            break
                        if self.table.clear_motion(pa, pb, departed, departed + seconds, owner, self.last_conflicts):
                            reached = departed + seconds
                            if reached < best.get(key, math.inf):
                                best[key], parents[key] = reached, (state, departed)
                                serial += 1
                                score = priority(nxt, reached)
                                if math.isfinite(score[0]):
                                    heapq.heappush(queue, (*score, serial, key))
                            break
                        departed += 1
        self.failures += 1
        self.last_failure = 'expansion_budget' if expanded >= self.max_expansions else 'no_path'
        return None
