import copy
import heapq
import itertools
import json
import math
from pathlib import Path
import random
import tempfile
from types import SimpleNamespace
import unittest

from backend.day1.nav import hold, position
from backend.traffic import swept_distance
from backend.scheduling_v2.matching import minimum_assignment
from backend.scheduling_v2.motion import MotionTable, SafeIntervalPlanner


class AssignmentTests(unittest.TestCase):
    def test_matches_exhaustive_rectangular_oracle(self):
        rng = random.Random(2710)
        for rows in range(1, 5):
            for cols in range(rows, 7):
                for _ in range(6):
                    cost = [[rng.randrange(-10, 50) for _ in range(cols)] for _ in range(rows)]
                    pairs = minimum_assignment(cost)
                    expected = min(sum(cost[i][j] for i, j in enumerate(p)) for p in itertools.permutations(range(cols), rows))
                    self.assertEqual(sum(cost[i][j] for i, j in pairs), expected)
                    self.assertEqual(len({j for _, j in pairs}), rows)

    def test_dummies_leave_forbidden_pairs_unassigned(self):
        pairs = minimum_assignment([[1e12, 100, 100], [1e12, 100, 100]])
        self.assertTrue(all(j != 0 for _, j in pairs))

    def test_rejects_invalid_or_nonfinite_matrix(self):
        for bad in [[[1], [2]], [[1, math.inf]], [[math.nan]]]:
            with self.assertRaises(ValueError):
                minimum_assignment(bad)


class GraphAuditTests(unittest.TestCase):
    def test_run_anchored_walks_and_original_count_checks(self):
        from backend.scheduling_v2.audit import audit_graph
        import re
        class Mirror:
            def __init__(self):
                self.calls = []
                self.task_count = 17
            def query(self, query, parameters):
                self.calls.append(query)
                self.assert_run = parameters['run']
                if 'cargo_locations' in query or 'AS location' in query:
                    return [{'location': 'SHIPPED', 'n': 1}]
                if 'WHCargo' in query:
                    return [{'n': 864}]
                label = re.search(r'\(n:(\w+)\)', query)[1]
                return [{'n': 100 if label == 'WHRobot' else self.task_count if label == 'WHTask' else 3}]
        mirror = Mirror()
        state = SimpleNamespace(run_id='audit-test', inside=864, cursor=17)
        audit = audit_graph(SimpleNamespace(mirror=mirror), state)
        self.assertEqual(audit['WHTask'], 17)
        self.assertTrue(all('USING INDEX r:WHRun(run_id)' in q and 'CALL (r)' in q for q in mirror.calls[:9]))
        mirror.task_count = 16
        with self.assertRaises(AssertionError):
            audit_graph(SimpleNamespace(mirror=mirror), state)


class TinyMap:
    def __init__(self):
        self.nodes = {0: {'x': 0, 'y': 0}, 1: {'x': 2, 'y': 2},
                      2: {'x': 2, 'y': -2}, 3: {'x': 4, 'y': 0}}
        self.edges = {0: [1, 2], 1: [0, 3], 2: [0, 3], 3: [1, 2]}
        self.locations = {'A': self.nodes[0], 'B': self.nodes[3]}
        self.base = self

    def clear(self, a, b, allowed):
        return False  # Only the two explicit corridors are legal for this fixture.

    def connections(self, location):
        return [0] if location == 'A' else [3]

    def distances(self, target, speed):
        distance, queue = {3: 1}, [(1, 3)]
        while queue:
            cost, k = heapq.heappop(queue)
            if cost != distance[k]:
                continue
            for nxt in self.edges[k]:
                a, b = self.nodes[k], self.nodes[nxt]
                proposed = cost + math.ceil(math.hypot(a['x'] - b['x'], a['y'] - b['y']) / speed - 1e-9)
                if proposed < distance.get(nxt, math.inf):
                    distance[nxt] = proposed
                    heapq.heappush(queue, (proposed, nxt))
        return distance


class TemporalTests(unittest.TestCase):
    def test_spacetime_index_and_scalar_kernel_match_all_span_oracle(self):
        rng, table, originals = random.Random(821), MotionTable(), []
        for owner in range(1, 13):
            parts = []
            for _ in range(5):
                start = rng.randrange(0, 35)
                parts.append({'a': {'x': rng.uniform(-6, 6), 'y': rng.uniform(-6, 6)},
                              'b': {'x': rng.uniform(-6, 6), 'y': rng.uniform(-6, 6)},
                              'start': start, 'end': start + rng.randrange(1, 20), 'phase': 'moving'})
            table.reserve(parts, owner=owner)
            originals.extend((owner, p) for p in parts)
        for _ in range(1200):
            start = rng.randrange(0, 50)
            candidate = {'a': {'x': rng.uniform(-7, 7), 'y': rng.uniform(-7, 7)},
                         'b': {'x': rng.uniform(-7, 7), 'y': rng.uniform(-7, 7)},
                         'start': start, 'end': start + rng.randrange(1, 16)}
            owner, expected = rng.randrange(1, 15), True
            for other_owner, part in originals:
                lo, hi = max(start, part['start']), min(candidate['end'], part['end'])
                if owner != other_owner and hi >= lo and swept_distance(position(candidate, lo), position(candidate, hi),
                        position(part, lo), position(part, hi)) < 1.1 - 1e-7:
                    expected = False
                    break
            self.assertEqual(table.clear_motion(candidate['a'], candidate['b'], start, candidate['end'], owner), expected)

    def test_collinear_compression_preserves_time_bucket_boundaries(self):
        table = MotionTable()
        parts = [{'a': {'x': float(i), 'y': 0.}, 'b': {'x': float(i + 1), 'y': 0.},
                  'start': i, 'end': i + 1, 'phase': 'moving'} for i in range(24)]
        table.reserve(parts, owner=1)
        self.assertEqual(len(table.owners[1]), 1)
        for second in [0, 7, 8, 15, 16, 23, 24]:
            point = {'x': float(second), 'y': 0.}
            self.assertFalse(table.clear_motion(point, point, second, second + 1, 2))

    def test_continuous_crossing_and_touching(self):
        table = MotionTable()
        table.reserve([{'a': {'x': 0, 'y': 1}, 'b': {'x': 2, 'y': 1},
                        'start': 0, 'end': 2, 'phase': 'moving'}], owner=1)
        self.assertFalse(table.clear_motion({'x': 1, 'y': 0}, {'x': 1, 'y': 2}, 0, 2, 2))
        self.assertTrue(table.clear_motion({'x': 0, 'y': 2.1}, {'x': 2, 'y': 2.1}, 0, 2, 2))

    def test_safe_wait_intervals_against_dense_geometric_oracle(self):
        rng = random.Random(820)
        for _ in range(80):
            table, point = MotionTable(), {'x': 0., 'y': 0.}
            other = {'a': {'x': rng.uniform(-3, 3), 'y': rng.uniform(-3, 3)},
                     'b': {'x': rng.uniform(-3, 3), 'y': rng.uniform(-3, 3)},
                     'start': 2, 'end': 18, 'phase': 'moving'}
            table.reserve([other], owner=1)
            for lo, hi in table.intervals(point, 0, 20, 2):
                left, right = max(lo, other['start']), min(hi, other['end'])
                if right >= left:
                    self.assertGreaterEqual(swept_distance(point, point, position(other, left), position(other, right)), 1.1 - 1e-6)

    def test_chooses_alternative_corridor_and_preserves_other_owner(self):
        graph, table = TinyMap(), MotionTable()
        obstacle = hold(graph.nodes[1], 'placing', 0, 50)
        table.reserve([obstacle], owner=9)
        planner = SafeIntervalPlanner(graph, table, speed=1, horizon=60)
        result = planner.plan('A', 'B', 0, 1, 'to_target', dwell=2, action='placing')
        self.assertIsNotNone(result)
        parts, end = result
        self.assertLess(end, 20)
        self.assertTrue(any(p['b']['y'] < 0 for p in parts))
        self.assertTrue(all(table.clear_motion(p['a'], p['b'], p['start'], p['end'], 1) for p in parts))
        self.assertIn(9, table.owners)

    def test_plan_failure_does_not_remove_committed_escape(self):
        graph, table = TinyMap(), MotionTable()
        escape = hold(graph.locations['A'], 'returning', 0, 5)
        table.reserve([escape], owner=1)
        table.reserve([hold(graph.locations['B'], 'placing', 0, 100)], owner=2)
        before = copy.deepcopy(table.owners[1])
        planner = SafeIntervalPlanner(graph, table, speed=1, horizon=20)
        self.assertIsNone(planner.plan('A', 'B', 0, 1, 'to_target', dwell=3, action='placing'))
        self.assertEqual(table.owners[1], before)

    def test_long_service_is_not_truncated_to_travel_horizon(self):
        graph, table = TinyMap(), MotionTable()
        planner = SafeIntervalPlanner(graph, table, speed=1, horizon=20)
        result = planner.plan('A', 'B', 0, 1, 'to_charge', dwell=120, action='charging')
        self.assertIsNotNone(result)
        self.assertEqual(result[0][-1]['end'] - result[0][-1]['start'], 120)
        self.assertLessEqual(result[0][-1]['start'], 20)

    def test_unavailable_goal_rejected_without_exhaustive_expansion(self):
        graph, table = TinyMap(), MotionTable()
        table.reserve([hold(graph.locations['B'], 'charging', 0, 1000)], owner=9)
        planner = SafeIntervalPlanner(graph, table, speed=1, horizon=20)
        self.assertIsNone(planner.plan('A', 'B', 0, 1, 'to_target', dwell=12, action='placing'))
        self.assertEqual(planner.expansions, 0)

    def test_replacement_keeps_new_future_and_other_robot(self):
        table = MotionTable()
        table.reserve([hold({'x': 0, 'y': 0}, 'placing', 0, 10)], owner=1)
        table.reserve([hold({'x': 8, 'y': 8}, 'placing', 0, 10)], owner=2)
        table.reserve([hold({'x': 4, 'y': 4}, 'placing', 2, 10)], owner=1)
        self.assertTrue(table.clear_motion({'x': 0, 'y': 0}, {'x': 0, 'y': 0}, 3, 4, 3))
        self.assertFalse(table.clear_motion({'x': 4, 'y': 4}, {'x': 4, 'y': 4}, 3, 4, 3))
        self.assertFalse(table.clear_motion({'x': 8, 'y': 8}, {'x': 8, 'y': 8}, 3, 4, 3))


class FleetTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from backend.day1.data import generate
        from backend.scheduling_v2.sim import Simulation, Config
        cls.temp = tempfile.TemporaryDirectory()
        cls.scene = Path(__file__).resolve().parents[1] / 'data/layouts/warehouse100-v1.json'
        cls.dataset = Path(cls.temp.name) / 'baseline'
        generate(cls.scene, cls.dataset, slot_interval_seconds=21600)
        cls.config = Config()
        cls.sim = Simulation(cls.scene, cls.dataset, 'v2-tests', cls.config)

    @classmethod
    def tearDownClass(cls):
        cls.temp.cleanup()

    def test_all_100_homes_and_24_docks_reach_public_roadmap(self):
        graph = self.sim.roadmap
        visited, stack = set(), [next(iter(graph.nodes))]
        while stack:
            k = stack.pop()
            if k not in visited:
                visited.add(k)
                stack.extend(graph.edges[k])
        for location in self.sim.nav.locations:
            self.assertTrue(any(n in visited for n in graph.connections(location)), location)
        self.assertEqual(len(graph.portals), 124)

    def test_static_eta_matches_empty_traffic_search_and_obstacle_geometry(self):
        graph = self.sim.roadmap
        planner = SafeIntervalPlanner(graph, MotionTable(), speed=.8, horizon=900)
        sources = ['P003', 'P100', 'IN-1', 'C-A02', 'R01-L1-C3-F1']
        targets = ['OUT-8', 'R36-L2-C8-F2', 'P003']
        for source, target in itertools.product(sources, targets):
            parts, end = planner.plan(source, target, 0, 1, 'moving')
            self.assertEqual(end, graph.travel_time(source, target, .8))
            self.assertTrue(all(self.sim.nav.clear(p['a'], p['b'], {source, target}) for p in parts))

    def test_real_execution_inventory_and_checkpoint_restore(self):
        from backend.scheduling_v2.sim import Simulation
        a = self.sim
        for _ in range(420):
            a.step()
        self.assertGreater(a.metrics['completed'], 0)
        a.assert_inventory()
        from backend.checkpoint import read_checkpoint, write_checkpoint
        path = Path(self.temp.name) / 'actual-checkpoint.json'
        write_checkpoint(path, a.checkpoint())
        checkpoint = read_checkpoint(path)
        b = Simulation(self.scene, self.dataset, 'v2-tests', self.config, checkpoint=checkpoint)
        for _ in range(90):
            a.step()
            b.step()
        # CPU timing and reservation-cache operation counts are not logical state.
        left, right = a.checkpoint(), b.checkpoint()
        for state in [left, right]:
            state['metrics'].pop('planning_seconds', None)
        self.assertEqual(left, right)
        self.assertEqual(a.inside, 864 + a.metrics['received'] - a.metrics['shipped'])
        self.assertGreaterEqual(a.metrics['min_separation_m'], 1.1 - 1e-6)
        self.assertGreater(a.metrics['motion_audit_pairs'], 0)

    def test_recovery_rejects_algorithm_or_config_change(self):
        from backend.scheduling_v2.sim import Simulation
        checkpoint = json.loads(json.dumps(self.sim.checkpoint()))
        checkpoint['run_metadata']['scheduler_code_sha256'] = 'changed'
        with self.assertRaises(ValueError):
            Simulation(self.scene, self.dataset, 'v2-tests', self.config, checkpoint=checkpoint)


if __name__ == '__main__':
    unittest.main()
