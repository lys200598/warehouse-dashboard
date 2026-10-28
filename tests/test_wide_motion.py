"""No new solver: regression of wide geometry and persistent body semantics."""
from collections import defaultdict
from copy import deepcopy
import math
from pathlib import Path
import random
import unittest

from backend.day1.nav import hold, position
from backend.scheduling_v2.motion import SafeIntervalPlanner
from backend.scheduling_v3.transactions import try_replace_many
from backend.traffic import swept_distance
from backend.wide.data import load_scene
from backend.wide.motion import (WideNavigator, WideRoadmap, WideMotionTable,
                                 idle_guard, terminal_tail, execution_parts)


class NoFullValuesScan(dict):
    def values(self):
        raise AssertionError('nearby query scanned all persistent bodies')


class TimeReadBudget(defaultdict):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.reads = 0

    def get(self, key, default=None):
        self.reads += 1
        if self.reads > 500:
            raise AssertionError('terminal guard enumerated time buckets')
        return super().get(key, default)


class PersistentBodyTests(unittest.TestCase):
    def test_terminal_tail_is_explicit_stationary_and_not_execution(self):
        parts = [hold({'x':2.,'y':3.},'placing',0,10,'delivered')]
        original = deepcopy(parts)
        reserved = terminal_tail(parts)
        self.assertEqual(parts, original)
        self.assertEqual(execution_parts(reserved), original)
        self.assertTrue(reserved[-1]['permanent_obstacle'])
        self.assertTrue(reserved[-1]['terminal_occupancy'])
        self.assertEqual(reserved[-1]['start'],10)
        self.assertEqual(terminal_tail(reserved),reserved)
        with self.assertRaises(ValueError):
            terminal_tail(parts, {'x':4.,'y':3.})
        with self.assertRaises(ValueError):
            terminal_tail([])

    def test_execution_strip_keeps_real_persistent_fault(self):
        fault = {**hold({'x':2.,'y':3.},'fault_wait',0,10**12), 'permanent_obstacle':True}
        self.assertEqual(execution_parts([fault]),[fault])
        self.assertEqual(terminal_tail([fault]),[fault])

    def test_100_idle_or_full_bodies_block_their_bay_without_full_scan(self):
        table = WideMotionTable()
        points = [{'x':1.5*i,'y':2.4} for i in range(100)]
        for owner, point in enumerate(points,1):
            table.reserve([idle_guard(point)],owner=owner)
        self.assertEqual(len(table.fixed),100)
        self.assertEqual(len(table.timegrid),0)
        table.fixed = NoFullValuesScan(table.fixed)
        for owner, point in enumerate(points,1):
            self.assertFalse(table.clear_motion(point,point,50000,50001,0))
            self.assertTrue(table.clear_motion(point,point,50000,50001,owner))
        middle = {'x':40.,'y':40.}
        self.assertEqual(table.nearby(middle,middle,0,0,1),[])

    def test_replacement_removes_old_body_only_after_verified_departure_plan(self):
        table = WideMotionTable()
        table.reserve([idle_guard({'x':2.,'y':0.})],owner=1)
        depart = [{'a':{'x':2.,'y':0.},'b':{'x':2.,'y':2.},'start':0,'end':3,'phase':'moving'}]
        crossing = [hold({'x':0.,'y':0.},'traffic_wait',0,5),
                    {'a':{'x':0.,'y':0.},'b':{'x':4.,'y':0.},'start':5,'end':9,'phase':'moving'}]
        self.assertTrue(try_replace_many(table,{1:terminal_tail(depart),2:terminal_tail(crossing)},now=0))
        self.assertTrue(table.clear_motion({'x':2.,'y':0.},{'x':2.,'y':0.},20,21,99))
        self.assertFalse(table.clear_motion({'x':2.,'y':2.},{'x':2.,'y':2.},20,21,99))

    def test_terminal_body_is_checked_against_later_committed_arrival(self):
        table = WideMotionTable()
        point = {'x':2.,'y':0.}
        table.reserve([{'a':{'x':0.,'y':0.},'b':{'x':4.,'y':0.},
                        'start':10,'end':14,'phase':'moving'}],owner=2)
        table.timegrid = TimeReadBudget(dict,table.timegrid)
        before = deepcopy(table.__dict__)
        candidate = terminal_tail([hold(point,'placing',0,5)])
        result = try_replace_many(table,{1:candidate},now=0)
        self.assertFalse(result)
        self.assertEqual(result.conflicts,(2,))
        self.assertEqual(table.__dict__,before)

    def test_two_alias_plans_cannot_reserve_the_same_end_bay(self):
        table = WideMotionTable()
        a = [{'a':{'x':0.,'y':0.},'b':{'x':2.,'y':0.},'start':0,'end':2,'phase':'moving'}]
        b = [{'a':{'x':4.,'y':2.},'b':{'x':2.,'y':0.},'start':0,'end':8,'phase':'moving'}]
        before = deepcopy(table.__dict__)
        self.assertFalse(try_replace_many(table,{1:terminal_tail(a),2:terminal_tail(b)},now=0))
        self.assertEqual(table.__dict__,before)

    def test_fixed_index_survives_prune_clone_and_multi_span_generator(self):
        table = WideMotionTable()
        point = {'x':2.,'y':2.}
        spans = terminal_tail([hold(point,'charging',0,100)])
        table.reserve((part for part in spans),owner=8)
        for at in (101,86400,604800):
            table.prune(at)
            self.assertFalse(table.clear_motion(point,point,at,at+1,0))
        clone = deepcopy(table)
        clone._erase(8)
        self.assertTrue(clone.clear_motion(point,point,604800,604801,0))
        self.assertFalse(table.clear_motion(point,point,604800,604801,0))

    def test_bad_direct_reservation_does_not_corrupt_body_index(self):
        table = WideMotionTable()
        table.reserve([idle_guard({'x':2.,'y':2.})],owner=8)
        before = deepcopy(table.__dict__)
        for bad in [hold({'x':math.nan,'y':0.},'idle',0,100),
                    hold({'x':2.,'y':2.},'idle',0,0)]:
            with self.assertRaises(ValueError):
                table.reserve([bad],owner=8)
            self.assertEqual(table.__dict__,before)

    def test_spatial_fixed_lookup_matches_independent_full_body_oracle(self):
        rng = random.Random(271027)
        table, bodies = WideMotionTable(), []
        for owner in range(1,101):
            point = {'x':rng.uniform(0,90),'y':rng.uniform(0,76)}
            start = rng.randrange(0,200)
            guard = idle_guard(point,start=start)
            bodies.append((owner,guard))
            table.reserve([guard],owner=owner)
        for _ in range(800):
            start = rng.randrange(0,500)
            end = start+rng.randrange(1,100)
            a = {'x':rng.uniform(0,90),'y':rng.uniform(0,76)}
            b = {'x':a['x']+rng.uniform(-10,10),'y':a['y']+rng.uniform(-10,10)}
            candidate = {'a':a,'b':b,'start':start,'end':end}
            ignore, expected = rng.randrange(0,102), True
            for owner, guard in bodies:
                lo, hi = max(start,guard['start']), min(end,guard['end'])
                if owner != ignore and hi >= lo and swept_distance(position(candidate,lo),position(candidate,hi),
                        guard['a'],guard['b']) < 1.1-1e-7:
                    expected = False
                    break
            self.assertEqual(table.clear_motion(a,b,start,end,ignore),expected)


class WideGeometryTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        workspace = Path(__file__).resolve().parents[2]
        cls.scene,cls.slots = load_scene(Path(__file__).resolve().parents[1]/'data/layouts/warehouse100-wide-v2.json')
        cls.nav = WideNavigator(cls.scene,cls.slots)
        cls.graph = WideRoadmap(cls.nav)

    def test_raster_uses_wide_extent_and_slot_pitch(self):
        self.assertEqual((self.nav.nx,self.nav.ny),(451,381))
        self.assertAlmostEqual(self.slots['R01-L1-C1-F1']['x'],18.9)
        self.assertAlmostEqual(self.slots['R01-L1-C8-F1']['x'],27.3)
        self.assertEqual(len(self.nav.fixed),38)
        self.assertEqual(self.nav.private,{})
        self.assertFalse(self.nav.clear({'x':.1,'y':2.},{'x':1.,'y':2.},set()))
        self.assertTrue(self.nav.clear({'x':80.,'y':70.},{'x':82.,'y':70.},set()))

    def test_p_c_share_one_portal_but_empty_pad_has_no_geometry(self):
        self.assertEqual(len(set(self.graph.portals.values())),100)
        for home in self.scene['homes']:
            canonical = home['alias_of']
            self.assertEqual(self.graph.connections(home['id']),self.graph.connections(canonical))
            self.assertTrue(self.nav.clear(home['point'],self.nav.locations[canonical],set()))

    def test_all_task_points_and_canonical_bays_reach_one_public_component(self):
        graph = self.graph
        visited,queue=set(),[next(iter(graph.nodes))]
        while queue:
            node=queue.pop()
            if node in visited:
                continue
            visited.add(node)
            queue.extend(graph.edges[node])
        self.assertEqual(len(visited),len(graph.nodes))
        for key in self.nav.locations:
            self.assertTrue(any(node in visited for node in graph.connections(key)),key)

    def test_empty_bay_can_be_crossed_but_parked_robot_cannot(self):
        dock = self.scene['docks'][0]
        point = dock['point']
        left = {'x':point['x']-.8,'y':point['y']}
        right = {'x':point['x']+.8,'y':point['y']}
        self.assertTrue(self.nav.clear(left,right,set()))
        table = WideMotionTable()
        self.assertTrue(table.clear_motion(left,right,0,3,2))
        table.reserve([idle_guard(point)],owner=1)
        self.assertFalse(table.clear_motion(left,right,0,3,2))

    def test_real_sipp_plan_retains_wide_world_and_static_obstacles(self):
        planner = SafeIntervalPlanner(self.graph,WideMotionTable(),speed=.8,horizon=900)
        result = planner.plan('IN-1','OUT-8',0,1,'moving')
        self.assertIsNotNone(result)
        parts,end = result
        self.assertGreater(end,0)
        self.assertTrue(all(self.nav.clear(p['a'],p['b'],set()) for p in parts))
        self.assertTrue(any(p['b']['x'] > 60 for p in parts))


if __name__=='__main__':
    unittest.main()
