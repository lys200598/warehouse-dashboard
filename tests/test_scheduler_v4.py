from copy import deepcopy
import itertools
import json
from pathlib import Path
import random
import tempfile
import unittest
from unittest.mock import patch

from backend.checkpoint import read_checkpoint, write_checkpoint
from backend.day1.nav import Reservations
from backend.knowledge import checkpoint_identity
from backend.scheduling_v2.motion import SafeIntervalPlanner
from backend.scheduling_v2.matching import minimum_assignment
from backend.scheduling_v3 import sim as coordinator
from backend.wide.data import load_scene
from backend.wide.scenarios import generate_scenario
from backend.wide.sim import Simulation as LegacySimulation, Config
from backend.wide.motion import WideMotionTable
from backend.scheduling_v4.geometry import (cached_cells, OptimizedMotionTable, OptimizedRoadmap,
                                          DISTANCE_CACHE_LIMIT)
from backend.scheduling_v4.matching import priority_assignment
from backend.scheduling_v4.planner import GoalWindowPlanner
from backend.scheduling_v4.sim import Simulation, execution_identity


ROOT = Path(__file__).resolve().parents[1]


def physical(sim):
    cp=sim.checkpoint()
    keys=['elapsed','goods','inside','ports','jobs','tasks','racks','pending','cursor',
          'scheduler_v2','scheduler_v3','wide_occupancy']
    return {**{key:cp[key] for key in keys},'robots':[robot['state'] for robot in cp['robots']]}


class PriorityTests(unittest.TestCase):
    def test_240_small_matrices_against_independent_exhaustive_oracle(self):
        rng=random.Random(20261027)
        for _ in range(240):
            rows,cols=rng.randint(1,5),rng.randint(0,4)
            matrix=[[1e12 if rng.random()<.25 else rng.randrange(-10,1000) for _ in range(cols)]+[1e8]*rows for _ in range(rows)]
            candidates=[]
            for chosen in itertools.product(range(-1,cols),repeat=rows):
                real=[x for x in chosen if x>=0]
                if len(real)!=len(set(real)) or any(c>=0 and matrix[r][c]>=1e8 for r,c in enumerate(chosen)):continue
                candidates.append((-len(real),tuple(-int(c>=0) for c in chosen),sum(matrix[r][c] for r,c in enumerate(chosen) if c>=0)))
            result={r:c for r,c in priority_assignment(matrix) if c<cols}
            actual=(-len(result),tuple(-int(r in result) for r in range(rows)),sum(matrix[r][c] for r,c in result.items()))
            self.assertEqual(actual,min(candidates))

    def test_expensive_urgent_row_is_not_starved_by_equal_dummy_cost(self):
        matrix=[[600,1e8,1e8],[50,1e8,1e8]]
        self.assertEqual([r for r,c in priority_assignment(matrix) if c==0],[0])
        self.assertEqual([r for r,c in minimum_assignment(matrix) if c==0],[1])

    def test_invalid_inputs_fail_explicitly(self):
        for matrix in [None,[[1]],[[1,1e8],[1,1e8]],[[float('nan'),1e8]],
                       [[float('inf'),1e8]],[[True,1e8]],[[1,0]],[[1e8]]*101]:
            with self.assertRaises(ValueError):priority_assignment(matrix)
        self.assertEqual(priority_assignment([]),[])


class GeometryTests(unittest.TestCase):
    def test_1200_exact_ordered_footprints(self):
        rng=random.Random(4146)
        for _ in range(1200):
            a={'x':rng.uniform(-20,100),'y':rng.uniform(-20,100)}
            b={'x':rng.uniform(-20,100),'y':rng.uniform(-20,100)}
            padding=rng.choice([0,1.1])
            value=cached_cells(a,b,padding)
            self.assertIsInstance(value,tuple)
            self.assertEqual(value,tuple(Reservations.cells(a,b,padding)))

    def test_invalid_geometry(self):
        with self.assertRaises(ValueError):cached_cells({'x':0,'y':0},{'x':float('nan'),'y':0})
        with self.assertRaises(ValueError):cached_cells({'x':0,'y':0},{'x':0,'y':0},-1)

    def test_traffic_mutations_never_reuse_cached_clearance(self):
        from backend.wide.motion import idle_guard
        table=OptimizedMotionTable();point={'x':2.,'y':3.}
        self.assertTrue(table.clear_motion(point,point,0,1,1))
        table.reserve([idle_guard(point)],owner=2)
        self.assertFalse(table.clear_motion(point,point,0,1,1))
        clone=deepcopy(table);clone._erase(2)
        self.assertTrue(clone.clear_motion(point,point,0,1,1))
        self.assertFalse(table.clear_motion(point,point,0,1,1))

    def test_static_aliases_and_bounded_lru(self):
        scene,slots=load_scene(ROOT/'data/layouts/warehouse100-wide-v2.json')
        from backend.wide.motion import WideNavigator
        roadmap=OptimizedRoadmap(WideNavigator(scene,slots))
        a=roadmap.distances('P001',.8)
        self.assertEqual(a,roadmap.distances('C-A01',.8))
        self.assertEqual(roadmap.cache_stats()['misses'],1)
        self.assertEqual(roadmap.distances('R01-L1-C1-F1',.8),roadmap.distances('R01-L3-C1-F1',.8))
        self.assertEqual(roadmap.cache_stats()['misses'],2)
        self.assertEqual(roadmap.distances('OUT-1',.8),roadmap._compute_distances('OUT-1',.8))
        with patch('backend.scheduling_v4.geometry.DISTANCE_CACHE_LIMIT',3):
            for target in ['IN-1','IN-2','IN-3','IN-4']:
                roadmap.distances(target,.8)
            self.assertLessEqual(roadmap.cache_stats()['currsize'],3)


class RuntimeTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        (ROOT/'data/local-check').mkdir(parents=True,exist_ok=True)
        cls.temp=tempfile.TemporaryDirectory(dir=ROOT/'data/local-check')
        cls.folder=Path(cls.temp.name);cls.scene=ROOT/'data/layouts/warehouse100-wide-v2.json'
        cls.dataset=cls.folder/'input'
        generate_scenario(cls.scene,cls.dataset,duration_seconds=1800,slot_interval_seconds=3600,active_slots=64)
    @classmethod
    def tearDownClass(cls):cls.temp.cleanup()
    def make(self,mode='optimized',name='v4-runtime'):
        return Simulation(self.scene,self.dataset,name,Config(),scheduler=mode)
    def carrier(self):
        sim=self.make()
        for _ in range(800):
            sim.step()
            donor=next((i for i,r in sim.robots.items() if r['carrying']),None)
            if donor is not None:return sim,donor
        self.fail('no carrying robot')

    def test_factories_do_not_patch_global_baseline(self):
        before=(coordinator.minimum_assignment,SafeIntervalPlanner.plan,Reservations.cells)
        a=self.make();b=self.make('baseline')
        self.assertEqual(before,(coordinator.minimum_assignment,SafeIntervalPlanner.plan,Reservations.cells))
        self.assertIsInstance(a.planner,GoalWindowPlanner)
        self.assertIsInstance(a.reservations,OptimizedMotionTable)
        self.assertIs(type(b.planner),SafeIntervalPlanner)
        self.assertIs(type(b.reservations),WideMotionTable)
        self.assertNotEqual(a.execution_identity,b.execution_identity)

    def test_baseline_mode_retains_legacy_physics_and_task_timestamps(self):
        legacy=LegacySimulation(self.scene,self.dataset,'baseline-pair',Config())
        baseline=self.make('baseline','baseline-pair')
        for _ in range(320):legacy.step();baseline.step()
        self.assertEqual(physical(legacy),physical(baseline))

    def test_disk_resume_during_handoff_keeps_state_and_future(self):
        sim,donor=self.carrier()
        self.assertTrue(sim.report_soc(donor,24.))
        self.assertIn(donor,sim.assists)
        sim.knowledge_seq=1
        path=self.folder/'handoff.json';write_checkpoint(path,sim.checkpoint())
        restored=Simulation(self.scene,self.dataset,sim.run_id,Config(),checkpoint=read_checkpoint(path))
        self.assertEqual(physical(sim),physical(restored))
        self.assertIs(restored.planner.table,restored.reservations)
        self.assertIsInstance(restored.planner,GoalWindowPlanner)
        self.assertIsInstance(restored.reservations,OptimizedMotionTable)
        for _ in range(240):sim.step();restored.step()
        self.assertEqual(physical(sim),physical(restored))
        self.assertEqual(sim.planner.expansions,restored.planner.expansions)

    def test_persistent_body_and_source_mode_refusal(self):
        sim,donor=self.carrier()
        self.assertTrue(sim.inject_stop(donor,None))
        state=sim.checkpoint()
        for mode in ['baseline']:
            with self.assertRaisesRegex(ValueError,'identity mismatch'):
                Simulation(self.scene,self.dataset,sim.run_id,Config(),checkpoint=state,scheduler=mode)
        bad=deepcopy(state);bad['scheduler_v4']['identity']['source_sha256']='tampered'
        with self.assertRaisesRegex(ValueError,'identity mismatch'):
            Simulation(self.scene,self.dataset,sim.run_id,Config(),checkpoint=bad)
        restored=Simulation(self.scene,self.dataset,sim.run_id,Config(),checkpoint=state)
        point=restored._point(restored.robots[donor])
        self.assertFalse(restored.reservations.clear_motion(point,point,restored.elapsed+1000,restored.elapsed+1001,0))

    def test_legacy_checkpoint_not_silently_migrated(self):
        legacy=LegacySimulation(self.scene,self.dataset,'legacy-not-v4',Config())
        with self.assertRaisesRegex(ValueError,'identity mismatch'):
            Simulation(self.scene,self.dataset,legacy.run_id,Config(),checkpoint=legacy.checkpoint())

    def test_cache_warmth_does_not_change_checkpoint_identity(self):
        sim=self.make()
        a=sim.checkpoint()
        sim.roadmap.distances('P001',.8)
        cached_cells({'x':2,'y':3},{'x':4,'y':3})
        self.assertEqual(checkpoint_identity(a),checkpoint_identity(sim.checkpoint()))


if __name__=='__main__':unittest.main()
