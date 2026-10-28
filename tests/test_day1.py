import json
from pathlib import Path
import tempfile
import unittest
import random
from backend.traffic import swept_distance
from backend.day1.data import generate,validate,read_dataset
from backend.day1.nav import Reservations,hold,audit_actual_motion
from backend.day1.sim import Config,Simulation

ROOT=Path(__file__).resolve().parents[1]
SCENE=ROOT/'data/layouts/warehouse100-v1.json'

class DayOneTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp=tempfile.TemporaryDirectory();cls.data=Path(cls.tmp.name)/'dataset'
        cls.manifest,cls.report=generate(SCENE,cls.data)
    @classmethod
    def tearDownClass(cls):cls.tmp.cleanup()
    def test_dataset_schema_counts_and_capacity(self):
        self.assertEqual(self.manifest['tasks'],20736);self.assertEqual(self.manifest['inbound'],10368)
        self.assertTrue(self.report['valid']);self.assertLessEqual(self.report['planned_max_inventory'],1728)
        self.assertEqual(self.report['planned_final_inventory'],864)
    def test_repeatable_and_no_robot_policy_in_task_table(self):
        m,_=generate(SCENE,Path(self.tmp.name)/'repeat')
        self.assertEqual(m['files'],self.manifest['files'])
        _,tasks,_=read_dataset(SCENE,self.data)
        self.assertFalse({'robot_id','battery','relay','charge_threshold'}&set(tasks[0]))
    def test_calibrated_uniform_baseline_is_separate_not_truncated_input(self):
        m,r=generate(SCENE,Path(self.tmp.name)/'uniform-baseline',slot_interval_seconds=21600)
        self.assertEqual(m['tasks'],6912);self.assertEqual(m['inbound'],3456);self.assertEqual(m['outbound'],3456)
        self.assertEqual(r['planned_final_inventory'],864)
        self.assertNotEqual(m['files']['tasks.csv'],self.manifest['files']['tasks.csv'])
    def test_tamper_rejected(self):
        target=Path(self.tmp.name)/'tamper';generate(SCENE,target)
        with (target/'tasks.csv').open('a') as f:f.write('tamper')
        with self.assertRaises(ValueError):validate(SCENE,target)
    def test_temporal_reservations_do_not_allow_crossings(self):
        r=Reservations();a=[{'a':{'x':0,'y':1},'b':{'x':2,'y':1},'start':0,'end':2,'phase':'moving'}]
        b=[{'a':{'x':1,'y':0},'b':{'x':1,'y':2},'start':0,'end':2,'phase':'moving'}]
        r.reserve(a,0);self.assertNotEqual(r.choose(b,0,10),0)
        r.reserve([hold({'x':3,'y':3},'picking',0,12)],0)
        self.assertIsNone(r.choose([hold({'x':3,'y':3},'placing',0,5)],0,3))
    def test_fast_departure_search_matches_exhaustive_oracle(self):
        rng=random.Random(47)
        def part():
            return {'a':{'x':rng.randrange(7),'y':rng.randrange(7)},'b':{'x':rng.randrange(7),'y':rng.randrange(7)},'start':0,'end':rng.randrange(1,20),'phase':'moving'}
        for _ in range(80):
            r=Reservations()
            for n in range(4):r.reserve([part()],rng.randrange(15))
            route=[part()];expected=None
            for offset in range(41):
                clear=True
                for t,a,b,_ in r.motions(route):
                    seen=set()
                    for cell in r.cells(a,b):
                        for idx,other in r.book.get(t+offset,{}).get(cell,[]):
                            if idx in seen:continue
                            seen.add(idx)
                            if swept_distance(a,b,other[0],other[1])<1.1-1e-7:clear=False
                if clear:expected=offset;break
            self.assertEqual(r.choose(route,0,40),expected)
    def test_actual_spatial_index_matches_all_pairs_geometry(self):
        rng=random.Random(415)
        for _ in range(300):
            before={i:{'x':rng.uniform(-4,4),'y':rng.uniform(-4,4)} for i in [1,2]}
            after={i:{'x':before[i]['x']+rng.uniform(-2,2),'y':before[i]['y']+rng.uniform(-2,2)} for i in [1,2]}
            collision=swept_distance(before[1],after[1],before[2],after[2])<1.1-1e-6
            if collision:
                with self.assertRaises(AssertionError):audit_actual_motion(before,after)
            else:audit_actual_motion(before,after)
    def test_runtime_smoke_recovery_and_real_cargo_conservation(self):
        a=Simulation(SCENE,self.data,'test-day1',Config())
        self.assertEqual(a.checkpoint()['tasks'],{})
        for _ in range(360):a.step()
        self.assertGreater(a.metrics['completed'],0);a.assert_inventory()
        state=json.loads(json.dumps(a.checkpoint()))
        b=Simulation(SCENE,self.data,'test-day1',Config(),checkpoint=state)
        for _ in range(80):a.step();b.step()
        self.assertEqual(a.checkpoint(),b.checkpoint())
        self.assertEqual(a.inside,864+a.metrics['received']-a.metrics['shipped'])
        self.assertEqual(len(a.robots),100)
        self.assertLessEqual(a.metrics['max_charging'],24)
        self.assertTrue(all(r['battery']>5 for r in a.robots.values()))
        self.assertGreater(a.metrics['motion_audit_pairs'],0)
        self.assertGreaterEqual(a.metrics['min_separation_m'],1.1-1e-6)

if __name__=='__main__':unittest.main()
