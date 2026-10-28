from copy import deepcopy
from pathlib import Path
import tempfile
import unittest

from backend.scheduling_v3.obstacles import MotionTable
from backend.day1.nav import hold
from backend.scheduling_v3.sim import Simulation
from backend.scheduling_v3.scenarios import generate_scenario


class PersistentObstacleTests(unittest.TestCase):
    def test_stationary_body_has_no_days_of_time_buckets_and_survives_pruning(self):
        table = MotionTable()
        p = {'x':20.,'y':20.}
        table.reserve([{**hold(p,'fault_wait',0,10**12),'permanent_obstacle':True}],owner=1)
        self.assertLess(len(table.timegrid),10)
        for at in (0,1000,86400,604800):
            self.assertFalse(table.clear_motion(p,p,at,at+1,2))
            self.assertEqual(table.intervals(p,at,at+100,2),[])
            table.prune(at)
            self.assertIn(1,table.fixed)
        self.assertTrue(table.clear_motion({'x':22.,'y':20.},{'x':22.,'y':21.},0,10,2))

    def test_faulted_vehicle_stays_while_helper_delivers_and_other_work_continues(self):
        base = Path(__file__).resolve().parents[1]
        with tempfile.TemporaryDirectory(dir=base/'data/local-check') as directory:
            scene = base/'data/layouts/warehouse100-v1.json'
            dataset = Path(directory)/'input'
            generate_scenario(scene,dataset,active_slots=64,slot_interval_seconds=3600,duration_seconds=1800)
            sim = Simulation(scene,dataset,'persistent-test')
            donor = None
            for _ in range(900):
                sim.step()
                donor = next((i for i,r in sim.robots.items() if r['carrying'] and r['stage']=='to_target'),None)
                if donor:
                    break
            self.assertIsNotNone(donor)
            point = sim._point(sim.robots[donor])
            task_id = sim.robots[donor]['task_id']
            completed = sim.metrics['completed']
            self.assertTrue(sim.inject_stop(donor,None))
            self.assertTrue(sim.request_assistance(donor,reason='persistent_drive_fault'))
            for _ in range(700):
                sim.step()
                self.assertEqual(sim._point(sim.robots[donor]),point)
            self.assertEqual(sim.tasks[task_id]['status'],'completed')
            self.assertGreater(sim.metrics['completed'],completed+1)
            self.assertFalse(sim.robots[donor]['carrying'])
            self.assertTrue(sim.faults[donor]['persistent'])
            self.assertIn(donor,sim.reservations.fixed)
            sim.assert_inventory()
            state = sim.checkpoint()
            restored = Simulation(scene,dataset,'persistent-test',checkpoint=state)
            for _ in range(30):
                sim.step(); restored.step()
            self.assertEqual(sim.robots,restored.robots)
            self.assertEqual(sim.goods,restored.goods)
            self.assertIn(donor,restored.reservations.fixed)


if __name__=='__main__':
    unittest.main()
