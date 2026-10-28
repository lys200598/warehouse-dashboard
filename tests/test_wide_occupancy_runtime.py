"""Read-only-review regressions for the wide world's explicit entity occupancy."""
from copy import deepcopy
from pathlib import Path
import tempfile
import unittest

from backend.checkpoint import read_checkpoint, write_checkpoint
from backend.wide.motion import idle_guard, terminal_tail
from backend.wide.scenarios import generate_scenario
from backend.wide.sim import Simulation


class WideOccupancyRuntimeTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.root = Path(__file__).resolve().parents[1]
        cls.scene = cls.root/'data/layouts/warehouse100-wide-v2.json'
        cls.temp = tempfile.TemporaryDirectory(dir=cls.root/'data/local-check')
        cls.dataset = Path(cls.temp.name)/'input'
        generate_scenario(cls.scene,cls.dataset,duration_seconds=1800,
                          slot_interval_seconds=3600,active_slots=64)

    @classmethod
    def tearDownClass(cls):
        cls.temp.cleanup()

    def make(self, suffix):
        return Simulation(self.scene,self.dataset,'wide-body-review-'+suffix)

    def carrier(self, suffix):
        sim = self.make(suffix)
        for _ in range(900):
            sim.step()
            donor = next((i for i,r in sim.robots.items() if r['carrying']),None)
            if donor is not None:
                return sim,donor
        self.fail('no real carrier within bounded fixture')

    def assert_complete_occupancy(self, sim):
        for owner,robot in sim.robots.items():
            if owner not in sim.jobs:
                self.assertIn(owner,sim.reservations.fixed)
                point = sim._point(robot)
                self.assertFalse(sim.reservations.clear_motion(point,point,sim.elapsed+600,
                                                                sim.elapsed+601,0))
            else:
                job = sim.jobs[owner]
                self.assertIn(owner,sim.reservations.fixed)
                if not job.get('persistent_fault'):
                    tail = sim.reservations.fixed[owner][-1]
                    self.assertTrue(tail.get('terminal_occupancy'))
                    self.assertEqual(tail['start'],job['start']+job['parts'][-1]['end'])
                    self.assertEqual(tail['a'],job['parts'][-1]['b'])

    def test_every_idle_charging_and_finished_entity_retains_its_body(self):
        sim = self.make('idle')
        self.assertEqual(len(sim.docks),100)
        self.assertEqual(len(set(sim.nav.aliases.values())),100)
        self.assert_complete_occupancy(sim)
        for _ in range(700):
            sim.step()
        self.assertGreater(sim.metrics['completed'],0)
        self.assert_complete_occupancy(sim)
        for robot in sim.robots.values():
            if robot.get('dock_id'):
                self.assertEqual(robot['dock_id'],sim.nav.canonical_location(robot['home_id']))

    def test_delay_retimes_execution_and_terminal_body_together(self):
        sim,owner = self.carrier('delay')
        old = deepcopy(sim.jobs[owner])
        before_end = old['start']+old['parts'][-1]['end']
        self.assertTrue(sim.inject_delay(owner,25))
        self.assertEqual(sim.reservations.fixed[owner][-1]['start'],before_end+25)
        self.assert_complete_occupancy(sim)
        for _ in range(250):
            sim.step()
        self.assert_complete_occupancy(sim)
        self.assertGreaterEqual(sim.metrics['min_separation_m'],1.1-1e-6)

    def test_handoff_recovery_keeps_both_terminal_bodies_and_unique_custody(self):
        sim,donor = self.carrier('handoff')
        task_id = sim.robots[donor]['task_id']
        goods_id = sim.robots[donor]['cargo_id']
        self.assertTrue(sim.report_soc(donor,24.))
        helper = sim.assists[donor]['receiver']
        self.assertEqual(sim.jobs[donor]['dock_id'],sim.nav.canonical_location(sim.robots[donor]['home_id']))
        self.assert_complete_occupancy(sim)
        self.assertTrue(sim.inject_delay(helper,20))
        self.assert_complete_occupancy(sim)
        for _ in range(700):
            sim.step()
        self.assertEqual(sim.tasks[task_id]['status'],'completed')
        events = [e for e in sim.pending_events if e.get('task_id')==task_id]
        self.assertEqual(sum(e['kind']=='cargo_handoff' for e in events),1)
        self.assertEqual(sum(e['kind'] in {'placed','shipped'} for e in events),1)
        self.assertFalse(sim.robots[donor]['carrying'])
        self.assertNotEqual(sim.goods[goods_id],f'ROBOT-{donor}')
        self.assert_complete_occupancy(sim)
        sim.assert_inventory()

    def test_actual_disk_restore_preserves_in_progress_handoff_and_all_idle_bodies(self):
        sim,donor = self.carrier('restore')
        self.assertTrue(sim.report_soc(donor,24.))
        helper = sim.assists[donor]['receiver']
        self.assertTrue(sim.inject_delay(helper,20))
        for _ in range(5):
            sim.step()
        path = Path(self.temp.name)/'wide-real-checkpoint.json'
        write_checkpoint(path,sim.checkpoint())
        restored = Simulation(self.scene,self.dataset,sim.run_id,checkpoint=read_checkpoint(path))
        self.assert_complete_occupancy(restored)
        for _ in range(180):
            sim.step()
            restored.step()
        self.assertEqual(sim.robots,restored.robots)
        self.assertEqual(sim.jobs,restored.jobs)
        self.assertEqual(sim.tasks,restored.tasks)
        self.assertEqual(sim.goods,restored.goods)
        self.assertEqual(sim.assists,restored.assists)
        self.assertEqual(sim.reservations.fixed,restored.reservations.fixed)
        self.assert_complete_occupancy(sim)
        self.assert_complete_occupancy(restored)


if __name__=='__main__':
    unittest.main()
