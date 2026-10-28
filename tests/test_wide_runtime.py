from copy import deepcopy
from pathlib import Path
import tempfile
import unittest

from backend.wide.data import load_scene
from backend.wide.scenarios import generate_scenario
from backend.wide.sim import Simulation


class WideRuntimeTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.root=Path(__file__).resolve().parents[1]
        cls.scene=cls.root/'data/layouts/warehouse100-wide-v2.json'
        cls.temp=tempfile.TemporaryDirectory(dir=cls.root/'data/local-check')
        cls.dataset=Path(cls.temp.name)/'input'
        generate_scenario(cls.scene,cls.dataset,duration_seconds=3600,slot_interval_seconds=7200,active_slots=192)

    @classmethod
    def tearDownClass(cls):
        cls.temp.cleanup()

    def make(self):
        return Simulation(self.scene,self.dataset,'wide-runtime')

    def test_real_tasks_and_all_idle_bodies_persist(self):
        sim=self.make()
        self.assertEqual(len(sim.reservations.fixed),100)
        self.assertEqual(len(sim.docks),100)
        for _ in range(700):
            sim.step()
        self.assertGreater(sim.metrics['completed'],0)
        for owner,r in sim.robots.items():
            if owner not in sim.jobs:
                self.assertIn(owner,sim.reservations.fixed)
        self.assertGreaterEqual(sim.metrics['min_separation_m'],1.1-1e-6)
        sim.assert_inventory()

    def test_restore_retains_terminal_bodies_and_exact_future(self):
        from backend.checkpoint import read_checkpoint,write_checkpoint
        sim=self.make()
        for _ in range(300):sim.step()
        path=Path(self.temp.name)/'checkpoint.json'
        write_checkpoint(path,sim.checkpoint())
        restored=Simulation(self.scene,self.dataset,'wide-runtime',checkpoint=read_checkpoint(path))
        for _ in range(100):sim.step();restored.step()
        self.assertEqual(sim.robots,restored.robots)
        self.assertEqual(sim.goods,restored.goods)
        self.assertEqual(sim.jobs,restored.jobs)
        for owner in sim.robots:
            self.assertEqual(sim.reservations.fixed.get(owner),restored.reservations.fixed.get(owner))


if __name__=='__main__':unittest.main()
