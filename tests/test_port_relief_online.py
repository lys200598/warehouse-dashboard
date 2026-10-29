from copy import deepcopy
import importlib.util
from pathlib import Path
from types import SimpleNamespace
import unittest


path=Path(__file__).resolve().parents[1]/'backend/scheduling_v5/port_relief.py'
spec=importlib.util.spec_from_file_location('port_relief_online',path)
module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)


class OnlineCadenceTests(unittest.TestCase):
    def make(self, picked=(), received=None, source='IN-1'):
        received=[0]*len(picked) if received is None else received
        tasks={f'H{i}':dict(task_id=f'H{i}',task_type='INBOUND',status='completed',
            source_id=source,picked_at=p,received_at=r) for i,(p,r) in enumerate(zip(picked,received))}
        return SimpleNamespace(tasks=tasks,elapsed=5000,
            config=SimpleNamespace(pick_seconds=10,place_seconds=12,speed=.8))

    def cadence(self,sim,source='IN-1'):
        return module.observed_pickup_cadences(sim,[source])[source]

    def test_fallback_ignores_unpublished_future_and_outbound_rows(self):
        sim=self.make()
        for key,status,typ,picked in [('future','completed','INBOUND',6000),
            ('unpublished','scheduled','INBOUND',1),('out','completed','OUTBOUND',2)]:
            sim.tasks[key]=dict(task_id=key,status=status,task_type=typ,source_id='IN-1',picked_at=picked,received_at=0)
        self.assertEqual(self.cadence(sim),10)

    def test_empty_port_intervals_are_excluded(self):
        sim=self.make([10,30,100,120],[0,5,90,95])
        self.assertEqual(self.cadence(sim),20)

    def test_recent_twelve_valid_gaps_not_whole_run_median(self):
        picked=[10]
        for gap in [50]*20+[12]*12:picked.append(picked[-1]+gap)
        sim=self.make(picked)
        self.assertEqual(self.cadence(sim),12)
        sim.tasks=dict(reversed(list(sim.tasks.items())))
        self.assertEqual(self.cadence(sim),12)

    def test_estimate_clips_at_pick_time_and_sixty_and_is_per_port(self):
        self.assertEqual(self.cadence(self.make([10,11,12])),10)
        sim=self.make([10,110,210]);self.assertEqual(self.cadence(sim),60)
        for key,row in self.make([10,22,34],source='IN-2').tasks.items():sim.tasks['other'+key]=row
        self.assertEqual(self.cadence(sim,'IN-1'),60)
        self.assertEqual(self.cadence(sim,'IN-2'),12)

    def test_context_uses_observed_density_without_mutating_facts(self):
        sim=self.make([100,120,140]);sim.pending=[]
        sim.slots={f'S{i}':{'occupied':False,'reservedBy':None} for i in range(3)}
        sim.ports={'IN-1':6};sim.inside=0;sim.elapsed=300
        sim.roadmap=SimpleNamespace(travel_time=lambda *args:58)
        for i,latest in enumerate([600,601,602]):
            key=f'W{i}';sim.pending.append(key)
            sim.tasks[key]=dict(task_id=key,task_type='INBOUND',status='waiting',source_id='IN-1',
                target_id=f'S{i}',release_second=0,due_second=latest+80)
        before=deepcopy(sim.tasks)
        self.assertEqual(module.context(sim)['IN-1']['latest_pickup'],562)
        self.assertEqual(sim.tasks,before)


if __name__=='__main__':unittest.main()
