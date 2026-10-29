import importlib.util
from pathlib import Path
from types import SimpleNamespace
import unittest


path=Path(__file__).resolve().parents[1]/'backend/scheduling_v5/port_relief.py'
spec=importlib.util.spec_from_file_location('port_relief_envelope',path)
module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)


class EnvelopeTests(unittest.TestCase):
    def make(self, latest, *, travel=None, due=None):
        travel=[58]*len(latest) if travel is None else travel
        due=[d+t+22 for d,t in zip(latest,travel)] if due is None else due
        tasks={str(i):dict(task_id=str(i),task_type='INBOUND',status='waiting',
            source_id='IN-1',target_id='S'+str(i),release_second=0,due_second=d)
            for i,d in enumerate(due)}
        return SimpleNamespace(tasks=tasks,pending=list(tasks),elapsed=300,inside=0,
            slots={'S'+str(i):{'occupied':False,'reservedBy':None} for i in range(len(latest))},
            ports={'IN-1':6},config=SimpleNamespace(speed=.8,pick_seconds=10,place_seconds=12),
            roadmap=SimpleNamespace(travel_time=lambda source,target,speed:travel[int(target[1:])]))

    def deadline(self,sim):return module.context(sim)['IN-1']['latest_pickup']

    def test_single_waiter_matches_original_bound(self):
        self.assertEqual(self.deadline(self.make([600])),600)

    def test_dense_deadlines_require_earlier_first_clearance(self):
        sim=self.make([600,601,602])
        self.assertEqual(self.deadline(sim),582)
        value=module.context(sim)['IN-1'];self.assertEqual(value['waiting'],3)
        self.assertNotIn('_deadlines',value)

    def test_sparse_deadlines_keep_earliest_deadline(self):
        self.assertEqual(self.deadline(self.make([600,620,640])),600)

    def test_ranking_uses_latest_admission_not_raw_due_or_pending_order(self):
        sim=self.make([478,570],travel=[200,58],due=[700,650])
        self.assertEqual(self.deadline(sim),478)
        sim.pending.reverse();self.assertEqual(self.deadline(sim),478)

    def test_unreleased_or_other_resource_blocked_demand_cannot_tighten_envelope(self):
        for block in ('unreleased','scheduled','occupied','reserved'):
            sim=self.make([600,0]);task=sim.tasks['1']
            if block=='unreleased':task['release_second']=301
            if block=='scheduled':task['status']='scheduled'
            if block=='occupied':sim.slots['S1']['occupied']=True
            if block=='reserved':sim.slots['S1']['reservedBy']='another'
            with self.subTest(block=block):self.assertEqual(self.deadline(sim),600)


if __name__=='__main__':unittest.main()
