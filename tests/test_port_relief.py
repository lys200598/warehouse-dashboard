from collections import Counter
from copy import deepcopy
from types import SimpleNamespace
import unittest

from backend.scheduling_v2.sim import Simulation as V2
from backend.scheduling_v5.sim import CandidateSimulation, Config, resolve_parameters
from backend.scheduling_v5 import port_relief as pr


class PortReliefTests(unittest.TestCase):
    def make(self):
        tasks = {
            'donor':dict(task_id='donor',task_type='INBOUND',status='waiting',
                source_id='IN-1',target_id='S1',release_second=1,due_second=330),
            'accepted':dict(task_id='accepted',task_type='INBOUND',status='accepted',
                source_id='IN-1',target_id='S2',release_second=1,due_second=5000,ready_at=300),
            'out':dict(task_id='out',task_type='OUTBOUND',status='waiting',
                source_id='S3',target_id='OUT-1',release_second=1,due_second=5100,ready_at=300),
            'future':dict(task_id='future',task_type='INBOUND',status='scheduled')}
        return SimpleNamespace(tasks=tasks,pending=list(tasks),elapsed=300,inside=0,
            slots={f'S{i}':{'occupied':False,'reservedBy':None} for i in range(1,5)},
            ports={'IN-1':6},roadmap=SimpleNamespace(travel_time=lambda *args:58),
            config=Config(port_relief_policy='deadline'))

    def test_config_defaults_and_bad_flags(self):
        c=Config();self.assertEqual((c.inbound_admission_policy,c.port_relief_policy),('fifo','off'))
        for key in ('inbound_admission_policy','port_relief_policy'):
            for bad in ('unknown',True,None):
                with self.subTest(key=key,bad=bad),self.assertRaises(ValueError):Config(**{key:bad})

    def test_admission_stable_edf_only_released_waiting_in(self):
        sim=self.make();sim.tasks['later']={**sim.tasks['donor'],'task_id':'later','due_second':600}
        sim.tasks['tie']={**sim.tasks['donor'],'task_id':'tie'};sim.pending=['later','tie',*sim.pending]
        self.assertEqual(pr.deadline_admission_order(sim),['donor','tie','later'])
        sim.tasks['later']['release_second']=301
        self.assertEqual(pr.deadline_admission_order(sim),['donor','tie'])

    def test_only_port_full_donates_not_other_resource_blocks(self):
        sim=self.make();before=deepcopy(sim.tasks);self.assertEqual(pr.context(sim)['IN-1']['waiting'],1)
        self.assertEqual(sim.tasks,before)
        for kind in ('port_not_full','occupied','reserved','warehouse_full'):
            s=self.make()
            if kind=='port_not_full':s.ports['IN-1']=5
            if kind=='occupied':s.slots['S1']['occupied']=True
            if kind=='reserved':s.slots['S1']['reservedBy']='another'
            if kind=='warehouse_full':s.inside=len(s.slots)
            with self.subTest(kind=kind):self.assertEqual(pr.context(s),{})

    def test_row_changes_only_real_feasible_columns_and_preserves_task(self):
        sim=self.make();task=sim.tasks['accepted'];before=deepcopy(task)
        row,slack,reward=pr.row_adjustment(sim,task,[100,1e12,1e8,1e8],[100,120],pr.context(sim),1e8)
        self.assertEqual(row,[160,1e12,1e8,1e8]);self.assertEqual(slack,-80);self.assertGreater(reward,300)
        self.assertEqual(task,before)
        row,slack,reward=pr.row_adjustment(sim,sim.tasks['out'],[100,1e8],[100],pr.context(sim),1e8)
        self.assertEqual((row,slack,reward),([100,1e8],None,0))

    def test_urgent_recipient_pool_key_without_factual_deadline_mutation(self):
        sim=self.make();before=deepcopy(sim.tasks)
        self.assertLess(pr.priority(sim.tasks['accepted'],pr.context(sim),300),
            pr.priority(sim.tasks['out'],pr.context(sim),300))
        self.assertEqual(sim.tasks,before)

    def select(self,sim,costs,times):
        sim.policy='port-guard';sim.policy_parameters=resolve_parameters('port-guard')
        sim.metrics=Counter();return CandidateSimulation._select_assignment(sim,costs,
            [sim.tasks['accepted'],sim.tasks['out']],times,[])

    def test_guard_donation_protects_pickup_without_changing_cardinality(self):
        sim=self.make();before=deepcopy(sim.tasks)
        selected=self.select(sim,[[100,1e8,1e8],[10,1e8,1e8]],[[100],[10]])
        self.assertIn((0,0),selected);self.assertEqual(sum(c==0 for r,c in selected),1)
        self.assertEqual(sim.tasks,before)

    def test_pickup_cost_gives_busy_port_nearer_robot(self):
        sim=self.make()
        selected=self.select(sim,[[100,120,1e8,1e8],[10,50,1e8,1e8]],[[100,120],[10,50]])
        self.assertEqual(dict(selected),{0:0,1:1})
        sim.config.port_relief_policy='off'
        selected=self.select(sim,[[100,120,1e8,1e8],[10,50,1e8,1e8]],[[100,120],[10,50]])
        self.assertEqual(dict(selected),{0:1,1:0})

    def test_admission_still_obeys_six_place_gate_and_reserves_only_one(self):
        for policy,expected in [('fifo','late'),('deadline','early')]:
            tasks={key:dict(task_id=key,task_type='INBOUND',status='waiting',source_id='IN-1',
                target_id=key,goods_id='G-'+key,release_second=0,due_second=due)
                for key,due in [('late',600),('early',300)]}
            sim=SimpleNamespace(tasks=tasks,pending=['late','early'],elapsed=1,cursor=0,release_order=[],
                slots={key:{'occupied':False,'reservedBy':None} for key in ['late','early','free']},
                inside=0,goods={},ports=Counter({'IN-1':5}),metrics=Counter(),event=lambda *a,**kw:None)
            sim._inbound_admission_order=lambda:pr.deadline_admission_order(sim) if policy=='deadline' else sim.pending
            V2._release(sim)
            self.assertEqual(sim.ports['IN-1'],6);self.assertEqual(sim.metrics['received'],1)
            self.assertEqual(sim.tasks[expected]['status'],'accepted');self.assertEqual(sim.slots[expected]['reservedBy'],expected)

    def test_capability_mask_precedes_relief_and_is_not_reenabled(self):
        sim=self.make();sim._fleet_is_mixed=True
        sim.task_capability_reason=lambda robot,task: 'high_shelf' if task['task_id']=='accepted' and robot['robot_id']==1 else None
        sim.capability_spare_cost=lambda robot,task:0.
        sim.policy='port-guard';sim.policy_parameters=resolve_parameters('port-guard');sim.metrics=Counter()
        costs=[[10,100,1e8,1e8],[20,30,1e8,1e8]];before=deepcopy(costs)
        result=CandidateSimulation._select_assignment(sim,costs,
            [sim.tasks['accepted'],sim.tasks['out']],[[100,180],[20,30]],
            [{'robot_id':1},{'robot_id':2}])
        self.assertEqual(dict(result),{0:1,1:0})
        self.assertEqual(costs,before)

    def test_failed_pair_and_dummy_columns_remain_forbidden_in_matcher(self):
        sim=self.make()
        selected=self.select(sim,[[1e12,120,1e8,1e8],[10,50,1e8,1e8]],[[100,120],[10,50]])
        self.assertEqual(dict(selected),{0:1,1:0})
        selected=self.select(sim,[[1e12,1e12,1e8,1e8],[10,50,1e8,1e8]],[[100,120],[10,50]])
        self.assertFalse(any(row==0 and col<2 for row,col in selected))
        self.assertEqual(sum(col<2 for row,col in selected),1)

    def test_edf_admission_preserves_all_capacity_slot_and_release_gates(self):
        for blocked in ('warehouse_full','occupied','reserved','port_full'):
            task=dict(task_id='early',task_type='INBOUND',status='waiting',source_id='IN-1',
                target_id='slot',goods_id='G',release_second=0,due_second=300)
            sim=SimpleNamespace(tasks={'early':task},pending=['early'],elapsed=1,cursor=0,release_order=[],
                slots={'slot':{'occupied':blocked=='occupied','reservedBy':'other' if blocked=='reserved' else None}},
                inside=1 if blocked=='warehouse_full' else 0,goods={},
                ports=Counter({'IN-1':6 if blocked=='port_full' else 0}),metrics=Counter(),event=lambda *a,**kw:None)
            sim._inbound_admission_order=lambda:pr.deadline_admission_order(sim)
            before=(deepcopy(sim.slots),deepcopy(sim.ports),sim.inside)
            V2._release(sim)
            with self.subTest(blocked=blocked):
                self.assertEqual(task['status'],'waiting');self.assertEqual(sim.metrics['received'],0)
                self.assertEqual((sim.slots,sim.ports,sim.inside),before)
                self.assertEqual(sim.goods,{})

    def test_newly_released_task_is_visible_to_edf_in_same_release_call(self):
        tasks={key:dict(task_id=key,task_type='INBOUND',status='scheduled',source_id='IN-1',
            target_id=key,goods_id='G-'+key,release_second=release,due_second=due)
            for key,release,due in [('now',1,200),('future',2,100)]}
        sim=SimpleNamespace(tasks=tasks,pending=[],elapsed=1,cursor=0,release_order=['now','future'],
            slots={key:{'occupied':False,'reservedBy':None} for key in tasks},inside=0,goods={},
            ports=Counter(),metrics=Counter(),event=lambda *a,**kw:None)
        sim._inbound_admission_order=lambda:pr.deadline_admission_order(sim)
        V2._release(sim)
        self.assertEqual(tasks['now']['status'],'accepted')
        self.assertEqual(tasks['future']['status'],'scheduled')
        self.assertEqual(sim.cursor,1);self.assertEqual(sim.metrics['received'],1)
        self.assertEqual(sim.goods,{'G-now':'IN-1'})


if __name__=='__main__':unittest.main()
