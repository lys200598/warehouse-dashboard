from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from backend.knowledge_runtime.policy import authorize, DecisionRejected
from backend.knowledge_runtime.sim import KnowledgeSimulation
from backend.real_scale.writer import Config


def context_for(sim,task):
    events=[deepcopy(e) for e in sim.pending_events if e.get('task_id')==task['task_id']]
    cargo=[e for e in events if e.get('goods_id')==task['goods_id'] and e.get('location_id')]
    goods=[{'seq':1,'event_seq':e['event_seq'],'location_id':e['location_id']} for e in cargo]
    coop=[{'seq':1,'event_seq':e['event_seq'],'event':e} for e in events
          if e['kind'] in {'assistance_requested','handoff_reserved','cargo_handoff','assistance_deferred'}]
    common={'run_id':sim.run_id,'seq':1,'graph_committed_seq':1,'has_more':False,'next_after_event_seq':None}
    return {'run_id':sim.run_id,'seq':1,'graph_committed_seq':1,'batch_checksum':'source',
            'task_id':task['task_id'],'goods_id':task['goods_id'],'focus_task':deepcopy(task),
            'request_cursors':{'goods_after':0,'cooperation_after':0},
            'goods_history':{**common,'goods_id':task['goods_id'],'rows':goods,'current_as_of':goods[-1]},
            'cooperation':{**common,'task_id':task['task_id'],'rows':coop}}


class LocalTestProvider:
    def __init__(self):self.calls=0;self.bad=False
    def context(self,sim,task):
        self.calls+=1
        result=context_for(sim,task)
        if self.bad:result['focus_task']['robot_id']=1000
        return result


class RuntimeKnowledgeTests(unittest.TestCase):
    root=Path(__file__).resolve().parents[1]

    def make(self,run='knowledge-runtime-unit',mode='required'):
        # Publication sample is real, small seven-field warehouse input.
        dataset=self.root/'data/examples/warehouse-v5'
        sim=KnowledgeSimulation(self.root/'data/layouts/warehouse100-wide-v2.json',dataset,run,
            Config(charging_policy='none'),knowledge_mode=mode,commit_seconds=5)
        sim.history_provider=LocalTestProvider()
        return sim

    def carrying(self,sim):
        for _ in range(1800):
            sim.step()
            carriers=[i for i,r in sim.robots.items() if r['carrying'] and i not in sim.assists]
            if carriers:return carriers[0]
        self.fail('fixture must produce an actual carrier')

    def test_required_context_rejection_prevents_planning_and_preserves_reservations(self):
        sim=self.make();owner=self.carrying(sim);sim.history_provider.bad=True
        before=deepcopy((sim.robots,sim.tasks,sim.jobs,sim.goods,sim.assists))
        with self.assertRaises(DecisionRejected):sim.request_assistance(owner)
        self.assertEqual(before,(sim.robots,sim.tasks,sim.jobs,sim.goods,sim.assists))
        self.assertEqual(sim.knowledge_stats['approved_plans'],0)

    def test_queries_gate_actual_plan_and_actual_physical_custody_transfer(self):
        sim=self.make();owner=self.carrying(sim)
        self.assertTrue(sim.request_assistance(owner))
        receiver=sim.assists[owner]['receiver'];task_id=sim.assists[owner]['task_id']
        while sim.metrics['handoffs']==0:sim.step()
        task=sim.tasks[task_id]
        self.assertEqual(task['robot_id'],receiver)
        self.assertEqual(sim.goods[task['goods_id']],f'ROBOT-{receiver}')
        self.assertEqual(sim.history_provider.calls,2)
        self.assertEqual(sim.knowledge_stats['approved_transfers'],1)
        actions=[e for e in sim.pending_events if e['kind']=='knowledge_action_applied']
        self.assertEqual([e['action'] for e in actions],['handoff_plan_reserved','cargo_transferred'])
        sim.assert_inventory()

    def test_transfer_read_failure_occurs_before_any_robot_moves(self):
        sim=self.make();owner=self.carrying(sim);self.assertTrue(sim.request_assistance(owner))
        at=sim.assists[owner]['handoff_at']
        while sim.elapsed<at-1:sim.step()
        sim.history_provider.bad=True
        before=deepcopy((sim.elapsed,sim.robots,sim.jobs,sim.goods,sim.tasks))
        with self.assertRaises(DecisionRejected):sim.step()
        self.assertEqual(before,(sim.elapsed,sim.robots,sim.jobs,sim.goods,sim.tasks))
        self.assertEqual(sim.metrics['handoffs'],0)

    def test_rules_mode_needs_no_provider(self):
        sim=self.make(mode='rules');sim.history_provider=None
        owner=self.carrying(sim);self.assertTrue(sim.request_assistance(owner))
        self.assertEqual(sim.knowledge_stats['queries'],0)

    def test_restore_preserves_mode_and_knowledge_counters(self):
        sim=self.make();self.carrying(sim);state=sim.checkpoint()
        restored=self.make();restored.restore(state)
        self.assertEqual(restored.knowledge_stats,sim.knowledge_stats)
        wrong=self.make(mode='rules')
        with self.assertRaisesRegex(ValueError,'mode/source'):wrong.restore(state)

    def test_missing_provider_is_explicit_and_not_silent_rules_fallback(self):
        sim=self.make();owner=self.carrying(sim);sim.history_provider=None
        with self.assertRaises(DecisionRejected):sim.request_assistance(owner)

    def test_foreign_future_truncated_and_wrong_receiver_knowledge_rejected(self):
        sim=self.make();owner=self.carrying(sim);task=sim.tasks[sim.robots[owner]['task_id']]
        base=context_for(sim,task)
        edits=[lambda c:c.update(run_id='foreign'),lambda c:c.update(seq=2),
               lambda c:c['goods_history'].update(has_more=True),
               lambda c:c['request_cursors'].update(goods_after=1)]
        for edit in edits:
            got=deepcopy(base);edit(got)
            with self.assertRaises(DecisionRejected):authorize(got,run=sim.run_id,donor=owner,task=task,goods=task['goods_id'])
        with self.assertRaises(DecisionRejected):authorize(base,run=sim.run_id,donor=owner,task=task,goods=task['goods_id'],receiver=99)


if __name__=='__main__':unittest.main()
