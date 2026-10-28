from copy import deepcopy
import json
from pathlib import Path
import unittest

from backend.knowledge import KnowledgeError
from backend.wide.graph import WideMirror, audit_wide
from backend.wide.queries import WideQueries
from tests.test_scheduler_v3_graph import Transaction


REPO = Path(__file__).resolve().parents[1]
SCENE = REPO/'data/layouts/warehouse100-wide-v2.json'
if not SCENE.exists():
    SCENE = REPO.parent/'designs/warehouse100-wide-v2-20261027/warehouse100-wide-v2.json'


def batch(seq=1):
    scene = json.loads(SCENE.read_text(encoding='utf-8'))
    robots = [{'robot_id':i+1,'robot_type_key':'X3','x':home['point']['x'],'y':home['point']['y'],
        'home_id':home['id'],'stage':'idle','dock_id':None,'task_id':None} for i,home in enumerate(scene['homes'])]
    event = {'event_id':'wide-test:event:1','event_seq':1,'tick':10,'sim_time':10,'kind':'cargo_handoff',
             'robot_id':2,'task_id':'T1','goods_id':'G1','location_id':'ROBOT-2',
             'donor_robot_id':1,'receiver_robot_id':2,'reason':'low_energy'}
    return {'run_id':'wide-test','seq':seq,'tick':10,'sim_time':10,'completed':0,
        'metadata':{'domain':'warehouse-wide-inbound-outbound','scene':scene},
        'config':{'chargers':100},'robots':robots,'tasks':[],'slots':[], 'racks':[], 'events':[event]}


class WideGraphTests(unittest.TestCase):
    def test_canonical_bays_and_aliases_not_two_hundred_physical_resources(self):
        tx = Transaction()
        WideMirror._apply(tx,[batch()])
        writes = [(q,p) for q,p in tx.calls if 'MERGE (l:WHLocation' in q]
        rows = writes[0][1]['rows']
        self.assertEqual(sum(r.get('physical_bay') is True for r in rows),100)
        self.assertEqual(sum(r['kind']=='home-alias' for r in rows),100)
        self.assertEqual(sum(r['kind']=='charger' for r in rows),100)
        alias = next(p['rows'] for q,p in tx.calls if 'MERGE (a)-[:ALIAS_OF]' in q)
        self.assertEqual(len(alias),100)
        self.assertEqual(alias[0],{'key':'wide-test:P001','bay_key':'wide-test:C-A01'})
        physical = next(p['rows'] for q,p in tx.calls if 'SET l:WHCharger:WHPhysicalBay' in q)
        self.assertEqual(len(physical),100)

    def test_all_hundred_charger_states_include_idle_fully_charged_and_faulted_bodies(self):
        value=batch()
        value['robots'][1]['stage']='charging';value['robots'][1]['dock_id']='C-A02'
        value['robots'][2]['stage']='fault_wait'
        tx=Transaction();WideMirror._apply(tx,[value])
        rows=next(p['rows'] for q,p in tx.calls if 'MERGE (v:WHChargerState' in q)
        self.assertEqual(len(rows),100)
        self.assertEqual(rows[0]['occupant_robot_ids'],[1])
        self.assertIsNone(rows[0]['charging_robot_id'])
        self.assertEqual(rows[1]['charging_robot_id'],2)
        self.assertEqual(rows[2]['occupant_stage'],'fault_wait')
        self.assertEqual({r['physical_bay_id'] for r in rows}, {d['id'] for d in value['metadata']['scene']['docks']})

    def test_duplicate_body_on_one_bay_is_rejected(self):
        value=batch();value['robots'][1].update(x=value['robots'][0]['x'],y=value['robots'][0]['y'])
        with self.assertRaises(ValueError):WideMirror._apply(Transaction(),[value])

    def test_handoff_custody_written_once_with_complete_relations(self):
        tx=Transaction();WideMirror._apply(tx,[batch()])
        goods=[p['rows'] for q,p in tx.calls if 'MERGE (v:WHGoodsState' in q]
        self.assertEqual(len(goods),1);self.assertEqual(len(goods[0]),1)
        self.assertEqual(goods[0][0]['location_id'],'ROBOT-2')
        handoffs=next(p['rows'] for q,p in tx.calls if 'MERGE (e)-[:DONOR]' in q)
        self.assertEqual(handoffs[0]['cargo_key'],'wide-test:G1')

    def test_replay_does_not_rewrite_current_custody_or_aliases(self):
        tx=Transaction();value=batch();WideMirror._apply(tx,[value]);before=len(tx.calls)
        WideMirror._apply(tx,[value])
        replay=tx.calls[before:]
        self.assertFalse(any('MERGE (v:WHGoodsState' in q or 'ALIAS_OF' in q for q,p in replay))
        modified=deepcopy(value);modified['events'][0]['reason']='changed'
        with self.assertRaises(KnowledgeError):WideMirror._apply(tx,[modified])

    def test_bad_registry_and_recovery_participants_are_rejected_before_write(self):
        for edit in [lambda b:b['metadata']['scene']['docks'].pop(),
                     lambda b:b['metadata']['scene']['homes'][0].update(alias_of='missing'),
                     lambda b:b['metadata']['scene']['homes'][1].update(alias_of='C-A01'),
                     lambda b:b['config'].update(chargers=101),
                     lambda b:b['events'][0].update(receiver_robot_id=1)]:
            value=batch();edit(value);tx=Transaction()
            with self.assertRaises(ValueError):WideMirror._apply(tx,[value])
            self.assertEqual(tx.calls,[])
        value=batch();value['events']=[{'event_id':'wide-test:e2','event_seq':2,'tick':10,'sim_time':10,
            'kind':'traffic_rerouted','robot_id':None,'task_id':None,'affected_robots':[3,1,2,3],'reason':'actual_stop'}]
        tx=Transaction();WideMirror._apply(tx,[value])
        rows=next(p['rows'] for q,p in tx.calls if 'MERGE (e)-[:AFFECTS]' in q)
        self.assertEqual(rows[0]['affected_robot_ids'],[1,2,3])

    def test_gap_and_mixed_run_preserve_parent_commit_identity_rules(self):
        with self.assertRaises(KnowledgeError):WideMirror._apply(Transaction(),[batch(2)])
        a,b=batch(),batch(2);b['run_id']='different'
        with self.assertRaises(KnowledgeError):WideMirror._apply(Transaction(),[a,b])


class QueryMirror:
    def __init__(self):self.calls=[];self.head=8;self.reply=[]
    def query(self,q,p):
        self.calls.append((q,deepcopy(p)))
        if 'RETURN r.committed_seq AS seq' in q:return [{'seq':self.head}]
        return deepcopy(self.reply)


class WideQueryTests(unittest.TestCase):
    def test_goods_history_has_stable_cutoff_and_explicit_pagination_not_mutable_current(self):
        m=QueryMirror();m.reply=[{'location_id':'ROBOT-2','event_seq':30,'sim_time':100,
            'history':[{'event_seq':1},{'event_seq':2},{'event_seq':3}]}]
        result=WideQueries(m).goods_history('wide-test','G1',seq=5,limit=2)
        self.assertEqual(result['seq'],5);self.assertTrue(result['has_more'])
        self.assertEqual(result['next_after_event_seq'],2)
        self.assertEqual(result['current_as_of']['event_seq'],30)
        q,p=m.calls[-1]
        self.assertIn('v.seq<=$seq',q);self.assertIn('h.seq<=$seq',q)
        self.assertNotIn('current_location_id',q)
        self.assertEqual(p['limit'],3)

    def test_robot_context_uses_exact_state_and_historical_task_not_current_task_properties(self):
        m=QueryMirror();m.reply=[{'robot_payload':json.dumps({'robot_id':100,'task_id':'T1'}),
            'task_payload':json.dumps({'task_id':'T1','status':'carrying'})}]
        result=WideQueries(m).robot_context('wide-test',100,seq=4)
        self.assertEqual(result['task']['status'],'carrying')
        q,p=m.calls[-1];self.assertEqual(p['state_key'],'wide-test:4:100')
        self.assertIn('ts.seq<=$seq',q);self.assertNotIn('t.status',q)

    def test_cooperation_task_and_run_paths_are_bounded_and_parameterized(self):
        m=QueryMirror();m.reply=[{'event_seq':5,'seq':2,'payload':'{"kind":"cargo_handoff"}'}]
        for task in [None,'T1']:
            result=WideQueries(m).cooperation_history('wide-test',task_id=task,seq=3)
            self.assertEqual(result['rows'][0]['event']['kind'],'cargo_handoff')
            q,p=m.calls[-1];self.assertIn('e.commit_seq<=$seq',q)
            self.assertIn('USING INDEX r:WHRun(run_id)',q)
            self.assertEqual(p['task_key'],'wide-test:T1' if task else None)

    def test_invalid_cutoffs_ids_and_limits_do_not_execute_data_query(self):
        m=QueryMirror();api=WideQueries(m)
        for fn in [lambda:api.goods_history('wide-test','G1',seq=9),
                   lambda:api.robot_context('wide-test',True),
                   lambda:api.robot_context('wide-test',101),
                   lambda:api.cooperation_history('wide-test',limit=1001),
                   lambda:api.goods_history('wide-test','G1',after_event_seq=-1),
                   lambda:api.goods_history('bad\nrun','G1')]:
            before=len(m.calls)
            with self.assertRaises(ValueError):fn()
            self.assertLessEqual(len(m.calls)-before,1)


if __name__=='__main__':unittest.main()
