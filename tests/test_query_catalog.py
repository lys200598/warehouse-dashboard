"""Independent fixed facts, adversarial source mutations and pack expectations."""
from copy import deepcopy
import json
from pathlib import Path
import threading
import unittest
from unittest.mock import patch

from backend.knowledge import digest
from backend.query_catalog.service import (QueryCatalogService, IncompleteContext, CATALOG,
    FLEET_QUERY, EVENTS_QUERY, MEMORY_QUERY, TASK_COUNTS_QUERY, TASK_POINT_QUERY, GOODS_QUERY, event_feedback)

ROOT=Path(__file__).resolve().parents[1]
SCENE=json.loads((ROOT/'data/layouts/warehouse100-wide-v2.json').read_text(encoding='utf-8'))
RUN='catalog-unit';SEQ=100;CHECKSUM='a'*64


def robot(i):
    return {'robot_id':i,'x':float(i),'y':20.,'battery':60.,'carrying':False,
        'cargo_id':None,'task_id':None,'stage':'idle','distance':100.}


def point():
    return {'run_id':RUN,'head':120,'created_at':'2026-10-28T00:00:00Z','batch_checksum':CHECKSUM}


def event(number,kind,time,*,commit=98,owner=1,task='T1'):
    payload={'event_seq':number,'kind':kind,'sim_time':time,'robot_id':owner,'task_id':task}
    return {'run_id':RUN,'commit_seq':commit,'event_seq':number,'payload':json.dumps(payload)}


class FakeStore:
    """Unit fixture, not simulation/Neo4j/performance evidence."""
    timeout=15
    def __init__(self):
        self.mirror=self;self.calls=[];self.mutation=None;self.closed=False
        self.task={'task_id':'T1','goods_id':'G1','status':'completed','robot_id':1}
        self.robots=[robot(i) for i in range(1,101)]
        self.events=[event(1,'task_released',10),event(2,'task_assigned',15),
            event(3,'picked',20),event(4,'shipped',40)]

    def query(self,query,params,timeout=5):
        self.calls.append((query,deepcopy(params)))
        row=point()
        if query==FLEET_QUERY:
            row.update(source_run=RUN,metadata={'scene':deepcopy(SCENE)},config={'task_reserve_soc':20,'emergency_reserve_soc':10},
                step={'seq':SEQ,'tick':500,'sim_time':500,'completed':1,'running':True},
                robots=[{'run_id':RUN,'seq':SEQ,'robot_id':p['robot_id'],'payload':deepcopy(p),
                    'real_payload_sha256':digest(p)} for p in self.robots],
                bays=[{'run_id':RUN,'seq':SEQ,'charger_id':d['id'],'physical_bay_id':d['id'],
                    'capacity':1,'reserved_robot_ids':[],'occupant_robot_id':None,'occupant_robot_ids':[],
                    'charging_robot_id':None,'occupant_stage':None} for d in SCENE['docks']])
        elif query==EVENTS_QUERY:
            row['events']=[deepcopy(e) for e in self.events if params['start']<=e['commit_seq']<=params['seq']
                and (params['robot'] is None or json.loads(e['payload'])['robot_id']==params['robot'])
                and (params['task'] is None or json.loads(e['payload'])['task_id']==params['task'])][:params['limit']]
        elif query==MEMORY_QUERY:
            p=self.robots[params['robot']-1]
            row['states']=[{'run_id':RUN,'seq':i,'robot_id':params['robot'],'payload':deepcopy(p),
                'real_payload_sha256':digest(p)} for i in range(params['start'],params['seq']+1)][:params['limit']]
        elif query==TASK_COUNTS_QUERY:
            row.update(entity_count=3,counts=[{'zone':'WN','status':'completed','tasks':1,'overdue':0},
                {'zone':'WN','status':'waiting','tasks':2,'overdue':1}])
        elif query==TASK_POINT_QUERY:
            if params['task']!='T1':return []
            row.update(task=deepcopy(self.task),task_seq=98)
        elif query==GOODS_QUERY:
            row['history']=[{'seq':98,'event_seq':3,'sim_time':20,'kind':'picked','location_id':'ROBOT-1','event_key':'e3'}]
        else:raise AssertionError('unknown query')
        if self.mutation:self.mutation(query,row)
        return [row]

    def robot_context(self,run,seq,ids):
        return {'run_id':run,'seq':seq,'graph_committed_seq':120,'batch_checksum':CHECKSUM,
            'robots':[{'robot_id':i,'robot':deepcopy(self.robots[i-1]),'robot_sha256':digest(self.robots[i-1]),
                'task':deepcopy(self.task) if i==1 else None} for i in ids]}

    def cooperation_context(self,run,seq,task,goods,**kw):
        return {'run_id':run,'seq':seq,'graph_committed_seq':120,'batch_checksum':CHECKSUM,
            'task_id':task,'goods_id':goods,'focus_task':deepcopy(self.task),
            'request_cursors':{'goods_after':0,'cooperation_after':0},
            'goods_history':{'run_id':run,'seq':seq,'graph_committed_seq':120,'goods_id':goods,
                'rows':[],'current_as_of':None,'has_more':False,'next_after_event_seq':None},
            'cooperation':{'run_id':run,'seq':seq,'graph_committed_seq':120,'task_id':task,
                'rows':[],'has_more':False,'next_after_event_seq':None}}

    def full_query(self,*args,**kwargs):
        return {'similar':[{'key':'source-1'}],'query_vector':[0.]*32}

    def close(self):self.closed=True


class CatalogTests(unittest.TestCase):
    def setUp(self):self.store=FakeStore();self.service=QueryCatalogService(self.store)
    def call(self,q,**kw):return self.service.execute({'query_id':q,'run_id':RUN,'seq':SEQ,**kw})['result']

    def test_catalog_and_all_56_explicit_mapping_statuses(self):
        self.assertEqual(len(QueryCatalogService.describe()['queries']),11)
        mapping=json.loads((ROOT/'backend/query_catalog/kg56_mapping.json').read_text(encoding='utf-8'))
        self.assertEqual({r['id'] for r in mapping['entries']},{'i'+str(i) for i in range(56)})
        self.assertTrue(all(r['warehouse_query_id'] in CATALOG and r['adaptation'] for r in mapping['entries']))
        self.assertEqual({r['id'] for r in mapping['entries'] if r['status']=='planner-owned'},{'i24','i38','i39','i40'})

    def test_self_and_readiness_use_lean_no_fleet_or_ann(self):
        for q in ('self','readiness'):
            r=self.call(q,robot_id=7)
            self.assertEqual(r['context']['robots'][0]['robot']['robot_id'],7)
            self.assertFalse(r['readiness']['ready_for_live_action'])
        self.assertEqual(self.store.calls,[])

    def test_task_discovers_goods_and_rejects_mismatched_supplied_goods(self):
        self.assertEqual(self.call('task',task_id='T1')['context']['goods_id'],'G1')
        with self.assertRaisesRegex(ValueError,'mismatch'):self.call('task',task_id='T1',goods_id='wrong')
        with self.assertRaisesRegex(ValueError,'absent'):self.call('task',task_id='not-present')

    def test_goods_only_has_custody_without_fictional_task(self):
        r=self.call('goods',goods_id='G1')
        self.assertTrue(r['found']);self.assertEqual(r['current_as_of']['location_id'],'ROBOT-1')
        self.assertNotIn('cooperation',r)

    def test_energy_canonical_physical_bays_and_no_route_claim(self):
        r=self.call('energy',robot_id=1)
        self.assertEqual(len(r['bays']),100);self.assertEqual(r['empty_physical_bays'],100)
        self.assertIn('route length and reservation',r['planning_required'])

    def test_assistance_filters_sorts_and_marks_candidate_truncation(self):
        self.store.robots[1]['carrying']=True
        self.store.robots[2]['stage']='charging'
        self.store.robots[3]['battery']=15.
        r=self.call('assistance',robot_id=1,limit=2)
        self.assertEqual([p['robot']['robot_id'] for p in r['candidates']],[5,6])
        self.assertTrue(r['candidate_list_truncated']);self.assertIn('live unique ownership',r['planning_required'])

    def test_progress_true_window_feedback_and_missing_start_remains_unknown(self):
        r=self.call('progress',task_id='T1')
        self.assertEqual(r['kind_counts'],{'task_released':1,'task_assigned':1,'picked':1,'shipped':1})
        f=r['feedback'][0]
        self.assertEqual(f['release_to_completion_seconds'],30)
        self.assertEqual(f['pickup_to_completion_seconds'],20)
        self.assertEqual(f['assignment_pickup_observations'][0]['assignment_to_pickup_seconds'],5)
        self.store.events=self.store.events[-1:]
        f=self.call('progress')['feedback'][0]
        self.assertIsNone(f['release_to_completion_seconds']);self.assertIsNone(f['pickup_to_completion_seconds'])

    def test_memory_complete_state_window_and_sample_not_duration(self):
        r=self.call('memory',start_seq=98)
        self.assertEqual([s['seq'] for s in r['states']],[98,99,100])
        self.assertEqual(r['observations']['stage_samples'],{'idle':3})
        self.assertFalse(r['observations']['sample_counts_are_continuous_durations'])

    def test_awareness_asof_task_counts_and_fleet_groups(self):
        g=self.call('global');l=self.call('local_awareness',zone='WN')
        self.assertEqual(sum(g['fleet_counts'].values()),100)
        self.assertEqual(sum(c['tasks'] for c in l['task_counts']),3)
        self.assertEqual(l['task_counts'][1]['overdue'],1)
        with self.assertRaisesRegex(ValueError,'zone'):self.call('local_awareness',zone='not-a-zone')

    def test_full_ann_is_explicit_advisory_and_not_lean(self):
        r=self.call('similar_history',variant='full-ann',task_id='T1',goods_id='G1')
        self.assertEqual(r['variant'],'full-ann');self.assertIn('cannot authorize',r['control_role'])

    def test_default_similarity_is_business_exact_without_task_coupling(self):
        with patch('backend.retrieval_eval.service.BusinessRetriever') as retriever:
            retriever.return_value.search.return_value={'feature_version':'warehouse-business-state-v1','rows':[]}
            r=self.call('similar_history',robot_id=7,limit=5)
            self.assertEqual(r['variant'],'business-exact')
            retriever.return_value.search.assert_called_once_with(RUN,SEQ,7,k=5,window_steps=60,
                embargo_steps=20,max_candidates=10000)

    def test_invalid_unknown_bool_bounds_and_nonfinite_requests_reject(self):
        cases=[{'query_id':'unknown'},{'run_id':'bad run'},{'seq':True},{'robot_id':False},
            {'robot_id':101},{'event_limit':0},{'history_limit':False},{'start_seq':101},{'surprise':1}]
        for change in cases:
            with self.subTest(change=change),self.assertRaises(ValueError):
                self.service.execute({'query_id':'self','run_id':RUN,'seq':SEQ,**change})
        for v in (True,float('nan'),-1):
            with self.assertRaises(ValueError):self.call('assistance',radius_m=v)

    def test_future_foreign_hash_corruption_and_missing_fleet_are_rejected(self):
        mutations=[lambda q,r:r.update(run_id='other'),lambda q,r:r.update(head=99),
            lambda q,r:r.update(batch_checksum='bad'),lambda q,r:r.update(created_at=''),
            lambda q,r:r.update(source_run='other') if q==FLEET_QUERY else None,
            lambda q,r:r['robots'].pop() if q==FLEET_QUERY else None,
            lambda q,r:r['robots'][0].update(seq=101) if q==FLEET_QUERY else None,
            lambda q,r:r['robots'][0].update(real_payload_sha256='b'*64) if q==FLEET_QUERY else None,
            lambda q,r:r['bays'][0].update(physical_bay_id='P001') if q==FLEET_QUERY else None]
        for fn in mutations:
            with self.subTest(fn=fn),self.assertRaises(ValueError):
                self.store.mutation=fn;self.call('energy')

    def test_multistatement_changed_batch_or_recreated_run_rejected(self):
        for field,val in [('batch_checksum','b'*64),('created_at','new-run')]:
            self.store.mutation=lambda q,r,f=field,v=val:r.update({f:v}) if q==EVENTS_QUERY else None
            with self.assertRaisesRegex(ValueError,'mixed source'):self.call('progress')

    def test_truncation_task_budget_and_missing_memory_are_explicit_failures(self):
        with self.assertRaises(IncompleteContext):self.call('progress',event_limit=1)
        with self.assertRaises(IncompleteContext):self.call('global',max_tasks=1)
        with self.assertRaises(IncompleteContext):self.call('memory',start_seq=98,history_limit=1)
        self.store.mutation=lambda q,r:r['states'].pop(1) if q==MEMORY_QUERY else None
        with self.assertRaisesRegex(ValueError,'incomplete'):self.call('memory',start_seq=98)

    def test_future_effective_time_and_wrong_task_summary_reject(self):
        self.store.events.append(event(5,'robot_stopped',501))
        with self.assertRaisesRegex(ValueError,'future event'):self.call('progress')
        self.store.events.pop()
        self.store.mutation=lambda q,r:r['counts'][0].update(tasks=0) if q==TASK_COUNTS_QUERY else None
        with self.assertRaisesRegex(IncompleteContext,'completion count'):self.call('global')

    def test_custody_context_foreign_future_cursor_and_truncation_rejected(self):
        original=self.store.cooperation_context
        changes=[lambda c:c['goods_history'].update(seq=101),
            lambda c:c['cooperation'].update(task_id='foreign'),
            lambda c:c['request_cursors'].update(goods_after=2),
            lambda c:c['goods_history'].update(has_more=True),
            lambda c:c['goods_history'].update(rows=[{'seq':101,'event_seq':8,'location_id':'ROBOT-1'}])]
        for change in changes:
            def changed(*a,**kw):
                value=original(*a,**kw);change(value);return value
            with self.subTest(change=change),patch.object(self.store,'cooperation_context',changed),self.assertRaises(ValueError):
                self.call('task',task_id='T1')

    def test_raw_goods_future_source_and_charger_alias_conflicts_reject(self):
        self.store.mutation=lambda q,r:r['history'][0].update(seq=101) if q==GOODS_QUERY else None
        with self.assertRaisesRegex(ValueError,'cargo history'):self.call('goods',goods_id='G1')
        self.store.mutation=lambda q,r:r['bays'][0].update(charging_robot_id=9) if q==FLEET_QUERY else None
        with self.assertRaisesRegex(ValueError,'physical occupant'):self.call('energy')
        self.store.mutation=lambda q,r:r['bays'][0].update(occupant_robot_id=9) if q==FLEET_QUERY else None
        with self.assertRaisesRegex(ValueError,'aliases'):self.call('energy')

    def test_task_current_payload_or_source_checksum_cannot_change_between_reads(self):
        original=self.store.cooperation_context
        for field,value in [('batch_checksum','b'*64),('focus_task',{'task_id':'T1','goods_id':'G1','status':'carrying'})]:
            def changed(*a,**kw):
                context=original(*a,**kw);context[field]=value;return context
            with self.subTest(field=field),patch.object(self.store,'cooperation_context',changed),self.assertRaises(ValueError):
                self.call('task',task_id='T1')

    def test_metrics_are_thread_local_and_close_owns_store(self):
        results=[]
        def go(q):
            self.call(q);results.append(self.service.latest_metrics)
        a=threading.Thread(target=go,args=('energy',));b=threading.Thread(target=go,args=('global',))
        a.start();b.start();a.join();b.join()
        self.assertEqual(sorted(x['statements'] for x in results),[1,3])
        self.assertTrue(all(x['service_ms']>=0 for x in results))
        self.service.close();self.assertTrue(self.store.closed)


if __name__=='__main__':unittest.main()
