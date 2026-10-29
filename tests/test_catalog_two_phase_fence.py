"""Fresh-head + independent full-key probes; fixtures do not prove a DB cause."""
from copy import deepcopy
import unittest

from backend.query_catalog.service import IncompleteContext
from backend.query_views.catalog import AcceleratedCatalog,HEAD_QUERY,POINT_QUERY
from test_query_catalog import RUN,SEQ,CHECKSUM,point,SCENE


class Mirror:
    timeout=15
    def __init__(self):
        self.mirror=self;self.calls=[];self.A_mutation=None;self.B_mutation=None;self.C_mutation=None
        self.A_rows=1;self.B_rows=1;self.C_rows=1;self.H=120;self.H2=120;self.H3=None
    def query(self,query,params,timeout=5):
        self.calls.append((query,deepcopy(params)))
        a={'run_id':RUN,'head':self.H,'created_at':'2026-10-28T00:00:00Z','source_run':RUN,
           'metadata':{'scene':deepcopy(SCENE)},'config':{'task_reserve_soc':20}}
        if query==HEAD_QUERY:
            if self.A_mutation:self.A_mutation(a)
            return [deepcopy(a) for _ in range(self.A_rows)]
        if query==POINT_QUERY:
            at=int(params['key'].rsplit(':',1)[1]);stage='B' if at==SEQ else 'C'
            current=self.H2 if stage=='B' or self.H3 is None else self.H3
            row={**a,'current_head':current,'point_exists':True,'point_key':params['key'],
                 'point_run_id':RUN,'point_seq':at,'point_checksum':CHECKSUM if at==SEQ else 'b'*64,
                 'step':{'seq':at,'sim_time':500,'completed':1}}
            mutation=getattr(self,stage+'_mutation')
            if mutation:mutation(row)
            return [deepcopy(row) for _ in range(getattr(self,stage+'_rows'))]
        raise AssertionError('unexpected read')


class TwoPhaseFenceTests(unittest.TestCase):
    def setUp(self):
        self.store=Mirror();self.service=AcceleratedCatalog(self.store)
        self.service._metrics={'statements':0,'observed_heads':[]}
    def fence(self):return self.service._fence(RUN,SEQ)

    def test_independent_full_keys_without_dynamic_expressions_or_two_step_join(self):
        row,token=self.fence()
        self.assertEqual(self.store.calls,[(HEAD_QUERY,{'run':RUN}),
            (POINT_QUERY,{'run':RUN,'key':f'{RUN}:{SEQ}'}),(POINT_QUERY,{'run':RUN,'key':f'{RUN}:120'})])
        self.assertNotIn('WHStep',HEAD_QUERY)
        self.assertEqual(POINT_QUERY.count('WHStep'),1)
        self.assertIn('{key:$key}',POINT_QUERY);self.assertNotIn('toString(',POINT_QUERY.replace('toString(r.created_at)',''))
        self.assertEqual(row['head'],120);self.assertEqual(token,(CHECKSUM,point()['created_at']))
        trace=self.service.latest_metrics['catalog_fence']
        self.assertEqual((trace['A_rows'],trace['B_rows'],trace['C_rows'],trace['outcome']),(1,1,1,'ok'))
        self.assertEqual(self.service.latest_metrics['statements'],3)

    def test_equal_request_and_head_reuses_one_verified_probe(self):
        self.store.H=self.store.H2=SEQ;row,_=self.fence()
        self.assertEqual(row['head_checksum'],CHECKSUM)
        self.assertEqual(len(self.store.calls),2)
        trace=self.service.latest_metrics['catalog_fence']
        self.assertTrue(trace['reused_requested_for_head']);self.assertTrue(trace['head_exists'])
        self.assertEqual(len(trace['probes']),1);self.assertIsNone(trace['C_rows'])

    def test_head_advancement_keeps_original_fresh_budget_point(self):
        self.store.H2=125;self.store.H3=127;row,_=self.fence()
        self.assertEqual(row['head'],120)
        trace=self.service.latest_metrics['catalog_fence']
        self.assertEqual((trace['head_A'],trace['head_B'],trace['head_C']),(120,125,127))
        self.assertEqual(self.service.latest_metrics['observed_heads'],[120])

    def test_future_request_fails_before_point_read_without_retry(self):
        self.store.H=SEQ-1
        with self.assertRaisesRegex(ValueError,'uncommitted'):self.fence()
        self.assertEqual(len(self.store.calls),1)
        self.assertIsNone(self.service.latest_metrics['catalog_fence']['B_rows'])

    def test_missing_or_duplicate_run_rows_fail_without_retry(self):
        for stage,count in (('A',0),('A',2),('B',0),('B',2),('C',0),('C',2)):
            with self.subTest(stage=stage,count=count):
                self.setUp();setattr(self.store,stage+'_rows',count)
                with self.assertRaises(ValueError):self.fence()
                self.assertEqual(len(self.store.calls),{'A':1,'B':2,'C':3}[stage])
                self.assertEqual(self.service.latest_metrics['catalog_fence'][stage+'_rows'],count)

    def test_each_probe_rechecks_recreation_metadata_config_and_source(self):
        for stage in ('B','C'):
            for key in ('created_at','metadata','config','source_run','run_id'):
                with self.subTest(stage=stage,key=key):
                    self.setUp();setattr(self.store,stage+'_mutation',lambda row,k=key:row.update({k:'changed'}))
                    with self.assertRaisesRegex(ValueError,'identity changed'):self.fence()
                    self.assertEqual(len(self.store.calls),2 if stage=='B' else 3)

    def test_missing_requested_and_head_have_full_key_diagnostics(self):
        for stage,flag,at in (('B','requested_exists',SEQ),('C','head_exists',120)):
            with self.subTest(stage=stage):
                self.setUp();setattr(self.store,stage+'_mutation',lambda row:row.update(point_exists=False))
                with self.assertRaises(ValueError):self.fence()
                trace=self.service.latest_metrics['catalog_fence']
                self.assertIs(trace[flag],False);self.assertEqual(trace['head_A'],120)
                self.assertEqual(trace['outcome'],'error')
                self.assertEqual(trace['probes'][-1]['key'],f'{RUN}:{at}')
                self.assertIs(trace['probes'][-1]['exists'],False)
                self.assertEqual(len(self.store.calls),2 if stage=='B' else 3)

    def test_each_probe_rejects_regressed_or_bool_head_and_foreign_step_identity(self):
        for stage in ('B','C'):
            for key,value in (('current_head',119),('current_head',True),('point_seq',99),
                              ('point_seq',True),('point_run_id','foreign'),('point_key','foreign:100')):
                with self.subTest(stage=stage,key=key,value=value):
                    self.setUp();setattr(self.store,stage+'_mutation',lambda row,k=key,v=value:row.update({k:v}))
                    with self.assertRaises(ValueError):self.fence()

    def test_each_probe_checksum_and_existing_token_mismatch_fail(self):
        for stage in ('B','C'):
            with self.subTest(stage=stage):
                self.setUp();setattr(self.store,stage+'_mutation',lambda row:row.update(point_checksum='z'*64))
                with self.assertRaises(ValueError):self.fence()
        self.setUp()
        with self.assertRaises(ValueError):self.service._fence(RUN,SEQ,('0'*64,point()['created_at']))

    def test_transport_error_is_not_retried_and_probe_timing_survives(self):
        self.store.C_mutation=lambda row:(_ for _ in ()).throw(RuntimeError('transport'))
        with self.assertRaisesRegex(RuntimeError,'transport'):self.fence()
        trace=self.service.latest_metrics['catalog_fence']
        self.assertEqual(len(self.store.calls),3);self.assertEqual(len(trace['probes']),2)
        self.assertIn('ms',trace['probes'][-1]);self.assertIsNone(trace['C_rows'])
        self.assertEqual(trace['outcome'],'error')

    def test_summary_entity_budget_uses_fresh_H_even_when_probes_see_newer_heads(self):
        self.store.H2=125;self.store.H3=127;row,token=self.fence();seen=[]
        class Repository:
            def status(self):return {'head':127}
            def read(self,run,seq,fleet,head,checksum):
                seen.append((head,checksum));return {'task_entity_count':1,'counts':[{
                    'zone':'W','status':'waiting','tasks':1,'overdue':0}]},20
        class Directory:
            def repository(self,*args):return Repository()
        self.service.summaries=Directory()
        fleet={'head':row['head'],'token':token,'_fresh_head_checksum':row['head_checksum']}
        with self.assertRaises(IncompleteContext):self.service._tasks(RUN,SEQ,fleet,10)
        self.assertEqual(seen,[(120,'b'*64)]);self.assertEqual(len(self.store.calls),3)


if __name__=='__main__':unittest.main()
