from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
import json
import threading
import time
import unittest

from backend.query_catalog.service import QueryCatalogService,EVENTS_QUERY,IncompleteContext
from backend.query_views.catalog import HEAD_QUERY,POINT_QUERY
from backend.query_views.event_counts import EventCountCatalog
from test_query_catalog import RUN,SEQ,FakeStore,event
from test_query_catalog_acceleration import FencedStore


class Store(FencedStore):
    def __init__(self):
        super().__init__();self.version=None;self.delay=0.;self.fail_events=False

    def query(self,query,params,timeout=5):
        if query==EVENTS_QUERY:
            if self.delay:time.sleep(self.delay)
            if self.fail_events:raise RuntimeError('event transport')
        rows=super().query(query,params,timeout)
        for row in rows:
            if 'head' in row:row['head']=self.head
        if self.version:
            for row in rows:row['created_at']=self.version
        return rows


class EventCountTests(unittest.TestCase):
    def setUp(self):
        self.store=Store();self.service=EventCountCatalog(self.store,cache_fleet=False)

    def call(self,query='global',**kwargs):
        return self.service.execute({'query_id':query,'run_id':RUN,'seq':SEQ,**kwargs})

    def event_reads(self):
        return sum(q==EVENTS_QUERY for q,_ in self.store.calls)

    def direct(self,**kwargs):
        self.service._metrics={'statements':0,'observed_heads':[]}
        fleet=self.service._fleet(RUN,SEQ)
        return self.service._event_kind_counts(RUN,SEQ,kwargs.pop('start',40),kwargs.pop('limit',2000),fleet,**kwargs)

    def test_base_default_and_cold_hot_outputs_exactly_equal(self):
        base=QueryCatalogService(FakeStore())
        for query in ('global','local_awareness'):
            request={'query_id':query,'run_id':RUN,'seq':SEQ}
            self.assertEqual(base.execute(request),self.call(query))
            self.assertEqual(base.execute(request),self.call(query))
        self.assertEqual(self.event_reads(),1)
        self.assertTrue(self.service.latest_metrics['event_count_cache_hit'])
        self.assertEqual(self.service.latest_metrics['event_count_events'],4)
        self.assertGreaterEqual(sum(q==HEAD_QUERY for q,_ in self.store.calls),4)

    def test_hits_still_read_fresh_head_and_reject_future_regression(self):
        self.call();self.store.head=121
        self.assertEqual(self.call()['result']['point']['graph_committed_seq'],121)
        self.assertEqual(self.event_reads(),1)
        self.store.head=99
        with self.assertRaises(ValueError):self.call()
        self.assertEqual(self.event_reads(),1)

    def test_limit_in_cache_key_semantics_and_truncation_not_cached(self):
        with self.assertRaises(IncompleteContext):self.call(event_limit=3)
        self.assertEqual(len(self.service._event_counts),0)
        self.call(event_limit=4);self.assertEqual(self.event_reads(),2)
        with self.assertRaises(IncompleteContext):self.call(event_limit=3)
        self.assertEqual(self.event_reads(),2)
        self.assertEqual(self.call(event_limit=9)['result']['recent_event_kind_counts']['picked'],1)

    def test_start_filters_empty_windows_and_copies(self):
        value=self.direct(robot=1,task='T1');value['picked']=999
        self.assertEqual(self.direct(robot=1,task='T1')['picked'],1)
        self.assertEqual(self.direct(robot=2),{})
        self.assertEqual(self.direct(task='OTHER'),{})
        self.assertEqual(self.direct(start=99),{})
        self.assertEqual(self.event_reads(),4)

    def test_recreated_run_never_uses_old_counts(self):
        self.call();self.store.version='2026-recreated'
        self.store.events=[event(1,'new-version-kind',20)]
        result=self.call()['result']['recent_event_kind_counts']
        self.assertEqual(result,{'new-version-kind':1})
        self.assertEqual(self.event_reads(),2)

    def test_distinct_runs_never_share_same_checksum_creation_cache(self):
        original=self.store.query
        def multi_run(query,params,timeout=5):
            rows=original(query,params,timeout)
            for row in rows:
                row['run_id']=params['run']
                if 'source_run' in row:row['source_run']=params['run']
                if 'point_run_id' in row:row['point_run_id']=params['run']
                for field in ('robots','bays','events'):
                    for child in row.get(field,[]):child['run_id']=params['run']
                if params['run']!='catalog-unit' and 'events' in row:
                    for child in row['events']:
                        payload=json.loads(child['payload']);payload['kind']='other-run'
                        child['payload']=json.dumps(payload)
            return rows
        self.store.query=multi_run
        original_counts=self.call()['result']['recent_event_kind_counts']
        other=self.service.execute({'query_id':'global','run_id':'other-run','seq':SEQ})
        self.assertEqual(other['result']['recent_event_kind_counts'],{'other-run':4})
        self.assertEqual(self.call()['result']['recent_event_kind_counts'],original_counts)
        self.assertEqual(self.event_reads(),2)

    def test_source_identity_changes_under_fixed_checksum_rejected(self):
        self.call()
        self.store.fence_mutation=lambda row:row['metadata'].update(changed=True)
        with self.assertRaises(ValueError):self.call()

    def test_future_time_bad_order_or_payload_errors_not_cached(self):
        for kind in ('time','order','payload','missing_kind'):
            with self.subTest(kind=kind):
                self.setUp()
                if kind=='time':self.store.events[0]=event(1,'future',501)
                elif kind=='order':self.store.events.reverse()
                elif kind=='payload':self.store.events[0]['payload']='not JSON'
                else:
                    payload=json.loads(self.store.events[0]['payload']);payload.pop('kind')
                    self.store.events[0]['payload']=json.dumps(payload)
                with self.assertRaises((ValueError,KeyError)):self.call()
                self.assertFalse(self.service._event_counts)

    def test_errors_not_cached_and_retry_is_explicit_new_request(self):
        self.store.fail_events=True
        with self.assertRaises(RuntimeError):self.call()
        self.assertFalse(self.service._event_counts)
        self.store.fail_events=False;self.call();self.assertEqual(self.event_reads(),1)

    def test_eviction_entry_and_byte_limits(self):
        self.service=EventCountCatalog(self.store,event_count_cache_size=1,cache_fleet=False)
        self.call(start_seq=40);self.call(start_seq=41);self.call(start_seq=40)
        self.assertEqual(self.event_reads(),3);self.assertEqual(len(self.service._event_counts),1)
        self.service=EventCountCatalog(self.store,event_count_cache_bytes=1,cache_fleet=False)
        self.call();self.call();self.assertFalse(self.service._event_counts)
        self.assertEqual(self.service.latest_metrics['event_count_cache_skip'],'entry_exceeds_byte_budget')

    def test_concurrent_misses_singleflight_but_each_request_fenced(self):
        self.store.delay=.02
        with ThreadPoolExecutor(max_workers=8) as pool:
            values=list(pool.map(lambda _:self.call(),range(16)))
        self.assertTrue(all(v==values[0] for v in values))
        self.assertEqual(self.event_reads(),1)
        self.assertGreaterEqual(sum(q==HEAD_QUERY for q,_ in self.store.calls),16)
        self.assertLessEqual(self.service._event_counts_bytes,self.service.event_count_cache_bytes)

    def test_memory_progress_assistance_keep_full_events_not_count_cache(self):
        for query in ('memory','progress','assistance'):
            self.call(query,start_seq=98)
        self.assertEqual(self.event_reads(),3)
        self.assertFalse(self.service._event_counts)


if __name__=='__main__':unittest.main()
