from copy import deepcopy
import unittest

from backend.query_views.catalog import AcceleratedCatalog,HEAD_QUERY,POINT_QUERY
from backend.query_catalog.service import FLEET_QUERY
from test_query_catalog import FakeStore,point,RUN,SEQ,SCENE


class FencedStore(FakeStore):
    def __init__(self):
        super().__init__();self.head=120;self.fence_mutation=None

    def query(self,query,params,timeout=5):
        if query in (HEAD_QUERY,POINT_QUERY):
            self.calls.append((query,deepcopy(params)))
            row=point();row.update(head=self.head,head_checksum='b'*64,source_run=RUN,
                metadata={'scene':deepcopy(SCENE)},config={'task_reserve_soc':20,'emergency_reserve_soc':10},
                step={'seq':SEQ,'tick':500,'sim_time':500,'completed':1,'running':True})
            if query==POINT_QUERY:
                at=int(params['key'].rsplit(':',1)[1])
                row.update(current_head=self.head,point_exists=True,point_key=params['key'],
                    point_run_id=RUN,point_seq=at,point_checksum='a'*64 if at==SEQ else 'b'*64)
                row['step']['seq']=at
                if self.fence_mutation:self.fence_mutation(row)
            return [row]
        return super().query(query,params,timeout)


class AccelerationTests(unittest.TestCase):
    def setUp(self):self.store=FencedStore();self.service=AcceleratedCatalog(self.store)

    def call(self):return self.service.execute({'query_id':'energy','run_id':RUN,'seq':SEQ})

    def test_cache_reuses_immutable_fleet_but_always_reads_fresh_point(self):
        first=self.call();second=self.call();self.assertEqual(first,second)
        self.assertEqual(sum(q==FLEET_QUERY for q,_ in self.store.calls),1)
        self.assertEqual(sum(q==POINT_QUERY for q,_ in self.store.calls),4)
        self.assertEqual(sum(q==HEAD_QUERY for q,_ in self.store.calls),2)
        self.assertTrue(self.service.latest_metrics['fleet_fragment_cache_hit'])

    def test_fresh_head_does_not_replace_historical_bodies_or_mutate_cache(self):
        first=self.call();first['result']['robot']['battery']=0
        self.store.head=121;second=self.call()
        self.assertEqual(second['result']['robot']['battery'],60.)
        self.assertEqual(second['result']['point']['graph_committed_seq'],121)
        self.assertEqual(second['seq'],SEQ)

    def test_same_point_metadata_config_or_step_change_is_not_hidden(self):
        for kind in ('metadata','config','step'):
            with self.subTest(kind=kind):
                self.setUp();self.call()
                def mutate(row):row[kind]['unexpected']='changed'
                self.store.fence_mutation=mutate
                with self.assertRaises(ValueError):self.call()

    def test_recreated_run_during_fill_and_missing_head_fail_closed(self):
        self.store.fence_mutation=lambda row:row.update(created_at='recreated')
        with self.assertRaises(ValueError):self.call()
        self.setUp();self.store.fence_mutation=lambda row:row.pop('point_checksum')
        with self.assertRaises(ValueError):self.call()


if __name__=='__main__':unittest.main()
