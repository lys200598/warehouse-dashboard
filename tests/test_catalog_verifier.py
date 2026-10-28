"""Source post-verifier tests; actual packs and expected reconstruction differ.

Synthetic fixed facts exercise verification logic. These unit tests do not
claim a physical simulation or Neo4j pressure result; the real CLI binds the
original journal and builds/opens the independent RealOracle.
"""
from copy import deepcopy
from contextlib import closing
import gzip
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

from backend.knowledge import digest
from backend.query_catalog.service import QueryCatalogService,GOODS_QUERY
from backend.retrieval_eval.service import BusinessRetriever,HEADER,CANDIDATES
from tests.test_query_catalog import FakeStore,RUN,SEQ,CHECKSUM,SCENE

ROOT=Path(__file__).resolve().parents[1]
SPEC=importlib.util.spec_from_file_location('catalog_verifier',ROOT/'ops/verify_catalog_responses.py')
VER=importlib.util.module_from_spec(SPEC);SPEC.loader.exec_module(VER)
TEMP=ROOT/'data/local-check'
TEMP.mkdir(parents=True,exist_ok=True)


class Source:
    prefix=120
    run_id=RUN
    head=120
    def __init__(self,store):
        self.store=store;self.oracle=self
        self.metadata={'scene':SCENE};self.config={'task_reserve_soc':20,'emergency_reserve_soc':10}
    def batch_checksum(self,seq):return CHECKSUM
    def batch(self,seq):return {'seq':seq,'tick':seq*5,'sim_time':seq*5,'completed':1,'running':True,'robots':deepcopy(self.store.robots)}
    def state(self,seq,robot):return deepcopy(self.store.robots[robot-1])
    def task(self,seq,task):return deepcopy(self.store.task) if task=='T1' else None
    def all_tasks(self,seq):
        return [deepcopy(self.store.task),{'task_id':'T2','slot_id':'R01-L1-C1-F1','status':'waiting','due_second':499},
            {'task_id':'T3','slot_id':'R01-L1-C2-F1','status':'waiting','due_second':501}]
    def events(self,start,end,robot=None,task=None):
        return [{'seq':r['commit_seq'],'event_seq':r['event_seq'],'event':json.loads(r['payload'])} for r in self.store.events
            if start<=r['commit_seq']<=end and (robot is None or json.loads(r['payload'])['robot_id']==robot)
            and (task is None or json.loads(r['payload'])['task_id']==task)]
    def goods(self,seq,goods,head):return {'run_id':RUN,'seq':seq,'graph_committed_seq':head,'goods_id':goods,
        'current_as_of':None,'rows':[],'has_more':False,'next_after_event_seq':None}
    def cooperation(self,seq,task,head):return {'run_id':RUN,'seq':seq,'graph_committed_seq':head,'task_id':task,
        'rows':[],'has_more':False,'next_after_event_seq':None}


class Store(FakeStore):
    def __init__(self):
        super().__init__()
        self.task.update(slot_id='R01-L1-C1-F1',due_second=500)
        for i,p in enumerate(self.robots):p['x']=float((i%10)*8+5);p['y']=float((i//10)*6+5)
        self.robots[0]['task_id']='T1'
    def query(self,query,params,timeout=5):
        if query==HEADER:
            p=self.robots[params['robot']-1]
            return [{'head':120,'checksum':CHECKSUM,'metadata':json.dumps({'scene':SCENE}),
                'query_payload':json.dumps(p),'query_sha256':digest(p)}]
        if query==CANDIDATES:
            rows=[]
            for seq in range(params['start'],params['end']+1):
                for p in self.robots:
                    if params['exclude_robot'] and p['robot_id']==params['robot']:continue
                    rows.append({'key':f"{RUN}:{seq}:{p['robot_id']}",'run_id':RUN,'seq':seq,'robot_id':p['robot_id'],
                        **{k:p[k] for k in ('x','y','battery','carrying','stage')},'source_payload_sha256':digest(p)})
            return rows[:params['budget']]
        rows=super().query(query,params,timeout)
        if query==GOODS_QUERY:rows[0]['history']=[]
        return rows


class VerifierTests(unittest.TestCase):
    def setUp(self):self.store=Store();self.source=Source(self.store);self.catalog=QueryCatalogService(self.store)
    def req(self,q,**kw):return {'query_id':q,'run_id':RUN,'seq':SEQ,**kw}
    def response(self,r):return self.catalog.execute(r)
    def check(self,r,wire):return VER.ResponseVerifier(self.source).check(r,wire)

    def test_all_non_ann_packs_reconstructed_from_fixed_source(self):
        requests=[self.req('self'),self.req('readiness',robot_id=2),self.req('task',task_id='T1'),self.req('goods',goods_id='G1'),
            self.req('goods',task_id='T1'),self.req('energy'),self.req('assistance',robot_id=1,limit=3),
            self.req('progress',task_id='T1'),self.req('memory',start_seq=98),self.req('global'),self.req('local_awareness',zone='WN')]
        for request in requests:
            with self.subTest(query=request['query_id']):self.assertEqual(self.check(request,self.response(request)),[])

    def test_independent_scalar_business_topk_and_no_self_leak(self):
        request=self.req('similar_history',variant='business-exact',limit=5,window_steps=2,embargo_steps=1,max_candidates=1000)
        wire=self.response(request)
        self.assertEqual(self.check(request,wire),[])
        bad=deepcopy(wire);bad['result']['context']['rows'][0]['robot_id']=1
        self.assertTrue(self.check(request,bad))
        bad=deepcopy(wire);bad['result']['context']['rows'][0]['score']-=.01
        self.assertTrue(self.check(request,bad))

    def test_typed_state_task_or_batch_tampering_rejected(self):
        request=self.req('self');wire=self.response(request)
        for field,value in [('battery',True),('task_id','foreign'),('x',999.)]:
            bad=deepcopy(wire);bad['result']['context']['robots'][0]['robot'][field]=value
            with self.subTest(field=field):self.assertTrue(self.check(request,bad))
        bad=deepcopy(wire);bad['result']['context']['batch_checksum']='b'*64
        self.assertTrue(self.check(request,bad))

    def test_event_loss_feedback_changed_and_memory_gap_reject(self):
        for q in ('progress','memory'):
            r=self.req(q,start_seq=98);wire=self.response(r);bad=deepcopy(wire)
            bad['result']['events'].pop()
            self.assertTrue(self.check(r,bad))
        r=self.req('progress');bad=self.response(r);bad['result']['feedback'][0]['release_to_completion_seconds']=1
        self.assertTrue(self.check(r,bad))
        r=self.req('memory',start_seq=98);bad=self.response(r);bad['result']['states'].pop(1)
        self.assertTrue(self.check(r,bad))

    def test_bay_alias_global_counts_and_peer_filters_reject(self):
        r=self.req('energy');bad=self.response(r);bad['result']['bays'][0]['physical_bay_id']='P001'
        self.assertTrue(self.check(r,bad))
        r=self.req('global');bad=self.response(r);bad['result']['task_counts'][0]['tasks']+=1
        self.assertTrue(self.check(r,bad))
        r=self.req('assistance',limit=3);bad=self.response(r);bad['result']['candidates'].reverse()
        self.assertTrue(self.check(r,bad))

    def test_unordered_task_group_order_is_not_a_false_mismatch(self):
        r=self.req('global');wire=self.response(r);wire['result']['task_counts'].reverse()
        self.assertEqual(self.check(r,wire),[])

    def test_foreign_future_head_and_legacy_ann_are_not_certified_as_business(self):
        r=self.req('self');wire=self.response(r)
        for head in (0,121,True):
            bad=deepcopy(wire);bad['result']['context']['graph_committed_seq']=head
            with self.assertRaises(ValueError):self.check(r,bad)
        r=self.req('similar_history',variant='full-ann',task_id='T1',goods_id='G1')
        with self.assertRaisesRegex(ValueError,'separately frozen'):self.check(r,self.response(r))

    def test_packets_fingerprint_is_not_enough_for_source_acceptance(self):
        r=self.req('self');wire=self.response(r);wire['result']['context']['robots'][0]['robot']['battery']=77.
        with tempfile.TemporaryDirectory(dir=TEMP) as folder:
            p=Path(folder);record={'request_id':'bad-state','round':1,'client_id':1,'run_id':RUN,'seq':SEQ,
                'point_kind':'fixed','start_ns':1,'end_ns':2,'ok':True,'response_sha256':VER.digest(wire),'request':r}
            (p/'requests.jsonl').write_text(json.dumps(record)+'\n',encoding='utf-8')
            with gzip.open(p/'responses.jsonl.gz','wt',encoding='utf-8') as f:f.write(json.dumps({'request_id':'bad-state','result':wire})+'\n')
            result=VER.verify_files(self.source,p/'requests.jsonl',p/'responses.jsonl.gz')
            self.assertFalse(result['passed']);self.assertEqual(result['request_count'],1)
            self.assertIn('battery',' '.join(result['errors'][0]['errors']))

    def test_missing_duplicate_failed_orphan_and_malformed_packets_all_retained(self):
        r=self.req('self');wire=self.response(r)
        base={'request_id':'one','run_id':RUN,'seq':SEQ,'start_ns':1,'end_ns':2,'ok':True,'response_sha256':VER.digest(wire),'request':r}
        with tempfile.TemporaryDirectory(dir=TEMP) as folder:
            p=Path(folder)
            records=[base,{**base,'request_id':'failed','ok':False},{**base,'request_id':'missing'},base]
            (p/'requests').write_text('\n'.join(json.dumps(x) for x in records)+'\n{broken\n',encoding='utf-8')
            (p/'responses').write_text(json.dumps({'request_id':'one','result':wire})+'\n'+json.dumps({'request_id':'orphan','result':wire})+'\n',encoding='utf-8')
            result=VER.verify_files(self.source,p/'requests',p/'responses')
            self.assertFalse(result['passed']);self.assertEqual(result['request_count'],5)
            self.assertGreaterEqual(result['error_count'],5)

    def test_duplicate_json_fields_and_nan_rejected(self):
        for text in ['{"a":1,"a":2}','{"value":NaN}']:
            with self.assertRaises(ValueError):VER.loads(text)

    def test_cli_missing_source_writes_failed_manifest(self):
        with tempfile.TemporaryDirectory(dir=TEMP) as folder:
            p=Path(folder)
            code=VER.main(['--journal',str(p/'missing.sqlite'),'--requests',str(p/'missing-requests'),
                '--responses',str(p/'missing-responses'),'--oracle-dir',str(p/'oracle'),'--output',str(p/'report.json')])
            self.assertEqual(code,2);self.assertFalse(json.loads((p/'report.json').read_text(encoding='utf-8'))['passed'])

    def test_real_sqlite_journal_oracle_cli_and_typed_goods_times(self):
        from tests.test_real_scale_oracle import make_journal,checkpoint,events,RUN as REAL_RUN
        import sqlite3
        with tempfile.TemporaryDirectory(dir=TEMP) as folder:
            p=Path(folder);journal=p/'journal.sqlite';make_journal(journal)
            with closing(sqlite3.connect(journal)) as db:checksum=db.execute('SELECT checksum FROM commits WHERE seq=3').fetchone()[0]
            cp=checkpoint(3);state=cp['robots'][1]['state']
            self_body={'context':{'run_id':REAL_RUN,'seq':3,'graph_committed_seq':4,'batch_checksum':checksum,
                'robots':[{'robot_id':2,'robot':state,'robot_sha256':digest(state),'task':cp['tasks']['T1']}]},
                'readiness':{'requested_point_present':True,'future_point_used':False,'known_committed_lag_steps':1,
                    'ready_for_live_action':False,'note':'historical read; live coordinator must recheck current ownership/reservations'}}
            history=[]
            for at in range(1,4):
                for e in events(at):
                    if e.get('goods_id')=='G1' and e.get('location_id'):
                        history.append({'event_seq':e['event_seq'],'seq':at,'sim_time':e['sim_time'],
                            'kind':e['kind'],'location_id':e['location_id'],'event_key':e['event_id']})
            goods_body={'goods_id':'G1','history':history,'found':True,'current_as_of':history[-1],
                'batch_checksum':checksum,'cooperation_scope':'supply task_id to request its cooperation context'}
            requests=[];responses=[]
            for i,(query,body,extra) in enumerate([('self',self_body,{'robot_id':2}),('goods',goods_body,{'goods_id':'G1'})]):
                request={'query_id':query,'run_id':REAL_RUN,'seq':3,**extra}
                wire={'catalog_version':'warehouse-query-catalog-v1','query_id':query,'run_id':REAL_RUN,'seq':3,
                    'read_only':True,'used_by_live_control':False,'result':body}
                requests.append({'request_id':str(i),'round':1,'client_id':1,'run_id':REAL_RUN,'seq':3,
                    'point_kind':'source-fixed','start_ns':10+i*2,'end_ns':11+i*2,'ok':True,
                    'response_sha256':VER.digest(wire),'request':request})
                responses.append({'request_id':str(i),'result':wire})
            (p/'requests').write_text(''.join(json.dumps(r)+'\n' for r in requests),encoding='utf-8')
            with gzip.open(p/'responses.gz','wt',encoding='utf-8') as stream:
                stream.write(''.join(json.dumps(r)+'\n' for r in responses))
            code=VER.main(['--journal',str(journal),'--requests',str(p/'requests'),'--responses',str(p/'responses.gz'),
                '--oracle-dir',str(p/'oracle'),'--output',str(p/'report.json')])
            report=json.loads((p/'report.json').read_text(encoding='utf-8'))
            self.assertEqual(code,0,report);self.assertTrue(report['passed']);self.assertEqual(report['request_count'],2)
            self.assertEqual(report['source_journal_sha256'],VER.file_sha(journal))


if __name__=='__main__':unittest.main()
