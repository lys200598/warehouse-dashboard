import json
import math
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch
import zlib

from backend.scale_benchmark.model import (DIMENSIONS,archive_state,canonical,cosine,
    feature_vector,fingerprint,identity,source_sequence,validate_history,validate_vectors,validate_business)
from backend.scale_benchmark.cli import benchmark,preload,parser,summary,task_oracles
from backend.knowledge import canonical as journal_canonical,digest
from backend.scale_benchmark.store import ScaleStore,BATCHED_QUERY,ANN_EXPANDED_QUERY,EXACT_ELIGIBLE_QUERY


class ModelTests(unittest.TestCase):
    def test_identity_rejects_untrusted_identifier(self):
        for value in ('','bad id','x` RETURN 1',None,'x'*102):
            with self.assertRaises(ValueError):identity(value)
        self.assertEqual(identity('wh-scale-v2-million'),'wh-scale-v2-million')

    def test_resampling_retains_endpoints_and_monotonic_source(self):
        values=[source_sequence(seq,10000,[1,2,5,10]) for seq in range(1,10001)]
        self.assertEqual((values[0],values[-1]),(1,10))
        self.assertEqual(sorted(values),values)
        with self.assertRaises(ValueError):source_sequence(0,10000,[1])
        for available in ([3,1],[1,1],[0,1],[True,2]):
            with self.assertRaises(ValueError):source_sequence(1,10,available)

    def test_vector_contract_unit_norm_deterministic_and_temporal_identity(self):
        payload={'robot_id':1,'x':5,'y':8,'soc':75,'stage':'idle'}
        vector=feature_vector(payload,10,1)
        self.assertEqual(len(vector),DIMENSIONS)
        self.assertAlmostEqual(sum(x*x for x in vector),1)
        self.assertEqual(vector,feature_vector(json.dumps(payload),10,1))
        self.assertNotEqual(vector,feature_vector(payload,11,1))
        self.assertAlmostEqual(cosine(vector,vector),1)

    def test_real_stage_names_are_distinct_and_unknown_explicit(self):
        vectors=[feature_vector({'robot_id':1,'stage':stage},10,1)
            for stage in ['to_source','to_target','returning','to_handoff','handoff_receive','unknown']]
        self.assertEqual(len({tuple(v) for v in vectors}),6)
        self.assertEqual(feature_vector({'robot_id':1,'stage':'new_unmodelled'},10,1),vectors[-1])

    def test_archive_does_not_claim_actual_execution(self):
        payload=json.dumps({'robot_id':1,'x':2})
        row=archive_state('archive',9,1,'business',2,payload)
        self.assertEqual(row['source_key'],'business:2:1')
        self.assertEqual(row['key'],'archive:9:1')
        self.assertIn('not additional execution',row['scale_kind'])
        self.assertEqual(row['payload'],payload)
        with self.assertRaises(ValueError):archive_state('archive',9,2,'business',2,payload)

    def test_history_independent_source_and_cutoff_oracle(self):
        templates={(s,1):json.dumps({'robot_id':1,'x':s}) for s in [1,3]}
        rows=[archive_state('archive',seq,1,'business',source_sequence(seq,10,[1,3]),
              templates[(source_sequence(seq,10,[1,3]),1)]) for seq in range(8,3,-1)]
        self.assertTrue(validate_history(rows,'archive',1,8,'business',[1,3],10,templates))
        rows[0]['payload']='{"robot_id":1,"x":100}'
        with self.assertRaises(ValueError):validate_history(rows,'archive',1,8,'business',[1,3],10,templates)

    def test_history_future_or_missing_row_fails(self):
        with self.assertRaises(ValueError):validate_history([{'seq':10}],'archive',1,8,'business',[1],10,{})

    def test_cosine_score_and_temporal_filter_oracle(self):
        v=feature_vector({'robot_id':1},10,1)
        row={'run_id':'archive','seq':10,'embedding':v,'score':1}
        self.assertTrue(validate_vectors([row],v,'archive',10))
        for changed in ({**row,'seq':11},{**row,'run_id':'wrong'},{**row,'score':.5},
                        {**row,'score':float('nan')},{**row,'embedding':[float('nan')]*DIMENSIONS}):
            with self.assertRaises(ValueError):validate_vectors([changed],v,'archive',10)
        with self.assertRaises(ValueError):validate_vectors([],v,'archive',10)

    def test_wrong_business_task_cannot_pass_all_correct(self):
        expected={'robot_id':1,'task_id':'T1'}
        result={'run_id':'business','seq':3,'robot':expected,'task':{'task_id':'wrong'}}
        with self.assertRaises(ValueError):validate_business(result,'business',3,expected,{'task_id':'T1'})
        result['task']={'task_id':'T1'}
        self.assertTrue(validate_business(result,'business',3,expected,{'task_id':'T1'}))

    def test_vector_candidate_wrong_identity_or_payload_fails(self):
        templates={(1,1):json.dumps({'robot_id':1,'x':2})}
        vector=feature_vector(templates[(1,1)],10,1)
        row={'run_id':'archive','seq':10,'robot_id':1,'key':'archive:10:1','embedding':vector,'score':1}
        self.assertTrue(validate_vectors([row],vector,'archive',10,'business',[1],10,templates))
        with self.assertRaises(ValueError):
            validate_vectors([{**row,'key':'archive:10:2'}],vector,'archive',10,'business',[1],10,templates)

    def test_summary_keeps_failed_latency_in_percentiles(self):
        s=summary([{'ok':True,'ms':1},{'ok':False,'ms':1000}],2)
        self.assertEqual(s['failures'],1)
        self.assertEqual(s['mean_ms_all_requests'],500.5)
        self.assertEqual(s['over_500ms_all_requests'],1)


class FakeStore:
    def __init__(self):
        self.templates_map={(seq,robot):json.dumps({'robot_id':robot,'x':seq,'stage':'idle'})
                            for seq in [1,2] for robot in range(1,101)}
        self.heads={'business':{'committed_seq':2}}
        self.archive_rows=[]
        self.mirror=self
    def query(self,*args,**kwargs):return [{'versions':['test-only']}]
    def task_templates(self,run):return []
    def head(self,run):return self.heads.get(run)
    def templates(self,run):return [1,2],self.templates_map
    def create_archive(self,run,business,steps,source_hash,resume):
        self.heads[run]={'source_run':business,'source_hash':source_hash,'archive_steps':steps,'committed_seq':0}
        return 0
    def append(self,run,rows,first,last):
        self.archive_rows.extend(rows);self.heads[run]['committed_seq']=last
    def ensure_vector_index(self):return [{'state':'ONLINE'}]
    def counts(self,run):return {'states':len(self.archive_rows),'vectors':len(self.archive_rows),
        'source_links':len(self.archive_rows),'robots':100,'first_seq':1,'last_seq':self.heads[run]['archive_steps']}
    def watermark(self,run):return {'seq':1,'tick':1}
    def full_query(self,business,business_seq,archive,robot,seq,query_mode='baseline'):
        point=next(r for r in self.archive_rows if r['seq']==seq and r['robot_id']==robot)
        rows=sorted((r for r in self.archive_rows if r['seq']<=seq and r['robot_id']==robot),
                    key=lambda r:r['seq'],reverse=True)[:5]
        return {'business':{'run_id':business,'seq':business_seq,
            'robot':json.loads(self.templates_map[(business_seq,robot)]),'task':None},
            'history':rows,'query_vector':point['embedding'],'similar':[{**point,'score':1}]}


class HarnessTests(unittest.TestCase):
    def batched_store(self,first,expanded=None,exact=None):
        class Mirror:
            def __init__(self):self.calls=[]
            def query(self,query,parameters,timeout=5):
                self.calls.append((query,parameters,timeout))
                if query==BATCHED_QUERY:
                    return [{'business_head':16,'robot_payload':'{"robot_id":28}',
                        'task_payload':'{"task_id":"T1"}','history':[],
                        'query_vector':[1]*32,'similar':first}]
                if query==ANN_EXPANDED_QUERY:return expanded
                if query==EXACT_ELIGIBLE_QUERY:return exact
                raise AssertionError('unexpected query')
        return ScaleStore(Mirror())

    def test_batched_normal_is_one_statement_no_hidden_fallback(self):
        store=self.batched_store([{'key':'archive:511:28'}])
        result=store.full_query('business',1,'archive',28,511,query_mode='batched')
        self.assertEqual(len(store.mirror.calls),1)
        self.assertEqual(result['bolt_statements'],1)
        self.assertEqual(result['retrieval_path'],'ann100')
        self.assertEqual(result['business']['task'],{'task_id':'T1'})

    def test_batched_empty_ann100_expands_candidates_and_records_cost(self):
        store=self.batched_store([],[{'key':'archive:511:28'}])
        result=store.full_query('business',1,'archive',28,511,query_mode='batched')
        self.assertEqual(len(store.mirror.calls),2)
        self.assertEqual(result['bolt_statements'],2)
        self.assertEqual(result['retrieval_path'],'ann1000_after_empty100')
        self.assertEqual(store.mirror.calls[1][2],15)

    def test_batched_empty_ann1000_uses_exact_eligible_database_query(self):
        store=self.batched_store([],[],[{'key':'archive:511:28'}])
        result=store.full_query('business',1,'archive',28,511,query_mode='batched')
        self.assertEqual(len(store.mirror.calls),3)
        self.assertEqual(result['retrieval_path'],'exact_eligible_after_empty1000')
        self.assertEqual(store.mirror.calls[2][1]['seq'],511)
        self.assertIn('vector.similarity.cosine',store.mirror.calls[2][0])
        self.assertEqual(store.mirror.calls[2][2],15)

    def test_batched_db_failure_propagates_not_cached_success(self):
        class Mirror:
            def query(self,*args,**kwargs):raise TimeoutError('bounded query timeout')
        with self.assertRaises(TimeoutError):
            ScaleStore(Mirror()).full_query('business',1,'archive',28,511,query_mode='batched')

    def test_unknown_query_mode_rejected(self):
        with self.assertRaises(ValueError):ScaleStore(None).full_query('b',1,'a',1,1,query_mode='invalid')

    def test_journal_oracle_replays_active_and_terminal_task_at_every_seq(self):
        parent=Path(__file__).resolve().parents[1]/'data/local-check'
        parent.mkdir(parents=True,exist_ok=True)
        with tempfile.TemporaryDirectory(dir=parent) as directory:
            templates={};source={'run_id':'business','batches':{},'identity':{'verified':True}}
            db=sqlite3.connect(Path(directory)/'journal.sqlite')
            db.execute('CREATE TABLE commits(run_id TEXT,seq INTEGER,checksum TEXT,batch BLOB)')
            for seq,status in enumerate(['assigned','carrying','completed'],1):
                task={'task_id':'T1','status':status,'robot_id':1}
                robots={r:{'robot_id':r,'task_id':'T1' if r==1 else None} for r in range(1,101)}
                source['batches'][seq]={'robots':robots}  # No task snapshot on *any* seq.
                batch={'run_id':'business','seq':seq,'tasks':[task]}
                db.execute('INSERT INTO commits VALUES(?,?,?,?)',('business',seq,digest(batch),zlib.compress(journal_canonical(batch))))
                templates.update({(seq,r):json.dumps(state) for r,state in robots.items()})
            db.commit();db.close()
            args=parser().parse_args(['benchmark','--business-run','business','--archive-run','archive',
                '--output-dir',directory,'--business-output',directory])
            with patch('backend.wide.verify.load_source',return_value=source):
                tasks,identity=task_oracles(args,None,[1,2,3],templates)
            self.assertEqual([tasks[(seq,1)]['status'] for seq in [1,2,3]],['assigned','carrying','completed'])
            self.assertIsNone(tasks[(2,2)])
            self.assertIn('journal',identity['kind'])

    def test_small_preload_and_full_benchmark_fake_is_not_db_qualification(self):
        parent=Path(__file__).resolve().parents[1]/'data/local-check'
        parent.mkdir(parents=True,exist_ok=True)
        with tempfile.TemporaryDirectory(dir=parent) as directory:
            store=FakeStore()
            args=parser().parse_args(['preload','--business-run','business','--archive-run','archive',
                '--steps','10','--output-dir',directory,'--chunk-steps','2'])
            preload(args,store)
            report=json.loads((Path(directory)/'report.json').read_text(encoding='utf-8'))
            self.assertEqual(report['counts']['states'],1000)
            self.assertTrue(report['business_head_unchanged'])
            other=Path(directory)/'query';other.mkdir()
            args=parser().parse_args(['benchmark','--business-run','business','--archive-run','archive',
                '--clients','3','--rounds','3','--samples','2','--min-states','1000','--output-dir',str(other)])
            benchmark(args,store)
            report=json.loads((other/'report.json').read_text(encoding='utf-8'))
            self.assertTrue(report['qualification']['all_correct'])
            self.assertFalse(report['qualification']['million_states'])
            self.assertFalse(report['qualification']['specified_edge_device'])
            self.assertFalse(report['qualification']['concurrent_observed_writer_commits'])
            self.assertEqual(len((other/'queries.jsonl').read_text().splitlines()),18)

    def test_create_archive_does_not_accept_existing_business_run(self):
        class Mirror:
            def query(self,*args,**kwargs):return [{'metadata':{'committed_seq':2}}]
        store=ScaleStore(Mirror())
        with self.assertRaises(ValueError):store.create_archive('same','same',10,'hash')
        with self.assertRaises(ValueError):store.create_archive('archive','business',10,'hash')
        with self.assertRaises(ValueError):store.create_archive('archive','business',10,'hash',True)

    def test_source_template_bounds_and_every_robot_required(self):
        class Mirror:
            def query(self,*args,**kwargs):return [{'seq':1,'robot':1,'payload':'{}'}]
        with self.assertRaises(ValueError):ScaleStore(Mirror()).templates('business')


if __name__=='__main__':unittest.main()
