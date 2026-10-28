from copy import deepcopy
import json
from pathlib import Path
import sqlite3
import tempfile
import time
import unittest
from unittest.mock import patch
import zlib

from backend.checkpoint import write_checkpoint
from backend.knowledge import canonical,digest
from backend.wide.verify import load_source,verify_correctness,concurrent_round,_stats


def fixture(directory):
    directory=Path(directory)
    run='verification-test'
    robots={i:{'robot_id':i,'carrying':False,'cargo_id':None,'task_id':None} for i in range(1,101)}
    task={'task_id':'T1','goods_id':'G1','source_id':'R01-L1-C1-F1','target_id':'OUT-1','status':'carrying','robot_id':1}
    events=[{'kind':'initial_stock','robot_id':None,'task_id':None,'goods_id':'G1','location_id':task['source_id']},
            {'kind':'picked','robot_id':1,'task_id':'T1','goods_id':'G1','location_id':'ROBOT-1'},
            {'kind':'cargo_handoff','robot_id':2,'task_id':'T1','goods_id':'G1','location_id':'ROBOT-2',
             'donor_robot_id':1,'receiver_robot_id':2,'reason':'low_energy'},
            {'kind':'shipped','robot_id':2,'task_id':'T1','goods_id':'G1','location_id':'SHIPPED'}]
    batches=[]
    for seq,event in enumerate(events,1):
        event.update(event_id=f'{run}:event:{seq}',event_seq=seq,sim_time=seq*10)
        if seq==2:robots[1].update(carrying=True,cargo_id='G1',task_id='T1')
        if seq==3:
            robots[1].update(carrying=False,cargo_id=None,task_id=None)
            robots[2].update(carrying=True,cargo_id='G1',task_id='T1');task['robot_id']=2
        if seq==4:
            robots[2].update(carrying=False,cargo_id=None,task_id=None);task['status']='completed'
        batches.append({'run_id':run,'seq':seq,'sim_time':seq*10,'events':[deepcopy(event)],
            'tasks':[deepcopy(task)] if seq>=2 else [],'robots':deepcopy(list(robots.values()))})
    cp={'run_id':run,'running':False,'knowledge_seq':4,'event_sequence':4,'elapsed':40,
        'goods':{'G1':'SHIPPED'},'inside':0,'cursor':1,'tasks':{'T1':task},
        'robots':[{'state':r} for r in robots.values()]}
    write_checkpoint(directory/'checkpoint.json',cp)
    db=sqlite3.connect(directory/'journal.sqlite')
    db.executescript('CREATE TABLE commits(run_id TEXT,seq INTEGER,checksum TEXT,batch BLOB); '
        'CREATE TABLE heads(run_id TEXT,seq INTEGER,checksum TEXT,checkpoint BLOB,mirrored_seq INTEGER);')
    for batch in batches:
        db.execute('INSERT INTO commits VALUES(?,?,?,?)',(run,batch['seq'],digest(batch),zlib.compress(canonical(batch))))
    db.execute('INSERT INTO heads VALUES(?,?,?,?,?)',(run,4,digest(cp),zlib.compress(canonical(cp)),4))
    db.commit();db.close()
    return cp


class VerificationSourceTests(unittest.TestCase):
    def temporary(self):
        return tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parents[1]/'data/local-check')

    def test_hash_and_independent_custody_replay_select_pre_handoff_cutoff(self):
        with self.temporary() as directory:
            fixture(directory);source=load_source(directory)
            self.assertEqual(source['identity']['journal_commits_verified'],4)
            self.assertEqual(source['historical_seq'],2)
            self.assertEqual(source['history']['G1'][1]['location_id'],'ROBOT-1')
            self.assertEqual(source['ledger']['G1'],'SHIPPED')

    def test_valid_hash_cannot_hide_invalid_custody_transfer(self):
        with self.temporary() as directory:
            fixture(directory)
            with sqlite3.connect(Path(directory)/'journal.sqlite') as db:
                blob=db.execute('SELECT batch FROM commits WHERE seq=3').fetchone()[0]
                batch=json.loads(zlib.decompress(blob));batch['events'][0]['donor_robot_id']=9
                db.execute('UPDATE commits SET checksum=?,batch=? WHERE seq=3',(digest(batch),zlib.compress(canonical(batch))))
            db.close()
            with self.assertRaisesRegex(AssertionError,'unique donor custody'):load_source(directory)

    def test_corrupted_archive_or_checkpoint_is_rejected(self):
        with self.temporary() as directory:
            fixture(directory)
            with sqlite3.connect(Path(directory)/'journal.sqlite') as db:
                db.execute("UPDATE commits SET checksum='bad' WHERE seq=2")
            db.close()
            with self.assertRaisesRegex(AssertionError,'checksum'):load_source(directory)
        with self.temporary() as directory:
            fixture(directory)
            path=Path(directory)/'checkpoint.json';env=json.loads(path.read_text());env['payload']['inside']=100
            path.write_text(json.dumps(env))
            with self.assertRaisesRegex(ValueError,'SHA256'):load_source(directory)

    def test_temporal_oracle_rejects_future_holder_leak(self):
        with self.temporary() as directory:
            fixture(directory);source=load_source(directory)
            class Mirror:
                def committed_point(self,run):return {'seq':4,'checksum':source['last_checksum']}
                def query(self,*args):return [{'goods_id':'G1','location_id':'SHIPPED'}]
            class API:
                leak=False
                def __init__(self,mirror):pass
                def goods_history(self,run,goods,seq,limit,after_event_seq):
                    rows=[r for r in source['history'][goods] if r['seq']<=seq]
                    current=rows[-1]
                    if self.leak:current=source['history'][goods][-1]
                    return {'seq':seq,'rows':rows,'current_as_of':{k:current[k] for k in ['location_id','event_seq','sim_time']},'has_more':False}
                def cooperation_history(self,*args,**kw):return {'rows':[{'event':source['handoffs'][0]}],'has_more':False}
                def robot_context(self,run,owner,seq):
                    b=source['batches'][seq];r=b['robots'][owner]
                    return {'seq':seq,'robot':r,'task':b['tasks'].get(r['task_id'])}
            with patch('backend.wide.verify.WideQueries',API),patch('backend.wide.verify.audit_wide',return_value={'ok':True}):
                result=verify_correctness(Mirror(),source)
                self.assertTrue(result['historical_and_final_holder_differ'])
                API.leak=True
                with self.assertRaisesRegex(AssertionError,'leaked'):verify_correctness(Mirror(),source)


class ConcurrencyProtocolTests(unittest.TestCase):
    def test_one_round_covers_100_distinct_full_query_api_calls(self):
        robots={i:{'robot_id':i,'task_id':None} for i in range(1,101)}
        source={'run_id':'smoke','checkpoint':{'knowledge_seq':1},'batches':{1:{'robots':robots,'tasks':{}}}}
        class Mirror:
            def query(self,q,p):
                time.sleep(.001)
                if 'RETURN r.committed_seq AS seq' in q:return [{'seq':1}]
                owner=int(p['state_key'].split(':')[-1])
                return [{'robot_payload':json.dumps(robots[owner]),'task_payload':None}]
        result=concurrent_round(Mirror(),source,1)
        self.assertTrue(result['all_correct'])
        self.assertEqual({r['robot_id'] for r in result['records']},set(range(1,101)))
        self.assertEqual(result['successful_request_latency']['count'],100)
        self.assertGreater(result['successful_throughput_qps'],0)

    def test_nearest_rank_percentiles_and_empty_case_are_explicit(self):
        self.assertEqual(_stats(list(range(1,101)))['p95_ms'],95)
        self.assertEqual(_stats(list(range(1,101)))['p99_ms'],99)
        self.assertIsNone(_stats([])['mean_ms'])


if __name__=='__main__':unittest.main()
