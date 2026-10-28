from contextlib import closing
from copy import deepcopy
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest
import zlib

from backend.knowledge import canonical,digest
from backend.query_views import JournalSummary,SummaryDirectory,ViewNotReady


class SummaryTests(unittest.TestCase):
    def setUp(self):
        parent=Path(__file__).resolve().parents[1]/'data/local-check/query-view-tests'
        parent.mkdir(parents=True,exist_ok=True)
        self.tmp=tempfile.TemporaryDirectory(dir=parent);self.root=Path(self.tmp.name)
        self.journal=self.root/'journal.sqlite';self.cache=self.root/'summary.sqlite'
        with closing(sqlite3.connect(self.journal)) as db:
            db.executescript('CREATE TABLE commits(run_id TEXT,seq INTEGER,checksum TEXT,batch BLOB,PRIMARY KEY(run_id,seq));CREATE TABLE heads(run_id TEXT PRIMARY KEY,seq INTEGER);');db.commit()
        self.metadata={'scene':{'racks':[{'id':'R1','zone':'W'},{'id':'R2','zone':'E'}]}}
        self.config={'speed':.8};self.batches=[]

    def tearDown(self):self.tmp.cleanup()

    def add(self,clock,tasks=(),completed=0):
        seq=len(self.batches)+1
        b={'run_id':'r','seq':seq,'sim_time':clock,'metadata':deepcopy(self.metadata),'config':deepcopy(self.config),
           'slots':[{'slot_id':'S1','rack_id':'R1'},{'slot_id':'S2','rack_id':'R2'}],
           'tasks':list(tasks),'completed':completed}
        with closing(sqlite3.connect(self.journal)) as db:
            db.execute('INSERT INTO commits VALUES (?,?,?,?)',('r',seq,digest(b),zlib.compress(canonical(b))))
            db.execute('INSERT OR REPLACE INTO heads VALUES (?,?)',('r',seq));db.commit()
        self.batches.append(b);return b

    def task(self,key='T1',status='waiting',due=10,slot='S1'):
        return {'task_id':key,'status':status,'due_second':due,'release_second':0,'slot_id':slot}

    def fleet(self,seq):
        b=self.batches[seq-1]
        return {'token':(digest(b),'created-a'),'metadata':self.metadata,'config':self.config,
                'step':{'sim_time':b['sim_time'],'completed':b['completed']}}

    def read(self,view,seq,head=None):
        head=head or len(self.batches)
        return view.read('r',seq,self.fleet(seq),head,digest(self.batches[head-1]))

    def test_exact_historical_clock_expiry_and_completion(self):
        self.add(0,[self.task()]);self.add(10);self.add(11);self.add(12,[self.task(status='completed')],1)
        view=JournalSummary(self.journal,self.cache,'r','created-a');view.catch_up()
        self.assertEqual(self.read(view,1)[0]['counts'],[{'zone':'W','status':'waiting','tasks':1,'overdue':0}])
        self.assertEqual(self.read(view,2)[0]['counts'][0]['overdue'],0)
        self.assertEqual(self.read(view,3)[0]['counts'][0]['overdue'],1)
        self.assertEqual(self.read(view,4)[0]['counts'],[{'zone':'W','status':'completed','tasks':1,'overdue':0}])

    def test_incremental_restart_and_equal_simulation_clock(self):
        self.add(0,[self.task()]);v=JournalSummary(self.journal,self.cache,'r','created-a');v.catch_up()
        self.add(0,[self.task('T2',slot='S2')]);v2=JournalSummary(self.journal,self.cache,'r','created-a')
        self.assertEqual(v2.catch_up()['consumed'],1)
        self.assertEqual(self.read(v2,1)[1],2)
        self.assertEqual(sum(c['tasks'] for c in self.read(v2,2)[0]['counts']),2)

    def test_future_graph_head_missing_view_and_wrong_binding_fail(self):
        self.add(0,[self.task()]);self.add(1)
        v=JournalSummary(self.journal,self.cache,'r','created-a');v.catch_up(1)
        with self.assertRaises(ViewNotReady):self.read(v,1)
        v.catch_up();f=self.fleet(1);f['token']=(f['token'][0],'other-created')
        with self.assertRaises(ValueError):v.read('r',1,f,2,digest(self.batches[1]))
        with self.assertRaises(ValueError):v.read('r',1,self.fleet(1),2,'0'*64)
        with self.assertRaises(ValueError):JournalSummary(self.journal,self.cache,'r','created-b')

    def test_bad_new_batch_rolls_back_entire_increment(self):
        self.add(0,[self.task()]);v=JournalSummary(self.journal,self.cache,'r','created-a');v.catch_up()
        self.add(1,[self.task('T2')]);self.add(2,[self.task('T3')])
        with closing(sqlite3.connect(self.journal)) as db:
            db.execute('UPDATE commits SET checksum=? WHERE seq=3',('0'*64,));db.commit()
        with self.assertRaises(ValueError):v.catch_up()
        self.assertEqual(v.status()['head'],1)
        self.assertEqual(self.read(v,1,head=1)[0]['task_entity_count'],1)

    def test_consumed_prefix_replacement_and_row_tampering_fail(self):
        self.add(0,[self.task()]);v=JournalSummary(self.journal,self.cache,'r','created-a');v.catch_up()
        with closing(sqlite3.connect(self.cache)) as db:
            db.execute('UPDATE view_points SET payload=?',('{}',));db.commit()
        with self.assertRaises(ValueError):self.read(v,1)
        with closing(sqlite3.connect(self.journal)) as db:
            db.execute('UPDATE commits SET checksum=?',('0'*64,));db.commit()
        with self.assertRaises(ValueError):v.catch_up()

    def test_future_task_duplicate_delta_and_source_clock_regression_fail(self):
        self.add(0,[self.task()]);v=JournalSummary(self.journal,self.cache,'r','created-a');v.catch_up()
        self.add(1,[self.task('T2'),self.task('T2')])
        with self.assertRaises(ValueError):v.catch_up()
        self.assertEqual(v.status()['head'],1)

    def test_never_overwrites_source_or_unrelated_sqlite(self):
        with self.assertRaises(ValueError):JournalSummary(self.journal,self.journal,'r','created-a')
        other=self.root/'other.sqlite'
        with closing(sqlite3.connect(other)) as db:db.execute('CREATE TABLE precious(value TEXT)');db.commit()
        raw=other.read_bytes()
        with self.assertRaises(ValueError):JournalSummary(self.journal,other,'r','created-a')
        self.assertEqual(raw,other.read_bytes())
        with self.assertRaises(ValueError):SummaryDirectory(self.root).repository('../outside','created')


if __name__=='__main__':unittest.main()
