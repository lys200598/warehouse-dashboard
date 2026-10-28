"""Synthetic report-format fixtures only; these do not establish physical capacity."""
from copy import deepcopy
from contextlib import closing
import csv
import importlib.util
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch
import zlib

from backend.checkpoint import write_checkpoint

ROOT=Path(__file__).resolve().parents[1]
spec=importlib.util.spec_from_file_location('meeting_capacity_summary', ROOT/'ops/summarize_meeting_capacity.py')
summary=importlib.util.module_from_spec(spec);spec.loader.exec_module(summary)


class CapacitySummaryTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory(dir=ROOT/'tests');self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name);(self.root/'logs').mkdir()
        self.plan=[self.case('baseline-x3'),self.case('current-x3')]
        self.save_plan()

    def save_plan(self):
        (self.root/'capacity-plan-v1.json').write_text(json.dumps(self.plan))

    def case(self,name,*,due=100):
        root=self.root;dataset=root/'inputs'/name;dataset.mkdir(parents=True)
        scene=dataset/'scene.json';scene.write_text('{"version":"fixture"}')
        config=dataset/'config.json';config.write_text('{"speed":0.8}')
        task={'task_id':'T1','task_type':'OUTBOUND','goods_id':'G1','release_time':'2026-10-28T00:00:00+08:00',
            'due_time':f'2026-10-28T00:{due//60:02d}:{due%60:02d}+08:00','source_id':'R01-L1-C1-F1','target_id':'OUT-1'}
        with (dataset/'tasks.csv').open('w',newline='') as f:
            w=csv.DictWriter(f,fieldnames=list(task));w.writeheader();w.writerow(task)
        (dataset/'initial_inventory.csv').write_text('goods_id,location_id\nG1,R01-L1-C1-F1\n')
        sha=summary.file_sha
        manifest={'tasks':1,'start':'2026-10-28T00:00:00+08:00','files':{'tasks.csv':sha(dataset/'tasks.csv'),'initial_inventory.csv':sha(dataset/'initial_inventory.csv')}}
        (dataset/'manifest.json').write_text(json.dumps(manifest))
        identity={'tasks_sha256':sha(dataset/'tasks.csv'),'initial_inventory_sha256':sha(dataset/'initial_inventory.csv'),
            'manifest_sha256':sha(dataset/'manifest.json'),'scene_sha256':sha(scene)}
        run='fixture-'+name;out=root/'runs'/name;out.mkdir(parents=True)
        robots=[{'robot_id':i,'robot_type_key':'X3','x':float(i),'y':1.,'battery':70.,'stage':'idle',
            'carrying':False,'task_id':None,'cargo_id':None,'completed':int(i==1),'missions':int(i==1),
            'active_seconds':10 if i==1 else 0,'charge_seconds':0,'queue_seconds':0,'distance':float(i==1)} for i in range(1,101)]
        cp={'run_id':run,'elapsed':10,'tick':10,'knowledge_seq':1,'event_sequence':1,
            'config':{'speed':.8},'run_metadata':identity,'metrics':{'completed':1,'late':int(10>due),'max_active_business':1},
            'robots':[{'state':r,'worker_pid':42} for r in robots],
            'tasks':{'T1':{**task,'status':'completed','release_second':0,'due_second':due,'completed_at':10}},
            'goods':{'G1':'SHIPPED'}}
        write_checkpoint(out/'checkpoint.json',cp)
        result={'run_id':run,'status':'complete','error':None,'elapsed':10,'wall_seconds':.5,'config':{'speed':.8},
            'input_identity':identity,'code_identity':'fixture-code','metrics':{'completed':1,'late':int(10>due),
                'planning_seconds':.2,'planning_calls':2,'dispatch_wall_seconds':{'count':1,'total':.1}},
            'tasks_total':1,'unfinished':0,'late_unfinished':0,
            'acceptance':{'all_input_completed':True,'no_recorded_safety_failure':True}}
        (out/'results.json').write_text(json.dumps(result))
        with closing(sqlite3.connect(out/'frames.sqlite')) as db:
            db.execute('CREATE TABLE frames(second INTEGER PRIMARY KEY,payload BLOB)')
            for second in [0,10]:
                f={'runId':run,'elapsed':second,'tick':second,'running':second==0,'robots':robots,
                    'metrics':{'completed':int(second==10)},'pending':int(second==0),'occupiedSlots':[],
                    'inside':int(second==0),'unreleased':0,'counts':{'queued':0,'charging':0},
                    'backend':name,'scheduler':{'new_metadata':name},'knowledge':{'seq':1}}
                db.execute('INSERT INTO frames VALUES (?,?)',(second,zlib.compress(summary.canonical(f))))
            db.commit()
        self.make_journal(out,run)
        (root/f'logs/peak-{name}.exit').write_text('0')
        return {'name':name,'layout':'original','mix':'x3','source':name,'scene':str(scene),'dataset':str(dataset),
            'config':str(config),'output':str(out),'tasks_sha256':identity['tasks_sha256'],
            'initial_sha256':identity['initial_inventory_sha256'],'deadlines':'unchanged','drain_seconds':100}

    def make_journal(self,out,run,*,kind='shipped'):
        with closing(sqlite3.connect(out/'journal.sqlite')) as db:
            db.execute('CREATE TABLE IF NOT EXISTS commits(run_id TEXT,seq INTEGER,checksum TEXT,batch BLOB)')
            db.execute('DELETE FROM commits')
            b={'run_id':run,'seq':1,'events':[{'event_id':run+':event:1','event_seq':1,'kind':kind,'sim_time':10,
                'robot_id':1,'task_id':'T1','goods_id':'G1','location_id':'SHIPPED'}]}
            db.execute('INSERT INTO commits VALUES (?,?,?,?)',(run,1,summary.digest(b),zlib.compress(summary.canonical(b))))
            db.commit()

    def report(self):return summary.summarize(self.root)

    def test_valid_short_reports_keep_long_duration_unaccepted_and_metadata_out_of_physics(self):
        report=self.report();self.assertTrue(report['passed_short_regression'])
        self.assertFalse(report['long_term_capacity_accepted']);self.assertIsNone(report['candidate_selected'])
        first=report['runs'][0]
        self.assertEqual(first['flow_seconds_completed_only']['mean'],10)
        self.assertEqual(first['by_type']['X3']['robots'],100)
        self.assertEqual(first['sampled']['peak_pending_sampled'],1)
        self.assertEqual(first['max_active_business_each_step'],1)
        self.assertEqual(report['baseline_current_physical_equivalence']['compared_sample_frames'],2)

    def test_missing_and_nonzero_exits_never_mark_completed(self):
        p=self.root/'logs/peak-current-x3.exit';p.unlink()
        self.assertFalse(self.report()['passed_short_regression'])
        p.write_text('2');r=self.report();self.assertFalse(r['runs'][1]['completed_run']);self.assertEqual(r['runs'][1]['exit_code'],2)

    def test_explicit_supervisor_exit_file_is_used_instead_of_legacy_default(self):
        legacy=self.root/'logs/peak-current-x3.exit';legacy.write_text('2')
        observed=self.root/'logs/final-current-x3.exit';observed.write_text('0\n')
        self.plan[1]['exit_file']='logs/final-current-x3.exit';self.save_plan()
        r=self.report();self.assertTrue(r['passed_short_regression'])
        self.assertEqual(r['runs'][1]['exit_file'],str(observed.resolve()))
        self.assertEqual(r['runs'][1]['artifacts']['exit'],summary.file_sha(observed))

    def test_explicit_exit_file_escape_missing_or_noninteger_remains_failure(self):
        self.plan[1]['exit_file']=str(self.root.parent/'outside.exit');self.save_plan()
        r=self.report();self.assertFalse(r['runs'][1]['completed_run'])
        self.assertTrue(any('escapes assessment root' in e for e in r['runs'][1]['errors']))
        observed=self.root/'logs/coverage-current-x3.exit'
        self.plan[1]['exit_file']=str(observed);self.save_plan()
        r=self.report();self.assertFalse(r['runs'][1]['completed_run'])
        self.assertTrue(any('exit code absent' in e for e in r['runs'][1]['errors']))
        observed.write_text('0.0')
        r=self.report();self.assertFalse(r['runs'][1]['completed_run'])
        self.assertTrue(any('invalid literal for int' in e for e in r['runs'][1]['errors']))

    def test_checkpoint_checksum_corruption_is_not_accepted(self):
        p=Path(self.plan[1]['output'])/'checkpoint.json';v=json.loads(p.read_text());v['payload']['elapsed']=11;p.write_text(json.dumps(v))
        r=self.report();self.assertFalse(r['passed_short_regression']);self.assertTrue(any('SHA256' in e for e in r['runs'][1]['errors']))

    def test_actual_input_file_change_fails_identity_check(self):
        p=Path(self.plan[1]['dataset'])/'tasks.csv';p.write_bytes(p.read_bytes()+b'\n')
        r=self.report();self.assertFalse(r['source_input_identical_across_runs']);self.assertFalse(r['passed_short_regression'])

    def test_input_changed_during_collection_is_detected_by_post_read_hash(self):
        original=summary.journal_events;out=Path(self.plan[1]['output'])
        def collect_then_mutate(path,*args):
            result=original(path,*args)
            if path.parent==out:
                source=Path(self.plan[1]['dataset'])/'tasks.csv';source.write_bytes(source.read_bytes()+b'\n')
            return result
        with patch.object(summary,'journal_events',side_effect=collect_then_mutate):report=self.report()
        self.assertFalse(report['runs'][1]['completed_run'])
        self.assertTrue(any('changed during' in e for e in report['runs'][1]['errors']))

    def test_different_valid_deadlines_are_not_same_load(self):
        self.plan[1]=self.case('changed-deadline',due=120);self.plan[1]['name']='changed-deadline';self.save_plan()
        r=summary.summarize(self.root,current='changed-deadline');self.assertTrue(all(x['completed_run'] for x in r['runs']))
        self.assertFalse(r['source_input_identical_across_runs']);self.assertFalse(r['passed_short_regression'])

    def test_source_status_incomplete_cannot_be_hidden_by_exit_zero(self):
        p=Path(self.plan[1]['output'])/'results.json';v=json.loads(p.read_text());v['status']='wall-time-limit';p.write_text(json.dumps(v))
        self.assertFalse(self.report()['passed_short_regression'])

    def test_completed_late_tasks_are_reported_and_do_not_approve_a_layout(self):
        self.plan=[self.case('baseline-late',due=5),self.case('current-late',due=5)];self.save_plan()
        report=summary.summarize(self.root,baseline='baseline-late',current='current-late')
        self.assertTrue(report['all_runs_completed_and_integrity_verified'])
        self.assertFalse(report['all_runs_zero_lateness'])
        self.assertEqual(report['runs'][1]['late_completed'],1)
        self.assertFalse(report['long_term_capacity_accepted']);self.assertIsNone(report['candidate_selected'])

    def test_rehashed_checkpoint_cannot_silently_change_source_task_deadline(self):
        p=Path(self.plan[1]['output'])/'checkpoint.json';cp=json.loads(p.read_text())['payload']
        cp['tasks']['T1']['due_second']=999;write_checkpoint(p,cp)
        report=self.report();self.assertFalse(report['runs'][1]['completed_run'])
        self.assertTrue(any('clock differs' in e for e in report['runs'][1]['errors']))

    def test_changed_real_event_is_compared_even_with_valid_source_checksum(self):
        out=Path(self.plan[1]['output']);self.make_journal(out,'fixture-current-x3',kind='different-event')
        r=self.report();self.assertTrue(all(x['completed_run'] for x in r['runs']))
        self.assertFalse(r['baseline_current_physical_equivalence']['checks']['events'])

    def test_sampled_robot_battery_difference_is_not_metadata(self):
        out=Path(self.plan[1]['output'])
        with closing(sqlite3.connect(out/'frames.sqlite')) as db:
            blob=db.execute('SELECT payload FROM frames WHERE second=0').fetchone()[0];f=json.loads(zlib.decompress(blob))
            f['robots'][0]['battery']=69.;db.execute('UPDATE frames SET payload=? WHERE second=0',(zlib.compress(summary.canonical(f)),));db.commit()
        r=self.report();self.assertFalse(r['baseline_current_physical_equivalence']['passed'])
        self.assertEqual(r['baseline_current_physical_equivalence']['different_sample_seconds'],[0])

    def test_source_files_remain_byte_identical_after_report(self):
        files=[p for p in self.root.rglob('*') if p.is_file()]
        before={str(p):summary.file_sha(p) for p in files};self.report()
        self.assertEqual(before,{str(p):summary.file_sha(p) for p in files})

    def test_missing_frame_database_is_not_inferred_from_final_checkpoint(self):
        (Path(self.plan[1]['output'])/'frames.sqlite').unlink()
        r=self.report();self.assertFalse(r['passed_short_regression']);self.assertFalse(r['baseline_current_physical_equivalence']['passed'])

    def test_duplicate_case_names_and_escaping_source_paths_are_rejected(self):
        self.plan[1]['name']=self.plan[0]['name'];self.save_plan()
        with self.assertRaises(ValueError):self.report()
        self.plan[1]['name']='current-x3';self.plan[1]['output']=str(self.root.parent/'outside');self.save_plan()
        with self.assertRaises(ValueError):self.report()


if __name__=='__main__':unittest.main()
