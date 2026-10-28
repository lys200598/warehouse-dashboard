"""Offline parser/composition fixtures; no physical or database benchmark claim."""
from copy import deepcopy
import hashlib
import importlib.util
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest
import zlib


ROOT=Path(__file__).resolve().parents[1]
SPEC=importlib.util.spec_from_file_location('platform_joint_auditor',ROOT/'ops/audit_platform_joint.py')
audit=importlib.util.module_from_spec(SPEC);SPEC.loader.exec_module(audit)
TEMP=ROOT/'data/local-check/platform-audit-tests';TEMP.mkdir(parents=True,exist_ok=True)


def write(path,value):
    path.parent.mkdir(parents=True,exist_ok=True)
    path.write_text(json.dumps(value,ensure_ascii=False,sort_keys=True),encoding='utf-8')


def write_lines(path,rows):
    path.parent.mkdir(parents=True,exist_ok=True)
    path.write_text(''.join(json.dumps(r,ensure_ascii=False,sort_keys=True)+'\n' for r in rows),encoding='utf-8')


class JointAuditTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory(prefix='joint-',dir=TEMP)
        self.root=Path(self.temp.name).resolve()
        self.assertTrue(self.root.is_relative_to(TEMP.resolve()))
        self.addCleanup(self.temp.cleanup)
        self.manifest=self.fixture()

    def fixture(self):
        root=self.root
        (root/'code').mkdir();(root/'code/source.py').write_text('frozen source\n',encoding='utf-8')
        write(root/'code/frozen-files.json',{'files':{'source.py':audit.file_sha(root/'code/source.py')}})
        (root/'input').mkdir();(root/'input/tasks.csv').write_text('task fixture\n',encoding='utf-8')
        (root/'input/initial_inventory.csv').write_text('stock fixture\n',encoding='utf-8')
        write(root/'scene.json',{'test_fixture':True})
        data={'tasks':1,'scene_sha256':audit.file_sha(root/'scene.json'),'files':{n:audit.file_sha(root/'input'/n) for n in ['tasks.csv','initial_inventory.csv']}}
        write(root/'input/manifest.json',data)
        identities={'scene_sha256':data['scene_sha256'],'manifest_sha256':audit.file_sha(root/'input/manifest.json'),
            'tasks_sha256':data['files']['tasks.csv'],'initial_inventory_sha256':data['files']['initial_inventory.csv']}
        metadata={**identities,'execution_code_sha256':'execution-fixture'};config={'robot_count':100}
        cp={'run_id':'joint-fixture','knowledge_seq':2,'robots':[{'state':{'robot_id':i}} for i in range(1,101)],
            'config':config,'run_metadata':metadata,'cursor':1,'tasks':{'t1':{'status':'completed'}},
            'knowledge_runtime':{'identity':{'mode':'required'}}}
        cp_sha=audit.digest(cp);write(root/'run/checkpoint.json',{'sha256':cp_sha,'payload':cp})
        result={'run_id':'joint-fixture','status':'complete','error':None,'input_identity':identities,
            'config':config,'code_identity':'execution-fixture','registered_robots':100,'tasks_total':1,'released':1,'unfinished':0,
            'late_unfinished':0,'metrics':{'completed':1,'late':0,'handoffs':1},
            'knowledge':{'durable_seq':2,'mirrored_seq':2,'last_error':None},'acceptance':{'no_recorded_safety_failure':True}}
        write(root/'run/results.json',result)
        db=sqlite3.connect(root/'run/journal.sqlite')
        db.execute('CREATE TABLE commits(run_id TEXT,seq INTEGER,checksum TEXT,batch BLOB)')
        db.execute('CREATE TABLE heads(run_id TEXT,seq INTEGER,checksum TEXT,checkpoint BLOB,mirrored_seq INTEGER)')
        self.batches={}
        for seq in (1,2):
            events=[] if seq==1 else [{'event_seq':i,'kind':kind,'sim_time':10} for i,kind in enumerate(['knowledge_decision','cargo_handoff','knowledge_action_applied'],1)]
            batch={'run_id':'joint-fixture','seq':seq,'sim_time':seq*5,'metadata':metadata,'config':config,
                'robots':[{'robot_id':i} for i in range(1,101)],'events':events}
            digest=audit.digest(batch);self.batches[seq]=digest
            db.execute('INSERT INTO commits VALUES(?,?,?,?)',('joint-fixture',seq,digest,zlib.compress(audit.canonical(batch))))
        db.execute('INSERT INTO heads VALUES(?,?,?,?,?)',('joint-fixture',2,cp_sha,zlib.compress(audit.canonical(cp)),2));db.commit();db.close()
        control={'passed':True,'failed_checks':[],'run_id':'joint-fixture','prefix':2,
            'source_files_before':{'journal':audit.file_sha(root/'run/journal.sqlite'),'results':audit.file_sha(root/'run/results.json')},
            'control_chain':{'counts':{'actual_cargo_handoffs':1,'decisions':1,'logged_applied_actions':1}}}
        control['source_files_after']=deepcopy(control['source_files_before']);write(root/'control.json',control)
        requests=[];responses=[]
        for client,seq,start,end in [(1,1,100,900),(2,2,200,800)]:
            payload={'run_id':'joint-fixture','seq':seq,'batch_checksum':self.batches[seq],'graph_committed_seq':2,'value':{'client':client}}
            requests.append({'request_id':str(client),'round':1,'client_id':client,'run_id':'joint-fixture','seq':seq,
                'point_kind':'latest','start_ns':start,'end_ns':end,'ok':True,'response_sha256':audit.digest(payload)})
            responses.append({'request_id':str(client),'result':payload})
        write_lines(root/'queries/requests.jsonl',requests);write_lines(root/'queries/responses.jsonl',responses)
        write(root/'queries/verification.json',{'passed':True,'request_count':2,
            'source_journal_sha256':audit.file_sha(root/'run/journal.sqlite'),
            'file_hashes':{n:audit.file_sha(root/'queries'/(n+'.jsonl')) for n in ['requests','responses']}})
        write_lines(root/'observations.jsonl',[{'observed_at_ns':time,'run_id':'joint-fixture','committed_seq':seq} for time,seq in [(90,1),(300,1),(700,2)]])
        (root/'run.log').write_text('fixture stage complete\n',encoding='utf-8')
        spec={'version':audit.VERSION,'source':{'root':'code','manifest':'code/frozen-files.json'},
            'dataset':{'directory':'input','scene':'scene.json'},'run':{'directory':'run','run_id':'joint-fixture','mode':'required'},
            'control_audit':'control.json','queries':{'requests':'queries/requests.jsonl','responses':'queries/responses.jsonl',
                'verification':'queries/verification.json','expected_clients':2},'writer_observations':'observations.jsonl',
            'stages':[{'name':'run','exit_code':0,'start_ns':1,'end_ns':1000,'log':'run.log','log_sha256':audit.file_sha(root/'run.log')}]}
        write(root/'manifest.json',spec)
        return root/'manifest.json'

    def mutate(self,name,fn):
        path=self.root/name;value=audit.load(path);fn(value);write(path,value)

    def mutate_lines(self,name,fn):
        path=self.root/name;rows=list(audit.lines(path));fn(rows);write_lines(path,rows)

    def report(self):return audit.audit(self.manifest)

    def wrap_catalog(self,query):
        values=list(audit.lines(self.root/'queries/responses.jsonl'))
        for value in values:
            p=value['result'];head={'batch_checksum':p['batch_checksum'],'graph_committed_seq':p['graph_committed_seq']}
            value['result']={'catalog_version':'catalog-fixture-v1','query_id':query,'run_id':p['run_id'],'seq':p['seq'],
                'result':{'context':p} if query=='self' else {'point':head,'value':p['value']}}
        write_lines(self.root/'queries/responses.jsonl',values)
        fingerprints={v['request_id']:audit.digest(v['result']) for v in values}
        self.mutate_lines('queries/requests.jsonl',lambda rows:[r.update(response_sha256=fingerprints[r['request_id']]) for r in rows])
        self.mutate('queries/verification.json',lambda d:d.update(file_hashes={n:audit.file_sha(self.root/'queries'/(n+'.jsonl')) for n in ['requests','responses']}))

    def test_catalog_context_and_fleet_wire_profiles_preserve_full_response_hash(self):
        self.wrap_catalog('self');self.assertTrue(self.report()['passed'])
        row=list(audit.lines(self.root/'queries/responses.jsonl'))[0]
        self.assertEqual(audit.response_point(row['result'])['seq'],1)

    def test_catalog_fleet_point_wire_profile_passes(self):
        self.wrap_catalog('energy');self.assertTrue(self.report()['passed'])

    def test_catalog_unobserved_or_conflicting_source_head_cannot_be_invented(self):
        payload={'catalog_version':'fixture','query_id':'goods','run_id':'joint-fixture','seq':1,
            'result':{'batch_checksum':self.batches[1]}}
        with self.assertRaisesRegex(ValueError,'observed head'):audit.response_point(payload)
        payload['query_id']='self';payload['result']={'context':{'run_id':'other','seq':1,
            'graph_committed_seq':2,'batch_checksum':self.batches[1]}}
        with self.assertRaisesRegex(ValueError,'inner/outer'):audit.response_point(payload)

    def test_complete_source_response_and_overlap_fixture_passes(self):
        result=self.report();self.assertTrue(result['passed']);self.assertEqual(result['query_api_peaks'],{'1':2})
        self.assertEqual(result['actual_robot_state_rows'],200)
        self.assertTrue(result['qualifications']['same_run_live_writer_overlap_each_round'])

    def test_slow_and_failed_requests_remain_in_statistics_and_fail_success_gate(self):
        self.mutate_lines('queries/requests.jsonl',lambda rows:rows[0].update(end_ns=600000100,ok=False))
        self.mutate('queries/verification.json',lambda d:d['file_hashes'].update(requests=audit.file_sha(self.root/'queries/requests.jsonl')))
        r=self.report();self.assertFalse(r['passed']);self.assertEqual(r['all_request_ms']['over_500ms'],1)
        self.assertEqual(r['all_request_ms']['failed'],1);self.assertGreater(r['all_request_ms']['max'],500)

    def test_foreign_or_future_query_cannot_pass(self):
        for values in [{'run_id':'other'},{'seq':3},{'seq':True}]:
            with self.subTest(values=values):
                old=(self.root/'queries/requests.jsonl').read_bytes()
                self.mutate_lines('queries/requests.jsonl',lambda rows:rows[0].update(values))
                with self.assertRaises(ValueError):self.report()
                (self.root/'queries/requests.jsonl').write_bytes(old)

    def test_changed_full_response_or_missing_response_rejected(self):
        old=(self.root/'queries/responses.jsonl').read_bytes()
        self.mutate_lines('queries/responses.jsonl',lambda rows:rows[0]['result']['value'].update(client=99))
        with self.assertRaisesRegex(ValueError,'fingerprint'):self.report()
        (self.root/'queries/responses.jsonl').write_bytes(old)
        self.mutate_lines('queries/responses.jsonl',lambda rows:rows.pop())
        self.assertIn('every_request_has_complete_response',self.report()['failed_checks'])

    def test_wrong_batch_even_if_fingerprint_updated_is_rejected(self):
        self.mutate_lines('queries/responses.jsonl',lambda rows:rows[0]['result'].update(batch_checksum='0'*64))
        payload=list(audit.lines(self.root/'queries/responses.jsonl'))[0]['result']
        self.mutate_lines('queries/requests.jsonl',lambda rows:rows[0].update(response_sha256=audit.digest(payload)))
        with self.assertRaisesRegex(ValueError,'batch differs'):self.report()

    def test_peak_is_observed_from_real_intervals_not_client_count(self):
        self.mutate_lines('queries/requests.jsonl',lambda rows:rows[1].update(start_ns=901,end_ns=950))
        r=self.report();self.assertEqual(r['query_api_peaks'],{'1':1});self.assertIn('actual_api_peak_each_round',r['failed_checks'])

    def test_head_outside_query_window_does_not_count_as_overlap(self):
        self.mutate_lines('observations.jsonl',lambda rows:rows[-1].update(observed_at_ns=901))
        r=self.report();self.assertFalse(r['qualifications']['same_run_live_writer_overlap_each_round'])
        self.assertIn('same_run_graph_write_advance_in_each_query_round',r['failed_checks'])

    def test_stale_latest_cutoff_fails_even_when_historical_cutoff_is_valid(self):
        self.mutate_lines('observations.jsonl',lambda rows:rows.insert(1,{'observed_at_ns':95,'run_id':'joint-fixture','committed_seq':2}))
        # Keep observations monotone after the early advance.
        self.mutate_lines('observations.jsonl',lambda rows:rows[2].update(committed_seq=2))
        r=self.report();self.assertIn('latest_cutoff_not_older_than_pre_request_observed_head',r['failed_checks'])

    def test_foreign_or_reversed_writer_observations_rejected(self):
        self.mutate_lines('observations.jsonl',lambda rows:rows[-1].update(run_id='other'))
        with self.assertRaisesRegex(ValueError,'writer observation'):self.report()

    def test_source_bytes_dataset_hash_and_stage_log_are_bound(self):
        (self.root/'code/source.py').write_text('changed source\n',encoding='utf-8')
        self.assertIn('all_selected_source_bytes_match',self.report()['failed_checks'])
        (self.root/'input/tasks.csv').write_text('changed dataset\n',encoding='utf-8')
        self.assertIn('dataset_tasks.csv_sha',self.report()['failed_checks'])
        (self.root/'run.log').write_text('changed log\n',encoding='utf-8')
        with self.assertRaisesRegex(ValueError,'log changed'):self.report()

    def test_unlisted_backend_module_is_detected(self):
        (self.root/'code/backend').mkdir()
        (self.root/'code/backend/new.py').write_text('unlisted source\n',encoding='utf-8')
        self.assertIn('all_backend_sources_are_listed',self.report()['failed_checks'])

    def test_bool_counts_do_not_compare_equal_to_integers(self):
        self.mutate('run/results.json',lambda d:d.update(tasks_total=True))
        with self.assertRaisesRegex(ValueError,'result tasks_total'):self.report()

    def test_source_future_event_and_bool_robot_id_fail_before_composition(self):
        db=sqlite3.connect(self.root/'run/journal.sqlite')
        for mutation,error in [(lambda b:b['events'][0].update(sim_time=11),'future'),
                               (lambda b:b['robots'][0].update(robot_id=True),'robot state')]:
            checksum,blob=db.execute('SELECT checksum,batch FROM commits WHERE seq=2').fetchone()
            batch=json.loads(zlib.decompress(blob));changed=deepcopy(batch);mutation(changed)
            db.execute('UPDATE commits SET checksum=?,batch=? WHERE seq=2',(audit.digest(changed),zlib.compress(audit.canonical(changed))));db.commit()
            with self.assertRaisesRegex(ValueError,error):self.report()
            db.execute('UPDATE commits SET checksum=?,batch=? WHERE seq=2',(checksum,blob));db.commit()
        db.close()

    def test_control_proof_cannot_be_reused_from_another_source(self):
        self.mutate('control.json',lambda d:d['source_files_before'].update(journal='0'*64))
        self.assertIn('control_audit_source_bound',self.report()['failed_checks'])

    def test_checkpoint_envelope_and_source_head_match(self):
        self.mutate('run/checkpoint.json',lambda d:d['payload'].update(cursor=2))
        r=self.report();self.assertIn('checkpoint_envelope_valid',r['failed_checks'])
        self.assertIn('journal_checkpoint_identical',r['failed_checks'])

    def test_incomplete_declared_full_run_is_not_success(self):
        self.mutate('run/results.json',lambda d:d.update(unfinished=1))
        r=self.report();self.assertIn('all_input_released_and_completed',r['failed_checks'])

    def test_hash_verification_and_stage_success_require_typed_evidence(self):
        self.mutate('queries/verification.json',lambda d:d.update(passed='true'))
        r=self.report();self.assertIn('independent_query_verification_passed',r['failed_checks'])
        self.mutate('manifest.json',lambda d:d['stages'][0].update(exit_code=False))
        self.assertIn('all_stage_exit_codes_zero',self.report()['failed_checks'])

    def test_missing_source_cli_writes_fail_closed_report_and_preserves_output(self):
        output=self.root/'failed-report.json';missing=self.root/'missing-manifest.json'
        self.assertEqual(audit.main(['--manifest',str(missing),'--output',str(output)]),2)
        self.assertFalse(audit.load(output)['passed'])
        with self.assertRaises(SystemExit):audit.main(['--manifest',str(self.manifest),'--output',str(output)])


if __name__=='__main__':unittest.main()
