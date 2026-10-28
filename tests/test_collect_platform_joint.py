"""Offline collector fixtures; no physical or 100-client performance claim."""
from copy import deepcopy
import gzip
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch


ROOT=Path(__file__).resolve().parents[1]
SPEC=importlib.util.spec_from_file_location('platform_joint_collector',ROOT/'ops/collect_platform_joint.py')
collector=importlib.util.module_from_spec(SPEC);SPEC.loader.exec_module(collector)
TEMP=ROOT/'data/local-check/platform-collector-tests';TEMP.mkdir(parents=True,exist_ok=True)


def write(path,value):
    path.parent.mkdir(parents=True,exist_ok=True)
    path.write_text(json.dumps(value,ensure_ascii=False),encoding='utf-8')


def write_rows(path,values):
    path.parent.mkdir(parents=True,exist_ok=True)
    opener=gzip.open if path.suffix=='.gz' else open
    with opener(path,'wt',encoding='utf-8') as out:
        for row in values:out.write(json.dumps(row,ensure_ascii=False)+'\n')


def observation(clock,head=1,run='fixture'):
    return {'run_id':run,'observed_at_ns':clock,'committed_seq':head}


def request(ident='r1',round_id=1,client=1,start=10,end=60,seq=1,query='self'):
    return {'request_id':ident,'round':round_id,'client_id':client,'run_id':'fixture','seq':seq,
        'point_kind':'latest','start_ns':start,'end_ns':end,'ok':True,
        'request':{'query_id':query,'run_id':'fixture','seq':seq,'robot_id':client}}


def decision():
    # Actual audit_events output flattens authorize() into the decision row:
    # action/seq, not the raw event's decision/knowledge_seq aliases.
    return {'event_seq':7,'commit_seq':2,'action':'authorize_assistance_planning',
        'run_id':'fixture','seq':1,'decision_sim_time':300}


class CollectorTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory(prefix='collector-',dir=TEMP)
        self.root=Path(self.temp.name).resolve()
        self.assertTrue(self.root.is_relative_to(TEMP.resolve()))
        self.addCleanup(self.temp.cleanup)

    def test_timing_true_multi_round_peak_is_not_sum_of_round_peaks(self):
        records=[request('a',1,1,10,30),request('b',1,2,20,40),
            request('c',4,1,100,130),request('d',4,2,110,140)]
        report=collector.timing(records)
        self.assertEqual(report['actual_peak'],2)
        self.assertEqual(report['clients'],2)
        self.assertEqual(report['requests'],4)

    def test_timing_end_start_ties_are_half_open_not_extra_concurrency(self):
        report=collector.timing([request('a',start=10,end=20),request('b',start=20,end=30)])
        self.assertEqual(report['actual_peak'],1)

    def test_timing_p99_and_errors_use_complete_denominator(self):
        records=[request(str(i),start=i*1_000_000_000,end=i*1_000_000_000+i*1_000_000) for i in range(1,101)]
        records[0]['ok']=False
        report=collector.timing(records)
        self.assertEqual(report['p99_ms'],99)
        self.assertEqual(report['mean_ms'],50.5)
        self.assertEqual(report['max_ms'],100)
        self.assertEqual(report['failed'],1)

    def test_timing_rejects_empty_reversed_zero_and_untyped_intervals(self):
        for values in ([],[request(start=10,end=10)],[request(start=20,end=10)],
                [request(start=True,end=10)],[{**request(),'ok':1}]):
            with self.subTest(values=values),self.assertRaises(ValueError):collector.timing(values)

    def test_plain_and_gzip_merge_retains_complete_unicode_rows(self):
        plain=self.root/'first.jsonl';zipped=self.root/'second.jsonl.gz';output=self.root/'out.jsonl.gz'
        plain.write_text(json.dumps({'value':'货物'},ensure_ascii=False),encoding='utf-8')
        write_rows(zipped,[{'value':'机器人'}])
        hashes={path:collector.sha(path) for path in (plain,zipped)}
        collector.concatenate([plain,zipped],output)
        self.assertEqual(list(collector.rows(output)),[{'value':'货物'},{'value':'机器人'}])
        self.assertEqual(hashes,{path:collector.sha(path) for path in (plain,zipped)})
        with self.assertRaises(FileExistsError):collector.concatenate([plain],output)

    def test_observer_splice_discards_other_session_earlier_samples(self):
        first=self.root/'a.jsonl';second=self.root/'b.jsonl';output=self.root/'out.jsonl'
        a=[observation(5,1),observation(60,2)]
        b=[observation(30,2),observation(70,2),observation(120,3)]
        write_rows(first,a);write_rows(second,b)
        report=collector.merge_observers(first,second,output)
        self.assertEqual(list(collector.rows(output)),a+b[1:])
        self.assertEqual(report['second_original_samples'],3)
        self.assertEqual(report['second_suffix_samples'],2)
        self.assertEqual(report['original_files_sha256'][str(first)],collector.sha(first))

    def test_observer_full_discarded_prefix_is_validated(self):
        first=self.root/'a.jsonl';second=self.root/'b.jsonl'
        write_rows(first,[observation(5,1),observation(60,2)])
        for bad in ([observation(30,2),observation(20,2),observation(70,2)],
                [observation(20,2),observation(30,1),observation(70,2)],
                [observation(20,1,run='other'),observation(70,2)],[]):
            write_rows(second,bad)
            with self.subTest(bad=bad),self.assertRaises(ValueError):
                collector.merge_observers(first,second,self.root/'unused.jsonl')

    def test_observer_no_later_suffix_or_backwards_splice_fails(self):
        first=self.root/'a.jsonl';second=self.root/'b.jsonl'
        write_rows(first,[observation(5,1),observation(60,3)])
        for b in ([observation(20,2)],[observation(70,2)]):
            write_rows(second,b)
            with self.subTest(b=b),self.assertRaises(ValueError):
                collector.merge_observers(first,second,self.root/'unused.jsonl')

    def test_observer_change_during_read_is_detected_before_output(self):
        first=self.root/'a.jsonl';second=self.root/'b.jsonl';output=self.root/'out.jsonl'
        write_rows(first,[observation(5,1),observation(60,2)])
        write_rows(second,[observation(70,2)])
        original=collector.rows
        def changed(path):
            yield from original(path)
            if path==first:
                with first.open('a',encoding='utf-8') as out:out.write(json.dumps(observation(65,2))+'\n')
        with patch.object(collector,'rows',side_effect=changed),self.assertRaisesRegex(ValueError,'changed during merge'):
            collector.merge_observers(first,second,output)
        self.assertFalse(output.exists())

    def test_own_observer_contract_uses_only_own_pre_request_head(self):
        a=[observation(5,1),observation(60,2)]
        # The second observer saw head2 before request a, but that independent
        # observation never selected a's cutoff and must not redefine it.
        b=[observation(8,2),observation(70,2),observation(120,3)]
        report=collector.own_observer_contract([request()],[request('r4',4,start=100,end=110,seq=2)],a,b)
        self.assertTrue(report['latest_cutoffs_checked_against_own_observer'])

    def test_own_observer_cross_session_or_removed_sample_fails(self):
        a=[observation(5,1),observation(60,2)]
        for req,b in [(request('r4',4,start=50,end=55,seq=2),[observation(8,2),observation(70,2)]),
                (request('r4',4,start=65,end=68,seq=2),[observation(8,2),observation(70,2)]),
                (request('r4',4,start=100,end=110,seq=1),[observation(8,2),observation(70,2)]),
                ({**request('r4',4,start=100,end=110,seq=2),'run_id':'other'},[observation(70,2)])]:
            with self.subTest(request=req),self.assertRaises(ValueError):
                collector.own_observer_contract([request()],[req],a,b)

    def telemetry(self):
        target=self.root/'writer.jsonl'
        rows=[{'run_id':'fixture','seq':seq,'started_ns':start,'observed_at_ns':end,
            'durable_seq':seq,'committed_seq':seq,'writer_error':None}
            for seq,start,end in [(1,15,20),(2,40,45)]]
        write_rows(target,rows)
        return target,rows

    def control(self):return {'run_id':'fixture','control_chain':{'decisions':[decision()]}}

    def test_actual_control_schema_action_seq_and_commit_bracket(self):
        writer,_=self.telemetry()
        report=collector.control_overlap(self.control(),writer,[request()])
        self.assertTrue(report['proven_control_during_pressure'])
        evidence=report['decisions'][0]
        self.assertEqual(evidence['action'],'authorize_assistance_planning')
        self.assertEqual(evidence['source_seq'],1);self.assertEqual(evidence['event_commit_seq'],2)
        self.assertEqual([evidence['bracket_start_ns'],evidence['bracket_end_ns']],[20,40])
        self.assertEqual(evidence['query_rounds_covering_entire_control_bracket_with_actual_inflight'],[1])

    def test_round_wall_span_alone_does_not_prove_actual_pressure(self):
        writer,_=self.telemetry()
        report=collector.control_overlap(self.control(),writer,[request(start=10,end=19),request('b',start=41,end=60)])
        self.assertFalse(report['proven_control_during_pressure'])
        evidence=report['decisions'][0]
        self.assertEqual(evidence['query_rounds_containing_entire_control_bracket'],[1])
        self.assertEqual(evidence['query_rounds_covering_entire_control_bracket_with_actual_inflight'],[])

    def test_touching_actual_requests_cover_bracket_without_gap(self):
        writer,_=self.telemetry()
        report=collector.control_overlap(self.control(),writer,[request(start=10,end=30),request('b',client=2,start=30,end=60)])
        self.assertTrue(report['proven_control_during_pressure'])

    def test_duplicate_foreign_unmirrored_or_reversed_writer_rejected(self):
        writer,values=self.telemetry()
        for bad in (values+values[:1],[{**values[0],'run_id':'other'},values[1]],
                [{**values[0],'committed_seq':0},values[1]],
                [{**values[0],'started_ns':30},values[1]]):
            write_rows(writer,bad)
            with self.subTest(rows=bad),self.assertRaises(ValueError):
                collector.control_overlap(self.control(),writer,[request()])

    def test_control_invalid_action_source_order_and_reversed_bracket_rejected(self):
        writer,values=self.telemetry()
        for change in ({'action':'invented'},{'seq':2},{'run_id':'other'}):
            value=self.control();value['control_chain']['decisions'][0].update(change)
            with self.subTest(change=change),self.assertRaises(ValueError):collector.control_overlap(value,writer,[request()])
        values[1].update(started_ns=19,observed_at_ns=45);write_rows(writer,values)
        with self.assertRaises(ValueError):collector.control_overlap(self.control(),writer,[request()])

    def fixture(self):
        root=self.root
        run=root/'runs/week-required-v2';run.mkdir(parents=True)
        write(run/'results.json',{'run_id':'fixture','status':'complete','error':None,'acceptance':{'all_input_completed':True}})
        writer,_=self.telemetry();(run/'writer.jsonl').write_bytes(writer.read_bytes())
        write(root/'control-source-audit.json',{'passed':True,**self.control()})
        for name in ('week-required-day1-window-v2','week-required-resume-v2','week-rules-complete-v2'):
            write(root/'logs'/f'{name}.stage.json',{'name':name,'exit_code':0,'start_ns':1,'end_ns':200,'log':'unused','log_sha256':'unused'})
        sessions=[root/'queries/week-live-v3',root/'queries/week-live-control-v5']
        for index,session in enumerate(sessions):
            pressure=request('r1') if index==0 else request('r4',4,start=100,end=110,seq=2)
            functional=request('f1',2,start=61,end=62,query='memory') if index==0 else request('f4',5,start=111,end=112,seq=2,query='energy')
            write_rows(session/'requests.jsonl',[pressure])
            write_rows(session/'functional-requests.jsonl',[functional])
            write_rows(session/'responses.jsonl.gz',[{'request_id':pressure['request_id'],'result':{'complete':'原始响应'}}])
            write_rows(session/'functional-responses.jsonl.gz',[{'request_id':functional['request_id'],'result':{'complete':'功能响应'}}])
            write_rows(session/'writer-head-observations.jsonl',
                [observation(5,1),observation(60,2)] if index==0 else [observation(8,2),observation(70,2),observation(120,2)])
            write(session/'load-result.json',{'monitor_errors':[]})
        return sessions

    def test_main_keeps_functional_out_of_pressure_and_preserves_originals(self):
        sessions=self.fixture();calls=[]
        def command(args,log):calls.append(args);return 0
        before={str(path):collector.sha(path) for s in sessions for path in s.iterdir()}
        with patch.object(collector,'command',side_effect=command):self.assertEqual(collector.main(['--root',str(self.root)]),0)
        final=self.root/'queries/final';summary=collector.load(final/'request-summary.json')
        self.assertEqual(summary['pressure']['requests'],2)
        self.assertEqual(summary['functional']['requests'],2)
        self.assertEqual(set(summary['rounds']),{'1','4'})
        self.assertEqual(len(calls),3)
        self.assertIn(str(final/'requests.jsonl'),calls[0])
        self.assertIn(str(final/'functional-requests.jsonl'),calls[1])
        self.assertIn('verify_catalog_responses.py',Path(calls[1][1]).name)
        self.assertEqual(before,{str(path):collector.sha(path) for s in sessions for path in s.iterdir()})
        original=summary['original_packet_sha256']
        self.assertIn(str(sessions[0]/'writer-head-observations.jsonl'),original)
        self.assertIn(str(self.root/'runs/week-required-v2/writer.jsonl'),original)

    def test_main_incomplete_window_or_failed_stage_rejected_before_output(self):
        self.fixture();run=self.root/'runs/week-required-v2/results.json'
        write(run,{'status':'complete','error':None,'acceptance':{'all_input_completed':False}})
        with self.assertRaisesRegex(ValueError,'window pause'):collector.main(['--root',str(self.root)])
        write(run,{'status':'complete','error':None,'acceptance':{'all_input_completed':True}})
        write(self.root/'logs/week-required-resume-v2.stage.json',{'exit_code':2})
        with self.assertRaisesRegex(ValueError,'stages'):collector.main(['--root',str(self.root)])
        self.assertFalse((self.root/'queries/final').exists())

    def test_main_detects_original_change_during_concatenation(self):
        sessions=self.fixture();original=collector.concatenate;changed=False
        def mutate(sources,target):
            nonlocal changed
            original(sources,target)
            if not changed:
                with (sessions[0]/'requests.jsonl').open('a',encoding='utf-8') as out:out.write('\n')
                changed=True
        with patch.object(collector,'concatenate',side_effect=mutate),self.assertRaisesRegex(ValueError,'changed during merge'):
            collector.main(['--root',str(self.root)])

    def test_main_failed_functional_verification_keeps_failure_and_raw_outputs(self):
        self.fixture()
        with patch.object(collector,'command',side_effect=[0,2,0]):
            self.assertEqual(collector.main(['--root',str(self.root)]),2)
        final=self.root/'queries/final';status=collector.load(final/'collection-result.json')
        self.assertFalse(status['passed']);self.assertEqual(status['command_exit_codes'],[0,2,0])
        self.assertTrue(status['original_packets_unchanged'])
        self.assertEqual(len(list(collector.rows(final/'functional-responses.jsonl.gz'))),2)

    def extra_session(self,start=200,end=210,round_id=5):
        session=self.root/'queries/week-live-late-v6'
        pressure=request('r5',round_id,start=start,end=end,seq=3)
        functional=request('f5',6,start=211,end=212,seq=3,query='global')
        write_rows(session/'requests.jsonl',[pressure])
        write_rows(session/'functional-requests.jsonl',[functional])
        write_rows(session/'responses.jsonl.gz',[{'request_id':'r5','result':{'complete':'第三会话原始响应'}}])
        write_rows(session/'functional-responses.jsonl.gz',[{'request_id':'f5','result':{'complete':'第三会话功能响应'}}])
        write_rows(session/'writer-head-observations.jsonl',
            [observation(80,2),observation(150,3),observation(220,4)])
        write(session/'load-result.json',{'monitor_errors':[],'think_seconds':0.1,
            'per_worker_pool':3,'clients':100,'rounds':[{'round':round_id}]})
        return session

    def test_main_three_sessions_preserve_pairwise_observers_and_workload_metadata(self):
        sessions=self.fixture();extra=self.extra_session()
        # Different real workload parameters must survive independently.
        write(sessions[0]/'load-result.json',{'monitor_errors':[],'think_seconds':1.0,'per_worker_pool':3})
        write(sessions[1]/'load-result.json',{'monitor_errors':[],'think_seconds':0.25,'per_worker_pool':3})
        before={str(p):collector.sha(p) for s in sessions+[extra] for p in s.iterdir()}
        with patch.object(collector,'command',return_value=0):
            self.assertEqual(collector.main(['--root',str(self.root),'--extra-session',extra.name]),0)
        final=self.root/'queries/final';summary=collector.load(final/'request-summary.json')
        self.assertEqual(summary['pressure']['requests'],3)
        self.assertEqual(summary['functional']['requests'],3)
        self.assertEqual(set(summary['rounds']),{'1','4','5'})
        self.assertEqual([s['round_ids'] for s in summary['session_summaries']],[[1],[4],[5]])
        self.assertEqual([s['load_result']['think_seconds'] for s in summary['session_summaries']],[1.0,0.25,0.1])
        provenance=summary['observer_provenance']
        self.assertEqual(provenance['session_order'],[s.name for s in sessions]+[extra.name])
        self.assertEqual(len(provenance['steps']),2)
        for step in provenance['steps']:
            self.assertTrue(Path(step['derived_file']).exists())
            self.assertEqual(step['derived_sha256'],collector.sha(step['derived_file']))
            self.assertTrue(step['latest_cutoffs_checked_against_own_observer'])
        merged=list(collector.rows(final/'writer-head-observations.jsonl'))
        self.assertEqual([r['observed_at_ns'] for r in merged],[5,60,70,120,150,220])
        self.assertEqual(before,{str(p):collector.sha(p) for s in sessions+[extra] for p in s.iterdir()})
        self.assertEqual([r['request_id'] for r in collector.rows(final/'responses.jsonl.gz')],['r1','r4','r5'])

    def test_main_third_session_before_previous_splice_rejected(self):
        self.fixture();extra=self.extra_session(start=100,end=110)
        with patch.object(collector,'command') as command,self.assertRaisesRegex(ValueError,'crosses own-observer splice'):
            collector.main(['--root',str(self.root),'--extra-session',extra.name])
        command.assert_not_called()
        # First successful derived splice remains available for investigation.
        self.assertTrue((self.root/'queries/final/writer-head-observations-splice-2.jsonl').exists())
        self.assertFalse((self.root/'queries/final/writer-head-observations-splice-3.jsonl').exists())

    def test_extra_session_only_single_queries_directory_names(self):
        for name in ('../elsewhere','queries/week-live-late-v6','a\\b','/tmp/escape','C:\\escape','.', '..',''):
            with self.subTest(name=name),self.assertRaisesRegex(ValueError,'one directory name'):
                collector.session_directory(self.root,name)
        self.assertEqual(collector.session_directory(self.root,'week-live-late-v6'),self.root/'queries/week-live-late-v6')

    def test_duplicate_sessions_and_reused_round_ids_are_rejected(self):
        self.fixture()
        with self.assertRaisesRegex(ValueError,'duplicate original session'):
            collector.main(['--root',str(self.root),'--extra-session','week-live-v3'])
        extra=self.extra_session(round_id=4)
        with self.assertRaisesRegex(ValueError,'round ID reused'):
            collector.main(['--root',str(self.root),'--extra-session',extra.name])


if __name__=='__main__':unittest.main()
