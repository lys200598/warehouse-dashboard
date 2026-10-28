"""Independent AFTER-load source verification and exact ASGI interval analysis."""
from pathlib import Path
from collections import Counter,defaultdict
import gzip,hashlib,importlib.util,json,math,sqlite3,sys,time,zlib

R=Path('/data/workspace/wz/warehouse-free-ports-20261029');S=R/'source-combined-v5'
version=sys.argv[1] if len(sys.argv)>1 else 'v7';E=R/f'live-joint-evidence-{version}'
ready=json.loads((E/'ready.json').read_text());run=ready['run_id'];OUT=E/'source-at-audit'/'verified'
sys.path.insert(0,str(S))
from backend.real_scale.oracle import RealOracle

def sha(path):return hashlib.sha256(Path(path).read_bytes()).hexdigest()
def stats(values):
    values=sorted(values);n=len(values)
    return {'count':n,'mean_ms':sum(values)/n if n else None,'p99_ms':values[math.ceil(.99*n)-1] if n else None,
            'max_ms':values[-1] if n else None,'over_500ms':sum(v>500 for v in values)}

begun=time.perf_counter();data=json.loads((E/'client-run.json').read_text())
assert data['run_id']==run
source_files=['journal.sqlite','checkpoint.json','results.json']
before={n:sha(OUT/n) for n in source_files};packets_before=sha(E/'api-responses.jsonl.gz')
source_manifest=json.loads((S/'source-s7-manifest.json').read_text())['files']
assert all(sha(S/p)==v for p,v in source_manifest.items())
live_manifest={'files':source_manifest}
assert ready['catalog_workers']==24 and len(ready['sidecars'])==8
assert all(sha(S/p)==v for p,v in live_manifest['files'].items())
with sqlite3.connect((OUT/'journal.sqlite').as_uri()+'?mode=ro',uri=True) as db:
    head=db.execute('SELECT seq FROM heads WHERE run_id=?',(run,)).fetchone()[0]
spec=importlib.util.spec_from_file_location('independent_verifier',S/'ops/verify_catalog_responses.py')
verify=importlib.util.module_from_spec(spec);spec.loader.exec_module(verify)
oracle=RealOracle.open(E/'postload-oracle.sqlite') if (E/'postload-oracle.sqlite').exists() else RealOracle(OUT/'journal.sqlite',prefix=head,index_path=E/'postload-oracle.sqlite')
assert oracle.prefix==head and oracle.run_id==run
truth=verify.JournalTruth(OUT/'journal.sqlite',oracle);validator=verify.ResponseVerifier(truth)
seen=set();errors=[];counts=Counter();groups=defaultdict(list);seq_counts=Counter();packets={};client_stages=defaultdict(list);phase_errors=[]
try:
    with gzip.open(E/'api-responses.jsonl.gz','rt',encoding='utf-8') as stream:
        for row in map(json.loads,stream):
            ident=row['request_id'];q=row['request']['query_id'];counts[q]+=1
            groups[('client',q)].append(row['client_ms']);groups[('client','all')].append(row['client_ms'])
            phase=row['http_phases']
            try:
                assert phase['request_start_ns']==row['client_start_ns'] and phase['client_end_ns']==row['client_end_ns']
                pairs=[('connect','connect_start_ns','connect_end_ns'),('headers_send','headers_send_start_ns','headers_send_complete_ns'),
                    ('headers_to_body_gap','headers_send_complete_ns','body_send_start_ns'),('body_send','body_send_start_ns','body_send_complete_ns'),
                    ('request_construct_and_send','request_start_ns','request_send_complete_ns'),('wait_response_headers','request_send_complete_ns','response_headers_complete_ns'),
                    ('response_body_receive','response_body_read_start_ns','response_body_complete_ns'),('json_decode_and_parse','json_parse_start_ns','json_parse_complete_ns')]
                for stage,start,end in pairs:
                    if phase.get(start) is not None and phase.get(end) is not None:
                        assert phase[end]>=phase[start],(stage,phase)
                        ms=(phase[end]-phase[start])/1e6;client_stages[(stage,q)].append(ms);client_stages[(stage,'all')].append(ms)
            except Exception as exc:phase_errors.append({'request_id':ident,'error':type(exc).__name__+': '+str(exc)})
            try:
                assert ident not in seen,'duplicate packet';seen.add(ident)
                assert row['status']==200 and row['error'] is None,(row['status'],row['error'])
                value=row['response'];request={**row['request'],'run_id':value['run_id'],'seq':value['seq']}
                assert request['run_id']==run
                seq_counts[value['seq']]+=1
                mismatches=validator.check(request,value)
                assert not mismatches,mismatches
                packets[ident]={'q':q,'ok':True,'seq':value['seq'],'robot_id':row['request']['robot_id'],'client_start_ns':row['client_start_ns'],'client_end_ns':row['client_end_ns'],'client_pid':row['client_pid'],'budget_head':row.get('response',{}).get('result',{}).get('point',{}).get('graph_committed_seq')}
            except Exception as exc:
                errors.append({'request_id':ident,'error':type(exc).__name__+': '+str(exc)})
                packets[ident]={'q':q,'ok':False,'robot_id':row['request']['robot_id'],'client_start_ns':row['client_start_ns'],'client_end_ns':row['client_end_ns'],'client_pid':row['client_pid'],'budget_head':row.get('response',{}).get('result',{}).get('point',{}).get('graph_committed_seq')}
finally:truth.close();oracle.close()

observations=[json.loads(line) for line in (E/'server-observations.jsonl').read_text().splitlines()]
sidecar_logs=sorted(E.glob('sidecar-*-observations.jsonl'));assert len(sidecar_logs)==8
for path in sidecar_logs:observations.extend(json.loads(line) for line in path.read_text().splitlines())
sidecar_meta=[json.loads(path.read_text()) for path in sorted(E.glob('sidecar-*-wrapper.json'))];assert len(sidecar_meta)==8
assert {x['pid'] for x in sidecar_meta}=={x['pid'] for x in ready['sidecars']}
assert all(x['runtime_created_by_wrapper'] is False for x in sidecar_meta)
apis=[row for row in observations if row['kind']=='api'];contexts=[row for row in observations if row['kind']=='history_context']
actions=[row for row in observations if row['kind']=='control_action'];api_ids=set();boundaries=[];server_errors=[]
for row in apis:
    ident=row['request_id']
    if ident in api_ids:server_errors.append({'request_id':ident,'error':'duplicate ASGI record'})
    api_ids.add(ident)
    if not row['body_send_completed']:server_errors.append({'request_id':ident,'error':'final body did not complete'})
    if row['status']!=200:server_errors.append({'request_id':ident,'error':'ASGI status '+str(row['status'])})
    q=packets.get(ident,{}).get('q','unknown');ms=(row['end_ns']-row['start_ns'])/1e6
    groups[('server',q)].append(ms);groups[('server','all')].append(ms)
    boundaries.extend([(row['start_ns'],1),(row['end_ns'],-1)])
active=peak=0
for at,delta in sorted(boundaries):active+=delta;peak=max(peak,active)
assert active==0
merged=[]
for start,end in sorted((row['start_ns'],row['end_ns']) for row in apis):
    if merged and start<=merged[-1][1]:merged[-1][1]=max(end,merged[-1][1])
    else:merged.append([start,end])
def coverage(start,end):
    covered=sum(max(0,min(end,b)-max(start,a)) for a,b in merged)
    return {'start_ns':start,'end_ns':end,'duration_ms':(end-start)/1e6,'covered_ms':covered/1e6,
            'fully_covered':any(a<=start and b>=end for a,b in merged),'uncovered_ms':(end-start-covered)/1e6}
brackets=[];used=set()
for action in sorted(actions,key=lambda x:x['at_ns']):
    candidates=[(i,c) for i,c in enumerate(contexts) if i not in used and c['task_id']==action['task_id'] and c['end_ns']<=action['at_ns']]
    if not candidates:brackets.append({'action':action,'error':'no preceding same-task history context'});continue
    i,context=max(candidates,key=lambda pair:pair[1]['end_ns']);used.add(i)
    brackets.append({'context':context,'action':action,'context_api_coverage':coverage(context['start_ns'],context['end_ns']),
                     'whole_control_bracket_api_coverage':coverage(context['start_ns'],action['at_ns'])})
executions=[row for row in observations if row['kind']=='catalog_execution'];execution_errors=[];stage_groups=defaultdict(list);fence_errors=[];fence_checked=0;fence_advanced=0
execution_by_robot=defaultdict(list);packet_by_robot=defaultdict(list);api_by_id={r['request_id']:r for r in apis};execution_boundaries=[]
for execution in executions:
    execution_by_robot[execution['request']['robot_id']].append(execution)
    execution_boundaries.extend([(execution['start_ns'],1),(execution['end_ns'],-1)])
for ident,packet in packets.items():packet_by_robot[packet['robot_id']].append((ident,packet))
for robot in sorted(set(execution_by_robot)|set(packet_by_robot)):
    selected=sorted(execution_by_robot[robot],key=lambda r:r['start_ns']);clients=sorted(packet_by_robot[robot],key=lambda pair:pair[1]['client_start_ns'])
    if len(selected)!=len(clients):execution_errors.append({'robot_id':robot,'error':'client/executor counts differ','clients':len(clients),'executions':len(selected)})
    for execution,(ident,packet) in zip(selected,clients):
        try:
            request=execution['request'];api=api_by_id[ident]
            assert request['run_id']==run and request['query_id']==packet['q'] and request['robot_id']==robot
            if packet['ok']:assert request['seq']==packet['seq'] and execution['outcome']=='ok'
            if request['query_id']=='global':
                fence=execution.get('catalog_fence');fence_checked+=1
                try:
                    assert execution.get('fence_observer_error') is None and isinstance(fence,dict)
                    assert fence['version']=='two-phase-fence-v1' and fence['run_id']==run and fence['requested_seq']==request['seq']
                    assert fence['A_rows']==fence['B_rows']==1 and request['seq']<=fence['head_A']<=fence['head_B']<=head
                    assert fence['requested_exists'] is True and fence['head_exists'] is True and fence['outcome']=='ok'
                    assert packet['ok'] and packet['budget_head']==fence['head_A']
                    fence_advanced+=int(fence['head_B']>fence['head_A'])
                except Exception as exc:fence_errors.append({'request_id':ident,'fence':fence,'error':type(exc).__name__+': '+str(exc)})
            assert api['start_ns']<=execution['start_ns']<=execution['end_ns']<=api['end_ns']
            for stage,start,end in [('pre_execution_wait',api['start_ns'],execution['start_ns']),('executor',execution['start_ns'],execution['end_ns']),('post_execution_send',execution['end_ns'],api['end_ns'])]:
                ms=(end-start)/1e6;stage_groups[(stage,packet['q'])].append(ms);stage_groups[(stage,'all')].append(ms)
            body_end=api['request_body_end_ns'];assert api['start_ns']<=body_end<=execution['start_ns']
            for stage,start,end in [('gateway_request_body_wait',api['start_ns'],body_end),('gateway_body_to_execution',body_end,execution['start_ns'])]:
                ms=(end-start)/1e6;stage_groups[(stage,packet['q'])].append(ms);stage_groups[(stage,'all')].append(ms)
        except Exception as exc:execution_errors.append({'request_id':ident,'error':type(exc).__name__+': '+str(exc),'execution':execution})
single_inflight_errors=[]
for robot,rows in packet_by_robot.items():
    ordered=sorted(rows,key=lambda pair:pair[1]['client_start_ns'])
    for previous,current in zip(ordered,ordered[1:]):
        if previous[1]['client_end_ns']>current[1]['client_start_ns']:single_inflight_errors.append({'robot_id':robot,'previous':previous[0],'current':current[0]})
execution_active=execution_peak=0
for at,delta in sorted(execution_boundaries):execution_active+=delta;execution_peak=max(execution_peak,execution_active)
assert execution_active==0
final=data['final'];frames=data['observed_frames'];watermark_errors=[]
for i,frame in enumerate(frames):
    k=frame['knowledge']
    if k['lag']!=0 or k['durable_seq']!=k['mirrored_seq'] or k['query_summary_lag']!=0:
        watermark_errors.append({'frame':i,'elapsed':frame['elapsed'],'knowledge':k})
result=json.loads((OUT/'results.json').read_text())
after={n:sha(OUT/n) for n in source_files}
qualification={'functional_complete':final['status']=='complete' and final['metrics']['completed']==19 and final['metrics']['late']==0 and final['metrics']['handoffs']==1,
               'full_source_verified':not errors and len(seen)==len(apis) and api_ids==seen,
               'all_api_intervals_complete':all(row['body_send_completed'] for row in apis),'all_api_http200':all(row['status']==200 for row in apis),'all_executions_correlated':not execution_errors and len(executions)==len(packets),'all_client_phase_timestamps_valid':not phase_errors,'all_global_two_stage_fences_verified':not fence_errors and fence_checked==counts['global'],'eight_client_processes_verified':len({p['client_pid'] for p in packets.values()})==8,'one_inflight_per_robot_verified':not single_inflight_errors,'all_observed_watermarks_lag0':not watermark_errors,
               'exact_final_body_send_intervals':all(r.get('end_semantics')=='last_response_body_send_completed' for r in apis),
               'two_actual_control_brackets':len(actions)==len(brackets)==2 and len(used)==2,
               'all_control_brackets_api_covered':len(brackets)==2 and all(b.get('whole_control_bracket_api_coverage',{}).get('fully_covered') for b in brackets),
               'all_actual_api_within_500ms':all(v<=500 for v in groups[('server','all')]),
               'all_client_requests_within_500ms':all(v<=500 for v in groups[('client','all')]),
               'source_packets_unchanged':before==after and sha(E/'api-responses.jsonl.gz')==packets_before,
               'frozen_source_unchanged':all(sha(S/p)==v for p,v in source_manifest.items()),'frozen_live_adapter_unchanged':all(sha(S/p)==v for p,v in live_manifest['files'].items())}
report={'version':version,'scope':'dynamic owned live / source-verified summary HTTP / actual control overlap; NOT million-node qualification',
        'passed':all(qualification.values()),'qualification':qualification,'run_id':run,'source':str(S),'output_root':str(OUT),
        'sidecar_cli_sha256':sha(S/'ops/warehouse_query_sidecar.py'),'catalog_workers':24,
        'sidecars_ready':ready['sidecars'],'sidecar_wrapper_sha256':sha(R/'deployment/free-live-query-sidecar-v7.py'),
        'sidecar_observations_sha256':{path.name:sha(path) for path in sidecar_logs},'raw_archive_manifest_sha256':sha(E/'source-at-audit/archive-manifest.json'),
        'source_full_manifest_sha256':sha(S/'source-s7-manifest.json'),'livepoint_files_sha256':live_manifest['files'],
        'source_manifest_sha256':sha(S/'source-s7-manifest.json'),'source_artifacts_sha256':before,'response_packets_sha256':packets_before,
        'server_observations_sha256':sha(E/'server-observations.jsonl'),'ready_sha256':sha(E/'ready.json'),'client_run_sha256':sha(E/'client-run.json'),
        'server_wrapper_sha256':ready['wrapper_sha256'],'client_wrapper_sha256':data['client_wrapper_sha256'],
        'postverify_tool_sha256':sha(Path(__file__)),'oracle_sha256':sha(E/'postload-oracle.sqlite'),
        'client_threads':data['clients'],'client_processes':data['client_processes'],'client_process_exitcodes':data['process_exitcodes'],'actual_api_peak':peak,'counter_api_peak':max((r['inflight_at_start'] for r in apis),default=0),
        'load_wall_seconds':(data['ended_ns']-data['begun_ns'])/1e9,'client_requests_per_wall_second':sum(counts.values())/((data['ended_ns']-data['begun_ns'])/1e9),
        'requests':sum(counts.values()),'query_mix':dict(counts),'verified':len(seen)-len(errors),'response_errors':errors,'server_errors':server_errors,
        'latencies':{f'{p}/{q}':stats(v) for (p,q),v in groups.items()},'query_source_cutoffs':dict(sorted(seq_counts.items())),
        'observed_live_frames':len(frames),'watermark_errors':watermark_errors,'final_knowledge':final['knowledge'],
        'final_status':final['status'],'final_elapsed':final['elapsed'],'final_metrics':final['metrics'],
        'actual_control_brackets':brackets,'unmatched_history_contexts':[c for i,c in enumerate(contexts) if i not in used],
        'actual_executor_peak':execution_peak,'executor_threads_observed':len({(row.get('pid'),row['thread_id']) for row in executions}),'executor_records':len(executions),
        'client_http_stages':{f'{stage}/{q}':stats(v) for (stage,q),v in client_stages.items()},'client_phase_errors':phase_errors,'single_inflight_errors':single_inflight_errors,
        'execution_correlation_errors':execution_errors,'stages':{f'{stage}/{q}':stats(v) for (stage,q),v in stage_groups.items()},
        'fence_diagnostics':{'checked_globals':fence_checked,'head_advanced_between_statements':fence_advanced,'errors':fence_errors},
        'production_catalog_errors':[row for row in observations if row['kind']=='catalog_error'],
        'knowledge_control_stats':result['scheduler']['knowledge_runtime']['stats'],'postload_seconds':time.perf_counter()-begun}
sha_values=[v for k,v in report.items() if k.endswith('_sha256') and k not in ('source_artifacts_sha256','livepoint_files_sha256','sidecar_observations_sha256')]+list(before.values())+list(live_manifest['files'].values())+[sha(path) for path in sidecar_logs]
assert isinstance(report['source_artifacts_sha256'],dict) and set(report['source_artifacts_sha256'])==set(source_files)
assert all(isinstance(v,str) and len(v)==64 and all(c in '0123456789abcdef' for c in v) for v in sha_values),'malformed SHA report field'
with (E/'postverify-report-v7.json').open('x') as stream:json.dump(report,stream,indent=2)
print(json.dumps({k:report[k] for k in ('passed','qualification','run_id','requests','actual_api_peak','query_mix','latencies','final_status','final_elapsed','knowledge_control_stats')}))
print('report_sha256',sha(E/'postverify-report-v7.json'))
raise SystemExit(0 if report['passed'] else 2)
