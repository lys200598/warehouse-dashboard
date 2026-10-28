"""Independent AFTER-load source verification and exact ASGI interval analysis."""
from pathlib import Path
from collections import Counter,defaultdict
import gzip,hashlib,importlib.util,json,math,sqlite3,sys,time,zlib

R=Path('/data/workspace/wz/warehouse-free-ports-20261029');S=R/'source-combined-v2'
version=sys.argv[1] if len(sys.argv)>1 else 'v3';E=R/f'live-joint-evidence-{version}'
ready=json.loads((E/'ready.json').read_text());run=ready['run_id'];OUT=Path(ready['output_root'])/run
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
source_manifest=json.loads((S/'source-manifest.json').read_text())['files']
assert all(sha(S/p)==v for p,v in source_manifest.items())
live_manifest=json.loads((R/'combined-livepoint-sha-v1.json').read_text())
assert all(sha(S/p)==v for p,v in live_manifest['files'].items())
with sqlite3.connect((OUT/'journal.sqlite').as_uri()+'?mode=ro',uri=True) as db:
    head=db.execute('SELECT seq FROM heads WHERE run_id=?',(run,)).fetchone()[0]
spec=importlib.util.spec_from_file_location('independent_verifier',S/'ops/verify_catalog_responses.py')
verify=importlib.util.module_from_spec(spec);spec.loader.exec_module(verify)
oracle=RealOracle.open(E/'postload-oracle.sqlite') if (E/'postload-oracle.sqlite').exists() else RealOracle(OUT/'journal.sqlite',prefix=head,index_path=E/'postload-oracle.sqlite')
assert oracle.prefix==head and oracle.run_id==run
truth=verify.JournalTruth(OUT/'journal.sqlite',oracle);validator=verify.ResponseVerifier(truth)
seen=set();errors=[];counts=Counter();groups=defaultdict(list);seq_counts=Counter();packets={}
try:
    with gzip.open(E/'api-responses.jsonl.gz','rt',encoding='utf-8') as stream:
        for row in map(json.loads,stream):
            ident=row['request_id'];q=row['request']['query_id'];counts[q]+=1
            groups[('client',q)].append(row['client_ms']);groups[('client','all')].append(row['client_ms'])
            try:
                assert ident not in seen,'duplicate packet';seen.add(ident)
                assert row['status']==200 and row['error'] is None,(row['status'],row['error'])
                value=row['response'];request={**row['request'],'run_id':value['run_id'],'seq':value['seq']}
                assert request['run_id']==run
                seq_counts[value['seq']]+=1
                mismatches=validator.check(request,value)
                assert not mismatches,mismatches
                packets[ident]={'q':q,'ok':True,'seq':value['seq']}
            except Exception as exc:
                errors.append({'request_id':ident,'error':type(exc).__name__+': '+str(exc)})
                packets[ident]={'q':q,'ok':False}
finally:truth.close();oracle.close()

observations=[json.loads(line) for line in (E/'server-observations.jsonl').read_text().splitlines()]
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
final=data['final'];frames=data['observed_frames'];watermark_errors=[]
for i,frame in enumerate(frames):
    k=frame['knowledge']
    if k['lag']!=0 or k['durable_seq']!=k['mirrored_seq'] or k['query_summary_lag']!=0:
        watermark_errors.append({'frame':i,'elapsed':frame['elapsed'],'knowledge':k})
result=json.loads((OUT/'results.json').read_text())
after={n:sha(OUT/n) for n in source_files}
qualification={'functional_complete':final['status']=='complete' and final['metrics']['completed']==19 and final['metrics']['late']==0 and final['metrics']['handoffs']==1,
               'full_source_verified':not errors and len(seen)==len(apis) and api_ids==seen,
               'all_api_intervals_complete':all(row['body_send_completed'] for row in apis),'all_api_http200':all(row['status']==200 for row in apis),'all_observed_watermarks_lag0':not watermark_errors,
               'exact_final_body_send_intervals':all(r.get('end_semantics')=='last_response_body_send_completed' for r in apis),
               'two_actual_control_brackets':len(actions)==len(brackets)==2 and len(used)==2,
               'all_control_brackets_api_covered':len(brackets)==2 and all(b.get('whole_control_bracket_api_coverage',{}).get('fully_covered') for b in brackets),
               'all_actual_api_within_500ms':all(v<=500 for v in groups[('server','all')]),
               'all_client_requests_within_500ms':all(v<=500 for v in groups[('client','all')]),
               'source_packets_unchanged':before==after and sha(E/'api-responses.jsonl.gz')==packets_before,
               'frozen_source_unchanged':all(sha(S/p)==v for p,v in source_manifest.items()),'frozen_live_adapter_unchanged':all(sha(S/p)==v for p,v in live_manifest['files'].items())}
report={'version':version,'scope':'dynamic owned live / source-verified summary HTTP / actual control overlap; NOT million-node qualification',
        'passed':all(qualification.values()),'qualification':qualification,'run_id':run,'source':str(S),'output_root':str(OUT),
        'livepoint_manifest_sha256':sha(R/'combined-livepoint-sha-v1.json'),'livepoint_files_sha256':live_manifest['files'],
        'source_manifest_sha256':sha(S/'source-manifest.json'),'source_artifacts_sha256':before,'response_packets_sha256':packets_before,
        'server_observations_sha256':sha(E/'server-observations.jsonl'),'ready_sha256':sha(E/'ready.json'),'client_run_sha256':sha(E/'client-run.json'),
        'server_wrapper_sha256':ready['wrapper_sha256'],'client_wrapper_sha256':data['client_wrapper_sha256'],
        'postverify_tool_sha256':sha(Path(__file__)),'oracle_sha256':sha(E/'postload-oracle.sqlite'),
        'client_threads':data['clients'],'actual_api_peak':peak,'counter_api_peak':max((r['inflight_at_start'] for r in apis),default=0),
        'load_wall_seconds':(data['ended_ns']-data['begun_ns'])/1e9,'client_requests_per_wall_second':sum(counts.values())/((data['ended_ns']-data['begun_ns'])/1e9),
        'requests':sum(counts.values()),'query_mix':dict(counts),'verified':len(seen)-len(errors),'response_errors':errors,'server_errors':server_errors,
        'latencies':{f'{p}/{q}':stats(v) for (p,q),v in groups.items()},'query_source_cutoffs':dict(sorted(seq_counts.items())),
        'observed_live_frames':len(frames),'watermark_errors':watermark_errors,'final_knowledge':final['knowledge'],
        'final_status':final['status'],'final_elapsed':final['elapsed'],'final_metrics':final['metrics'],
        'actual_control_brackets':brackets,'unmatched_history_contexts':[c for i,c in enumerate(contexts) if i not in used],
        'production_catalog_errors':[row for row in observations if row['kind']=='catalog_error'],
        'knowledge_control_stats':result['scheduler']['knowledge_runtime']['stats'],'postload_seconds':time.perf_counter()-begun}
sha_values=[v for k,v in report.items() if k.endswith('_sha256') and k not in ('source_artifacts_sha256','livepoint_files_sha256')]+list(before.values())+list(live_manifest['files'].values())
assert isinstance(report['source_artifacts_sha256'],dict) and set(report['source_artifacts_sha256'])==set(source_files)
assert all(isinstance(v,str) and len(v)==64 and all(c in '0123456789abcdef' for c in v) for v in sha_values),'malformed SHA report field'
with (E/'postverify-report-v3.json').open('x') as stream:json.dump(report,stream,indent=2)
print(json.dumps({k:report[k] for k in ('passed','qualification','run_id','requests','actual_api_peak','query_mix','latencies','final_status','final_elapsed','knowledge_control_stats')}))
print('report_sha256',sha(E/'postverify-report-v3.json'))
raise SystemExit(0 if report['passed'] else 2)
