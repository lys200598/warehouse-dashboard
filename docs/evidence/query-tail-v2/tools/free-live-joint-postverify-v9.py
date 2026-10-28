"""Independent AFTER-load source verification and exact ASGI interval analysis."""
from pathlib import Path
from collections import Counter,defaultdict
import gzip,hashlib,importlib.util,json,math,sqlite3,sys,time,zlib

R=Path('/data/workspace/wz/warehouse-free-ports-20261029');S=R/'source-combined-v7'
version=sys.argv[1] if len(sys.argv)>1 else 'v9';E=R/f'live-joint-evidence-{version}'
ready=json.loads((E/'ready.json').read_text());run=ready['run_id'];OUT=E/'source-at-audit'/'verified'
sys.path.insert(0,str(S))
from backend.real_scale.oracle import RealOracle

def sha(path):return hashlib.sha256(Path(path).read_bytes()).hexdigest()
"""Pure after-load S8 evidence checks; embedded into the standalone audit tool."""
from collections import Counter,defaultdict
import hashlib,json,math

POINT_VERSION='warehouse-live-query-point-v1'
POINT_KEYS={'version','run_id','seq','runtime_instance_id','source_sha256'}
NATIVE_BINARY_SHA='40ab7ce04f0d0af77e54bc73055131a8f1330c7ec5f4763fb5e8b8bb4fe8a2d8'
NATIVE_CONFIG_SHA='88bf4c077a50ac152c32c170bb46862bbc8e7cbde08ceda02632fe73dc053615'

def point_canonical(value):
    return json.dumps(value,ensure_ascii=False,sort_keys=True,separators=(',',':'),allow_nan=False).encode('utf-8')

def point_payload_sha(payload):return hashlib.sha256(point_canonical(payload)).hexdigest()

def point_envelope_bytes(payload):
    raw=point_canonical(payload);checksum=hashlib.sha256(raw).hexdigest().encode('ascii')
    return b'{"format":"warehouse-checkpoint-v2","sha256":"'+checksum+b'","payload":'+raw+b'}'

def point_validate_payload(payload,run,owner,source_sha,source_seqs):
    assert isinstance(payload,dict) and set(payload)==POINT_KEYS,'point payload fields differ'
    assert payload['version']==POINT_VERSION and payload['run_id']==run,'point version/run differs'
    assert type(payload['seq']) is int and payload['seq'] in source_seqs,'point seq absent from source'
    assert payload['runtime_instance_id']==owner and payload['source_sha256']==source_sha,'point owner/source differs'
    assert isinstance(source_sha,str) and len(source_sha)==64 and all(c in '0123456789abcdef' for c in source_sha),'invalid source SHA'
    return point_payload_sha(payload)

def audit_owner_points(*,ready,packets,apis,executions,publications,reads,head,final_raw):
    run=ready['run_id'];owner=ready['runtime_instance_id'];source_sha=ready['initial_snapshot']['metadata']['source_sha256']
    seqs=set(range(1,head+1));errors=[];pubs=sorted(publications,key=lambda row:row['start_ns']);pub_index=defaultdict(list)
    checked_summary_fields=Counter();initial_none=0;last_seq=0
    for i,pub in enumerate(pubs):
        try:
            assert pub['kind']=='published_query_point' and pub['start_ns']<=pub['end_ns'],'invalid publish interval'
            assert pub.get('outcome')=='ok' and pub.get('observer_error') is None,'point publication failed or observation invalid'
            digest=point_validate_payload(pub['payload'],run,owner,source_sha,seqs)
            assert pub['payload_sha256']==digest,'published payload SHA differs'
            assert pub['envelope_sha256']==hashlib.sha256(point_envelope_bytes(pub['payload'])).hexdigest(),'published envelope SHA differs'
            assert pub['payload']['seq']>=last_seq,'published seq regressed';last_seq=pub['payload']['seq']
            if pub.get('cached_knowledge_seq') is not None:assert pub['cached_knowledge_seq']==pub['payload']['seq'],'cached source knowledge seq differs'
            cached=pub.get('cached_summary')
            if cached is None:
                k=ready['initial_snapshot']['knowledge'];initial_none+=1
                assert pub['payload']['seq']==k['durable_seq']==k['mirrored_seq'] and k['lag']==0,'unproved initial published point'
                assert k['query_summary_lag']==0,'initial query summary lag'
                summary=k.get('query_summary')
                if isinstance(summary,dict):
                    for field in ('published_seq','head','graph_head'):
                        if field in summary:assert summary[field]==pub['payload']['seq'],('initial summary differs',field)
            else:
                assert isinstance(cached,dict),'cached summary must be dict or initial None'
                fields=[field for field in ('published_seq','head','graph_head') if field in cached]
                assert fields,'cached summary has no watermark proof'
                for field in fields:
                    assert type(cached[field]) is int and cached[field]==pub['payload']['seq'],('cached summary differs',field)
                    checked_summary_fields[field]+=1
            pub_index[(digest,pub['envelope_sha256'])].append(i)
        except Exception as exc:errors.append({'publication_index':i,'error':type(exc).__name__+': '+str(exc),'publication':pub})
    if not pubs:errors.append({'error':'no actual source point publication observations'})
    api_by_id=defaultdict(list);read_by_id=defaultdict(list);execute_by_id=defaultdict(list)
    for row in apis:api_by_id[row['request_id']].append(row)
    for row in reads:read_by_id[row['request_id']].append(row)
    for row in executions:
        nested=(row.get('owner_point') or {}).get('request_id')
        execute_by_id[row.get('request_id',nested)].append(row)
    checked=0;interleaved=0;omitted=0
    for ident,packet in packets.items():
        try:
            assert packet['point_omitted'] is True,'client supplied a run/seq in omitted-point protocol';omitted+=1
            assert len(api_by_id[ident])==len(read_by_id[ident])==len(execute_by_id[ident])==1,'API/read/executor not unique'
            api=api_by_id[ident][0];read=read_by_id[ident][0];execution=execute_by_id[ident][0]
            assert read['kind']=='owner_point_read' and read['request_id']==ident,'owner read identity differs'
            assert (read['pid'],read['port'])==(api['pid'],api['port'])==(execution['pid'],execution['port']),'sidecar identity differs'
            assert api['start_ns']<=api['request_body_end_ns']<=read['start_ns']<=read['end_ns']<=execution['start_ns'],'owner read is outside request/executor interval'
            digest=point_validate_payload(read['payload'],run,owner,source_sha,seqs)
            assert read['expected_envelope_sha256']==hashlib.sha256(point_envelope_bytes(read['payload'])).hexdigest(),'owner read envelope SHA differs'
            candidates=[]
            for i in pub_index[(digest,read['expected_envelope_sha256'])]:
                pub=pubs[i]
                # Atomic rename may be observed before the publisher wrapper returns.
                if pub['start_ns']>read['end_ns']:continue
                if i+1<len(pubs) and pubs[i+1]['end_ns']<read['start_ns']:continue
                candidates.append(i)
            assert candidates,'owner read has no causally possible, fresh source publication'
            chosen=pubs[max(candidates)];interleaved+=int(read['start_ns']<chosen['end_ns'])
            assert execution['owner_point']==read,'executor owner_point differs from its actual owner read'
            if 'request_id' in execution:assert execution['request_id']==ident,'executor outer request ID differs'
            assert execution['owner_point']['request_id']==ident and execution['request']['run_id']==read['payload']['run_id'] and execution['request']['seq']==read['payload']['seq'],'executor did not use its actual owner point'
            if packet['ok']:assert packet['seq']==read['payload']['seq'],'response seq differs from owner point'
            checked+=1
        except Exception as exc:errors.append({'request_id':ident,'error':type(exc).__name__+': '+str(exc)})
    extra_reads=sorted(set(read_by_id)-set(packets));extra_executions=sorted(set(execute_by_id)-set(packets),key=str)
    if extra_reads:errors.append({'error':'owner reads outside saved client requests','request_ids':extra_reads})
    if extra_executions:errors.append({'error':'executor records outside saved client requests','request_ids':extra_executions})
    final=None
    try:
        envelope=json.loads(final_raw)
        assert isinstance(envelope,dict) and set(envelope)=={'format','sha256','payload'} and envelope['format']=='warehouse-checkpoint-v2','final point envelope differs'
        payload=envelope['payload'];digest=point_validate_payload(payload,run,owner,source_sha,seqs)
        assert envelope['sha256']==digest and final_raw==point_envelope_bytes(payload),'final point raw envelope differs'
        assert payload['seq']==head,'final point is not archived source head'
        raw_sha=hashlib.sha256(final_raw).hexdigest()
        assert pub_index[(digest,raw_sha)],'final point not observed as actually published'
        final={'payload':payload,'payload_sha256':digest,'envelope_sha256':raw_sha,'matches_source_head':True}
    except Exception as exc:errors.append({'error':'final point: '+type(exc).__name__+': '+str(exc)})
    return {'passed':not errors,'errors':errors,'publications':len(pubs),'owner_reads':len(reads),'checked_requests':checked,
            'omitted_point_requests':omitted,'rename_interleaving_reads':interleaved,'initial_none_publications_proved':initial_none,
            'cached_summary_fields_checked':dict(checked_summary_fields),'final_archived_point':final,
            'runtime_instance_id':owner,'source_sha256':source_sha,'does_not_require_every_source_seq_to_be_published':True}

def audit_native_access(*,ready,packets,apis,rows,parse_errors):
    errors=list(parse_errors);native=ready['native_nginx'];by_id=defaultdict(list);api_by_id=defaultdict(list);durations=defaultdict(list)
    try:
        assert native['binary_sha256']==NATIVE_BINARY_SHA and native['config_sha256']==NATIVE_CONFIG_SHA,'native binary/config identity differs'
        assert type(native['pid']) is int and native['pid']>0 and isinstance(native['prefix'],str) and native['prefix'],'invalid native process identity'
    except Exception as exc:errors.append({'error':'native identity: '+type(exc).__name__+': '+str(exc)})
    all_routes=Counter()
    for i,row in enumerate(rows):
        try:
            assert isinstance(row,dict),'native record is not object'
            all_routes[(row['method'],row['uri'],row['status'])]+=1
            by_id[row['request_id']].append(row)
        except Exception as exc:errors.append({'native_line':i+1,'error':type(exc).__name__+': '+str(exc)})
    for row in apis:api_by_id[row['request_id']].append(row)
    matched=0
    for ident,packet in packets.items():
        try:
            assert len(by_id[ident])==1,'native measurement ID not unique/present'
            row=by_id[ident][0]
            assert row['method']=='POST' and row['uri']=='/api/knowledge/query','native route differs'
            assert type(row['status']) is int and row['status']==packet['status'],'native/client status differs'
            assert type(row['bytes']) is int and row['bytes']>=0,'invalid native bytes'
            duration=float(row['duration_s']);ended=float(row['end_s'])
            assert math.isfinite(duration) and duration>=0 and math.isfinite(ended) and ended>0,'invalid native duration/end'
            durations[packet['q']].append(duration*1000);durations['all'].append(duration*1000)
            assert len(api_by_id[ident])==1,'native request lacks unique sidecar ASGI record'
            api=api_by_id[ident][0];assert row['status']==api['status'],'native/sidecar status differs'
            assert row['upstream']=='127.0.0.1:'+str(api['port']),'native upstream differs from actual sidecar'
            upstream=float(row['upstream_s']);assert math.isfinite(upstream) and upstream>=0,'invalid upstream response time'
            matched+=1
        except Exception as exc:errors.append({'request_id':ident,'error':type(exc).__name__+': '+str(exc)})
    unknown_measurement_ids=sorted(k for k in by_id if isinstance(k,str) and k.startswith('live19-') and k not in packets)
    if unknown_measurement_ids:errors.append({'error':'native measurement IDs lack saved client packets','request_ids':unknown_measurement_ids})
    return {'passed':not errors,'errors':errors,'records_total':len(rows),'measurement_requests_checked':matched,
            'all_routes_including_startup_ui':{str(k):v for k,v in all_routes.items()},
            'nonmeasurement_records':sum(len(v) for k,v in by_id.items() if k not in packets),
            'native_identity':native,'duration_semantics':'Nginx $request_time, seconds in access record; distinct from client full JSON end-to-end and sidecar ASGI',
            'durations_ms':dict(durations)}


"""Pure fixed-key fence and actual DB-call evidence checks, embedded in v9."""
from collections import Counter,defaultdict
import math

def fence_int(value,low,high,label):
    assert type(value) is int and low<=value<=high,(label,value)
    return value

def fence_ms(value,label):
    assert type(value) in (int,float) and math.isfinite(value) and value>=0,(label,value)
    return value

def verify_fixed_key_fence(trace,*,run,requested_seq,source_head,budget_head,observer_error=None):
    assert observer_error is None and isinstance(trace,dict),'fence observation invalid'
    assert trace['version']=='fixed-key-point-fence-v2' and trace['run_id']==run and trace['requested_seq']==requested_seq,'fixed-key fence identity differs'
    S=fence_int(requested_seq,1,source_head,'requested seq');H=fence_int(trace['head_A'],S,source_head,'head A')
    assert type(trace['A_rows']) is int and trace['A_rows']==1,'HEAD statement row count differs'
    assert trace['requested_key']==f'{run}:{S}' and trace['head_key']==f'{run}:{H}','fence did not bind full external keys'
    assert trace['requested_exists'] is True and trace['head_exists'] is True and trace['outcome']=='ok','fixed-key point missing/error'
    assert type(budget_head) is int and budget_head==H,'response budget moved away from fresh A head'
    probes=trace['probes'];assert isinstance(probes,list) and len(probes)==(1 if H==S else 2),'unexpected point probes/retry count'
    expected=[('requested',S,'B')]+([] if H==S else [('head',H,'C')])
    fence_ms(trace['A_ms'],'HEAD ms');advanced=False
    for probe,(role,seq,label) in zip(probes,expected):
        assert probe['role']==role and probe['key']==f'{run}:{seq}' and type(probe['expected_seq']) is int and probe['expected_seq']==seq,'probe key/role/cursor differs'
        assert type(probe['rows']) is int and probe['rows']==1 and type(trace[label+'_rows']) is int and trace[label+'_rows']==1,'POINT row count differs'
        current=fence_int(probe['current_head'],H,source_head,'POINT current head')
        assert trace['head_'+label]==current and probe['exists'] is True,'POINT trace existence/head differs'
        fence_ms(probe['ms'],'probe ms');fence_ms(trace[label+'_ms'],'trace point ms')
        assert probe['ms']==trace[label+'_ms'],'probe/statement timing differs'
        advanced|=current>H
    if H==S:
        assert trace['reused_requested_for_head'] is True and trace['C_rows'] is None and trace['head_C'] is None,'same-head requested probe was not uniquely reused'
    else:
        assert trace['reused_requested_for_head'] is False,'distinct fresh budget head was silently reused'
    return {'H':H,'requested_seq':S,'point_probes':len(probes),'head_advanced_during_resolution':advanced,'reused_requested_for_head':H==S}

def db_numeric_stats(values,*,milliseconds=False):
    values=sorted(values);n=len(values)
    result={'count':n,'sum':sum(values),'mean':sum(values)/n if n else None,'p99':values[math.ceil(.99*n)-1] if n else None,'max':values[-1] if n else None}
    if milliseconds:result['unit']='ms'
    return result

def audit_global_db_stages(*,records,executions,packets,run,source_head):
    errors=[];by_id=defaultdict(list);execution_by_id=defaultdict(list);metrics=defaultdict(lambda:defaultdict(list));outcomes=defaultdict(Counter)
    for execution in executions:
        ident=execution.get('request_id',(execution.get('owner_point') or {}).get('request_id'))
        execution_by_id[ident].append(execution)
    globals_={ident:packet for ident,packet in packets.items() if packet['q']=='global'}
    for row in records:
        ident=row.get('request_id');by_id[ident].append(row)
        stage=row.get('stage','INVALID');outcomes[stage][row.get('outcome','missing')]+=1
        # Failed calls remain in duration/count summaries whenever their raw
        # observation is typed. Their failure still fails the qualification.
        if (type(row.get('start_ns')) is int and type(row.get('end_ns')) is int and row['end_ns']>=row['start_ns']):
            metrics[stage]['duration_ms'].append((row['end_ns']-row['start_ns'])/1e6)
        for field in ('rows','event_count','robot_count'):
            value=row.get(field)
            if type(value) is int and value>=0:metrics[stage][field].append(value)
        try:
            assert ident in globals_,'DB-call record is not a saved global request'
            assert len(execution_by_id[ident])==1,'DB-call lacks unique actual executor'
            execution=execution_by_id[ident][0];request=execution['request'];observed=row['request']
            assert observed['query_id']=='global' and observed['run_id']==run and observed['seq']==request['seq'] and observed['robot_id']==request['robot_id'],'DB-call request identity/cursor differs'
            assert all(key in request and request[key]==value for key,value in observed.items()),'DB-call small request differs from actual executor input'
            assert (row['pid'],row['port'])==(execution['pid'],execution['port']),'DB-call sidecar identity differs'
            assert execution['start_ns']<=row['start_ns']<=row['end_ns']<=execution['end_ns'],'DB-call outside actual executor interval'
            stage=row['stage'];assert stage in ('HEAD','POINT','FLEET','EVENTS','OTHER') or isinstance(stage,str) and stage.startswith('OTHER:'),'unknown DB stage'
            assert row['outcome']=='ok' and row.get('exception') is None,'DB-call error observed'
            params=row['params'];assert isinstance(params,dict) and params.get('run')==run,'DB-call bound run differs'
            if stage=='HEAD':assert set(params)=={'run'},'HEAD params differ'
            elif stage=='POINT':assert set(params)=={'run','key'} and isinstance(params['key'],str),'POINT must bind a full literal key'
            elif stage in ('FLEET','EVENTS'):
                assert params.get('seq')==request['seq'],'DB-call requested seq differs'
                if 'start' in params:fence_int(params['start'],1,request['seq'],'event window start')
                if 'limit' in params:assert type(params['limit']) is int and params['limit']>=1,'invalid event limit'
            for field in ('rows','event_count','robot_count'):
                value=row.get(field)
                if value is not None:
                    assert type(value) is int and value>=0,('invalid count',field,value)
            assert type(row['rows']) is int and row['rows']>=0,'DB-call rows absent'
        except Exception as exc:errors.append({'request_id':ident,'stage':row.get('stage'),'error':type(exc).__name__+': '+str(exc),'record':row})
    counts_by_global={};per_global_total_ms=[];event_call_distribution=Counter();fleet_call_distribution=Counter();probe_calls=0
    for ident,packet in globals_.items():
        rows=sorted(by_id[ident],key=lambda row:row['start_ns']);counts=Counter(row['stage'] for row in rows);counts_by_global[ident]=dict(counts)
        event_call_distribution[counts['EVENTS']]+=1;fleet_call_distribution[counts['FLEET']]+=1
        per_global_total_ms.append(sum((row['end_ns']-row['start_ns'])/1e6 for row in rows))
        try:
            assert len(execution_by_id[ident])==1,'global lacks unique actual executor'
            execution=execution_by_id[ident][0];trace=execution['catalog_fence']
            proof=verify_fixed_key_fence(trace,run=run,requested_seq=execution['request']['seq'],source_head=source_head,budget_head=packet.get('budget_head'),observer_error=execution.get('fence_observer_error'))
            heads=[row for row in rows if row['stage']=='HEAD'];points=[row for row in rows if row['stage']=='POINT']
            assert len(heads)==1 and len(points)==proof['point_probes'],'actual HEAD/POINT calls differ from trace (missing call or retry)'
            assert heads[0]['rows']==trace['A_rows']==1,'HEAD rows differ from fence'
            previous_end=heads[0]['end_ns']
            for call,probe in zip(points,trace['probes']):
                assert call['params']=={'run':run,'key':probe['key']},'actual bound POINT key differs from probe'
                assert call['rows']==probe['rows']==1 and previous_end<=call['start_ns'],'POINT call rows/order differ'
                previous_end=call['end_ns'];probe_calls+=1
            # FLEET is legitimately absent on a fragment cache hit. EVENTS is
            # reported, not made an unsupported per-request cardinality gate.
        except Exception as exc:errors.append({'request_id':ident,'error':type(exc).__name__+': '+str(exc),'call_counts':dict(counts)})
    return {'passed':not errors,'errors':errors,'global_requests':len(globals_),'records':len(records),'actual_point_probes_checked':probe_calls,
            'per_stage':{stage:{field:db_numeric_stats(values,milliseconds=field=='duration_ms') for field,values in fields.items()} for stage,fields in metrics.items()},
            'per_global_call_counts':counts_by_global,'total_db_read_ms_per_global':db_numeric_stats(per_global_total_ms,milliseconds=True),
            'per_stage_outcomes':{stage:dict(counts) for stage,counts in outcomes.items()},
            'events_call_count_distribution':dict(event_call_distribution),'fleet_call_count_distribution':dict(fleet_call_distribution),
            'observed_other_stage_labels':sorted(stage for stage in metrics if stage=='OTHER' or isinstance(stage,str) and stage.startswith('OTHER:')),
            'events_call_cardinality_is_observational_not_a_gate':True,'fleet_cache_hit_zero_calls_allowed':True,
            'semantics':'delegated original _read calls only; no additional DB reads or payload encoding; durations are within actual executor intervals'}


def stats(values):
    values=sorted(values);n=len(values)
    return {'count':n,'mean_ms':sum(values)/n if n else None,'p99_ms':values[math.ceil(.99*n)-1] if n else None,
            'max_ms':values[-1] if n else None,'over_500ms':sum(v>500 for v in values)}

begun=time.perf_counter();data=json.loads((E/'client-run.json').read_text())
assert data['run_id']==run
assert data['client_wrapper_sha256']==sha(R/f'deployment/free-live-joint-client-{version}.py')
source_files=['journal.sqlite','checkpoint.json','results.json']
before={n:sha(OUT/n) for n in source_files};packets_before=sha(E/'api-responses.jsonl.gz')
EXPECTED_MANIFEST_SHA='d057e847ff37f1ea4d29510e5dc81d66889b3ca0e14acd4aea894500626b42d7'
assert sha(S/'source-s9-manifest.json')==EXPECTED_MANIFEST_SHA
source_manifest=json.loads((S/'source-s9-manifest.json').read_text())['files'];assert len(source_manifest)==242
assert ready['source']==str(S) and ready['source_full_manifest_sha256']==EXPECTED_MANIFEST_SHA
backend_map={p.relative_to(S).as_posix():sha(p) for p in sorted((S/'backend').rglob('*')) if p.is_file() and p.suffix in ('.py','.json')}
assert all(source_manifest.get(k)==v for k,v in backend_map.items())
independent_runtime_source_sha256=hashlib.sha256(point_canonical(backend_map)).hexdigest()
assert independent_runtime_source_sha256==ready['initial_snapshot']['metadata']['source_sha256']
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
                packets[ident]={'q':q,'ok':True,'seq':value['seq'],'robot_id':row['request']['robot_id'],'client_start_ns':row['client_start_ns'],'client_end_ns':row['client_end_ns'],'client_pid':row['client_pid'],'status':row['status'],'point_omitted':('run_id' not in row['request'] and 'seq' not in row['request']),'budget_head':(row.get('response') or {}).get('result',{}).get('point',{}).get('graph_committed_seq')}
            except Exception as exc:
                errors.append({'request_id':ident,'error':type(exc).__name__+': '+str(exc)})
                packets[ident]={'q':q,'ok':False,'robot_id':row['request']['robot_id'],'client_start_ns':row['client_start_ns'],'client_end_ns':row['client_end_ns'],'client_pid':row['client_pid'],'status':row['status'],'point_omitted':('run_id' not in row['request'] and 'seq' not in row['request']),'budget_head':(row.get('response') or {}).get('result',{}).get('point',{}).get('graph_committed_seq')}
finally:truth.close();oracle.close()

OBS=E/'observations-at-audit'
producer_observations=[json.loads(line) for line in (OBS/'server-observations.jsonl').read_text().splitlines()]
assert all(row['kind'] in ('history_context','control_action','published_query_point') for row in producer_observations)
observations=list(producer_observations)
sidecar_logs=sorted(OBS.glob('sidecar-*-observations.jsonl'));assert len(sidecar_logs)==8
for path in sidecar_logs:observations.extend(json.loads(line) for line in path.read_text().splitlines())
sidecar_meta=[json.loads(path.read_text()) for path in sorted(E.glob('sidecar-*-wrapper.json'))];assert len(sidecar_meta)==8
assert {x['pid'] for x in sidecar_meta}=={x['pid'] for x in ready['sidecars']}
assert all(x['runtime_created_by_wrapper'] is False for x in sidecar_meta)
assert {x['wrapper_sha256'] for x in sidecar_meta}=={sha(R/f'deployment/free-live-query-sidecar-{version}.py')}
assert ready['wrapper_sha256']==sha(R/f'deployment/free-live-joint-server-{version}.py')
sidecar_identities={(x['pid'],x['port']) for x in sidecar_meta}
assert len(sidecar_identities)==8 and len({x['pid'] for x in sidecar_meta})==8 and len({x['port'] for x in sidecar_meta})==8
assert all(x['health']['runtime_created'] is False and x['health']['catalog_workers']==3 and x['health']['source_identity']['source_manifest_sha256']==EXPECTED_MANIFEST_SHA and x['health']['source_identity']['verified_files']==242 and x['health']['source_identity']['manifest_verified'] is True for x in ready['sidecars'])
apis=[row for row in observations if row['kind']=='api'];contexts=[row for row in observations if row['kind']=='history_context']
actions=[row for row in observations if row['kind']=='control_action'];api_ids=set();boundaries=[];server_errors=[]
for row in apis:
    ident=row['request_id']
    assert (row['pid'],row['port']) in sidecar_identities
    assert row['start_ns']<=row['request_body_end_ns']<=row['end_ns']
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
executions=[row for row in observations if row['kind']=='catalog_execution'];execution_errors=[];stage_groups=defaultdict(list);fence_errors=[];fence_checked=0;fence_advanced=0;fence_reused=0;fence_probe_counts=Counter()
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
            assert (execution['pid'],execution['port'])==(api['pid'],api['port'])
            observed_id=execution.get('request_id',(execution.get('owner_point') or {}).get('request_id'));assert observed_id==ident
            assert request['run_id']==run and request['query_id']==packet['q'] and request['robot_id']==robot
            if packet['ok']:assert request['seq']==packet['seq'] and execution['outcome']=='ok'
            if request['query_id']=='global':
                fence=execution.get('catalog_fence');fence_checked+=1
                try:
                    proof=verify_fixed_key_fence(fence,run=run,requested_seq=request['seq'],source_head=head,budget_head=packet['budget_head'],observer_error=execution.get('fence_observer_error'))
                    assert packet['ok'],'global response not source-verified'
                    fence_advanced+=int(proof['head_advanced_during_resolution'])
                    fence_reused+=int(proof['reused_requested_for_head']);fence_probe_counts[proof['point_probes']]+=1
                except Exception as exc:fence_errors.append({'request_id':ident,'fence':fence,'error':type(exc).__name__+': '+str(exc)})
            assert api['start_ns']<=execution['start_ns']<=execution['end_ns']<=api['end_ns']
            for stage,start,end in [('pre_execution_wait',api['start_ns'],execution['start_ns']),('executor',execution['start_ns'],execution['end_ns']),('post_execution_send',execution['end_ns'],api['end_ns'])]:
                ms=(end-start)/1e6;stage_groups[(stage,packet['q'])].append(ms);stage_groups[(stage,'all')].append(ms)
            body_end=api['request_body_end_ns'];assert api['start_ns']<=body_end<=execution['start_ns']
            for stage,start,end in [('sidecar_request_body_wait',api['start_ns'],body_end),('sidecar_body_to_execution',body_end,execution['start_ns'])]:
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
db_stage_records=[row for row in observations if row['kind']=='catalog_db_stage']
db_stage_check=audit_global_db_stages(records=db_stage_records,executions=executions,packets=packets,run=run,source_head=head)
publications=[row for row in producer_observations if row['kind']=='published_query_point']
owner_reads=[row for row in observations if row['kind']=='owner_point_read']
point_original=E/'source-at-audit/live-query-point.json.original'
point_check=audit_owner_points(ready=ready,packets=packets,apis=apis,executions=executions,publications=publications,reads=owner_reads,head=head,final_raw=point_original.read_bytes())
native_path=E/'native-access-at-audit.jsonl';native_rows=[];native_parse_errors=[]
for line,raw in enumerate(native_path.read_text().splitlines(),1):
    try:native_rows.append(json.loads(raw))
    except Exception as exc:native_parse_errors.append({'native_line':line,'error':type(exc).__name__+': '+str(exc)})
native_check=audit_native_access(ready=ready,packets=packets,apis=apis,rows=native_rows,parse_errors=native_parse_errors)
for q,values in native_check.pop('durations_ms').items():groups[('native_frontend',q)].extend(values)
native_file_identity={'binary_sha256':sha(ready['native_nginx']['binary']),'config_sha256':sha(ready['native_nginx']['config'])}
assert native_file_identity=={'binary_sha256':NATIVE_BINARY_SHA,'config_sha256':NATIVE_CONFIG_SHA}
assert data['clients']==100 and data['client_processes']==8 and len(data['process_exitcodes'])==8 and all(code==0 for code in data['process_exitcodes'])

after={n:sha(OUT/n) for n in source_files}
qualification={'all_owner_point_reads_source_published_and_used':point_check['passed'],'all_native_access_ids_and_statuses_verified':native_check['passed'],
               'functional_complete':final['status']=='complete' and final['metrics']['completed']==19 and final['metrics']['late']==0 and final['metrics']['handoffs']==1,
               'full_source_verified':not errors and len(seen)==len(apis) and api_ids==seen,
               'all_api_intervals_complete':all(row['body_send_completed'] for row in apis),'all_api_http200':all(row['status']==200 for row in apis),'all_executions_correlated':not execution_errors and len(executions)==len(packets),'all_client_phase_timestamps_valid':not phase_errors,'all_global_fixed_key_fences_verified':not fence_errors and fence_checked==counts['global'],'all_global_db_stages_correlated_and_keys_verified':db_stage_check['passed'],'eight_client_processes_verified':len({p['client_pid'] for p in packets.values()})==8,'one_inflight_per_robot_verified':not single_inflight_errors,'all_observed_watermarks_lag0':not watermark_errors,
               'exact_final_body_send_intervals':all(r.get('end_semantics')=='last_response_body_send_completed' for r in apis),
               'two_actual_control_brackets':len(actions)==len(brackets)==2 and len(used)==2,
               'all_control_brackets_api_covered':len(brackets)==2 and all(b.get('whole_control_bracket_api_coverage',{}).get('fully_covered') for b in brackets),
               'all_actual_api_within_500ms':all(v<=500 for v in groups[('server','all')]),
               'all_client_requests_within_500ms':all(v<=500 for v in groups[('client','all')]),
               'source_packets_unchanged':before==after and sha(E/'api-responses.jsonl.gz')==packets_before,
               'frozen_source_unchanged':all(sha(S/p)==v for p,v in source_manifest.items()),'frozen_live_adapter_unchanged':all(sha(S/p)==v for p,v in live_manifest['files'].items())}
report={'version':version,'scope':'S9 fixed-key fence / actual DB-call stages / Native frontend / eight query-only sidecar ASGI / one owner Runtime; full source-verified responses and real control overlap; NOT million-node qualification',
        'passed':all(qualification.values()),'qualification':qualification,'run_id':run,'source':str(S),'output_root':str(OUT),
        'sidecar_cli_sha256':sha(S/'ops/warehouse_query_sidecar.py'),'catalog_workers':24,
        'sidecars_ready':ready['sidecars'],'sidecar_wrapper_sha256':sha(R/f'deployment/free-live-query-sidecar-{version}.py'),
        'sidecar_observations_sha256':{path.name:sha(path) for path in sidecar_logs},'raw_archive_manifest_sha256':sha(E/'source-at-audit/archive-manifest.json'),
        'source_full_manifest_sha256':sha(S/'source-s9-manifest.json'),'livepoint_files_sha256':live_manifest['files'],
        'source_manifest_sha256':sha(S/'source-s9-manifest.json'),'source_artifacts_sha256':before,'response_packets_sha256':packets_before,
        'server_observations_sha256':sha(OBS/'server-observations.jsonl'),'ready_sha256':sha(E/'ready.json'),'client_run_sha256':sha(E/'client-run.json'),
        'server_wrapper_sha256':ready['wrapper_sha256'],'client_wrapper_sha256':data['client_wrapper_sha256'],
        'postverify_tool_sha256':sha(Path(__file__)),'oracle_sha256':sha(E/'postload-oracle.sqlite'),
        'client_threads':data['clients'],'client_processes':data['client_processes'],'client_process_exitcodes':data['process_exitcodes'],'actual_api_peak':peak,'max_sidecar_local_counter':max((r['inflight_at_start'] for r in apis),default=0),'api_peak_semantics':'global sweep of all eight sidecars; local counters are not a global concurrency count','sidecar_local_counter_peaks':{str(port):max((r['inflight_at_start'] for r in apis if r['port']==port),default=0) for pid,port in sidecar_identities},
        'load_wall_seconds':(data['ended_ns']-data['begun_ns'])/1e9,'client_requests_per_wall_second':sum(counts.values())/((data['ended_ns']-data['begun_ns'])/1e9),
        'requests':sum(counts.values()),'query_mix':dict(counts),'verified':len(seen)-len(errors),'response_errors':errors,'server_errors':server_errors,
        'latencies':{f'{p}/{q}':stats(v) for (p,q),v in groups.items()},'query_source_cutoffs':dict(sorted(seq_counts.items())),
        'observed_live_frames':len(frames),'watermark_errors':watermark_errors,'final_knowledge':final['knowledge'],
        'final_status':final['status'],'final_elapsed':final['elapsed'],'final_metrics':final['metrics'],
        'actual_control_brackets':brackets,'unmatched_history_contexts':[c for i,c in enumerate(contexts) if i not in used],
        'actual_executor_peak':execution_peak,'executor_threads_observed':len({(row.get('pid'),row['thread_id']) for row in executions}),'executor_records':len(executions),
        'client_http_stages':{f'{stage}/{q}':stats(v) for (stage,q),v in client_stages.items()},'client_phase_errors':phase_errors,'single_inflight_errors':single_inflight_errors,
        'execution_correlation_errors':execution_errors,'stages':{f'{stage}/{q}':stats(v) for (stage,q),v in stage_groups.items()},
        'fence_diagnostics':{'version':'fixed-key-point-fence-v2','checked_globals':fence_checked,'head_advanced_during_point_resolution':fence_advanced,'same_requested_reused_count':fence_reused,'point_probe_counts':dict(fence_probe_counts),'errors':fence_errors},'global_db_stage_audit':db_stage_check,
        'production_catalog_errors':[row for row in observations if row['kind']=='catalog_error'],
        'owner_point_audit':point_check,'native_access_audit':native_check,'latency_scope_labels':{'client':'original full end-to-end including Native and JSON parse','native_frontend':'Native $request_time duration_s','server':'eight sidecar ASGI request enter through final response body send'},
        'native_access_sha256':sha(native_path),'final_point_original_sha256':sha(point_original),'native_binary_sha256':native_file_identity['binary_sha256'],'native_config_sha256':native_file_identity['config_sha256'],'independent_runtime_source_sha256':independent_runtime_source_sha256,
        'knowledge_control_stats':result['scheduler']['knowledge_runtime']['stats'],'postload_seconds':time.perf_counter()-begun}
sha_values=[v for k,v in report.items() if k.endswith('_sha256') and k not in ('source_artifacts_sha256','livepoint_files_sha256','sidecar_observations_sha256')]+list(before.values())+list(live_manifest['files'].values())+[sha(path) for path in sidecar_logs]
assert isinstance(report['source_artifacts_sha256'],dict) and set(report['source_artifacts_sha256'])==set(source_files)
assert all(isinstance(v,str) and len(v)==64 and all(c in '0123456789abcdef' for c in v) for v in sha_values),'malformed SHA report field'
with (E/'postverify-report-v9.json').open('x') as stream:json.dump(report,stream,indent=2)
print(json.dumps({k:report[k] for k in ('passed','qualification','run_id','requests','actual_api_peak','query_mix','latencies','final_status','final_elapsed','knowledge_control_stats')}))
print('report_sha256',sha(E/'postverify-report-v9.json'))
raise SystemExit(0 if report['passed'] else 2)
