"""Read-only composition audit for a frozen warehouse joint experiment.

Manifest contract is documented in docs/PLATFORM_JOINT_AUDIT.md. A passed run,
correct query batch, and actual overlap are checked separately, then coupled
by source/data/run hashes. All query durations, including failures, are kept.
"""
import argparse
from collections import Counter
import gzip
import hashlib
import json
import math
from pathlib import Path
import sqlite3
import zlib


VERSION='warehouse-platform-joint-v1'


def canonical(value):
    return json.dumps(value,ensure_ascii=False,sort_keys=True,separators=(',',':'),allow_nan=False).encode('utf-8')


def digest(value):
    return hashlib.sha256(canonical(value)).hexdigest()


def file_sha(path):
    result=hashlib.sha256()
    with Path(path).open('rb') as stream:
        for part in iter(lambda:stream.read(1024*1024),b''):result.update(part)
    return result.hexdigest()


def integer(value,name,minimum=0):
    if type(value) is not int or value<minimum:raise ValueError('invalid '+name)
    return value


def load(path):
    return json.loads(Path(path).read_text(encoding='utf-8'))


def lines(path):
    opener=gzip.open if Path(path).suffix=='.gz' else open
    with opener(path,'rt',encoding='utf-8') as stream:
        for line in stream:
            if line.strip():yield json.loads(line)


def peak(records):
    points=[]
    for record in records:
        points.extend([(record['start_ns'],1),(record['end_ns'],-1)])
    active=answer=0
    for _,delta in sorted(points):
        active+=delta;answer=max(active,answer)
    return answer


def response_point(payload):
    """Extract explicit source metadata without modifying the complete reply.

    QueryCatalog-v1 wraps existing immutable contexts; fleet packs expose a
    point object. The goods-only/no-task form has no returned observed head,
    so strict joint verification rejects that form rather than invent a head.
    """
    if 'catalog_version' not in payload:return payload
    query=payload.get('query_id');inner=payload.get('result')
    if not isinstance(inner,dict):raise ValueError('catalog result object required')
    if query in ('self','readiness','task','goods','similar_history'):
        point=inner.get('context')
        if not isinstance(point,dict):raise ValueError('strict catalog context requires observed head; goods query must supply task_id')
    elif query in ('energy','assistance','progress','memory','local_awareness','global'):
        point=inner.get('point')
        if not isinstance(point,dict):raise ValueError('catalog fleet source point required')
    else:raise ValueError('unsupported catalog wire profile')
    for key in ('run_id','seq'):
        if key in point and (point[key]!=payload[key] or key=='seq' and type(point[key]) is not int):
            raise ValueError('catalog inner/outer source point differs')
    if 'graph_committed_seq' not in point or 'batch_checksum' not in point:
        raise ValueError('catalog source metadata lacks observed head/checksum')
    return {**point,'run_id':payload['run_id'],'seq':payload['seq']}


def audit(manifest_path):
    manifest_path=Path(manifest_path).resolve();spec=load(manifest_path)
    if spec.get('version')!=VERSION:raise ValueError('unsupported joint manifest version')
    base=manifest_path.parent
    def path(value):
        p=Path(value)
        return p.resolve() if p.is_absolute() else (base/p).resolve()
    checks={};files={}
    def check(name,value):
        if type(value) is not bool:raise ValueError('audit check must be boolean')
        checks[name]=value
    def artifact(value):
        p=path(value);files[str(p)]=file_sha(p);return p
    selected=spec['source'];source=path(selected['root']);frozen=load(artifact(selected['manifest']))
    mapping=frozen['files']
    if not isinstance(mapping,dict) or not mapping:raise ValueError('empty frozen source manifest')
    matched=True
    for name,expected in mapping.items():
        p=(source/name).resolve()
        if not p.is_relative_to(source):raise ValueError('frozen source escapes root')
        matched=matched and p.is_file() and file_sha(p)==expected
    check('all_selected_source_bytes_match',matched)
    actual_backend={p.relative_to(source).as_posix() for p in (source/'backend').rglob('*.py')}
    check('all_backend_sources_are_listed',actual_backend.issubset(mapping))
    data=spec['dataset'];directory=path(data['directory']);scene=artifact(data['scene'])
    data_manifest=load(artifact(str(directory/'manifest.json')))
    identities={'manifest_sha256':file_sha(directory/'manifest.json'),'scene_sha256':file_sha(scene)}
    check('dataset_scene_matches',data_manifest['scene_sha256']==identities['scene_sha256'])
    check('dataset_file_set',set(data_manifest['files'])=={'tasks.csv','initial_inventory.csv'})
    for name,key in [('tasks.csv','tasks_sha256'),('initial_inventory.csv','initial_inventory_sha256')]:
        identities[key]=file_sha(artifact(str(directory/name)))
        check('dataset_'+name+'_sha',identities[key]==data_manifest['files'][name])
    run=spec['run'];output=path(run['directory']);result_path=artifact(str(output/'results.json'))
    result=load(result_path);envelope=load(artifact(str(output/'checkpoint.json')))
    checkpoint=envelope['payload'];journal=artifact(str(output/'journal.sqlite'))
    integer(checkpoint['knowledge_seq'],'checkpoint knowledge sequence',1)
    check('checkpoint_envelope_valid',digest(checkpoint)==envelope['sha256'])
    check('run_identity',result['run_id']==checkpoint['run_id']==run['run_id'])
    check('successful_run',result['status']=='complete' and result['error'] is None)
    check('run_input_identity',result['input_identity']==identities and all(checkpoint['run_metadata'].get(k)==v for k,v in identities.items()))
    check('run_config_identity',digest(result['config'])==digest(checkpoint['config']))
    check('run_execution_identity',result['code_identity']==checkpoint['run_metadata']['execution_code_sha256'])
    mode=checkpoint['knowledge_runtime']['identity']['mode']
    check('explicit_control_mode',mode==run['mode'] and mode in ('required','rules'))
    robots=integer(result['registered_robots'],'robot count',100)
    check('unique_final_robot_population',len(checkpoint['robots'])==robots and
          {r['state']['robot_id'] for r in checkpoint['robots']}==set(range(1,robots+1))
          and all(type(r['state']['robot_id']) is int for r in checkpoint['robots']))
    for key in ('tasks_total','released','unfinished','late_unfinished'):integer(result[key],'result '+key)
    for key in ('completed','late','handoffs'):integer(result['metrics'][key],'metric '+key)
    integer(data_manifest['tasks'],'manifest tasks')
    check('declared_full_input_size',result['tasks_total']==data_manifest['tasks'])
    if run.get('require_complete',True):
        check('all_input_released_and_completed',result['released']==result['tasks_total']==result['metrics']['completed']
              and result['unfinished']==0 and checkpoint['cursor']==result['tasks_total']
              and len(checkpoint['tasks'])==result['tasks_total']
              and all(t['status']=='completed' for t in checkpoint['tasks'].values()))
    check('no_recorded_safety_failure',result['acceptance']['no_recorded_safety_failure'] is True)
    if run.get('require_zero_late',True):check('no_late_tasks',result['metrics']['late']==result['late_unfinished']==0)
    if run.get('require_handoffs',mode=='required'):check('actual_handoffs_exercised',integer(result['metrics']['handoffs'],'handoffs')>0)
    clock={};batches={};last_event=0
    db=sqlite3.connect(journal.as_uri()+'?mode=ro',uri=True)
    try:
        db.execute('BEGIN')
        heads=db.execute('SELECT run_id,seq,checksum,checkpoint,mirrored_seq FROM heads').fetchall()
        if len(heads)!=1:raise ValueError('one original run per journal required')
        head_run,head,cp_sha,cp_blob,mirrored=heads[0]
        check('journal_checkpoint_identical',head_run==run['run_id'] and cp_sha==envelope['sha256']
              and zlib.decompress(cp_blob)==canonical(checkpoint))
        check('journal_head_caught_up',head==mirrored==checkpoint['knowledge_seq']==result['knowledge']['durable_seq']
              ==result['knowledge']['mirrored_seq'] and result['knowledge']['last_error'] is None)
        seqs=[];events=Counter()
        for seq,checksum,blob in db.execute('SELECT seq,checksum,batch FROM commits WHERE run_id=? ORDER BY seq',(head_run,)):
            raw=zlib.decompress(blob);batch=json.loads(raw)
            if integer(seq,'commit',1)!=len(seqs)+1 or hashlib.sha256(raw).hexdigest()!=checksum or digest(batch)!=checksum:
                raise ValueError('journal commit gap or corrupted source')
            if batch['run_id']!=head_run or type(batch['seq']) is not int or batch['seq']!=seq:
                raise ValueError('foreign/mixed source commit')
            if (len(batch['robots'])!=robots or {r['robot_id'] for r in batch['robots']}!=set(range(1,robots+1))
                    or not all(type(r['robot_id']) is int for r in batch['robots'])):
                raise ValueError('incomplete actual robot state batch')
            if digest(batch['metadata'])!=digest(checkpoint['run_metadata']) or digest(batch['config'])!=digest(checkpoint['config']):
                raise ValueError('source configuration or metadata differs')
            for event in batch['events']:
                if integer(event['event_seq'],'event',1)!=last_event+1:raise ValueError('event sequence gap')
                event_time=event['sim_time']
                if (isinstance(event_time,bool) or not isinstance(event_time,(int,float)) or not math.isfinite(event_time)
                        or not 0<=event_time<=batch['sim_time']):raise ValueError('future or invalid source event time')
                last_event=event['event_seq'];events[event['kind']]+=1
            clock[seq]=integer(batch['sim_time'],'source sim time');batches[seq]=checksum;seqs.append(seq)
        check('complete_source_prefix',len(seqs)==head and head>0)
    finally:db.close()
    if mode=='required':
        control=load(artifact(spec['control_audit']))
        check('independent_control_audit_passed',control.get('passed') is True and control.get('failed_checks')==[])
        check('control_audit_source_bound',control['run_id']==head_run and control['prefix']==head
              and control['source_files_before']['journal']==file_sha(journal)
              and control['source_files_before']['results']==file_sha(result_path)
              and control['source_files_before']==control['source_files_after'])
        counts=control['control_chain']['counts']
        check('actual_control_chain_counts',counts['actual_cargo_handoffs']==events['cargo_handoff']==result['metrics']['handoffs']
              and counts['decisions']==events['knowledge_decision']
              and counts['logged_applied_actions']==events['knowledge_action_applied'])
    q=spec['queries'];request_path=artifact(q['requests']);response_path=artifact(q['responses']);verification=load(artifact(q['verification']))
    requests={};by_round={};last_client={};all_durations=[];latest=historical=0
    for row in lines(request_path):
        ident=row['request_id']
        if not isinstance(ident,str) or not ident or ident in requests:raise ValueError('missing or duplicate query ID')
        client=integer(row['client_id'],'query client',1);rnd=integer(row['round'],'query round',1)
        start=integer(row['start_ns'],'query start',1);end=integer(row['end_ns'],'query end',1)
        if start>=end:raise ValueError('query timing must be a real nonempty interval')
        if last_client.get((rnd,client),0)>start:raise ValueError('one client has overlapping serial requests')
        last_client[(rnd,client)]=end
        seq=integer(row['seq'],'query cutoff',1)
        if row['run_id']!=head_run or seq not in batches:raise ValueError('query refers to foreign/future source')
        if row.get('point_kind') not in ('latest','historical'):raise ValueError('explicit query point role required')
        latest+=row['point_kind']=='latest';historical+=row['point_kind']=='historical'
        if type(row.get('ok')) is not bool:raise ValueError('typed query success required')
        requests[ident]=row;by_round.setdefault(rnd,[]).append(row);all_durations.append((end-start)/1e6)
    if not requests:raise ValueError('empty joint query evidence')
    seen=set()
    for response in lines(response_path):
        ident=response['request_id']
        if ident not in requests or ident in seen:raise ValueError('foreign or duplicate full response')
        row=requests[ident];payload=response['result']
        if digest(payload)!=row['response_sha256']:raise ValueError('complete response fingerprint changed')
        point=response_point(payload)
        if point['run_id']!=head_run or type(point['seq']) is not int or point['seq']!=row['seq']:
            raise ValueError('response mixes run or cutoff')
        if point['batch_checksum']!=batches[row['seq']]:raise ValueError('response batch differs from source')
        if type(point['graph_committed_seq']) is not int or not row['seq']<=point['graph_committed_seq']<=head:
            raise ValueError('response has invalid graph watermark')
        seen.add(ident)
    check('every_request_has_complete_response',len(seen)==len(requests))
    check('all_requests_successful',all(r['ok'] for r in requests.values()))
    check('independent_query_verification_passed',verification.get('passed') is True and verification['request_count']==len(requests))
    check('query_verification_artifact_binding',verification['source_journal_sha256']==file_sha(journal)
          and verification['file_hashes']=={'requests':file_sha(request_path),'responses':file_sha(response_path)})
    expected=integer(q.get('expected_clients',100),'expected query clients',1)
    check('every_round_has_expected_clients',all({r['client_id'] for r in rows}==set(range(1,expected+1)) for rows in by_round.values()))
    peaks={str(rnd):peak(rows) for rnd,rows in by_round.items()}
    if q.get('require_peak',True):check('actual_api_peak_each_round',all(v>=expected for v in peaks.values()))
    check('latest_requests_exercised',latest>0 if q.get('require_latest',True) else True)
    observations=list(lines(artifact(spec['writer_observations'])))
    previous_clock=previous_head=0
    for row in observations:
        time=integer(row['observed_at_ns'],'writer observation clock',1);seq=integer(row['committed_seq'],'writer head',1)
        if row['run_id']!=head_run or seq not in batches or time<previous_clock or seq<previous_head:
            raise ValueError('foreign, reversed or invalid writer observation')
        previous_clock,previous_head=time,seq
    overlap={}
    for rnd,rows in by_round.items():
        start=min(r['start_ns'] for r in rows);end=max(r['end_ns'] for r in rows)
        inside=[r for r in observations if start<=r['observed_at_ns']<=end]
        overlap[str(rnd)]={'observations_inside':len(inside),'head_first':inside[0]['committed_seq'] if inside else None,
            'head_last':inside[-1]['committed_seq'] if inside else None,
            'observed_head_advanced':len(inside)>1 and inside[-1]['committed_seq']>inside[0]['committed_seq']}
    if q.get('require_writer_overlap',True):check('same_run_graph_write_advance_in_each_query_round',all(v['observed_head_advanced'] for v in overlap.values()))
    observed_fresh=True
    for row in requests.values():
        if row['point_kind']!='latest':continue
        prior=[o for o in observations if o['observed_at_ns']<=row['start_ns']]
        if not prior or row['seq']<prior[-1]['committed_seq']:observed_fresh=False
    if q.get('require_latest',True):check('latest_cutoff_not_older_than_pre_request_observed_head',observed_fresh)
    stages=spec['stages']
    if not isinstance(stages,list) or not stages:raise ValueError('nonempty executed stages required')
    if len({s['name'] for s in stages})!=len(stages):raise ValueError('duplicate executed stage names')
    check('all_stage_exit_codes_zero',all(type(s['exit_code']) is int and s['exit_code']==0 for s in stages))
    for stage in stages:
        if integer(stage['start_ns'],'stage start',1)>=integer(stage['end_ns'],'stage end',1):raise ValueError('invalid stage clock')
        if file_sha(artifact(stage['log']))!=stage['log_sha256']:raise ValueError('stage log changed')
    check('all_audited_artifacts_unchanged',all(file_sha(p)==h for p,h in files.items()))
    ordered=sorted(all_durations)
    def percentile(fraction):return ordered[min(len(ordered)-1,max(0,math.ceil(len(ordered)*fraction)-1))]
    return {'version':VERSION,'passed':all(checks.values()),'checks':checks,
        'failed_checks':[k for k,v in checks.items() if not v],
        'scope':'read-only composition of same-source actual run, full query responses, independent verification and observed write overlap; independent of hardware/third-party certification',
        'run_id':head_run,'source_commits':head,'actual_robot_state_rows':head*robots,
        'actual_event_counts':dict(events),'query_count':len(requests),'latest_requests':latest,'historical_requests':historical,
        'query_api_peaks':peaks,'write_overlap':overlap,
        'qualifications':{'same_run_live_writer_overlap_each_round':all(v['observed_head_advanced'] for v in overlap.values()),
            'latest_cutoff_observed_fresh':observed_fresh and latest>0,
            'all_requests_under_500ms':all(x<500 for x in ordered)},
        'all_request_ms':{'mean':sum(ordered)/len(ordered),'p99':percentile(.99),'max':ordered[-1],
            'over_500ms':sum(x>500 for x in ordered),'failed':sum(not r['ok'] for r in requests.values())},
        'artifact_sha256':files,'manifest_sha256':file_sha(manifest_path)}


def main(argv=None):
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--manifest',required=True,type=Path);parser.add_argument('--output',required=True,type=Path)
    args=parser.parse_args(argv)
    if args.output.exists():parser.error('preserve previous audit; select a fresh output file')
    try:report=audit(args.manifest)
    except Exception as exc:report={'version':VERSION,'passed':False,'error':type(exc).__name__+': '+str(exc)[:3000]}
    args.output.parent.mkdir(parents=True,exist_ok=True)
    with args.output.open('x',encoding='utf-8') as f:json.dump(report,f,ensure_ascii=False,indent=2,allow_nan=False)
    print(json.dumps({k:report[k] for k in ('passed','failed_checks','error') if k in report}))
    return 0 if report['passed'] else 2


if __name__=='__main__':raise SystemExit(main())
