"""Merge original load sessions and audit a completed, frozen joint experiment.

This tool performs no database writes and no measured production queries. Raw
sessions remain intact. Its observation timeline follows each session's OWN
observer, splicing the later session only after the first observer has ended.
"""
import argparse
from bisect import bisect_right
from collections import Counter, defaultdict
import gzip
import hashlib
import json
import math
from pathlib import Path
import re
import subprocess
import sys
import time


def sha(path):
    h=hashlib.sha256()
    with Path(path).open('rb') as stream:
        for block in iter(lambda:stream.read(1024*1024),b''):h.update(block)
    return h.hexdigest()


def load(path):return json.loads(Path(path).read_text(encoding='utf-8'))


def rows(path):
    opener=gzip.open if Path(path).suffix=='.gz' else open
    with opener(path,'rt',encoding='utf-8') as stream:
        for line in stream:
            if line.strip():yield json.loads(line)


def save(path,value):
    with Path(path).open('x',encoding='utf-8') as out:
        json.dump(value,out,ensure_ascii=False,indent=2,allow_nan=False)
        out.write('\n')


def positive_int(value,label):
    if type(value) is not int or value<=0:raise ValueError('positive integer '+label+' required')
    return value


def timing(records):
    if not records:raise ValueError('empty request group')
    for r in records:
        positive_int(r['start_ns'],'request start');positive_int(r['end_ns'],'request end')
        if r['start_ns']>=r['end_ns']:raise ValueError('nonempty actual request interval required')
        if type(r['ok']) is not bool:raise ValueError('typed request success required')
    values=sorted((r['end_ns']-r['start_ns'])/1e6 for r in records)
    if values[0]<0:raise ValueError('reversed actual request interval')
    points=sorted((p,d) for r in records for p,d in ((r['start_ns'],1),(r['end_ns'],-1)))
    active=peak=0
    for _,delta in points:active+=delta;peak=max(peak,active)
    return {'requests':len(records),'clients':len({r['client_id'] for r in records}),
        'actual_peak':peak,'mean_ms':sum(values)/len(values),'p99_ms':values[math.ceil(.99*len(values))-1],
        'max_ms':values[-1],'over_500ms':sum(x>500 for x in values),
        'failed':sum(not r['ok'] for r in records),'query_ids':dict(Counter(r['request']['query_id'] for r in records)),
        'start_ns':min(r['start_ns'] for r in records),'end_ns':max(r['end_ns'] for r in records)}


def concatenate(sources,target):
    target=Path(target)
    opener=gzip.open if target.suffix=='.gz' else open
    kwargs={'encoding':'utf-8'}
    with opener(target,'xt',**kwargs) as out:
        for source in sources:
            source_opener=gzip.open if source.suffix=='.gz' else open
            with source_opener(source,'rt',encoding='utf-8') as stream:
                for line in stream:
                    out.write(line)
                    # A JSONL source may end with a valid non-newline last row.
                    # Keep that complete row separate from the next source.
                    if not line.endswith('\n'):out.write('\n')


def observer_stream(records):
    if not records:raise ValueError('original observer must not be empty')
    run=records[0]['run_id'];previous_clock=previous_head=0
    if not isinstance(run,str) or not run:raise ValueError('observer run required')
    for row in records:
        clock=positive_int(row['observed_at_ns'],'observer clock')
        head=positive_int(row['committed_seq'],'observer head')
        if row['run_id']!=run or clock<previous_clock or head<previous_head:
            raise ValueError('source observer time/head/run reversed; preserve and investigate')
        previous_clock,previous_head=clock,head
    return run


def own_observer_contract(first_requests,second_requests,first,second):
    """Pressure cutoffs keep the observer that actually selected them.

    Functional reads are independently source-verified, not included in this
    pressure/freshness qualification. The two pressure sessions must occupy
    separate sides of the splice; no alternate observer can fill a missing
    pre-request sample after measurement.
    """
    if observer_stream(first)!=observer_stream(second):raise ValueError('observer runs differ')
    cut=first[-1]['observed_at_ns']
    for name,requests,observations in [('first',first_requests,first),('second',second_requests,second)]:
        clocks=[o['observed_at_ns'] for o in observations]
        if not requests:raise ValueError('both original pressure sessions required')
        for request in requests:
            if name=='first' and request['end_ns']>cut or name=='second' and request['start_ns']<=cut:
                raise ValueError('pressure session crosses own-observer splice')
            position=bisect_right(clocks,request['start_ns'])-1
            if position<0:raise ValueError('request has no own pre-request observer sample')
            observed=observations[position]
            if name=='second' and observed['observed_at_ns']<=cut:
                raise ValueError('second request own latest sample would be removed by splice')
            if request['run_id']!=observed['run_id']:raise ValueError('request/own-observer run differs')
            if request['point_kind']=='latest' and request['seq']<observed['committed_seq']:
                raise ValueError('latest request is older than its own observed head')
    return {'pressure_sessions_separated_at_splice':True,'latest_cutoffs_checked_against_own_observer':True}


def merge_observers(first,second,target):
    original={str(p):sha(p) for p in (first,second)}
    a=list(rows(first));b=list(rows(second))
    if observer_stream(a)!=observer_stream(b):raise ValueError('observer runs differ')
    # A latest-observed request takes its cutoff under its own observer lock.
    # Keeping another independent observer's earlier samples during that
    # session would silently change the freshness contract after measurement.
    cut=a[-1]['observed_at_ns']
    suffix=[r for r in b if r['observed_at_ns']>cut]
    if not suffix:raise ValueError('later observer has no post-first-session observations')
    merged=a+suffix
    for x,y in zip(merged,merged[1:]):
        if y['observed_at_ns']<x['observed_at_ns'] or y['committed_seq']<x['committed_seq']:
            raise ValueError('source observer time/head reversed; preserve and investigate')
    if any(sha(Path(p))!=h for p,h in original.items()):raise ValueError('original observer changed during merge')
    with target.open('x',encoding='utf-8') as out:
        for row in merged:out.write(json.dumps(row,allow_nan=False)+'\n')
    return {'method':'own-session observers; all first-session samples then strictly later second-session samples',
        'first_samples':len(a),'second_original_samples':len(b),'second_suffix_samples':len(suffix),
        'splice_after_ns':cut,'original_files_sha256':original}


def active_intervals(records):
    merged=[]
    for start,end in sorted((r['start_ns'],r['end_ns']) for r in records):
        if start>=end:raise ValueError('nonempty actual request interval required')
        if merged and start<=merged[-1][1]:merged[-1][1]=max(end,merged[-1][1])
        else:merged.append([start,end])
    return merged


def control_overlap(control,writer,requests):
    telemetry={}
    for row in rows(writer):
        seq=positive_int(row['seq'],'writer seq')
        if seq in telemetry:raise ValueError('duplicate actual writer telemetry sequence')
        if row['run_id']!=control['run_id']:raise ValueError('control/writer run differs')
        positive_int(row['started_ns'],'writer start');positive_int(row['observed_at_ns'],'writer end')
        if row['started_ns']>row['observed_at_ns']:raise ValueError('writer commit timing reversed')
        if row['writer_error'] is not None or min(row['durable_seq'],row['committed_seq'])<seq:
            raise ValueError('control source writer is not durably mirrored')
        telemetry[seq]=row
    groups=defaultdict(list)
    for row in requests:groups[row['round']].append(row)
    intervals={r:(min(x['start_ns'] for x in v),max(x['end_ns'] for x in v)) for r,v in groups.items()}
    inflight={r:active_intervals(v) for r,v in groups.items()}
    evidence=[]
    for decision in control['control_chain']['decisions']:
        if decision['run_id']!=control['run_id']:raise ValueError('decision/control run differs')
        if decision['action'] not in ('authorize_assistance_planning','authorize_transfer'):
            raise ValueError('unknown audited control action')
        if decision['seq']>=decision['commit_seq']:raise ValueError('decision source is not an earlier commit')
        before=telemetry[decision['seq']];after=telemetry[decision['commit_seq']]
        begin,end=before['observed_at_ns'],after['started_ns']
        if begin>end:raise ValueError('decision telemetry bracket reversed')
        overlap=[r for r,(a,b) in intervals.items() if a<=begin<=end<=b]
        covered=[r for r,spans in inflight.items() if any(a<=begin and end<=b and begin<b for a,b in spans)]
        evidence.append({'event_seq':decision['event_seq'],'action':decision['action'],
            'source_seq':decision['seq'],'event_commit_seq':decision['commit_seq'],
            'decision_sim_time':decision['decision_sim_time'],'bracket_start_ns':begin,'bracket_end_ns':end,
            'query_rounds_containing_entire_control_bracket':overlap,
            'query_rounds_covering_entire_control_bracket_with_actual_inflight':covered})
    return {'scope':'control execution is bracketed by actual source-commit completion and event-commit start; no invented per-decision clock',
        'proven_control_during_pressure':any(r['query_rounds_covering_entire_control_bracket_with_actual_inflight'] for r in evidence),
        'decisions':evidence,'writer_file_sha256':sha(writer)}


def command(args,log):
    start=time.time_ns()
    with log.open('x',encoding='utf-8') as out:
        result=subprocess.run(args,stdout=out,stderr=subprocess.STDOUT,check=False)
    stage={'name':log.stem,'start_ns':start,'end_ns':time.time_ns(),'exit_code':result.returncode,
        'command':args,'log':str(log),'log_sha256':sha(log)}
    save(log.with_suffix('.stage.json'),stage)
    return result.returncode


def session_directory(root,name):
    """Select one literal directory below queries/, never a path or symlink escape."""
    if not isinstance(name,str) or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_-]*',name):
        raise ValueError('session must be one directory name under queries/')
    parent=(root/'queries').resolve();selected=parent/name
    if selected.resolve().parent!=parent:raise ValueError('session must remain under queries/')
    return selected


def main(argv=None):
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--root',type=Path,required=True)
    p.add_argument('--tools-dir',type=Path,default=Path(__file__).resolve().parent)
    p.add_argument('--output-name',default='final')
    p.add_argument('--control-session',default='week-live-control-v5',help='original later pressure session directory under queries/')
    p.add_argument('--extra-session',action='append',default=[],help='append an original session; one directory name under queries/; repeat in measured order')
    args=p.parse_args(argv);root=args.root.resolve();tools=args.tools_dir.resolve()
    sessions=[session_directory(root,name) for name in ('week-live-v3',args.control_session,*args.extra_session)]
    if len(set(sessions))!=len(sessions):raise ValueError('duplicate original session directory')
    source=root/'source-v2';run=root/'runs/week-required-v2';control=root/'control-source-audit.json'
    stage_paths=[root/'logs'/name for name in ('week-required-day1-window-v2.stage.json',
        'week-required-resume-v2.stage.json','week-rules-complete-v2.stage.json')]
    stages=[load(path) for path in stage_paths]
    if any(type(s['exit_code']) is not int or s['exit_code']!=0 for s in stages):
        raise ValueError('all original writer stages must finish successfully first')
    result=load(run/'results.json')
    if (result['status']!='complete' or result.get('error') is not None
            or not result['acceptance']['all_input_completed']):
        raise ValueError('full original writer must finish first; a window pause is incomplete')
    if not load(control)['passed']:raise ValueError('original control source audit must pass first')
    for session in sessions:
        if load(session/'load-result.json')['monitor_errors']:raise ValueError('original observer error retained in '+str(session))
    final=root/'queries'/args.output_name;final.mkdir(exist_ok=False)
    packets=('requests.jsonl','responses.jsonl.gz','functional-requests.jsonl','functional-responses.jsonl.gz')
    original_paths=[s/n for s in sessions for n in (*packets,'writer-head-observations.jsonl','load-result.json')]
    original_paths += [run/'results.json',run/'writer.jsonl',control,*stage_paths]
    original={str(path):sha(path) for path in original_paths}
    session_requests=[list(rows(s/'requests.jsonl')) for s in sessions]
    session_functional=[list(rows(s/'functional-requests.jsonl')) for s in sessions]
    round_sets=[{r['round'] for r in records} for records in session_requests]
    seen_rounds=set()
    for rounds in round_sets:
        if seen_rounds&rounds:raise ValueError('pressure round ID reused across sessions')
        seen_rounds.update(rounds)
    previous_observer=sessions[0]/'writer-head-observations.jsonl'
    previous_requests=list(session_requests[0]);steps=[]
    for index,next_session in enumerate(sessions[1:],1):
        next_observer=next_session/'writer-head-observations.jsonl'
        own=own_observer_contract(previous_requests,session_requests[index],
            list(rows(previous_observer)),list(rows(next_observer)))
        derived=final/f'writer-head-observations-splice-{index+1}.jsonl'
        step=merge_observers(previous_observer,next_observer,derived)
        step.update(own)
        step.update(next_session=next_session.name,derived_file=str(derived),derived_sha256=sha(derived))
        steps.append(step)
        previous_observer=derived;previous_requests.extend(session_requests[index])
    for name in packets:concatenate([s/name for s in sessions],final/name)
    concatenate([previous_observer],final/'writer-head-observations.jsonl')
    provenance={'method':'pairwise own-session observer splice in declared session order',
        'session_order':[s.name for s in sessions],'steps':steps,
        'final_derived_file_sha256':sha(final/'writer-head-observations.jsonl'),
        'pressure_sessions_separated_at_splice':all(s['pressure_sessions_separated_at_splice'] for s in steps),
        'latest_cutoffs_checked_against_own_observer':all(s['latest_cutoffs_checked_against_own_observer'] for s in steps)}
    if any(sha(Path(n))!=h for n,h in original.items()):raise ValueError('original experiment changed during merge')
    requests=list(rows(final/'requests.jsonl'));functional=list(rows(final/'functional-requests.jsonl'))
    ids=[r['request_id'] for r in requests+functional]
    if len(ids)!=len(set(ids)):raise ValueError('duplicate original request ID')
    grouped=defaultdict(list)
    for record in requests:grouped[record['round']].append(record)
    session_summaries=[{'name':s.name,'directory':str(s),'round_ids':sorted(round_sets[i]),
        'load_result':load(s/'load-result.json'),'pressure':timing(session_requests[i]),
        'functional':timing(session_functional[i]),
        'original_files_sha256':{str(s/n):original[str(s/n)]
            for n in (*packets,'writer-head-observations.jsonl','load-result.json')}} for i,s in enumerate(sessions)]
    summary={'pressure':timing(requests),'rounds':{str(k):timing(v) for k,v in grouped.items()},
        'pressure_aggregate_scope':'descriptive mixture of original sessions; compare workloads using session_summaries and their own parameters',
        'session_summaries':session_summaries,
        'functional':timing(functional),'functional_rows':[
            {'request_id':r['request_id'],'query_id':r['request']['query_id'],'seq':r['seq'],
             'service_ms':(r['end_ns']-r['start_ns'])/1e6,'ok':r['ok']} for r in functional],
        'observer_provenance':provenance,'original_packet_sha256':original,
        'full_responses_retained':True,'validation_during_measured_requests':False}
    save(final/'request-summary.json',summary)
    save(final/'control-during-pressure.json',control_overlap(load(control),run/'writer.jsonl',requests))
    codes=[]
    for stem in ('','functional-'):
        codes.append(command([sys.executable,str(tools/'verify_catalog_responses.py'),
            '--journal',str(run/'journal.sqlite'),'--requests',str(final/(stem+'requests.jsonl')),
            '--responses',str(final/(stem+'responses.jsonl.gz')),'--oracle-dir',str(root/'control-oracle'),
            '--output',str(final/(stem+'source-verification.json'))],final/(stem+'verification.log')))
    manifest={'version':'warehouse-platform-joint-v1',
        'source':{'root':str(source),'manifest':str(source/'source-manifest.json')},
        'dataset':{'directory':str(root/'inputs/seven-patterns'),'scene':str(source/'data/layouts/warehouse100-wide-v2.json')},
        'run':{'directory':str(run),'run_id':result['run_id'],'mode':'required',
            'require_complete':True,'require_zero_late':True,'require_handoffs':True},
        'control_audit':str(control),'queries':{'requests':str(final/'requests.jsonl'),
            'responses':str(final/'responses.jsonl.gz'),'verification':str(final/'source-verification.json'),
            'expected_clients':100,'require_peak':True,'require_latest':True,'require_writer_overlap':True},
        'writer_observations':str(final/'writer-head-observations.jsonl'),'stages':stages,
        'functional_verification':str(final/'functional-source-verification.json'),
        'control_overlap':str(final/'control-during-pressure.json')}
    save(final/'joint-manifest.json',manifest)
    codes.append(command([sys.executable,str(tools/'audit_platform_joint.py'),
        '--manifest',str(final/'joint-manifest.json'),'--output',str(final/'joint-audit.json')],final/'joint-audit.log'))
    unchanged=all(sha(Path(n))==h for n,h in original.items())
    passed=not any(codes) and unchanged
    save(final/'collection-result.json',{'passed':passed,'command_exit_codes':codes,'original_packets_unchanged':unchanged})
    print(json.dumps({'passed':passed,'pressure':summary['pressure'],'functional':summary['functional']}),flush=True)
    return 0 if passed else 2


if __name__=='__main__':raise SystemExit(main())
