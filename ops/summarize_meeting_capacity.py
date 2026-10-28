"""Read-only same-input capacity report; never converts a short run into approval.

The source databases/checkpoints are opened read-only. Missing outputs and
missing/nonzero exits remain explicit failures. This report is not a 24-hour,
million-node, concurrent-query or hardware-calibrated acceptance certificate.
"""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from contextlib import closing
from datetime import datetime
import csv
import hashlib
import json
from pathlib import Path
import sqlite3
import sys
import zlib

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


VERSION = 'warehouse-meeting-capacity-summary-v1'
BUSINESS_STAGES = {'to_source', 'to_target', 'picking', 'placing', 'to_handoff', 'handoff_receive'}
PHYSICAL_FRAME_FIELDS = ('elapsed', 'tick', 'running', 'counts', 'inside', 'pending', 'unreleased',
    'bufferCounts', 'totalTasks', 'chargerCount', 'reservedChargers', 'physicalBayCount')


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()


def digest(value):
    return hashlib.sha256(canonical(value)).hexdigest()


def file_sha(path):
    h = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            h.update(block)
    return h.hexdigest()


def load_json(path):
    return json.loads(Path(path).read_text(encoding='utf-8'),
        parse_constant=lambda value: (_ for _ in ()).throw(ValueError('nonfinite JSON ' + value)))


def inside(root, value):
    path = Path(value)
    path = (path if path.is_absolute() else root/path).resolve()
    if not path.is_relative_to(root):
        raise ValueError('plan path escapes assessment root')
    return path


def db_reader(path):
    db = sqlite3.connect(path.resolve().as_uri() + '?mode=ro', uri=True)
    db.execute('PRAGMA query_only=ON')
    return db


def quantile(values, fraction):
    values = sorted(values)
    return values[min(len(values)-1, int((len(values)-1)*fraction))] if values else None


def stats(values):
    return {'count': len(values), 'mean': sum(values)/len(values) if values else None,
        'p95': quantile(values, .95), 'p99': quantile(values, .99), 'max': max(values) if values else None}


def raw_robots(checkpoint):
    rows = [r['state'] for r in checkpoint['robots']]
    ids = [r['robot_id'] for r in rows]
    if len(ids) != 100 or set(ids) != set(range(1, 101)) or any(type(i) is not int for i in ids):
        raise ValueError('checkpoint does not contain 100 unique source robots')
    return sorted(rows, key=lambda r:r['robot_id'])


def physical_frame(frame):
    """All robot fields retained. Only source/UI/knowledge/computation metadata omitted."""
    rows = sorted(frame['robots'], key=lambda r:r['robot_id'])
    if len(rows) != 100 or {r['robot_id'] for r in rows} != set(range(1, 101)) or any(type(r['robot_id']) is not int for r in rows):
        raise ValueError('frame does not contain 100 unique source robots')
    return {**{k: frame[k] for k in PHYSICAL_FRAME_FIELDS if k in frame},
        'robots': rows, 'occupiedSlots': sorted(frame.get('occupiedSlots', []))}


def sampled_frames(path, run_id):
    signatures, series, max_by_type = {}, [], Counter()
    with closing(db_reader(path)) as db:
        for second, payload in db.execute('SELECT second,payload FROM frames ORDER BY second'):
            frame = json.loads(zlib.decompress(payload))
            if frame['runId'] != run_id or frame['elapsed'] != second or frame['tick'] != second:
                raise ValueError('frame key/run/tick identity mismatch')
            projection = physical_frame(frame)
            signatures[second] = digest(projection)
            active = Counter(r.get('robot_type_key', 'unknown') for r in frame['robots'] if r['stage'] in BUSINESS_STAGES)
            for key, value in active.items():
                max_by_type[key] = max(max_by_type[key], value)
            series.append({'second':second, 'pending':frame['pending'],
                'completed':frame['metrics']['completed'], 'business_active':sum(active.values()),
                'queued_robots':frame.get('counts', {}).get('queued'),
                'charging_robots':frame.get('counts', {}).get('charging')})
    if not series:
        raise ValueError('empty source frame database')
    return signatures, {'sampling':'stored frames only; no interpolation or unsaved peak inference',
        'frames':len(series), 'first_second':series[0]['second'], 'last_second':series[-1]['second'],
        'peak_pending_sampled':max(x['pending'] for x in series),
        'pending_at_last_sample':series[-1]['pending'],
        'peak_business_active_sampled':max(x['business_active'] for x in series),
        'peak_business_active_sampled_by_type':dict(max_by_type), 'series':series}


def journal_events(path, run_id, expected_head, expected_events):
    """Verify original commit checksums, then compare complete semantic events."""
    events, last_seq, event_sequences = [], 0, []
    with closing(db_reader(path)) as db:
        for seq, checksum, payload in db.execute('SELECT seq,checksum,batch FROM commits WHERE run_id=? ORDER BY seq', (run_id,)):
            batch = json.loads(zlib.decompress(payload))
            if seq != last_seq+1 or batch['run_id'] != run_id or batch['seq'] != seq or digest(batch) != checksum:
                raise ValueError('source journal gap/identity/checksum mismatch')
            for row in batch['events']:
                value = dict(row)
                ident = value.get('event_id')
                if not isinstance(ident, str) or not ident.startswith(run_id+':'):
                    raise ValueError('event source identity mismatch')
                value['event_id'] = '[RUN]'+ident[len(run_id):]
                if 'run_id' in value:
                    if value['run_id'] != run_id:raise ValueError('foreign event run')
                    value['run_id'] = '[RUN]'
                event_sequences.append(value['event_seq'])
                events.append(value)
            last_seq = seq
    if last_seq != expected_head or event_sequences != list(range(1, expected_events+1)):
        raise ValueError('journal/checkpoint head or complete event sequence mismatch')
    return events


def summarize_case(root, case):
    name = case['name']; output = inside(root, case['output'])
    report = {'name':name, 'layout':case.get('layout'), 'mix':case.get('mix'),
        'source':case.get('source'), 'output':str(output), 'completed_run':False,
        'errors':[], 'artifacts':{}}
    private = None
    try:
        exit_file = inside(root, case.get('exit_file', f'logs/peak-{name}.exit'))
        report['exit_file'] = str(exit_file)
        if not exit_file.is_file():raise ValueError('exit code absent; running or unobserved exit')
        exit_code = int(exit_file.read_text().strip());report['exit_code']=exit_code
        if exit_code != 0:report['errors'].append(f'nonzero exit {exit_code}')
        for file in ('results.json','checkpoint.json','frames.sqlite','journal.sqlite'):
            path = output/file
            if not path.is_file():raise ValueError('missing '+file)
            report['artifacts'][file] = file_sha(path)
        report['artifacts']['exit'] = file_sha(exit_file)
        result = load_json(output/'results.json')
        envelope = load_json(output/'checkpoint.json')
        if envelope.get('format') != 'warehouse-checkpoint-v2':raise ValueError('SHA-enveloped checkpoint required')
        from backend.checkpoint import read_checkpoint
        cp = read_checkpoint(output/'checkpoint.json')
        run = result['run_id'];report['run_id']=run
        if cp['run_id'] != run or cp['elapsed'] != result['elapsed']:
            raise ValueError('result/checkpoint run or clock mismatch')
        if digest(cp['config']) != digest(result['config']):raise ValueError('result/checkpoint config mismatch')
        dataset, scene, config = (inside(root, case[k]) for k in ('dataset','scene','config'))
        config_file_sha=file_sha(config)
        manifest = load_json(dataset/'manifest.json')
        metadata = cp['run_metadata'];actual = {
            'tasks_sha256':file_sha(dataset/'tasks.csv'),
            'initial_inventory_sha256':file_sha(dataset/'initial_inventory.csv'),
            'manifest_sha256':file_sha(dataset/'manifest.json'), 'scene_sha256':file_sha(scene)}
        if (actual['tasks_sha256'] != case['tasks_sha256'] or actual['initial_inventory_sha256'] != case['initial_sha256']):
            raise ValueError('plan/actual input hash mismatch')
        for key, value in actual.items():
            if metadata.get(key) != value or result['input_identity'].get(key) != value:
                raise ValueError('result/checkpoint/actual input hash mismatch: '+key)
        if manifest['files']['tasks.csv'] != actual['tasks_sha256'] or manifest['files']['initial_inventory.csv'] != actual['initial_inventory_sha256']:
            raise ValueError('manifest CSV hash mismatch')
        for key, value in load_json(config).items():
            if digest(value) != digest(cp['config'].get(key)):raise ValueError('config declaration differs: '+key)
        report['input_identity']=actual;report['code_identity']=result.get('code_identity')
        with (dataset/'tasks.csv').open(encoding='utf-8', newline='') as f:source_tasks=list(csv.DictReader(f))
        task_count=len(source_tasks);by_id={t['task_id']:t for t in source_tasks}
        if len(by_id)!=task_count:raise ValueError('duplicate source task IDs')
        start=datetime.fromisoformat(manifest['start'])
        for task_id, task in cp['tasks'].items():
            original=by_id.get(task_id)
            if original is None or any(task.get(k)!=v for k,v in original.items()):
                raise ValueError('checkpoint task static fields differ from immutable CSV')
            for field in ('release','due'):
                seconds=int((datetime.fromisoformat(original[field+'_time'])-start).total_seconds())
                if type(task.get(field+'_second')) is not int or task[field+'_second']!=seconds:
                    raise ValueError('checkpoint task clock differs from immutable CSV')
        tasks = list(cp['tasks'].values());finished=[t for t in tasks if t['status']=='completed']
        flow = [t['completed_at']-t['release_second'] for t in finished]
        late = sum(t['completed_at']>t['due_second'] for t in finished)
        late_unfinished=sum(t['status']!='completed' and t['due_second']<cp['elapsed'] for t in tasks)
        completed = len(finished)
        if completed != result['metrics']['completed'] or completed != cp['metrics']['completed']:
            raise ValueError('task/checkpoint/result completion mismatch')
        if late != result['metrics']['late'] or late_unfinished != result['late_unfinished']:
            raise ValueError('task/result lateness mismatch')
        if task_count != result['tasks_total'] or task_count != manifest['tasks']:
            raise ValueError('declared/source task count mismatch')
        fleet=raw_robots(cp);by_type=defaultdict(lambda:defaultdict(int))
        for robot in fleet:
            row=by_type[robot['robot_type_key']];row['robots']+=1
            for field in ('completed','missions','active_seconds','charge_seconds','queue_seconds','distance'):
                row[field]+=robot[field]
        frames, sample = sampled_frames(output/'frames.sqlite', run)
        if sample['last_second'] != cp['elapsed']:raise ValueError('final source frame missing')
        events=journal_events(output/'journal.sqlite',run,cp['knowledge_seq'],cp['event_sequence'])
        report.update(status=result['status'],elapsed=cp['elapsed'],wall_seconds=result['wall_seconds'],
            tasks_total=task_count,completed=completed,unfinished_total=task_count-completed,
            unfinished_released=len(tasks)-completed,unreleased=task_count-len(tasks),
            late_completed=late,late_unfinished=late_unfinished,zero_lateness=(late==0 and late_unfinished==0),
            last_task_completed_at=max((t['completed_at'] for t in finished),default=None),
            flow_seconds_completed_only=stats(flow),
            by_type={k:dict(v) for k,v in by_type.items()},sampled=sample,
            max_active_business_each_step=cp['metrics'].get('max_active_business'),
            planning_seconds=result['metrics'].get('planning_seconds'),
            planning_calls=result['metrics'].get('planning_calls'),dispatch_wall_seconds=result['metrics'].get('dispatch_wall_seconds'),
            full_event_count=len(events))
        if result['status']!='complete' or result.get('error'):
            report['errors'].append('producer did not complete cleanly')
        if task_count!=completed or result.get('unfinished')!=0:
            report['errors'].append('input tasks incomplete')
        acceptance=result.get('acceptance',{})
        if acceptance.get('all_input_completed') is not True or acceptance.get('no_recorded_safety_failure') is not True:
            report['errors'].append('producer strict completion/safety flags not true')
        after={'tasks_sha256':file_sha(dataset/'tasks.csv'),
            'initial_inventory_sha256':file_sha(dataset/'initial_inventory.csv'),
            'manifest_sha256':file_sha(dataset/'manifest.json'),'scene_sha256':file_sha(scene)}
        if after!=actual or file_sha(config)!=config_file_sha:
            raise ValueError('input/config changed during read-only collection')
        for filename in ('results.json','checkpoint.json','frames.sqlite','journal.sqlite'):
            if file_sha(output/filename)!=report['artifacts'][filename]:
                raise ValueError('source artifact changed during read-only collection: '+filename)
        if file_sha(exit_file)!=report['artifacts']['exit']:raise ValueError('exit observation changed during collection')
        report['input_and_source_unchanged_during_collection']=True
        report['completed_run']=not report['errors']
        private={'tasks':cp['tasks'],'goods':cp['goods'],'robots':fleet,'events':events,'frames':frames}
    except Exception as exc:
        report['errors'].append(type(exc).__name__+': '+str(exc))
    return report, private


def equivalent(a, b):
    if a is None or b is None:return {'passed':False,'reason':'one or both validated source traces unavailable'}
    checks={key:digest(a[key])==digest(b[key]) for key in ('tasks','goods','robots','events')}
    shared=sorted(set(a['frames'])&set(b['frames']))
    differences=[t for t in shared if a['frames'][t]!=b['frames'][t]]
    checks['sample_times_equal']=set(a['frames'])==set(b['frames'])
    checks['sampled_physical_frames_equal']=not differences and bool(shared)
    return {'passed':all(checks.values()),'checks':checks,'compared_sample_frames':len(shared),
        'different_sample_seconds':differences[:20], 'different_sample_count':len(differences),
        'comparison_semantics':'All final task/goods/robot fields and every journal event retained. Only run_id/event_id run prefixes normalized; worker PID wrappers excluded. Sample frames retain all robot fields plus physical counters, stock and timing; backend/scheduler/knowledge/source metadata and wall-clock performance counters are excluded.'}


def summarize(root, *, baseline='baseline-x3', current='current-x3'):
    root=Path(root).resolve();plan_path=root/'capacity-plan-v1.json';plan=load_json(plan_path)
    if not isinstance(plan,list) or not plan or any(not isinstance(c,dict) for c in plan):raise ValueError('capacity plan must be a nonempty list')
    names=[c['name'] for c in plan]
    if len(set(names))!=len(names) or any(not isinstance(n,str) or not n or '/' in n or '\\' in n for n in names):raise ValueError('duplicate/invalid case names')
    reports, traces = [], {}
    for case in plan:
        report, trace = summarize_case(root,case);reports.append(report);traces[case['name']]=trace
    identities=[(r.get('input_identity') or {}).get('tasks_sha256') for r in reports]
    stocks=[(r.get('input_identity') or {}).get('initial_inventory_sha256') for r in reports]
    shared_inputs=all(identities) and all(stocks) and len(set(identities))==len(set(stocks))==1
    declared_inputs=len({c['tasks_sha256'] for c in plan})==len({c['initial_sha256'] for c in plan})==1
    comparison=equivalent(traces.get(baseline),traces.get(current))
    complete=all(r['completed_run'] for r in reports)
    baseline_report=next((r for r in reports if r['name']==baseline),None)
    def pct(value, reference):
        return (value/reference-1)*100 if isinstance(value,(int,float)) and isinstance(reference,(int,float)) and reference else None
    if baseline_report and baseline_report.get('flow_seconds_completed_only'):
        for report in reports:
            if not report.get('flow_seconds_completed_only'):continue
            report['relative_to_baseline']={
                'baseline':baseline,'flow_mean_change_percent':pct(report['flow_seconds_completed_only']['mean'],baseline_report['flow_seconds_completed_only']['mean']),
                'flow_p99_change_percent':pct(report['flow_seconds_completed_only']['p99'],baseline_report['flow_seconds_completed_only']['p99']),
                'planning_wall_change_percent':pct(report['planning_seconds'],baseline_report['planning_seconds']),
                'late_completed_change':report['late_completed']-baseline_report['late_completed'],
                'sampled_peak_pending_change':report['sampled']['peak_pending_sampled']-baseline_report['sampled']['peak_pending_sampled']}
    return {'version':VERSION,'scope':'same-input short peak capacity regression; not a long-duration acceptance',
        'plan_sha256':file_sha(plan_path),'source_input_identical_across_runs':bool(shared_inputs and declared_inputs),
        'all_runs_completed_and_integrity_verified':complete,
        'baseline_current_physical_equivalence':comparison,'runs':reports,
        'passed_short_regression':bool(complete and shared_inputs and declared_inputs and comparison['passed']),
        'short_regression_pass_semantics':'clean completion, input/source integrity and baseline equivalence only; lateness/KPI degradation remain separate and do not approve a layout',
        'all_runs_zero_lateness':all(r.get('zero_lateness') is True for r in reports),
        'long_term_capacity_accepted':False,'candidate_selected':None,
        'remaining_validation':['full new-rack coverage under matched task rates','steady-day and multi-day backlog/charging recovery','type-specific high-shelf demand and backups','separate graph/query and independent Agent read/write acceptance']}


def main(argv=None):
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root',type=Path,required=True);parser.add_argument('--output',type=Path)
    parser.add_argument('--source-root',type=Path,default=ROOT,help='source tree supplying backend.checkpoint when tool is copied separately')
    parser.add_argument('--baseline',default='baseline-x3');parser.add_argument('--current',default='current-x3')
    args=parser.parse_args(argv)
    if not (args.source_root/'backend/checkpoint.py').is_file():parser.error('--source-root must select the checkpoint reader source tree')
    sys.path.insert(0,str(args.source_root.resolve()))
    report=summarize(args.root,baseline=args.baseline,current=args.current)
    text=json.dumps(report,ensure_ascii=False,indent=2,allow_nan=False)+'\n'
    if args.output:
        args.output.parent.mkdir(parents=True,exist_ok=True)
        with args.output.open('x',encoding='utf-8') as f:f.write(text)
    else:print(text,end='')
    return 0 if report['passed_short_regression'] else 2


if __name__=='__main__':raise SystemExit(main())
