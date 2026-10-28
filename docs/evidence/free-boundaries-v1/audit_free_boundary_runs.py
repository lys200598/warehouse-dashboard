"""Read-only audit of the three existing, source-frozen boundary experiments."""
from collections import Counter, defaultdict
import csv
import hashlib
import importlib.util
import json
from pathlib import Path
import sqlite3
import subprocess
import sys
import time
import zlib

R=Path('/data/workspace/wz/warehouse-free-ports-20261029')
S=R/'source-ports-v1'
sys.path.insert(0,str(S))
spec=importlib.util.spec_from_file_location('capacity_reader',R/'tools/summarize_meeting_capacity_v1.py')
reader=importlib.util.module_from_spec(spec);spec.loader.exec_module(reader)
from backend.checkpoint import read_checkpoint
from backend.boundary_contract import validate_binding_history


def rows(directory):
    with (Path(directory)/'tasks.csv').open(encoding='utf-8',newline='') as f:
        return list(csv.DictReader(f))


def raw_events(output):
    events=[]
    with sqlite3.connect((Path(output)/'journal.sqlite').resolve().as_uri()+'?mode=ro',uri=True) as db:
        db.execute('PRAGMA query_only=ON')
        for seq,checksum,payload in db.execute('SELECT seq,checksum,batch FROM commits ORDER BY seq'):
            batch=json.loads(zlib.decompress(payload));assert reader.digest(batch)==checksum
            events.extend(batch['events'])
    return events


def source_identity(source, checkpoint):
    seconds=checkpoint['real_scale']['identity']['commit_seconds']
    metadata=checkpoint['run_metadata'];runtime=metadata['knowledge_runtime']
    code="""import json
from backend.real_scale.writer import real_identity
from backend.knowledge_runtime import VERSION
from backend.knowledge import digest
data={'version':VERSION,'mode':%r,'source':real_identity(%r)['execution_sha256']}
if %r:
 from backend.resource_graph import VERSION as RESOURCE
 data['resource_graph']=RESOURCE
print(digest(data))
""" % (runtime['mode'],seconds,'resource_graph' in runtime)
    return subprocess.check_output([sys.executable,'-c',code],cwd=source,text=True).strip()


def physical_boundary_audit(case):
    cp=read_checkpoint(Path(case['output'])/'checkpoint.json')
    events=raw_events(case['output']);tasks=cp['tasks']
    identity=source_identity(Path(case['source']),cp)
    result=reader.load_json(Path(case['output'])/'results.json')
    assert identity==result['code_identity']
    station_kinds={p['id']:p['kind'] for p in cp['run_metadata']['scene']['stations']}
    ports=Counter({p:0 for p,k in station_kinds.items() if k=='inbound'})
    peak=ports.copy();received=ports.copy();departed=ports.copy()
    outbound=Counter({p:0 for p,k in station_kinds.items() if k=='outbound'})
    binding_count=Counter();source_events={};target_events={}
    for event in events:
        task=tasks.get(event.get('task_id'));kind=event['kind']
        if kind=='boundary_port_bound':
            binding_count[event['endpoint']]+=1
            (source_events if event['endpoint']=='source' else target_events)[event['task_id']]=event
            assert 'goods_id' not in event and 'location_id' not in event
        elif kind=='received':
            assert task and task['task_type']=='INBOUND'
            port=event['location_id'];assert port==task['source_id'] and station_kinds[port]=='inbound'
            ports[port]+=1;received[port]+=1;peak[port]=max(peak[port],ports[port])
        elif kind=='picked' and task and task['task_type']=='INBOUND':
            ports[task['source_id']]-=1;departed[task['source_id']]+=1
        elif kind=='shipped':
            assert task and task['task_type']=='OUTBOUND' and event['location_id']=='SHIPPED'
            outbound[task['target_id']]+=1
        assert all(0<=v<=6 for v in ports.values())
    assert not any(ports.values()),ports
    assert sum(received.values())==result['metrics']['received']==sum(departed.values())
    assert sum(outbound.values())==result['metrics']['shipped']
    boundary=validate_binding_history(tasks,events,cp['run_metadata'],cutoff=cp['elapsed'])
    return {'source_identity_recomputed':identity,'source_identity_matches':True,
        'boundary_history':boundary,'all_observed_inbound_counters_within_0_6':True,
        'inbound_received_by_port':dict(received),'inbound_peak_by_port':dict(peak),
        'inbound_picked_by_port':dict(departed),'final_inbound_counts':dict(ports),
        'outbound_shipped_by_port':dict(outbound),'binding_counts':dict(binding_count),
        'empty_distance_m':result['metrics']['empty_distance_m'],
        'loaded_distance_m':result['metrics']['loaded_distance_m'],
        'planning_calls':result['metrics']['planning_calls'],
        'planning_failures':result['metrics']['planning_failures'],
        'planning_seconds':result['metrics']['planning_seconds'],
        'dispatch_wall_seconds':result['metrics']['dispatch_wall_seconds'],
        'actual_business_peak':result['metrics']['max_active_business'],
        'outbound_late':sum(t['late'] for t in tasks.values() if t['task_type']=='OUTBOUND'),
        'scope':'complete source/event audit; computation seconds are not API latency'}


plan=reader.load_json(R/'free-boundary-plan-v1.json')
started=time.monotonic()
while not all(Path(j['exit_file']).exists() for j in plan):
    if time.monotonic()-started>3600:raise TimeoutError('existing jobs did not produce observed exits; not rerun')
    time.sleep(120)
fixed=next(j for j in plan if j['name']=='fixed-control')
logical=next(j for j in plan if j['name']=='free-base')
a,b=rows(fixed['dataset']),rows(logical['dataset']);assert len(a)==len(b)==1728
changed=Counter()
for old,new in zip(a,b):
    endpoint='source_id' if old['task_type']=='INBOUND' else 'target_id'
    expected={**old,endpoint:old['task_type']}
    assert new==expected,(old,new)
    changed[endpoint]+=old[endpoint]!=new[endpoint]
assert (Path(fixed['dataset'])/'initial_inventory.csv').read_bytes()==(Path(logical['dataset'])/'initial_inventory.csv').read_bytes()
reports=[];traces={}
for case in plan:
    report,trace=reader.summarize_case(R,case)
    if report['completed_run']:
        report['physical_boundary']=physical_boundary_audit(case)
    reports.append(report);traces[case['name']]=trace
    print(json.dumps({k:report.get(k) for k in ('name','completed_run','errors','elapsed','completed','late_completed','flow_seconds_completed_only')},ensure_ascii=False),flush=True)

B=Path('/data/workspace/wz/warehouse-meeting-candidates-20261028')
basecase={'name':'original-balanced-mix70','source':str(B/'source-v4'),
    'scene':str(B/'source-v4/data/layouts/expansion-candidates/balanced/layout.json'),
    'dataset':str(B/'inputs/peak-balanced-full-coverage'),'config':str(B/'inputs/mix70.json'),
    'output':str(B/'runs/final-balanced-mix70'),'exit_file':str(B/'logs/final-balanced-mix70.exit'),
    'tasks_sha256':fixed['tasks_sha256'],'initial_sha256':fixed['initial_sha256']}
base_report,base_trace=reader.summarize_case(B,basecase)
assert base_report['completed_run'],base_report
comparison=reader.equivalent(base_trace,traces['fixed-control'])
frozen=reader.load_json(R/'ports-source-backend-sha-v1.json')
# Do not substitute the subsequently developed query source for this producer.
assert all(reader.file_sha(S/p)==h for p,h in frozen.items())
report={'version':'warehouse-free-boundary-audit-v1','scope':'same goods/racks/times/counts, fixed versus logical boundary scenario',
    'auditor_sha256':reader.file_sha(Path(__file__)),
    'summarizer_sha256':reader.file_sha(R/'tools/summarize_meeting_capacity_v1.py'),
    'plan_sha256':reader.file_sha(R/'free-boundary-plan-v1.json'),
    'producer_source_manifest_sha256':reader.file_sha(R/'ports-source-backend-sha-v1.json'),
    'producer_backend_bytes_verified':True,
    'conversion':{'only_allowed_boundary_fields_changed':True,'source_id_changes':changed['source_id'],
        'target_id_changes':changed['target_id'],'initial_inventory_bytes_identical':True,
        'task_order_id_goods_type_rack_release_due_preserved':True,
        'csv_bytes_identical_between_scenarios':False},
    'all_existing_runs_completed_and_integrity_verified':all(j['completed_run'] for j in reports),
    'fixed_control_baseline_equivalence':comparison,
    'free_base_vs_online_physical_equivalence':reader.equivalent(traces['free-base'],traces['free-online']),
    'all_runs_zero_lateness':all(j.get('zero_lateness') is True for j in reports),
    'free_runs_zero_lateness':all(j.get('zero_lateness') is True for j in reports if j['name'].startswith('free-')),
    'long_term_capacity_or_million_query_qualification':False,'runs':reports}
with (R/'free-boundary-audit-v1.json').open('x',encoding='utf-8') as f:json.dump(report,f,ensure_ascii=False,indent=2)
print('FINAL',json.dumps({k:report[k] for k in ('all_existing_runs_completed_and_integrity_verified','free_runs_zero_lateness','fixed_control_baseline_equivalence','free_base_vs_online_physical_equivalence')}),flush=True)
print('SHA256',reader.file_sha(R/'free-boundary-audit-v1.json'),flush=True)
raise SystemExit(0 if report['all_existing_runs_completed_and_integrity_verified'] and comparison['passed'] else 2)
