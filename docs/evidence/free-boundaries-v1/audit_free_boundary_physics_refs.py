"""Supplement v1 equivalence by normalizing only boundary source-event refs."""
from copy import deepcopy
import importlib.util
import json
from pathlib import Path
import sys

R=Path('/data/workspace/wz/warehouse-free-ports-20261029');S=R/'source-ports-v1'
sys.path.insert(0,str(S))
spec=importlib.util.spec_from_file_location('capacity_reader',R/'tools/summarize_meeting_capacity_v1.py')
m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
from backend.checkpoint import read_checkpoint


plan=m.load_json(R/'free-boundary-plan-v1.json')
traces={};rewards={};counts={}
for j in plan:
    if not j['name'].startswith('free-'):continue
    report,trace=m.summarize_case(R,j);assert report['completed_run'],report
    cp=read_checkpoint(Path(j['output'])/'checkpoint.json');trace=deepcopy(trace)
    for task in trace['tasks'].values():
        for binding in task.get('boundary_bindings',{}).values():
            actual=binding['event_id'];prefix=cp['run_id']+':event:'
            assert actual==prefix+str(binding['event_seq'])
            binding['event_id']='[RUN]:event:'+str(binding['event_seq'])
    traces[j['name']]=trace
    result=m.load_json(Path(j['output'])/'results.json')
    rewards[j['name']]={k:result['metrics'].get(k,0) for k in ('port_relief_rows','v5_port_rewarded_rows','v5_rewarded_rows')}
comparison=m.equivalent(traces['free-base'],traces['free-online'])
assert comparison['passed'],comparison
assert rewards['free-online']['port_relief_rows']==0,rewards
report={'version':'warehouse-free-boundary-event-ref-equivalence-v1',
    'scope':'Exact additional normalization of binding source-event run prefixes; no physical field removed',
    'original_v1_audit_sha256':m.file_sha(R/'free-boundary-audit-v1.json'),
    'auditor_sha256':m.file_sha(Path(__file__)),
    'comparison':comparison,'relief_counters':rewards,
    'normalization':'boundary_bindings[*].event_id run prefix only, after verifying exact event_seq correspondence',
    'policy_coverage':'Logical waiting source is INBOUND, not a physical full port; online donor rows are zero. Do not attribute this scenario gain to port relief.'}
with (R/'free-boundary-physical-comparison-v1.json').open('x') as f:json.dump(report,f,indent=2)
print(json.dumps(report));print('SHA256',m.file_sha(R/'free-boundary-physical-comparison-v1.json'))
