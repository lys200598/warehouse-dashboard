"""Read-only run-scoped real resource/history/control audit, no schema/write/replay."""
from pathlib import Path
from copy import deepcopy
import hashlib,json,os,sqlite3,sys,time,zlib,csv

R=Path('/data/workspace/wz/warehouse-free-ports-20261029');S=R/'source-combined-v3'
version=sys.argv[1] if len(sys.argv)>1 else 'v4';E=R/f'live-joint-evidence-{version}'
ready=json.loads((E/'ready.json').read_text());OUT=Path(ready['output_root'])/ready['run_id'];sys.path.insert(0,str(S))
from backend.checkpoint import read_checkpoint
from backend.knowledge import digest
from backend.knowledge_graph import Neo4jMirror
from backend.resource_graph import RackInventoryService,build_package,VERSION as RESOURCE_VERSION
from backend.real_scale.writer import real_identity
from backend.knowledge_runtime import VERSION
from backend.boundary_contract import validate_task, validate_binding_history, plan_endpoints
from backend.resource_graph.model import materialize_task_records
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
files=['results.json','checkpoint.json','journal.sqlite'];before={n:sha(OUT/n) for n in files}
result=json.loads((OUT/'results.json').read_text());cp=read_checkpoint(OUT/'checkpoint.json');run=cp['run_id'];seq=cp['knowledge_seq']
assert result['status']=='complete' and result['unfinished']==0 and result['metrics']['completed']==19
assert (R/f'logs/live-joint-client-{version}.exit').read_text().strip()=='0'
assert all(t['status']=='completed' for t in cp['tasks'].values()) and len(cp['tasks'])==19
assert result['acceptance']['all_input_completed'] and result['acceptance']['no_recorded_safety_failure']
manifest=json.loads((R/'combined-backend-sha-v1.json').read_text());assert all(sha(S/k)==v for k,v in manifest.items())
backend=S/'backend';backend_actual=hashlib.sha256(json.dumps({p.relative_to(backend).as_posix():sha(p) for p in sorted(backend.rglob('*.py'))},sort_keys=True).encode()).hexdigest();assert backend_actual==cp['run_metadata']['code_sha256']
identity={'version':VERSION,'mode':'required','source':real_identity(30)['execution_sha256'],'resource_graph':RESOURCE_VERSION}
assert identity==result['scheduler']['knowledge_runtime']['identity'] and digest(identity)==result['code_identity']
live_manifest=json.loads((R/'combined-livepoint-sha-v1.json').read_text())
worker_manifest=json.loads((R/'combined-workers-sha-v1.json').read_text())
live_manifest['files'].update(worker_manifest['files'])
assert all(sha(S/p)==v for p,v in live_manifest['files'].items())
assert result['live_runtime']['adapter_sha256']==sha(S/'ops/warehouse_live_runtime.py')
input_dir=R/'inputs/logical19'
actual_inputs={'scene_sha256':sha(S/'data/layouts/expansion-candidates/balanced/layout.json'),
 'tasks_sha256':sha(input_dir/'tasks.csv'),'initial_inventory_sha256':sha(input_dir/'initial_inventory.csv'),'manifest_sha256':sha(input_dir/'manifest.json')}
assert all(v==result['input_identity'][k]==cp['run_metadata'][k] for k,v in actual_inputs.items())
batches=[];checksums=[]
with sqlite3.connect((OUT/'journal.sqlite').as_uri()+'?mode=ro',uri=True) as db:
 db.execute('PRAGMA query_only=ON')
 for no,checksum,raw in db.execute('SELECT seq,checksum,batch FROM commits WHERE run_id=? ORDER BY seq',(run,)):
  b=json.loads(zlib.decompress(raw));assert b['seq']==no==len(batches)+1 and b['run_id']==run and digest(b)==checksum
  assert len(b['robots'])==100 and {r['robot_id'] for r in b['robots']}==set(range(1,101))
  batches.append(b);checksums.append(checksum)
assert len(batches)==seq
events=[e for b in batches for e in b['events']];assert [e['event_seq'] for e in events]==list(range(1,cp['event_sequence']+1))

with (input_dir/'tasks.csv').open(encoding='utf-8',newline='') as stream:raw_tasks={t['task_id']:t for t in csv.DictReader(stream)}
assert len(raw_tasks)==19
binding_audit=validate_binding_history(cp['tasks'],events,cp['run_metadata'],cutoff=cp['elapsed'])
versions={};binding_records={}
for b in batches:
 for t in b['tasks']:
  plan_source,plan_target=validate_task(t,b['metadata'],cutoff=b['sim_time'])
  plan={**t,'source_id':plan_source,'target_id':plan_target}
  assert all(plan[k]==v for k,v in raw_tasks[t['task_id']].items())
  for side,record in t.get('boundary_bindings',{}).items():
   key=(t['task_id'],side)
   if key in binding_records:assert binding_records[key]==record
   binding_records[key]=deepcopy(record)
  versions[(t['task_id'],b['seq'])]=deepcopy(t)
assert binding_audit['bindings']==19 and len(binding_records)==19

dist=result['disturbances'];assert len(dist)==1 and dist[0]['success'] and dist[0]['kind']=='soc'
owner=dist[0]['owner'];at=dist[0]['applied_at']
soc_boundaries=[{'seq':b['seq'],'state':r} for b in batches if b['sim_time']==at for r in b['robots'] if r['robot_id']==owner and r['carrying']]
after_boundary=next(x for x in soc_boundaries if x['state']['battery']==24)
before_boundary=next(x for x in soc_boundaries if x['seq']<after_boundary['seq'])
measured=after_boundary['state']
assert before_boundary['state']['cargo_id']==measured['cargo_id'] and before_boundary['state']['task_id']==measured['task_id']
assert measured['cargo_id'] and measured['task_id'] and measured['battery']==24
handoffs=[e for e in events if e['kind']=='cargo_handoff'];assert len(handoffs)==1 and handoffs[0]['donor_robot_id']==owner and handoffs[0]['goods_id']==measured['cargo_id']
applied=[e for e in events if e['kind']=='knowledge_action_applied'];assert {e['action'] for e in applied}=={'handoff_plan_reserved','cargo_transferred'}
decisions=[e for e in events if e['kind']=='knowledge_decision'];assert len(decisions)==2
for e in decisions:
 assert e['knowledge_batch_checksum']==checksums[e['knowledge_seq']-1]
 assert e['knowledge_seq']<=seq and all(i<e['event_seq'] for i in e['history_event_seqs'])
stats=result['scheduler']['knowledge_runtime']['stats'];assert stats['approved_plans']==stats['approved_transfers']==1 and stats['rejected']==0
assert result['knowledge']['durable_seq']==result['knowledge']['mirrored_seq']==seq and result['knowledge']['lag']==0
# Parse private credentials without printing them; instantiate the read interface
# directly, deliberately bypassing from_env(), which would CREATE schema.
for line in Path('/data/workspace/wz/warehouse-meeting-candidates-20261028/graph-test/neo4j.env').read_text().splitlines():
 if line and not line.startswith('#'):
  k,v=line.split('=',1);os.environ[k]=v.strip().strip('"').strip("'")
assert os.environ['NEO4J_URI']=='bolt://127.0.0.1:17691'
from neo4j import GraphDatabase
driver=GraphDatabase.driver(os.environ['NEO4J_URI'],auth=(os.environ.get('NEO4J_USER','neo4j'),os.environ['NEO4J_PASSWORD']),max_connection_pool_size=2)
mirror=Neo4jMirror(driver,os.environ.get('NEO4J_DATABASE','neo4j'));service=RackInventoryService(mirror)
try:
 count=mirror.query('''MATCH (run:WHRun {run_id:$run})-[:USES_LAYOUT]->(w:Warehouse)
 RETURN run.committed_seq AS head,run.resource_graph_seq AS resource_head,
 COUNT { (w)-[:HAS_RACK]->(:Rack) } AS racks,
 COUNT { (w)-[:HAS_RACK]->(:Rack)-[:HAS_SLOT]->(:Location) } AS slots''',{'run':run})[0]
 assert count=={'head':seq,'resource_head':seq,'racks':40,'slots':1920},count
 states=mirror.query('''MATCH (s:WHAgentState {run_id:$run}) RETURN s.seq AS seq,count(s) AS n,collect(DISTINCT s.robot_id) AS robots ORDER BY seq''',{'run':run})
 assert len(states)==seq and [x['seq'] for x in states]==list(range(1,seq+1))
 assert all(x['n']==100 and set(x['robots'])==set(range(1,101)) for x in states)
 graph_actions=mirror.query('''MATCH (e:WHEvent {run_id:$run}) WHERE e.kind='knowledge_action_applied' RETURN e.event_seq AS event_seq,e.kind AS kind ORDER BY event_seq''',{'run':run})
 assert [x['event_seq'] for x in graph_actions]==[x['event_seq'] for x in applied]
 racks=[x['rack_id'] for x in build_package(batches[0])['racks']];assert len(racks)==40
 points=sorted({1,max(2,seq//2),seq});slots={};checked=0;historical={};timings=[]
 for b in batches:
  slots.update({s['slot_id']:deepcopy(s) for s in b['slots']})
  if b['seq'] not in points:continue
  historical[str(b['seq'])]={}
  for rack in racks:
   start=time.perf_counter();value=service.inventory(run,b['seq'],rack);timings.append((time.perf_counter()-start)*1000)
   expected={k:v for k,v in slots.items() if v['rack_id']==rack}
   assert value['batch_checksum']==checksums[b['seq']-1] and {v['location_id'] for v in value['slots']}==set(expected)
   for row in value['slots']:
    source=expected[row['location_id']];assert (row['occupied'],row['goods_id'])==(source['occupied'],source['cargo_id'])
   checked+=len(expected);historical[str(b['seq'])][rack]=value['occupied']
 assert checked==1920*len(points)

 groups=mirror.query("""MATCH (r:WHRun {run_id:$run})-[:USES_LAYOUT]->(:Warehouse)-[:HAS_LOCATION]->(g:Location:BoundaryGroup)
 RETURN g.location_id AS id,g.kind AS kind,g.physical AS physical,g.x AS x,g.y AS y ORDER BY id""",{'run':run})
 assert [g['id'] for g in groups]==['INBOUND','OUTBOUND'] and all(g['physical'] is False and g['x'] is None and g['y'] is None for g in groups)
 static_tasks=mirror.query("""MATCH (r:WHRun {run_id:$run})-[:USES_DATASET]->(:Dataset)-[:INCLUDES_TASK]->(t:WarehouseTask)
 OPTIONAL MATCH (t)-[:FROM]->(a:Location) OPTIONAL MATCH (t)-[:TO]->(b:Location)
 RETURN t.task_id AS task_id,t.source_id AS source,t.target_id AS target,collect(DISTINCT a.location_id) AS sources,collect(DISTINCT b.location_id) AS targets ORDER BY task_id""",{'run':run})
 assert len(static_tasks)==19
 for row in static_tasks:
  raw=raw_tasks[row['task_id']];assert (row['source'],row['target'],row['sources'],row['targets'])==(raw['source_id'],raw['target_id'],[raw['source_id']],[raw['target_id']])
 run_task_plans=mirror.query("""MATCH (t:WHTask {run_id:$run}) OPTIONAL MATCH (t)-[:FROM_LOCATION]->(a:WHLocation) OPTIONAL MATCH (t)-[:TO_LOCATION]->(b:WHLocation)
 RETURN t.task_id AS task_id,collect(DISTINCT a.location_id) AS sources,collect(DISTINCT b.location_id) AS targets ORDER BY task_id""",{'run':run})
 assert len(run_task_plans)==19
 for row in run_task_plans:
  raw=raw_tasks[row['task_id']];assert (row['sources'],row['targets'])==([raw['source_id']],[raw['target_id']])
 execution=mirror.query("""MATCH (t:WHTaskState {run_id:$run}) OPTIONAL MATCH (t)-[:EXECUTION_SOURCE]->(a:WHLocation) OPTIONAL MATCH (t)-[:EXECUTION_TARGET]->(b:WHLocation)
 RETURN t.task_id AS task_id,t.seq AS seq,t.payload AS payload,collect(DISTINCT a.location_id) AS sources,collect(DISTINCT b.location_id) AS targets ORDER BY seq,task_id""",{'run':run})
 assert len(execution)==len(versions)
 seen_unbound=0
 for row in execution:
  task=versions[(row['task_id'],row['seq'])];assert json.loads(row['payload'])==task
  for side,field in [('source','sources'),('target','targets')]:
   actual=task[side+'_id'];expected=[] if actual in ('INBOUND','OUTBOUND') else [actual]
   assert row[field]==expected,(row['task_id'],row['seq'],side,row[field],expected)
   seen_unbound+=int(not expected)
 graph_bindings=mirror.query("""MATCH (e:WHEvent {run_id:$run,kind:'boundary_port_bound'}) OPTIONAL MATCH (e)-[:BINDS_BOUNDARY]->(l:WHLocation)
 RETURN e.event_seq AS event_seq,e.task_id AS task_id,e.endpoint AS endpoint,e.actual_location_id AS actual,collect(DISTINCT l.location_id) AS locations ORDER BY event_seq""",{'run':run})
 source_bindings=[e for e in events if e['kind']=='boundary_port_bound'];assert len(graph_bindings)==len(source_bindings)==19
 for actual,source in zip(graph_bindings,source_bindings):
  assert actual=={'event_seq':source['event_seq'],'task_id':source['task_id'],'endpoint':source['endpoint'],'actual':source['actual_location_id'],'locations':[source['actual_location_id']]}
 source_custody=[{'event_seq':e['event_seq'],'goods_id':e['goods_id'],'location_id':e['location_id']} for e in events if 'goods_id' in e and 'location_id' in e]
 graph_custody=mirror.query("""MATCH (g:WHGoodsState {run_id:$run}) RETURN g.event_seq AS event_seq,g.goods_id AS goods_id,g.location_id AS location_id ORDER BY event_seq""",{'run':run})
 assert graph_custody==source_custody and all(x['location_id'] not in ('INBOUND','OUTBOUND') for x in graph_custody)
 boundary_graph={'static_tasks_checked':len(static_tasks),'run_plan_edges_checked':len(run_task_plans),'task_versions_and_actual_edges_checked':len(execution),
  'unbound_endpoint_versions_observed':seen_unbound,'binding_events_checked':len(graph_bindings),'source_custody_events_checked':len(source_custody),'graph_custody_states_checked':len(graph_custody),'logical_custody_states':0,
  'logical_groups':groups,'all_static_plan_edges_match_original_csv':True,'all_actual_version_edges_match_source':True,'all_source_bindings_have_graph_event_relations':True,'binding_audit':binding_audit}

finally:mirror.close()
assert before=={n:sha(OUT/n) for n in files}
report={'passed':True,'audit_tool_version':4,'scope':'read-only run-scoped actual required control/resource audit; no schema, replay, injection, global node count or performance qualification',
 'source':str(S),'run_id':run,'exit':0,'physical_completed':19,'late':result['metrics']['late'],'elapsed':result['elapsed'],
 'actual_inputs_sha256':actual_inputs,'source_backend_manifest_sha256':sha(R/'combined-backend-sha-v1.json'),'source_backend_code_sha256':backend_actual,
 'actual_code_identity':digest(identity),'source_artifacts_sha256':before,'knowledge':result['knowledge'],'knowledge_control':stats,'disturbance':dist,
 'same_tick_soc_boundaries':soc_boundaries,'actual_carrying_robot_at_soc_boundary':measured,'handoffs':handoffs,'source_knowledge_decisions':decisions,'source_knowledge_actions_applied':applied,'graph_knowledge_actions_applied':graph_actions,
 'journal_batches':seq,'source_actual_agent_states':sum(len(b['robots']) for b in batches),'graph_actual_agent_states':sum(x['n'] for x in states),
 'every_seq_has_100_unique_source_robots':True,'every_graph_seq_has_100_unique_robots':True,'relationships':count,'sampled_cutoffs':points,
 'checked_slot_point_values':checked,'per_point_occupied':historical,'inventory_query_observations_ms':{'count':len(timings),'mean':sum(timings)/len(timings),'max':max(timings)},
 'million_or_concurrent_query_acceptance':False}
report['boundary_graph']=boundary_graph
report['livepoint_manifest_sha256']=sha(R/'combined-livepoint-sha-v1.json')
report['livepoint_files_sha256']=live_manifest['files']
report['workers_manifest_sha256']=sha(R/'combined-workers-sha-v1.json')
report['audit_tool_sha256']=sha(Path(__file__))
sha_values=[report['source_backend_code_sha256'],report['actual_code_identity'],report['source_backend_manifest_sha256'],report['audit_tool_sha256'],*actual_inputs.values(),*before.values(),report['livepoint_manifest_sha256'],report['workers_manifest_sha256'],*live_manifest['files'].values()]
assert all(isinstance(value,str) and len(value)==64 and all(c in '0123456789abcdef' for c in value) for value in sha_values),'malformed SHA report field'
path=E/'resource-kg-audit-v4.json'
with path.open('x') as f:json.dump(report,f,indent=2)
print(json.dumps({k:report[k] for k in ['passed','run_id','physical_completed','late','elapsed','knowledge_control','journal_batches','source_actual_agent_states','graph_actual_agent_states','relationships','checked_slot_point_values']}))
print('report_sha256',sha(path))
