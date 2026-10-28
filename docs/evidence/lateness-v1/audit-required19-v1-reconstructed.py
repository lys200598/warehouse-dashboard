"""Read-only run-scoped real resource/history/control audit, no schema/write/replay."""
from pathlib import Path
from copy import deepcopy
import hashlib,json,os,sqlite3,sys,time,zlib

R=Path('/data/workspace/wz/warehouse-lateness-20261029');S=R/'source-online-slack-v1'
OUT=R/'runs/online-slack-required19';sys.path.insert(0,str(S))
from backend.checkpoint import read_checkpoint
from backend.knowledge import digest
from backend.knowledge_graph import Neo4jMirror
from backend.resource_graph import RackInventoryService,build_package,VERSION as RESOURCE_VERSION
from backend.real_scale.writer import real_identity
from backend.knowledge_runtime import VERSION
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
files=['results.json','checkpoint.json','journal.sqlite'];before={n:sha(OUT/n) for n in files}
result=json.loads((OUT/'results.json').read_text());cp=read_checkpoint(OUT/'checkpoint.json');run=cp['run_id'];seq=cp['knowledge_seq']
assert result['status']=='complete' and result['unfinished']==0 and result['metrics']['completed']==19
assert (R/'logs/online-slack-required19.exit').read_text().strip()=='0'
assert all(t['status']=='completed' for t in cp['tasks'].values()) and len(cp['tasks'])==19
assert result['acceptance']['all_input_completed'] and result['acceptance']['no_recorded_safety_failure']
manifest=json.loads((R/'online-slack-backend-sha-v1.json').read_text());assert all(sha(S/k)==v for k,v in manifest.items())
backend=S/'backend';actual=hashlib.sha256(json.dumps({p.relative_to(backend).as_posix():sha(p) for p in sorted(backend.rglob('*.py'))},sort_keys=True).encode()).hexdigest();assert actual==cp['run_metadata']['code_sha256']
identity={'version':VERSION,'mode':'required','source':real_identity(30)['execution_sha256'],'resource_graph':RESOURCE_VERSION}
assert identity==result['scheduler']['knowledge_runtime']['identity'] and digest(identity)==result['code_identity']
input_dir=R/'inputs/online-slack-smoke19-balanced'
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
dist=result['disturbances'];assert len(dist)==1 and dist[0]['success'] and dist[0]['kind']=='soc'
owner=dist[0]['owner'];at=dist[0]['applied_at'];measured=next(r for b in batches if b['sim_time']==at for r in b['robots'] if r['robot_id']==owner and r['carrying'])
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
finally:mirror.close()
assert before=={n:sha(OUT/n) for n in files}
report={'passed':True,'scope':'read-only run-scoped actual required control/resource audit; no schema, replay, injection, global node count or performance qualification',
 'source':str(S),'run_id':run,'exit':0,'physical_completed':19,'late':result['metrics']['late'],'elapsed':result['elapsed'],
 'actual_inputs_sha256':actual_inputs,'source_backend_manifest_sha256':sha(R/'online-slack-backend-sha-v1.json'),'source_backend_code_sha256':actual,
 'actual_code_identity':digest(identity),'source_artifacts_sha256':before,'knowledge':result['knowledge'],'knowledge_control':stats,'disturbance':dist,
 'actual_carrying_robot_at_soc_boundary':measured,'handoffs':handoffs,'source_knowledge_decisions':decisions,'source_knowledge_actions_applied':applied,'graph_knowledge_actions_applied':graph_actions,
 'journal_batches':seq,'source_actual_agent_states':sum(len(b['robots']) for b in batches),'graph_actual_agent_states':sum(x['n'] for x in states),
 'every_seq_has_100_unique_source_robots':True,'every_graph_seq_has_100_unique_robots':True,'relationships':count,'sampled_cutoffs':points,
 'checked_slot_point_values':checked,'per_point_occupied':historical,'inventory_query_observations_ms':{'count':len(timings),'mean':sum(timings)/len(timings),'max':max(timings)},
 'million_or_concurrent_query_acceptance':False}
path=R/'online-slack-required19-resource-audit-v1.json'
with path.open('x') as f:json.dump(report,f,indent=2)
print(json.dumps({k:report[k] for k in ['passed','run_id','physical_completed','late','elapsed','knowledge_control','journal_batches','source_actual_agent_states','graph_actual_agent_states','relationships','checked_slot_point_values']}))
print('report_sha256',sha(path))
