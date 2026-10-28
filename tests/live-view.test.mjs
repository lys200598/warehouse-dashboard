import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import * as view from '../dist/live-model.mjs';

const source=readFileSync(new URL('../dist/live.js',import.meta.url),'utf8');
const html=readFileSync(new URL('../dist/live.html',import.meta.url),'utf8');
const scene=JSON.parse(readFileSync(new URL('../data/layouts/warehouse100-wide-v2.json',import.meta.url),'utf8'));
const robot=(id=1,changes={})=>({robot_id:id,x:2.25,y:2.4,battery:53,carrying:false,stage:'idle',task_id:null,cargo_id:null,...changes});
const frame=(changes={})=>({runId:'actual-fixture',tick:123,elapsed:123,running:false,status:'paused',speed:1,
  robots:[robot()],knowledge:{durable_seq:5,mirrored_seq:4,lag:1,committed_sim_time:120},metadata:{commit_seconds:120,knowledge_mode:'required'},...changes});

test('90x76 physical floor projection remains invertible in both real camera modes',()=>{
  for(const mode of ['iso','top']){
    const camera={...view.newCamera(),view:mode,panX:20,panY:10,zoom:1.6};
    const m=view.projectionMetrics(scene,camera,1000,650);
    assert.equal(m.cx,45);assert.equal(m.cy,38);
    for(const p of [{x:0,y:0},{x:90,y:76},{x:2.25,y:2.4},{x:18.9,y:13.3}]){
      const round=view.unproject(view.project(p,m),m);assert(Math.abs(round.x-p.x)<1e-8);assert(Math.abs(round.y-p.y)<1e-8);
    }
  }
});
test('left rotates, right/middle/Shift pan as in original app controls',()=>{
  const c=view.newCamera(),before={...c};assert.equal(view.dragCamera(c,0,20,10),'orbit');
  assert.equal(c.yaw,before.yaw+.18);assert.equal(c.panX,0);
  for(const [button,shift] of [[2,false],[1,false],[0,true]]){
    const p=view.newCamera(),b={...p};assert.equal(view.dragCamera(p,button,20,10,shift),'pan');
    assert.equal(p.yaw,b.yaw);assert.equal(p.pitch,b.pitch);assert.equal(p.panX,20);assert.equal(p.panY,10);
  }
});
test('top-view left drag pans and unsupported button leaves camera intact',()=>{
  const c={...view.newCamera(),view:'top'};assert.equal(view.dragCamera(c,0,10,5),'pan');
  const before={...c};assert.equal(view.dragCamera(c,4,5,6),null);assert.deepEqual(c,before);
});
test('wheel zoom anchors world point and never becomes a pan-only control',()=>{
  const c=view.newCamera(),p={x:380,y:270};const world=view.unproject(p,view.projectionMetrics(scene,c,1000,650));
  view.zoomCamera(c,-1,p,scene,1000,650);assert(c.zoom>1);
  const actual=view.project(world,view.projectionMetrics(scene,c,1000,650));assert(Math.abs(actual.x-p.x)<1e-8);assert(Math.abs(actual.y-p.y)<1e-8);
});
test('live frame uses source robot IDs and accepts controller acknowledgement wrapper',()=>{
  const f=frame();assert.equal(view.normalizeFrame(f),f);assert.equal(view.normalizeFrame({snapshot:f,control:{ok:true}}),f);
  assert.equal(view.normalizeFrame(f).robots[0].task_id,null);
});
test('invalid, duplicate and untyped robot records fail instead of generating roster',()=>{
  for(const f of [{},frame({runId:''}),frame({robots:[robot(),robot()]}),frame({robots:[robot(true)]}),
    frame({robots:[robot(1,{x:NaN})]}),frame({robots:[robot(1,{carrying:1})]})])assert.throws(()=>view.normalizeFrame(f));
});
test('old in-flight HTTP frame cannot rewind same run or overwrite newer same-tick control state',()=>{
  const f=frame({capturedAtMs:200});assert.equal(view.acceptsFrame(f,frame({tick:122})),false);
  assert.equal(view.acceptsFrame(f,frame({capturedAtMs:199})),false);
  assert.equal(view.acceptsFrame(f,frame({tick:130,capturedAtMs:201})),true);
  assert.equal(view.acceptsFrame(f,frame({runId:'new-actual-run',tick:0,elapsed:0})),true);
  assert.equal(view.acceptsFrame(frame({liveRevision:3}),frame({liveRevision:2,capturedAtMs:999})),false);
});
test('same service global revision fences delayed old-run WS after new-run HTTP acknowledgement',()=>{
  const old=frame({runtimeInstanceId:'instance-A',liveRevision:40,capturedAtMs:1000});
  const next=frame({runtimeInstanceId:'instance-A',liveRevision:41,runId:'new-run',tick:0,elapsed:0,capturedAtMs:1001});
  assert.equal(view.acceptsFrame(old,next),true);
  assert.equal(view.acceptsFrame(next,{...old,capturedAtMs:2000}),false);
  assert.equal(view.acceptsFrame(next,{...old,liveRevision:41,capturedAtMs:2000}),false);
});
test('restart instance can restore same run to durable lower tick while retired instances stay fenced',()=>{
  const before=frame({runtimeInstanceId:'instance-A',liveRevision:400,capturedAtMs:1000});
  const restored=frame({runtimeInstanceId:'instance-B',liveRevision:1,tick:120,elapsed:120,capturedAtMs:999});
  assert.equal(view.acceptsFrame(before,restored),true);
  const retired=new Set(['instance-A']);
  assert.equal(view.acceptsFrame(restored,{...before,liveRevision:999,capturedAtMs:5000},retired),false);
  assert.equal(view.acceptsFrame(restored,{...restored,liveRevision:2,capturedAtMs:1001},retired),true);
  assert.match(source,/retiredInstances\.add\(previousInstance\)/);
  assert.match(source,/if\(state\.socket!==ws\)return/);
});
test('legacy cross-run capture clocks reject stale source and control POST binds current run',()=>{
  const current=frame({runId:'new-run',tick:0,elapsed:0,capturedAtMs:2000});
  assert.equal(view.acceptsFrame(current,frame({capturedAtMs:1000})),false);
  assert.equal(view.acceptsFrame(current,frame({runId:'newer-run',tick:0,capturedAtMs:2001})),true);
  const payload=view.controlPayload('new_run',current,{dataset_id:'lys-day',expected_run_id:'injected'});
  assert.deepEqual(payload,{action:'new_run',dataset_id:'lys-day',expected_run_id:'new-run'});
  assert.throws(()=>view.controlPayload('pause',null),/真实run身份/);
  assert.match(source,/controlPayload\(action,state\.frame,extra\)/);
});
test('dataset directory accepts server id and runtime dataset_id aliases without inventing IDs',()=>{
  const result=view.normalizeDatasets({datasets:[{dataset_id:'sample',label:'小样例'},{id:'week',label:'七日'}]});
  assert.deepEqual(result.map(d=>d.id),['sample','week']);
  assert.throws(()=>view.normalizeDatasets({datasets:[{label:'缺少编号'}]}));
  assert.throws(()=>view.normalizeDatasets([{id:'duplicate'},{dataset_id:'duplicate'}]));
});
test('knowledge time and physical time remain separate at 120-second cadence',()=>{
  const k=view.knowledgeFacts(frame());assert.equal(k.physicalTime,123);assert.equal(k.committedTime,120);
  assert.equal(k.cadence,120);assert.equal(k.durable,5);assert.equal(k.mirrored,4);assert.equal(k.lag,1);
});
test('knowledge query binds mirrored cutoff not current physical tick or durable-only head',()=>{
  const q=view.buildQuery('self',frame(),{robotId:1});assert.equal(q.run_id,'actual-fixture');assert.equal(q.seq,4);
  assert.throws(()=>view.buildQuery('self',frame(),{seq:5}),/已经镜像/);
  assert.throws(()=>view.buildQuery('self',frame({knowledge:{mirrored_seq:0}})),/已经镜像/);
});
test('business exact default and full ANN are explicit distinct query variants',()=>{
  const q=view.buildQuery('similar_history',frame());assert.equal(q.variant,'business-exact');
  assert.throws(()=>view.buildQuery('similar_history',frame(),{variant:'full-ann'}),/任务ID与货物ID/);
  assert.equal(view.buildQuery('similar_history',frame(),{variant:'full-ann',taskId:'T1',goodsId:'G1'}).variant,'full-ann');
});
test('task/goods fields retain actual IDs and missing IDs are not fabricated',()=>{
  assert.throws(()=>view.buildQuery('task',frame()),/需要任务ID/);
  assert.throws(()=>view.buildQuery('goods',frame()),/需要货物ID或任务ID/);
  const q=view.buildQuery('task',frame(),{taskId:'lys-task-001'});assert.equal(q.task_id,'lys-task-001');assert.equal('goods_id' in q,false);
});
test('occupied box geometry uses actual eight-column two-face slots and current level filter',()=>{
  const f=frame({occupiedSlots:['R01-L1-C1-F1','R01-L3-C8-F2','R36-L2-C1-F1']});
  const boxes=view.slotBoxes(scene,f);assert.equal(boxes.length,3);assert.equal(boxes[1].column,8);assert.equal(boxes[1].face,1);
  assert(Math.abs(boxes[1].x-(scene.racks[0].x+7.5*1.2-.18))<1e-8);
  assert.equal(view.slotBoxes(scene,f,'3').length,1);
});
test('box display never invents inventory for missing or foreign slot IDs',()=>{
  assert.deepEqual(view.slotBoxes(scene,frame()),[]);
  assert.deepEqual(view.slotBoxes(scene,frame({occupiedSlots:['unknown','R99-L1-C1-F1','R01-L4-C1-F1','R01-L1-C9-F1']})),[]);
});
test('actual traffic waiting, charging and low SOC attention remain different categories',()=>{
  assert.equal(view.robotCategory(robot(1,{stage:'traffic_wait'})),'waiting');
  assert.equal(view.robotCategory(robot(1,{stage:'charging',battery:15})),'charging');
  assert.equal(view.attentionReason(robot(1,{stage:'charging',battery:15})),null);
  assert.equal(view.attentionReason(robot(1,{stage:'fault_wait'})),'故障 / 阻塞');
  assert.equal(view.attentionReason(robot(1,{stage:'to_target',battery:24})),'低SOC / 协作观察');
});
test('filtering and search operate on returned roster only',()=>{
  const f=frame({robots:[robot(1),robot(3,{stage:'to_target'}),robot(5,{stage:'fault_wait'})]});
  assert.deepEqual(view.filteredRobots(f,'moving').map(r=>r.robot_id),[3]);
  assert.deepEqual(view.filteredRobots(f,'attention').map(r=>r.robot_id),[5]);
  assert.deepEqual(view.filteredRobots(f,'all','003').map(r=>r.robot_id),[3]);
});
test('start/pause/step lifecycle is real and busy state blocks repeated commands',()=>{
  assert.equal(view.controlAvailability(null).start,false);
  assert.equal(view.controlAvailability(frame()).step,true);
  assert.equal(view.controlAvailability(frame({status:'running',running:true})).pause,true);
  assert.equal(view.controlAvailability(frame({status:'running',running:true})).step,false);
  assert.equal(view.controlAvailability(frame({status:'complete'})).start,false);
  assert.equal(view.controlAvailability(frame(),true).new_run,false);
});
test('export contains only exact returned state and safely quotes IDs',()=>{
  const csv=view.exportRows(frame({robots:[robot(1,{task_id:'lys,"A"'})]}));
  assert.match(csv,/"lys,""A"""/);assert.doesNotMatch(csv,/TK-|BX-/);
});
test('multiday clock continues beyond a seven-day calendar boundary',()=>{
  assert.equal(view.formatClock(7*86400+3661),'第8天 01:01:01');
});
test('old live bookmark redirects to the original dashboard implementation',()=>{
  assert.match(html,/index\.html/);assert.match(html,/location\.replace/);
  assert.doesNotMatch(html,/live\.js|liveCanvas/);
});

test('frontend transports real snapshots; no browser simulation, generated heatmap or chord interpolation',()=>{
  assert.match(source,/new WebSocket/);assert.match(source,/\/api\/snapshot/);assert.match(source,/\/api\/control/);
  assert.match(source,/\/api\/knowledge\/query/);assert.match(source,/pendingDatasetId/);
  assert.doesNotMatch(source,/Math\.random|initializeSimulation|updateRobot|renderFromX|lerp\(|createRobot/);
  assert.match(source,/setInterval\([\s\S]+,1000\)/);
});
