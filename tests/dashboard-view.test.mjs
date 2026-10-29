import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {progressPresentation,platformStageProgress,platformTaskPresentation,samePhysicalFrame,shouldPickRobot} from '../dist/dashboard-view.mjs';
import {exportRows} from '../dist/live-model.mjs';

test('unknown progress clears CSS width rather than retaining an earlier battery percentage',()=>{
  assert.deepEqual(progressPresentation(null),{known:false,width:'0%',text:'阶段进度未提供'});
  for(const value of [undefined,NaN,-1,101,'80',true])assert.equal(progressPresentation(value).width,'0%');
  assert.equal(progressPresentation(37.5).width,'37.5%');
});
test('progress is owned by the current task and charging reports actual SOC',()=>{
  assert.equal(platformStageProgress({taskId:'A',work:{task_id:'B',phase_progress:99}}),null);
  assert.equal(platformStageProgress({taskId:'A',work:{task_id:'A',phase_progress:42}}),42);
  assert.equal(platformStageProgress({rawStage:'charging',battery:63}),63);
});
test('outbound retains actual endpoint, level and operation label',()=>{
  const robot={taskId:'A',label:'送往目标点',cargoId:null,carrying:false,targetLevel:3,targetSlotId:'R03-L3-C8-F2',
    work:{task_id:'A',task_type:'OUTBOUND',source_id:'R03-L3-C8-F2',target_id:'OUT-2',goods_id:'G',status:'assigned'}};
  const v=platformTaskPresentation(robot);
  assert.equal(v.lastStep,'出库');assert.equal(v.title,'出库：R03-L3-C8-F2 → OUT-2');
  assert.equal(v.level,'L3 · R03-L3-C8-F2');assert.equal(v.goods,'G');
  assert.match(platformTaskPresentation({...robot,work:{...robot.work,status:'completed'}}).title,/任务已完成/);
});
test('repeat heartbeat preserves animation only for the same physical run and positions',()=>{
  const a={runId:'R',runtimeInstanceId:'I',tick:7,robots:[{robot_id:1,x:2,y:3}]};
  assert.equal(samePhysicalFrame(a,{...a,status:'paused'}),true);
  assert.equal(samePhysicalFrame(a,{...a,runtimeInstanceId:'new'}),false);
  assert.equal(samePhysicalFrame(a,{...a,tick:8}),false);
  assert.equal(samePhysicalFrame(a,{...a,robots:[{robot_id:1,x:2,y:4}]}),false);
});
test('robot picking works in top/iso modes without treating camera drags or right clicks as selection',()=>{
  assert.equal(shouldPickRobot(false,0),true);
  assert.equal(shouldPickRobot(true,0),false);assert.equal(shouldPickRobot(false,2),false);
  assert.equal(shouldPickRobot(false,0,true),false);
});
test('CSV preserves old detail columns, current identities and safe quoting without invented stock',()=>{
  const csv=exportRows({runId:'R',tick:7,occupiedSlots:['R03-L3-C8-F2'],robots:[{robot_id:1,robot_type_key:'X3',
    task_id:'A',carrying:false,work:{task_id:'A',goods_id:'G,"quoted"',slot_id:'R03-L3-C8-F2',source_id:'R03-L3-C8-F2',target_id:'OUT-2'}}]});
  const head=csv.split('\r\n')[0].split(',');
  for(const key of ['robot_type','max_level','task','cargo','rack','bay','depth_row','level','slot_occupied','run_id','tick'])assert.ok(head.includes(key));
  assert.match(csv,/"G,""quoted"""/);assert.match(csv,/"X3"/);assert.match(csv,/"R03","8","1","3","true"/);
});
test('original app renders real future routes and keeps repeated-frame animation state',()=>{
  const source=readFileSync(new URL('../dist/app.js',import.meta.url),'utf8');
  const routes=source.slice(source.indexOf('  function drawRoutes()'),source.indexOf('  function roundedRect('));
  assert.match(routes,/platformRoutePoints/);assert.doesNotMatch(routes,/observedTrail/);
  assert.match(source,/platformRenderPosition\(robot,interpolation\)/);
  assert.match(source,/physicalRepeat&&previous/);assert.match(source,/if\(!physicalRepeat\)backendSnapshotAt/);
  assert.doesNotMatch(source,/style\.width = progress===null\?"阶段已确认"/);
  assert.match(source,/p\.textContent=`\$\{r\.label\} · \$\{platformTaskPresentation\(r\)\.goods\}`/);
});
