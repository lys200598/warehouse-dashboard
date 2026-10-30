import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {progressPresentation,platformStageProgress,platformTaskPresentation,samePhysicalFrame,shouldPickRobot,nextFrameInterval,FRAME_INTERVAL_MAX_MS,robotGraphPresentation,legacyAQueriesEnabled} from '../dist/dashboard-view.mjs';
import {exportRows} from '../dist/live-model.mjs';

test('explicitly disabled legacy A queries keep C status separate and old sources compatible',()=>{
  assert.equal(legacyAQueriesEnabled({metadata:{legacy_a_graph:false}}),false);
  assert.equal(legacyAQueriesEnabled({metadata:{legacy_a_graph:true}}),true);
  assert.equal(legacyAQueriesEnabled({}),true);
  const panel=readFileSync(new URL('../dist/original-knowledge.mjs',import.meta.url),'utf8');
  assert.match(panel,/legacyKnowledgeControls'\)\.hidden=!legacyEnabled/);
  assert.match(panel,/旧版 A 查询已停用；C 图谱状态见运行状态栏/);
});

test('runtime modes and graph watermarks use observed complete steps',()=>{
  const view=robotGraphPresentation({tick:105,robotProcesses:'ray',kgStateSource:'neo4j',
    robotGraph:{latestCompleteStep:102,currentTick:105,lag:0,errors:2,lastError:'write failed'}});
  assert.equal(view.modeText,'机器人进程 ray · 派单状态 neo4j');
  assert.equal(view.graphText,'完整 Step 102 · 落后 3 步 · 写入错误 2');
  assert.equal(view.attention,true);assert.equal(view.error,'write failed');
});
test('unknown graph watermarks are not presented as zero lag or successful writes',()=>{
  const unknown=robotGraphPresentation({tick:100,metadata:{robot_processes:'local',kg_state_source:'memory'}});
  assert.equal(unknown.modeText,'机器人进程 local · 派单状态 memory');
  assert.equal(unknown.graphText,'完整 Step — · 滞后 — · 写入错误 —');
  const synced=robotGraphPresentation({tick:0,robotGraph:{latestCompleteStep:0,errors:0}});
  assert.equal(synced.graphText,'完整 Step 0 · 已同步 · 写入错误 0');
  assert.equal(synced.attention,false);
});

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

test('interpolation follows the real one-second frame cadence instead of stopping at 500 ms',()=>{
  let state={samples:[],duration:250};
  for(const gap of [1010,1360,990,1400,1020])state=nextFrameInterval(state.samples,gap);
  assert.ok(state.duration>=1000&&state.duration<=1400,'duration '+state.duration);
});
test('a save stall or reconnect gap does not stretch later motion',()=>{
  let state={samples:[],duration:250};
  for(const gap of [1000,1000,1000])state=nextFrameInterval(state.samples,gap);
  const before=state.duration;
  state=nextFrameInterval(state.samples,4029);
  assert.equal(state.duration,before);
  assert.equal(state.samples.length,3);
});
test('accelerated playback shortens interpolation and stays bounded',()=>{
  let state={samples:[],duration:250};
  for(let i=0;i<7;i++)state=nextFrameInterval(state.samples,55);
  assert.equal(state.duration,80);
  for(let i=0;i<7;i++)state=nextFrameInterval(state.samples,2400);
  assert.equal(state.duration,FRAME_INTERVAL_MAX_MS);
  assert.equal(nextFrameInterval([],NaN).duration,250);
});
