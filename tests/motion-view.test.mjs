import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {adaptPlatformFrame,platformMotionOrigin,platformRenderPosition,platformRoutePoints} from '../dist/original-platform.mjs';

const scene=JSON.parse(readFileSync(new URL('../data/layouts/expansion-candidates/medium/layout.json',import.meta.url)));
const raw=(id=6,x=9.75,y=2.4)=>({robot_id:id,robot_type_key:'X3',x,y,battery:70,stage:'to_source',carrying:false,task_id:'T1',cargo_id:null});
const frame=(tick,r,extra={})=>({runId:'motion-run',runtimeInstanceId:'motion-instance',tick,elapsed:tick,
  robots:[r],metrics:{completed:0},occupiedSlots:[],events:[],...extra});
const evidence=(tick,start,end,extra={})=>({tick,plan_id:'real-job-6',path:[{x:end.x,y:end.y},{x:7.6,y:33}],
  executed:{from_tick:tick-1,to_tick:tick,points:[{...start,t:tick-1},{...end,t:tick}]},heading:null,...extra});
const diagonal=()=>{
  const a=raw(),b=raw(6,9.647619047619047,3.8571428571428568);
  b.motion=evidence(191,a,b);
  const f0=frame(190,a),f1=frame(191,b),trails=new Map();
  const before=adaptPlatformFrame(f0,scene,null,trails).robots[0];
  before.renderTargetX=before.x;before.renderTargetY=before.y;
  const after=adaptPlatformFrame(f1,scene,f0,trails).robots[0];
  const origin=platformMotionOrigin(before,after,true);
  return {before,after,f0,f1,trails,rendered:{...after,renderFromX:origin.x,renderFromY:origin.y,
    renderTargetX:after.x,renderTargetY:after.y}};
};

test('real 1.4607m diagonal is smoothly interpolated only with exact executed evidence',()=>{
  const {before,after,rendered}=diagonal();
  assert.deepEqual(platformRenderPosition(rendered,0),{x:before.x,y:before.y});
  const middle=platformRenderPosition(rendered,.5);
  assert.ok(Math.abs(middle.x-(before.x+after.x)/2)<1e-9);
  assert.ok(Math.abs(middle.y-(before.y+after.y)/2)<1e-9);
  assert.deepEqual(platformRenderPosition(rendered,1),{x:after.x,y:after.y});
  assert.deepEqual(after.path,[{x:after.x,y:after.y},{x:7.6,y:33}]);
  assert.equal(after.pathIndex,1);
});

test('executed turns retain their intermediate corner in animation and route display',()=>{
  const a=raw(6,10,10),b=raw(6,11,11),f0=frame(10,a);
  b.motion=evidence(11,a,b,{path:[{x:11,y:11},{x:11,y:20}],
    executed:{from_tick:10,to_tick:11,points:[{x:10,y:10,t:10},{x:11,y:10,t:10.5},{x:11,y:11,t:11}]}});
  const previous=adaptPlatformFrame(f0,scene).robots[0];
  const after=adaptPlatformFrame(frame(11,b),scene,f0).robots[0];
  const origin=platformMotionOrigin(previous,after,true);
  const rendered={...after,renderFromX:origin.x,renderFromY:origin.y,renderTargetX:after.x,renderTargetY:after.y};
  assert.deepEqual(platformRenderPosition(rendered,.25),{x:10.5,y:10});
  assert.deepEqual(platformRenderPosition(rendered,.5),{x:11,y:10});
  assert.deepEqual(platformRenderPosition(rendered,.75),{x:11,y:10.5});
  assert.deepEqual(platformRoutePoints(rendered,.25),[{x:10.5,y:10},{x:11,y:10},{x:11,y:11},{x:11,y:20}]);
  assert.deepEqual(platformRoutePoints(rendered,.75),[{x:11,y:10.5},{x:11,y:11},{x:11,y:20}]);
  assert.deepEqual(platformRoutePoints(rendered,1),[{x:11,y:11},{x:11,y:20}]);
});

test('unproved legacy diagonal snaps to source endpoint and never fabricates a route',()=>{
  const a=raw(6,10,10),b=raw(6,11,11),f0=frame(0,a);
  const previous=adaptPlatformFrame(f0,scene).robots[0];
  const after=adaptPlatformFrame(frame(1,b),scene,f0).robots[0];
  assert.equal(after.renderMotion,null);
  assert.deepEqual(platformMotionOrigin(previous,after,true),{x:11,y:11});
  assert.deepEqual(platformRoutePoints(after,.25),[]);
});

test('duplicate ticks preserve the 94-degree heading but cannot replay the preceding movement',()=>{
  const {after,f1,trails}=diagonal();
  const repeat=adaptPlatformFrame(f1,scene,f1,trails).robots[0];
  assert.ok(Math.abs(after.heading*180/Math.PI-94.01907891705984)<1e-8);
  assert.equal(repeat.heading,after.heading);
  assert.equal(repeat.renderMotion,null);
  assert.deepEqual(platformMotionOrigin(after,repeat,false),{x:repeat.x,y:repeat.y});
  const stopped={...f1.robots[0],motion:{...f1.robots[0].motion,tick:192,executed:null,heading:null}};
  const still=adaptPlatformFrame(frame(192,stopped),scene,f1,trails).robots[0];
  assert.equal(still.heading,after.heading);
});

test('gaps and run/instance changes never connect or animate across identities',()=>{
  const {after,f1,trails,rendered}=diagonal();
  const next={...f1.robots[0],motion:{...f1.robots[0].motion,tick:195,executed:null,heading:null}};
  const gap=adaptPlatformFrame(frame(195,next),scene,f1,trails).robots[0];
  assert.equal(gap.renderMotion,null);assert.equal(gap.observedTrail.length,1);
  for(const identity of [{runId:'other-run'},{runtimeInstanceId:'other-instance'}]){
    const changed=adaptPlatformFrame({...f1,...identity},scene,f1,trails).robots[0];
    assert.equal(changed.renderMotion,null);
    assert.deepEqual(platformMotionOrigin(after,changed,true),{x:changed.x,y:changed.y});
  }
  const foreign={...rendered,motionIdentity:{...rendered.motionIdentity,runId:'foreign'}};
  assert.deepEqual(platformRenderPosition(foreign,0),{x:foreign.x,y:foreign.y});
  assert.deepEqual(platformRoutePoints(foreign,0),[{x:foreign.x,y:foreign.y},{x:7.6,y:33}]);
});

test('malformed or unfenced motion is rejected instead of becoming a visible line',()=>{
  const {f0,f1}=diagonal();
  const cases=[
    m=>m.tick--,
    m=>m.path[0].x++,
    m=>m.path.push({x:Infinity,y:3}),
    m=>m.path.push({x:121,y:3}),
    m=>m.path=Array.from({length:4097},()=>({x:1,y:1})),
    m=>m.plan_id=null,
    m=>m.executed.from_tick--,
    m=>m.executed.points[0].x++,
    m=>m.executed.points[1].y++,
    m=>m.executed.points[1].t=m.executed.points[0].t,
    m=>m.heading=NaN,
  ];
  for(const alter of cases){
    const bad=structuredClone(f1);alter(bad.robots[0].motion);
    assert.throws(()=>adaptPlatformFrame(bad,scene,f0),/真实运动证据无效/);
  }
});

test('out-of-order and inconsistent repeated physical positions are rejected',()=>{
  const f=frame(10,raw());
  assert.throws(()=>adaptPlatformFrame(frame(9,raw()),scene,f),/顺序倒退/);
  assert.throws(()=>adaptPlatformFrame(frame(10,raw(6,11,12)),scene,f),/同一tick/);
});

test('target slot display and reservation recover from valid work without triggering legacy arms',()=>{
  const r=raw();r.work={task_id:'T1',task_type:'INBOUND',status:'assigned',source_id:'IN-1',target_id:'R01-L2-C8-F2',slot_id:'R01-L2-C8-F2'};
  const view=adaptPlatformFrame(frame(0,r),scene),robot=view.robots[0];
  assert.equal(robot.targetSlotId,'R01-L2-C8-F2');assert.equal(robot.targetRackId,'R01');
  assert.equal(robot.targetLevel,2);assert.equal(robot.targetBay,8);assert.equal(robot.targetFace,2);
  assert.equal(robot.rackId,null);assert.equal(robot.slotId,null);
  assert.equal(view.racks.flatMap(x=>x.slots).find(s=>s.slotId===robot.targetSlotId).reservedBy,6);
});

test('unknown or ambiguous work cannot fall back to a different slot or another robot',()=>{
  const r=raw();r.work={task_id:'T1',status:'assigned',slot_id:'unknown-slot'};
  const unknown=adaptPlatformFrame(frame(0,r),scene);
  assert.equal(unknown.robots[0].targetSlotId,null);
  assert.ok(unknown.racks.flatMap(x=>x.slots).every(s=>s.reservedBy===null));
  r.work.slot_id='R01-L1-C1-F1';
  const two=adaptPlatformFrame(frame(0,r,{robots:[r,{...r,robot_id:7}]}),scene);
  assert.ok(two.racks.flatMap(x=>x.slots).every(s=>s.reservedBy===null));
});
