/* Real V5 -> existing dashboard view model. Never generates tasks or motion. */
import {normalizeFrame, robotCategory, attentionReason, stageLabel, formatClock} from './live-model.mjs';
import {platformMotionEvidence} from './motion-view.mjs';
export {platformMotionOrigin,platformRenderPosition,platformRoutePoints} from './motion-view.mjs';
const viewStates=new WeakMap();
const sameRun=(a,b)=>a?.runId===b.runId&&a?.runtimeInstanceId===b.runtimeInstanceId;

export function adaptPlatformFrame(value, scene, previous=null, trails=new Map()) {
  const f=normalizeFrame(value), spec=scene.spec;
  if(!(spec?.width>0&&spec?.depth>0)||!Array.isArray(scene.racks))throw new Error('真实仓库场景尚未就绪');
  const consecutive=previous?.runId===f.runId&&previous?.runtimeInstanceId===f.runtimeInstanceId&&f.tick===previous.tick+1;
  const repeated=previous?.runId===f.runId&&previous?.runtimeInstanceId===f.runtimeInstanceId&&f.tick===previous.tick;
  if(!consecutive&&!repeated)trails.clear();
  if(previous&&sameRun(previous,f)&&f.tick<previous.tick)throw new Error('真实运行帧顺序倒退');
  let viewState=viewStates.get(trails);
  if(!viewState||!sameRun(viewState,f)||f.tick<viewState.tick){
    viewState={runId:f.runId,runtimeInstanceId:f.runtimeInstanceId,tick:f.tick,headings:new Map()};
    viewStates.set(trails,viewState);
  }
  const occupied=new Set(f.occupiedSlots||[]);
  const racks=scene.racks.map(r=>({...r,width:r.w,depth:r.d,platform:true,slots:
    Array.from({length:spec.levels*spec.faces*spec.baysPerFace},(_,i)=>{
      const bay=i%spec.baysPerFace,depthRow=Math.floor(i/spec.baysPerFace)%spec.faces,level=Math.floor(i/(spec.faces*spec.baysPerFace))+1;
      const slotId=`${r.id}-L${level}-C${bay+1}-F${depthRow+1}`;
      return {slotId,rackId:r.id,level,bay,depthRow,occupied:occupied.has(slotId),reservedBy:null,cargoId:null,colorIndex:0,placedAt:null};
    })}));
  const slotsById=new Map(racks.flatMap(r=>r.slots.map(s=>[s.slotId,s])));
  const taskTargets=new Map();
  for(const r of f.robots){
    const work=r.work;
    if(!work||work.task_id!==r.task_id||!['assigned','carrying'].includes(work.status)||!slotsById.has(work.slot_id))continue;
    const candidates=taskTargets.get(work.task_id)||[];candidates.push(r);taskTargets.set(work.task_id,candidates);
  }
  for(const candidates of taskTargets.values())if(candidates.length===1){
    slotsById.get(candidates[0].work.slot_id).reservedBy=candidates[0].robot_id;
  }
  const before=new Map((previous?.robots||[]).map(r=>[r.robot_id,r]));
  const robots=f.robots.map(r=>{
    const old=before.get(r.robot_id),p={x:r.x,y:r.y};
    if(repeated&&old&&(Math.abs(old.x-r.x)>1e-6||Math.abs(old.y-r.y)>1e-6))throw new Error('同一tick的机器人位置发生改变');
    const evidence=platformMotionEvidence(r,f,old,previous,consecutive,spec);
    let path=trails.get(r.robot_id)||[];
    if(!path.length||path.at(-1).x!==p.x||path.at(-1).y!==p.y)path=[...path,p].slice(-80);
    trails.set(r.robot_id,path);
    let heading=viewState.headings.get(r.robot_id)??0;
    if(evidence.sourceHeading!==null)heading=evidence.sourceHeading;
    else if(consecutive&&old&&(old.x!==r.x||old.y!==r.y))heading=Math.atan2(r.y-old.y,r.x-old.x);
    viewState.headings.set(r.robot_id,heading);
    const target=r.work?.task_id===r.task_id?slotsById.get(r.work?.slot_id):null;
    const category=robotCategory(r),attention=attentionReason(r);
    return {...r,id:r.robot_id,platform:true,rawStage:r.stage,stage:({to_source:'toPick',to_target:'toRack',to_charge:'returning'})[r.stage]||r.stage,
      taskId:r.task_id,cargoId:r.cargo_id,typeKey:r.robot_type_key,
      category:attention?'warning':['charging','waiting'].includes(category)?'idle':category,
      label:stageLabel(r.stage),rackId:null,slotId:null,level:null,bay:null,depthRow:null,
      actionTimer:0,actionDuration:0,phase:(r.distance||0)*9,
      heading,motionIdentity:{runId:f.runId,runtimeInstanceId:f.runtimeInstanceId,robotId:r.robot_id,tick:f.tick},
      renderMotion:evidence.renderMotion,repeatedFrame:repeated,
      path:evidence.path,pathIndex:evidence.path.length>1?1:0,observedTrail:path,
      targetSlotId:target?.slotId??null,targetRackId:target?.rackId??null,targetLevel:target?.level??null,
      targetBay:target?target.bay+1:null,targetDepthRow:target?.depthRow??null,targetFace:target?target.depthRow+1:null,
      blocked:/fault|blocked/.test(r.stage),alert:attention,
      distanceTravelled:0,stageDistance:0};
  });
  viewState.tick=f.tick;
  const activationLabel=f.activation?` · 激活 ${f.activation.active} / ${f.activation.limit} 台`:'';
  return {...f,continuousFrame:consecutive,scene:{...scene,name:'WAREHOUSE CONTROL / V5 · 真实数据'+activationLabel,inboundLayout:'west-eight'},racks,robots,
    completed:f.metrics?.completed??0,flowRate:0,
    metrics:{...f.metrics,assignedRobots:f.robots.filter(r=>r.task_id).length,freeSlots:scene.capacity-occupied.size,occupiedSlots:occupied.size},
    events:[...(f.events||[])].reverse().map(e=>({time:formatClock(e.sim_time),robot:e.robot_id?`HR-${String(e.robot_id).padStart(3,'0')}`:'SYSTEM',
      text:[e.kind,e.task_id,e.goods_id,e.location_id].filter(Boolean).join(' · ')}))};
}

export function platformControl(action,value,frame,datasetId) {
  if(!frame?.runId)throw new Error('等待真实运行身份');
  const mapped=({set_speed:'speed',reset:'new_run'})[action]||action;
  if(!['start','resume','pause','step','speed','checkpoint','new_run'].includes(mapped))throw new Error('固定数据集不接受随机任务或流量修改');
  return {action:mapped,expected_run_id:frame.runId,...(mapped==='new_run'?{dataset_id:datasetId}:value===null?{}:{value})};
}
