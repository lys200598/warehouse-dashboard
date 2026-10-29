/* Pure adapters for the real V5 live console. No generated tasks or motion. */
export const clamp=(v,a,b)=>Math.max(a,Math.min(b,v));
export const newCamera=()=>({view:'iso',zoom:1,panX:0,panY:0,yaw:Math.PI/4,pitch:Math.PI/6});
export function dragCamera(camera,button,dx,dy,shift=false){
  if(![0,1,2].includes(button))return null;
  if(button===0&&!shift&&camera.view==='iso'){
    camera.yaw+=dx*.009;camera.pitch=clamp(camera.pitch-dy*.006,.16,1.23);return 'orbit';
  }
  camera.panX+=dx;camera.panY+=dy;return 'pan';
}
export function projectionMetrics(scene,camera,width,height){
  const spec=scene.spec||scene,c=Math.cos(camera.yaw),n=Math.sin(camera.yaw);
  const sp=camera.view==='top'?1:Math.sin(camera.pitch),cp=camera.view==='top'?0:Math.cos(camera.pitch);
  const rw=Math.abs(spec.width*c)+Math.abs(spec.depth*n),rd=Math.abs(spec.width*n)+Math.abs(spec.depth*c);
  const scale=Math.max(.01,Math.min((width-60)/rw,(height-70)/(rd*sp+3*cp)))*camera.zoom;
  return {c,n,sp,cp,scale,cx:spec.width/2,cy:spec.depth/2,ox:width/2+camera.panX,oy:height/2+camera.panY};
}
export function project(p,m,z=0){const a=p.x-m.cx,b=p.y-m.cy;return {x:m.ox+(a*m.c-b*m.n)*m.scale,y:m.oy+(a*m.n+b*m.c)*m.scale*m.sp-z*m.scale*m.cp};}
export function unproject(p,m){const a=(p.x-m.ox)/m.scale,b=(p.y-m.oy)/(m.scale*m.sp);return {x:m.cx+a*m.c+b*m.n,y:m.cy-a*m.n+b*m.c};}
export function zoomCamera(camera,delta,point,scene,width,height){
  if(!delta)return;
  const p=unproject(point,projectionMetrics(scene,camera,width,height));
  camera.zoom=clamp(camera.zoom*(delta<0?1.08:1/1.08),.3,5);
  const changed=project(p,projectionMetrics(scene,camera,width,height));
  camera.panX+=point.x-changed.x;camera.panY+=point.y-changed.y;
}
export function formatClock(seconds){
  const value=Math.max(0,Math.floor(seconds||0)),day=Math.floor(value/86400);
  return `${day?`第${day+1}天 `:''}${[Math.floor(value%86400/3600),Math.floor(value%3600/60),value%60].map(n=>String(n).padStart(2,'0')).join(':')}`;
}
export function normalizeFrame(value){
  const frame=value?.snapshot||value;
  if(!frame||typeof frame.runId!=='string'||!frame.runId||!Number.isInteger(frame.tick)||frame.tick<0
    ||!Number.isFinite(frame.elapsed)||frame.elapsed<0||!Array.isArray(frame.robots))throw new Error('真实运行帧格式无效');
  const ids=new Set();
  for(const r of frame.robots){
    if(!Number.isInteger(r.robot_id)||r.robot_id<1||r.robot_id>100||ids.has(r.robot_id)
      ||!Number.isFinite(r.x)||!Number.isFinite(r.y)||!Number.isFinite(r.battery)
      ||typeof r.carrying!=='boolean'||typeof r.stage!=='string')throw new Error('机器人状态缺失、重复或类型无效');
    ids.add(r.robot_id);
  }
  return frame;
}
export function acceptsFrame(previous,next,retiredInstances=new Set()){
  const beforeInstance=previous?.runtimeInstanceId,afterInstance=next.runtimeInstanceId;
  if(afterInstance&&retiredInstances.has(afterInstance))return false;
  if(!previous)return true;
  if(beforeInstance&&afterInstance){
    // A service restart may restore the same run to an earlier durable tick.
    // Within one service, its global revision also orders DIFFERENT run IDs.
    if(beforeInstance!==afterInstance)return true;
    if(Number.isInteger(previous.liveRevision)&&Number.isInteger(next.liveRevision)){
      if(next.liveRevision<previous.liveRevision)return false;
      if(previous.runId!==next.runId)return next.liveRevision>previous.liveRevision;
    }
  }
  const before=previous.capturedAtMs??previous.generatedAtMs,after=next.capturedAtMs??next.generatedAtMs;
  if(previous.runId!==next.runId){
    // Legacy sources have no instance/revision pair; retain their real capture
    // clock as the available cross-run freshness fence.
    return !Number.isFinite(before)||!Number.isFinite(after)||after>=before;
  }
  if(Number.isInteger(previous.liveRevision)&&Number.isInteger(next.liveRevision)&&next.liveRevision<previous.liveRevision)return false;
  if(next.tick!==previous.tick)return next.tick>previous.tick;
  return !Number.isFinite(before)||!Number.isFinite(after)||after>=before;
}
export function controlPayload(action,frame,extra={}){
  if(!frame||typeof frame.runId!=='string'||!frame.runId)throw new Error('控制操作需要当前真实run身份');
  return {...extra,action,expected_run_id:frame.runId};
}
export function normalizeDatasets(value){
  const rows=Array.isArray(value)?value:value?.datasets;
  if(!Array.isArray(rows))throw new Error('固定数据集目录格式无效');
  const seen=new Set();
  return rows.map(item=>{
    const id=item.id??item.dataset_id;
    if(typeof id!=='string'||!id||id.length>128||seen.has(id))throw new Error('固定数据集ID缺失或重复');
    seen.add(id);return {...item,id};
  });
}
export function robotCategory(robot){
  if(/fault|blocked/.test(robot.stage))return 'warning';
  if(['picking','placing','handoff','handoff_receive','handoff_give'].includes(robot.stage))return 'handling';
  if(['charging','swapping'].includes(robot.stage))return 'charging';
  if(['idle','standby'].includes(robot.stage))return 'idle';
  if(/wait|queue|yield/.test(robot.stage))return 'waiting';
  return 'moving';
}
export function attentionReason(robot){
  if(/fault|blocked/.test(robot.stage))return '故障 / 阻塞';
  if(robot.battery<25&&!['charging','swapping','to_charge','returning','idle'].includes(robot.stage))return '低SOC / 协作观察';
  return null;
}
export function stageLabel(stage){return ({idle:'待命',standby:'待命',to_source:'前往取货点',to_target:'送往目标点',picking:'取货',placing:'放货',to_charge:'前往补能',returning:'返回泊位',charging:'充电',swapping:'换电中',to_handoff:'前往交接',handoff:'货物交接',handoff_receive:'接收货物',handoff_give:'交出货物',traffic_wait:'交通预约等待',recovery_wait:'恢复等待',fault_wait:'故障等待',yielding:'让行',charge_queue:'补能等待',mission_queue:'任务等待'})[stage]||stage;}
export function knowledgeFacts(frame){
  const k=frame.knowledge||{},meta=frame.metadata||{};
  return {durable:k.durable_seq??0,mirrored:k.mirrored_seq??0,lag:k.lag??0,
    committedTime:k.committed_sim_time??null,cadence:meta.commit_seconds??null,
    physicalTime:frame.elapsed,mode:meta.knowledge_mode??'—',error:k.last_error??null};
}
export function buildQuery(kind,frame,{robotId=1,taskId='',goodsId='',rackId='',seq=null,variant='business-exact'}={}){
  const cutoff=seq??frame.knowledge?.mirrored_seq;
  if(!Number.isInteger(cutoff)||cutoff<1||cutoff>(frame.knowledge?.mirrored_seq??0))throw new Error('请选择已经镜像提交的知识序号');
  if(!Number.isInteger(robotId)||robotId<1||robotId>100)throw new Error('机器人编号为1–100');
  const request={query_id:kind,run_id:frame.runId,seq:cutoff,robot_id:robotId,limit:5};
  for(const [key,value] of [['task_id',taskId],['goods_id',goodsId]]){
    if(value){if(typeof value!=='string'||value.trim()!==value||value.length>128)throw new Error('实体编号长度或空白无效');request[key]=value;}
  }
  if(kind==='rack_inventory'){if(!/^R[0-9]+$/.test(rackId))throw new Error('货架查询需要实际货架编号，如R01');request.rack_id=rackId;}
  if(kind==='task'&&!taskId)throw new Error('任务查询需要任务ID');
  if(kind==='goods'&&!goodsId&&!taskId)throw new Error('货物查询需要货物ID或任务ID');
  if(kind==='similar_history'){
    request.variant=variant;
    if(variant==='full-ann'&&(!taskId||!goodsId))throw new Error('完整ANN需要任务ID与货物ID');
  }
  return request;
}
export function slotBoxes(scene,frame,level='all'){
  const racks=new Map((scene.racks||[]).map(r=>[r.id,r]));
  return (frame.occupiedSlots||[]).flatMap(id=>{
    const match=/^(R\d+)-L(\d+)-C(\d+)-F(\d+)$/.exec(id);if(!match)return [];
    const rack=racks.get(match[1]),l=Number(match[2]),column=Number(match[3]),face=Number(match[4])-1,spec=scene.spec;
    if(!rack||l<1||l>spec.levels||column<1||column>spec.baysPerFace||face<0||face>=spec.faces||level!=='all'&&l!==Number(level))return [];
    return [{id,rack_id:rack.id,level:l,face,column,x:rack.x+(column-.5)*spec.slotPitch-.18,
      y:face===0?rack.y+.08:rack.y+rack.d-.38,w:.36,d:.3,z:(l-1)*(spec.rackHeight||1.8)/spec.levels+.1,h:.25}];
  });
}
export function filteredRobots(frame,filter='all',search=''){
  return (frame?.robots||[]).filter(r=>(filter==='all'||filter==='attention'&&attentionReason(r)||robotCategory(r)===filter)
    &&(!search||String(r.robot_id).padStart(3,'0').includes(search.replace(/\D/g,''))));
}
export function controlAvailability(frame,busy=false){
  const ready=Boolean(frame)&&!busy,phase=frame?.status||(frame?.running?'running':'paused');
  return {start:ready&&['ready','paused'].includes(phase),pause:ready&&phase==='running',
    step:ready&&['ready','paused'].includes(phase),speed:ready&&phase!=='failed',new_run:ready};
}
export function exportRows(frame){
  const fields=['robot_id','stage','x','y','battery','carrying','task_id','cargo_id','dock_id'];
  const escape=v=>`"${String(v??'').replaceAll('"','""')}"`;
  return [fields.join(','),...frame.robots.map(r=>fields.map(key=>escape(r[key])).join(','))].join('\r\n');
}
