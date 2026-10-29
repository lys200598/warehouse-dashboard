/* Pure presentation of returned state; no simulation or invented progress. */
const finite=v=>typeof v==='number'&&Number.isFinite(v);
export function samePhysicalFrame(a,b){
  if(!a||!b||a.runId!==b.runId||a.runtimeInstanceId!==b.runtimeInstanceId||a.tick!==b.tick
      ||a.robots.length!==b.robots.length)return false;
  const previous=new Map(a.robots.map(r=>[r.robot_id,r]));
  return b.robots.every(r=>{const old=previous.get(r.robot_id);return old&&old.x===r.x&&old.y===r.y;});
}
export function progressPresentation(value){
  const known=finite(value)&&value>=0&&value<=100;
  return {known,width:known?`${value}%`:'0%',text:known?`${Math.round(value)}%`:'阶段进度未提供'};
}
export function platformStageProgress(robot){
  if(robot.rawStage==='charging'||robot.stage==='charging')return robot.battery;
  const work=robot.work;
  return work&&work.task_id===robot.taskId&&finite(work.phase_progress)?work.phase_progress:null;
}
export function platformTaskPresentation(robot){
  const work=robot.work?.task_id===robot.taskId?robot.work:null;
  const hasTask=Boolean(robot.taskId);
  const outbound=work?.task_type==='OUTBOUND';
  const finished=work?.status==='completed';
  const endpoints=work?.source_id&&work?.target_id?`${work.source_id} → ${work.target_id}`:null;
  const title=!hasTask?(robot.dock_id?'补能 / 返回待命位':'等待主进程派单 / 待命'):
    finished?'任务已完成，返回待命位':endpoints?`${outbound?'出库':'入库'}：${endpoints}`:
      `${robot.label} · 位置标识 ${robot.location_id||'—'}`;
  return {hasTask,title,finished,lastStep:outbound?'出库':'上架',
    level:robot.targetLevel?`L${robot.targetLevel} · ${robot.targetSlotId}`:'—',
    goods:robot.cargoId||work?.goods_id||'—'};
}
export function shouldPickRobot(dragMoved,button,shift=false){
  return !dragMoved&&button===0&&!shift;
}

// Physical frames arrive at the simulator's real cadence (about 1 s at speed 1,
// faster when accelerated). Interpolate over the recent median interval so a
// robot moves for the whole gap instead of finishing early and standing still.
// A save stall or reconnect gap is not a cadence sample; the robot simply rests
// at its last real position until the next frame.
export const FRAME_INTERVAL_MIN_MS=80,FRAME_INTERVAL_MAX_MS=2000;
export function nextFrameInterval(samples,interval,keep=7){
  const valid=Number.isFinite(interval)&&interval>=FRAME_INTERVAL_MIN_MS/2&&interval<=FRAME_INTERVAL_MAX_MS*1.25;
  const next=valid?[...samples,interval].slice(-keep):[...samples];
  if(!next.length)return {samples:next,duration:250};
  const sorted=[...next].sort((a,b)=>a-b),median=sorted[sorted.length>>1];
  return {samples:next,duration:Math.min(FRAME_INTERVAL_MAX_MS,Math.max(FRAME_INTERVAL_MIN_MS,median))};
}
