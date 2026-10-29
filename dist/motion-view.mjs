/* Render only producer-supplied motion evidence; never invent a planned route. */
const EPS=1e-6;
const MAX_POINTS=4096;
const finite=Number.isFinite;
const close=(a,b)=>Math.abs(a-b)<=EPS;
const same=(a,b)=>!!a&&!!b&&close(a.x,b.x)&&close(a.y,b.y);
const fail=message=>{throw new Error('真实运动证据无效: '+message);};
const endpoint=robot=>({x:robot.renderTargetX??robot.x,y:robot.renderTargetY??robot.y});
const identityMatches=(a,b)=>!!a&&!!b&&a.runId===b.runId&&a.runtimeInstanceId===b.runtimeInstanceId&&a.robotId===b.robotId;

function point(value,spec,withTime=false){
  if(!value||!finite(value.x)||!finite(value.y)||value.x < -EPS||value.y < -EPS
      ||value.x>spec.width+EPS||value.y>spec.depth+EPS)fail('point outside scene or nonfinite');
  if(withTime&&!finite(value.t))fail('nonfinite execution time');
  return withTime?{x:value.x,y:value.y,t:value.t}:{x:value.x,y:value.y};
}

export function platformMotionEvidence(robot,frame,old,previous,consecutive,spec){
  const motion=robot.motion;
  if(motion===undefined||motion===null)return {path:[],renderMotion:null,sourceHeading:null};
  if(typeof motion!=='object'||Array.isArray(motion)||motion.tick!==frame.tick)fail('motion tick differs from frame');
  if(motion.plan_id!==null&&(typeof motion.plan_id!=='string'||!motion.plan_id||motion.plan_id.length>256))fail('invalid plan identity');
  if(!Array.isArray(motion.path)||motion.path.length>MAX_POINTS)fail('invalid or oversized planned path');
  const path=motion.path.map(p=>point(p,spec));
  if(path.length&&!same(path[0],robot))fail('planned path does not start at actual position');
  if(path.length>1&&motion.plan_id===null)fail('planned route lacks an execution identity');
  if(motion.heading!==undefined&&motion.heading!==null&&!finite(motion.heading))fail('nonfinite heading');
  let renderMotion=null;
  if(motion.executed!==undefined&&motion.executed!==null){
    const e=motion.executed;
    if(!e||typeof e!=='object'||Array.isArray(e)||!Number.isInteger(e.from_tick)||!Number.isInteger(e.to_tick)
        ||e.from_tick<0||e.from_tick!==frame.tick-1||e.to_tick!==frame.tick)fail('execution interval differs from frame');
    if(!Array.isArray(e.points)||e.points.length<2||e.points.length>MAX_POINTS)fail('invalid or oversized executed polyline');
    const points=e.points.map(p=>point(p,spec,true));
    if(!close(points[0].t,e.from_tick)||!close(points.at(-1).t,e.to_tick)||!same(points.at(-1),robot))fail('execution endpoints differ from frame');
    for(let i=1;i<points.length;i++)if(!(points[i].t>points[i-1].t))fail('execution times must strictly increase');
    if(consecutive){
      if(!old||previous.tick!==e.from_tick||!same(points[0],old))fail('execution does not start at previous actual position');
      renderMotion={from_tick:e.from_tick,to_tick:e.to_tick,points,
        identity:{runId:frame.runId,runtimeInstanceId:frame.runtimeInstanceId,robotId:robot.robot_id,tick:frame.tick}};
    }
  }
  return {path,renderMotion,sourceHeading:finite(motion.heading)?motion.heading:null};
}

export function platformMotionOrigin(previous,payload,continuous){
  const e=payload.renderMotion;
  if(continuous&&e&&identityMatches(e.identity,payload.motionIdentity)
      &&identityMatches(previous?.motionIdentity,payload.motionIdentity)
      &&previous.motionIdentity.tick===e.from_tick&&payload.motionIdentity.tick===e.to_tick
      &&same(endpoint(previous),e.points[0])&&same(endpoint(payload),e.points.at(-1))){
    return {x:e.points[0].x,y:e.points[0].y};
  }
  return endpoint(payload);
}

export function platformRenderPosition(robot,amount){
  const e=robot.renderMotion,target=endpoint(robot);
  // A repeated, skipped or foreign frame has no validated segment to replay.
  if(!e||!identityMatches(e.identity,robot.motionIdentity)||robot.motionIdentity?.tick!==e.to_tick||!same(target,e.points.at(-1))
      ||!same({x:robot.renderFromX,y:robot.renderFromY},e.points[0]))return target;
  const a=finite(amount)?Math.max(0,Math.min(1,amount)):1;
  const t=e.from_tick+(e.to_tick-e.from_tick)*a;
  for(let i=1;i<e.points.length;i++){
    const left=e.points[i-1],right=e.points[i];
    if(t<=right.t){
      const fraction=Math.max(0,Math.min(1,(t-left.t)/(right.t-left.t)));
      return {x:left.x+(right.x-left.x)*fraction,y:left.y+(right.y-left.y)*fraction};
    }
  }
  return target;
}

export function platformRoutePoints(robot,amount){
  const e=robot.renderMotion,planned=robot.path||[];
  if(!e&&!planned.length)return [];
  const points=[];
  const append=p=>{if(!points.length||!same(points.at(-1),p))points.push({x:p.x,y:p.y});};
  append(platformRenderPosition(robot,amount));
  if(e&&identityMatches(e.identity,robot.motionIdentity)&&robot.motionIdentity?.tick===e.to_tick&&same(endpoint(robot),e.points.at(-1))
      &&same({x:robot.renderFromX,y:robot.renderFromY},e.points[0])){
    const a=finite(amount)?Math.max(0,Math.min(1,amount)):1;
    const time=e.from_tick+(e.to_tick-e.from_tick)*a;
    for(const p of e.points)if(p.t>time+EPS)append(p);
  }
  for(const p of planned.slice(Math.max(1,robot.pathIndex||0)))append(p);
  return points;
}
