/* Parameterized Canvas view of actual frames. Old preview pages are untouched. */
export const clamp=(value,low,high)=>Math.max(low,Math.min(high,value));
export const newCamera=()=>({view:'iso',zoom:1,panX:0,panY:0,yaw:-Math.PI/6,pitch:Math.PI/3.6});
export function projectionMetrics(spec,camera,width,height){
  const c=Math.cos(camera.yaw),n=Math.sin(camera.yaw),sp=camera.view==='top'?1:Math.sin(camera.pitch),cp=camera.view==='top'?0:Math.cos(camera.pitch);
  const rw=Math.abs(spec.width*c)+Math.abs(spec.depth*n),rd=Math.abs(spec.width*n)+Math.abs(spec.depth*c);
  const s=Math.max(.01,Math.min((width-64)/rw,(height-72)/(rd*sp+3*cp)))*camera.zoom;
  return {s,c,n,sp,cp,cx:spec.width/2,cy:spec.depth/2,ox:width/2+camera.panX,oy:height/2+camera.panY};
}
export function project(point,m,z=0){const a=point.x-m.cx,b=point.y-m.cy;return {x:m.ox+(a*m.c-b*m.n)*m.s,y:m.oy+(a*m.n+b*m.c)*m.s*m.sp-z*m.s*m.cp};}
export function unproject(point,m){const u=(point.x-m.ox)/m.s,v=(point.y-m.oy)/(m.s*m.sp);return {x:m.cx+u*m.c+v*m.n,y:m.cy-u*m.n+v*m.c};}
export function dragCamera(camera,button,dx,dy){
  if(button===1||button===2){camera.panX+=dx;camera.panY+=dy;return 'pan';}
  if(button===0){camera.yaw+=dx*.009;camera.pitch=clamp(camera.pitch-dy*.006,.25,1.45);return 'orbit';}
  return null;
}
export function zoomCamera(camera,delta,point,spec,width,height){
  if(!delta)return;
  const world=unproject(point,projectionMetrics(spec,camera,width,height));
  camera.zoom=clamp(camera.zoom*(delta<0?1.08:1/1.08),.3,5);
  const changed=project(world,projectionMetrics(spec,camera,width,height));
  camera.panX+=point.x-changed.x;camera.panY+=point.y-changed.y;
}
export function canonicalBays(scene){
  const unique=new Map();
  for(const bay of scene.docks||[]){const key=bay.alias_of||bay.id;if(!unique.has(key))unique.set(key,bay);}
  return [...unique.values()];
}
export function queryURL(kind,id,seq){
  if(!['goods','robot','cooperation'].includes(kind))throw new Error('未知查询类型');
  if(typeof id!=='string'||!id.trim()||id.length>128)throw new Error('请输入有效实体编号');
  if(!Number.isInteger(seq)||seq<1)throw new Error('截止序号须为正整数');
  if(kind==='robot'&&(!/^\d+$/.test(id)||Number(id)<1||Number(id)>100))throw new Error('机器人编号为1–100');
  return `/api/knowledge/${kind}?${new URLSearchParams({[kind==='cooperation'?'task':'id']:id,seq:String(seq)})}`;
}
export function formatClock(seconds){const value=Math.max(0,Math.floor(seconds||0)),day=Math.floor(value/86400),h=Math.floor(value%86400/3600),m=Math.floor(value%3600/60),s=value%60;return `${day?`第${day+1}天 `:''}${[h,m,s].map(v=>String(v).padStart(2,'0')).join(':')}`;}
export function realRobots(frame){return Array.isArray(frame?.robots)?frame.robots.filter(r=>Number.isInteger(r.robot_id)&&Number.isFinite(r.x)&&Number.isFinite(r.y)):[];}
export function robotColor(robot){const phase=robot.stage||'';return phase==='charging'?'#68c7ff':phase==='idle'?'#91a499':/wait|queue|fault|blocked/.test(phase)?'#f0a65a':'#c9f46c';}

export function startWidePage(doc=document){
  const $=id=>doc.getElementById(id),canvas=$('wideCanvas'),ctx=canvas.getContext('2d'),stage=$('wideStage');
  const state={camera:newCamera(),scene:null,status:null,frame:null,selected:null,cursor:0,latest:0,playing:false,busy:false,pending:null,manualSeq:false};
  let width=1,height=1,drag=null,lastTime=0,lastFetch=0;
  const set=(id,value)=>{$(id).textContent=String(value??'—');};
  const world=(x,y,z=0)=>project({x,y},projectionMetrics(state.scene.spec,state.camera,width,height),z);
  const palette={floor:'#18251c',rackTop:'#6a8774',rackLeft:'#40594b',rackRight:'#526e5b'};
  function polygon(points,fill,stroke='#9cb59f30'){
    ctx.beginPath();ctx.moveTo(points[0].x,points[0].y);for(const p of points.slice(1))ctx.lineTo(p.x,p.y);ctx.closePath();
    if(fill){ctx.fillStyle=fill;ctx.fill();}if(stroke){ctx.strokeStyle=stroke;ctx.lineWidth=.6;ctx.stroke();}
  }
  function rectangle(r,fill,z=0){polygon([world(r.x,r.y,z),world(r.x+r.w,r.y,z),world(r.x+r.w,r.y+r.d,z),world(r.x,r.y+r.d,z)],fill);}
  function line(a,b,color='#9cb59f40',weight=.7){ctx.beginPath();ctx.moveTo(a.x,a.y);ctx.lineTo(b.x,b.y);ctx.strokeStyle=color;ctx.lineWidth=weight;ctx.stroke();}
  function label(text,x,y,color='#b6c7bb',z=0){const p=world(x,y,z);ctx.fillStyle=color;ctx.font='10px Consolas, monospace';ctx.textAlign='center';ctx.fillText(text,p.x,p.y);}
  function prism(r,h=1.8){
    const b=[world(r.x,r.y),world(r.x+r.w,r.y),world(r.x+r.w,r.y+r.d),world(r.x,r.y+r.d)],t=[world(r.x,r.y,h),world(r.x+r.w,r.y,h),world(r.x+r.w,r.y+r.d,h),world(r.x,r.y+r.d,h)];
    if(state.camera.view!=='top'){
      const m=projectionMetrics(state.scene.spec,state.camera,width,height),depth=(x,y)=>(x-m.cx)*m.n+(y-m.cy)*m.c;
      const faces=[{p:[b[0],b[1],t[1],t[0]],d:depth(r.x+r.w/2,r.y),c:palette.rackLeft},{p:[b[1],b[2],t[2],t[1]],d:depth(r.x+r.w,r.y+r.d/2),c:palette.rackRight},{p:[b[2],b[3],t[3],t[2]],d:depth(r.x+r.w/2,r.y+r.d),c:palette.rackLeft},{p:[b[3],b[0],t[0],t[3]],d:depth(r.x,r.y+r.d/2),c:palette.rackRight}];
      faces.sort((a,b)=>a.d-b.d).forEach(f=>polygon(f.p,f.c));
      for(let level=1;level<state.scene.spec.levels;level++)line(world(r.x,r.y,h*level/state.scene.spec.levels),world(r.x+r.w,r.y,h*level/state.scene.spec.levels),'#a5b7a36b');
    }
    polygon(t,palette.rackTop);for(let col=1;col<state.scene.spec.baysPerFace;col++){const x=r.x+col*state.scene.spec.slotPitch;line(world(x,r.y,h),world(x,r.y+r.d,h),'#1f382c80');}
    label(r.id,r.x+r.w/2,r.y+r.d/2,'#d2dfd0',h+.12);
  }
  function drawRobot(robot){
    const m=projectionMetrics(state.scene.spec,state.camera,width,height),s=m.s,color=robotColor(robot),p=world(robot.x,robot.y),selected=state.selected===robot.robot_id;
    if($('showEnvelope').checked||selected)polygon(Array.from({length:24},(_,i)=>world(robot.x+Math.cos(i*Math.PI/12)*state.scene.spec.robotRadius,robot.y+Math.sin(i*Math.PI/12)*state.scene.spec.robotRadius,.02)),`${color}18`,`${color}95`);
    ctx.fillStyle='#0006';ctx.beginPath();ctx.ellipse(p.x,p.y,Math.max(2,s*.35),Math.max(1,s*.15),0,0,Math.PI*2);ctx.fill();
    if(state.camera.view==='top'){ctx.beginPath();ctx.arc(p.x,p.y,Math.max(2,s*.27),0,Math.PI*2);ctx.fillStyle=color;ctx.fill();}
    else{
      const foot=world(robot.x,robot.y,.05),hip=world(robot.x,robot.y,.7),shoulder=world(robot.x,robot.y,1.22),head=world(robot.x,robot.y,1.55);
      ctx.lineWidth=Math.max(1,s*.105);ctx.strokeStyle='#b9c9bc';ctx.lineCap='round';ctx.beginPath();ctx.moveTo(foot.x-s*.19,foot.y);ctx.lineTo(hip.x-s*.12,hip.y);ctx.moveTo(foot.x+s*.19,foot.y);ctx.lineTo(hip.x+s*.12,hip.y);ctx.moveTo(shoulder.x-s*.2,shoulder.y);ctx.lineTo(hip.x-s*.34,hip.y);ctx.moveTo(shoulder.x+s*.2,shoulder.y);ctx.lineTo(hip.x+s*.34,hip.y);ctx.stroke();
      ctx.fillStyle='#b3c3b3';ctx.fillRect(shoulder.x-s*.2,shoulder.y,Math.max(2,s*.4),Math.max(2,hip.y-shoulder.y));ctx.fillStyle=color;ctx.fillRect(shoulder.x-s*.2,shoulder.y+1,Math.max(2,s*.4),Math.max(1,s*.1));ctx.beginPath();ctx.arc(head.x,head.y,Math.max(1.5,s*.16),0,Math.PI*2);ctx.fillStyle='#dce7d4';ctx.fill();
    }
    if(robot.carrying){ctx.fillStyle='#c69b62';ctx.fillRect(p.x-s*.25,p.y-s*.6,Math.max(2,s*.5),Math.max(2,s*.28));}
    if(selected)label(`R${robot.robot_id}`,robot.x,robot.y,color,2.15);
  }
  function draw(){
    ctx.clearRect(0,0,width,height);if(!state.scene)return;
    const scene=state.scene,m=projectionMetrics(scene.spec,state.camera,width,height);rectangle({x:0,y:0,w:scene.spec.width,d:scene.spec.depth},palette.floor);
    for(let x=0;x<=scene.spec.width;x+=6)line(world(x,0),world(x,scene.spec.depth),'#3b514029');
    for(let y=0;y<=scene.spec.depth;y+=6)line(world(0,y),world(scene.spec.width,y),'#3b514029');
    for(const buffer of scene.buffers||[])rectangle(buffer,'#493e284d');
    for(const bay of canonicalBays(scene))rectangle(bay,'#27443655');
    for(const p of scene.stations||[]){const q=world(p.x,p.y);ctx.fillStyle=p.kind==='inbound'?'#c9f46c':'#68c7ff';ctx.fillRect(q.x-2,q.y-2,4,4);label(p.id,p.x,p.y,ctx.fillStyle,.25);}
    const depth=o=>(o.x-m.cx)*m.n+(o.y-m.cy)*m.c;
    const objects=[...(scene.racks||[]).map(r=>({x:r.x+r.w/2,y:r.y+r.d/2,draw:()=>prism(r,scene.spec.rackHeight||1.8)})),...realRobots(state.frame).map(r=>({x:r.x,y:r.y,draw:()=>drawRobot(r)}))];
    objects.sort((a,b)=>depth(a)-depth(b)).forEach(o=>o.draw());
  }
  function resize(){const rect=stage.getBoundingClientRect();width=Math.max(1,rect.width);height=Math.max(1,rect.height);const ratio=globalThis.devicePixelRatio||1;canvas.width=Math.round(width*ratio);canvas.height=Math.round(height*ratio);ctx.setTransform(ratio,0,0,ratio,0,0);draw();}
  function facts(id,rows){const fragment=doc.createDocumentFragment();for(const [key,value] of rows){const row=doc.createElement('div'),dt=doc.createElement('dt'),dd=doc.createElement('dd');dt.textContent=key;dd.textContent=String(value??'—');row.append(dt,dd);fragment.append(row);}$(id).replaceChildren(fragment);}
  function selectedRobot(){return realRobots(state.frame).find(r=>r.robot_id===state.selected);}
  function selection(){const r=selectedRobot();facts('robotFacts',r?[['机器人',r.robot_id],['SOC',Number(r.battery).toFixed(2)+'%'],['阶段',r.stage],['位置',`${r.x.toFixed(2)}, ${r.y.toFixed(2)} m`],['货物',r.cargo_id],['任务',r.task_id],['初始泊位别名',r.home_id],['充电位',r.dock_id]]:[['状态','点击或选择实际机器人']]);$('robotSelect').value=r?String(r.robot_id):'';}
  async function fetchJSON(url){const response=await fetch(url,{cache:'no-store'});const value=await response.json();if(!response.ok)throw new Error(value.error||`HTTP ${response.status}`);return value;}
  function error(message){$('runStatus').classList.add('wide-error');set('runStatus',message);}
  async function refresh(){
    try{
      const status=await fetchJSON('/api/status');state.status=status;state.latest=Math.max(0,Number(status.latest_second)||0);$('runTimeline').max=String(state.latest);
      const knowledge=status.knowledge||{};facts('knowledgeFacts',[['运行',status.run_id],['持久化序号',knowledge.durable_seq],['图谱已提交序号',knowledge.mirrored_seq],['镜像滞后',knowledge.lag],['后端',knowledge.backend]]);
      set('runStatus',`${status.run_id} · ${status.results?.status||(status.running?'running':'已记录')} · 最新 ${formatClock(state.latest)} · 图谱 ${knowledge.mirrored_seq??0}/${knowledge.durable_seq??0}`);$('runStatus').classList.remove('wide-error');
      return status;
    }catch(e){error(e.message);throw e;}
  }
  async function showFrame(second){
    second=clamp(Math.floor(second),0,state.latest);if(state.busy){state.pending=second;return;}
    state.busy=true;
    try{
      const response=await fetchJSON(`/api/frame?second=${second}`),frame=response.frame||response;state.frame=frame;state.cursor=Number(frame.elapsed??frame.tick??second);$('runTimeline').value=String(state.cursor);set('frameClock',formatClock(state.cursor));set('cursorValue',formatClock(state.cursor));
      const robots=realRobots(frame);set('robotCount',robots.length);set('completed',frame.metrics?.completed);set('inside',frame.inside);for(const name of ['moving','handling','charging','queued','standby'])set(name,frame.counts?.[name]);
      const signature=robots.map(r=>r.robot_id).join(',');if($('robotSelect').dataset.signature!==signature){const options=[new Option('请选择机器人',''),...robots.map(r=>new Option(`机器人 ${r.robot_id}`,String(r.robot_id)))];$('robotSelect').replaceChildren(...options);$('robotSelect').dataset.signature=signature;}
      if(!state.manualSeq){const seq=frame.knowledge?.mirrored_seq??state.status?.knowledge?.mirrored_seq;if(seq>0)$('querySeq').value=String(seq);}
      selection();draw();
    }catch(e){state.playing=false;set('playRun','播放回放');error(e.message);}
    finally{state.busy=false;if(state.pending!==null){const pending=state.pending;state.pending=null;showFrame(pending);}}
  }
  async function query(kind=$('queryKind').value,id=$('queryId').value){
    try{const seq=Number($('querySeq').value);if(seq>(state.status?.knowledge?.mirrored_seq??0))throw new Error('截止序号超过已提交图谱');$('queryKind').value=kind;$('queryId').value=id;set('queryStatus','正在查询…');const value=await fetchJSON(queryURL(kind,id,seq));set('knowledgeResult',JSON.stringify(value,null,2));set('queryStatus',`服务端查询结果 · ${kind} / ${id} · cutoff seq=${seq}；历史截止以该序号为准`);}catch(e){set('queryStatus',e.message);}
  }
  $('robotSelect').addEventListener('change',()=>{state.selected=Number($('robotSelect').value)||null;selection();draw();});
  for(const [id,kind,key] of [['queryRobot','robot','robot_id'],['queryGoods','goods','cargo_id'],['queryCoop','cooperation','task_id']])$(id).addEventListener('click',()=>{const r=selectedRobot();if(!r||r[key]===null||r[key]===undefined){set('queryStatus','选中机器人在本帧没有该实体；可在上方输入历史ID查询。');return;}query(kind,String(r[key]));});
  $('runQuery').addEventListener('click',()=>query());$('querySeq').addEventListener('input',()=>{state.manualSeq=Boolean($('querySeq').value);});
  $('copyKnowledge').addEventListener('click',async()=>{try{await navigator.clipboard.writeText($('knowledgeResult').textContent);set('queryStatus','结果已复制');}catch{set('queryStatus','浏览器未授权自动复制，可直接选中结果文本复制。');}});
  for(const [id,view] of [['viewIso','iso'],['viewTop','top']])$(id).addEventListener('click',()=>{state.camera.view=view;if(view==='top')state.camera.yaw=0;else{state.camera.yaw=-Math.PI/6;state.camera.pitch=Math.PI/3.6;}$('viewIso').setAttribute('aria-pressed',String(view==='iso'));$('viewTop').setAttribute('aria-pressed',String(view==='top'));draw();});
  $('resetView').addEventListener('click',()=>{state.camera=newCamera();$('viewIso').setAttribute('aria-pressed','true');$('viewTop').setAttribute('aria-pressed','false');draw();});$('showEnvelope').addEventListener('change',draw);
  canvas.addEventListener('contextmenu',event=>event.preventDefault());
  canvas.addEventListener('pointerdown',event=>{if(![0,1,2].includes(event.button))return;event.preventDefault();drag={button:event.button,x:event.clientX,y:event.clientY,startX:event.clientX,startY:event.clientY};canvas.setPointerCapture(event.pointerId);});
  canvas.addEventListener('pointermove',event=>{if(!drag)return;dragCamera(state.camera,drag.button,event.clientX-drag.x,event.clientY-drag.y);drag.x=event.clientX;drag.y=event.clientY;draw();});
  canvas.addEventListener('pointerup',event=>{if(drag?.button===0&&Math.hypot(event.clientX-drag.startX,event.clientY-drag.startY)<4&&state.scene){const box=canvas.getBoundingClientRect(),point={x:event.clientX-box.left,y:event.clientY-box.top},m=projectionMetrics(state.scene.spec,state.camera,width,height);const nearby=realRobots(state.frame).map(r=>({robot:r,d:Math.hypot(project(r,m).x-point.x,project(r,m).y-point.y)})).sort((a,b)=>a.d-b.d)[0];if(nearby?.d<16){state.selected=nearby.robot.robot_id;selection();draw();}}drag=null;});
  canvas.addEventListener('pointercancel',()=>{drag=null;});
  canvas.addEventListener('wheel',event=>{event.preventDefault();if(!state.scene)return;const box=canvas.getBoundingClientRect();zoomCamera(state.camera,event.deltaY,{x:event.clientX-box.left,y:event.clientY-box.top},state.scene.spec,width,height);draw();},{passive:false});
  $('runTimeline').addEventListener('input',()=>{state.playing=false;set('playRun','播放回放');showFrame(Number($('runTimeline').value));});
  $('playRun').addEventListener('click',()=>{if(state.cursor>=state.latest)showFrame(0);state.playing=!state.playing;set('playRun',state.playing?'暂停':'播放回放');lastTime=0;});
  $('latestRun').addEventListener('click',async()=>{state.playing=false;set('playRun','播放回放');try{await refresh();showFrame(state.latest);}catch{/* refresh already displays the server error */}});
  $('refreshRun').addEventListener('click',async()=>{try{await refresh();showFrame(state.cursor||state.latest);}catch{/* keep the last real frame on a failed refresh */}});
  globalThis.addEventListener('resize',resize);
  function tick(now){if(state.playing&&!state.busy&&now-lastFetch>100){const delta=lastTime?Math.min(1,(now-lastTime)/1000):.1;lastTime=lastFetch=now;const next=Math.min(state.latest,state.cursor+Math.max(10,delta*Number($('replaySpeed').value)));showFrame(next);if(next>=state.latest){state.playing=false;set('playRun','播放回放');}}requestAnimationFrame(tick);}
  requestAnimationFrame(tick);
  Promise.all([fetchJSON('/api/scene'),refresh()]).then(([data])=>{const scene=data.scene||data;if(!(scene.spec?.width>0&&scene.spec?.depth>0))throw new Error('场景尺寸无效');state.scene=scene;set('sceneSize',`${scene.spec.width}×${scene.spec.depth}m`);set('bayCount',canonicalBays(scene).length);set('slotCount',scene.capacity??scene.racks.reduce((sum,r)=>sum+r.capacity,0));set('sceneLabel',`${scene.spec.version} · 实际运行地图`);resize();showFrame(state.latest);}).catch(e=>error(e.message));
  return state;
}
if(typeof document!=='undefined'&&document.getElementById('wideCanvas'))startWidePage();
