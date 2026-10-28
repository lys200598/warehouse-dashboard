import {newCamera,dragCamera,projectionMetrics,project,zoomCamera,normalizeFrame,acceptsFrame,normalizeDatasets,
  robotCategory,attentionReason,stageLabel,formatClock,knowledgeFacts,buildQuery,slotBoxes,
  filteredRobots,controlAvailability,controlPayload,exportRows} from './live-model.mjs';

export function startLiveConsole(doc=document){
  const $=id=>doc.getElementById(id),canvas=$('liveCanvas'),ctx=canvas.getContext('2d'),stage=$('liveStage');
  const state={frame:null,scene:null,health:null,datasets:[],camera:newCamera(),selected:null,filter:'all',
    search:'',level:'all',envelope:false,busy:false,socket:null,lastWS:0,lastReceived:0,polling:false,epoch:0,pendingDatasetId:null,
    retiredInstances:new Set()};
  let width=1,height=1,drag=null,reconnect=null,toastTimer=null,drawPending=false;
  const text=(id,value)=>{$(id).textContent=String(value??'—');};
  function toast(message,error=false){text('toastText',message);text('toastIcon',error?'!':'✓');$('toast').classList.add('show');clearTimeout(toastTimer);toastTimer=setTimeout(()=>$('toast').classList.remove('show'),3500);}
  async function json(url,payload){
    const response=await fetch(url,payload===undefined?{cache:'no-store'}:{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(payload)});
    let value;try{value=await response.json();}catch{throw new Error(`HTTP ${response.status} 未返回JSON`);}
    if(!response.ok)throw new Error(value.error||value.detail||`HTTP ${response.status}`);
    return value;
  }
  function facts(id,values){
    const fragment=doc.createDocumentFragment();
    for(const [label,value] of values){const row=doc.createElement('div'),dt=doc.createElement('dt'),dd=doc.createElement('dd');dt.textContent=label;dd.textContent=String(value??'—');row.append(dt,dd);fragment.append(row);}
    $(id).replaceChildren(fragment);
  }
  function selected(){return state.frame?.robots.find(r=>r.robot_id===state.selected);}
  function controls(){
    const allowed=controlAvailability(state.frame,state.busy);
    for(const [id,key] of [['startButton','start'],['pauseButton','pause'],['stepButton','step'],['speedSelect','speed'],['newRunButton','new_run']])$(id).disabled=!allowed[key];
    $('newRunButton').disabled||=state.datasets.length===0;
    $('exportButton').disabled=!state.frame;
    $('runQuery').disabled=!(state.frame?.knowledge?.mirrored_seq>0)||state.busy;
  }
  function details(){
    const r=selected();text('selectedId',r?`HR-${String(r.robot_id).padStart(3,'0')}`:'—');
    facts('robotFacts',r?[['阶段',stageLabel(r.stage)],['原始状态',r.stage],['电量',`${r.battery.toFixed(2)}%`],
      ['坐标 / m',`${r.x.toFixed(2)}, ${r.y.toFixed(2)}`],['任务',r.task_id],['货物',r.cargo_id],
      ['实际载货',r.carrying?'是':'否'],['当前位置ID',r.location_id],['充电位',r.dock_id],['完成任务',r.completed],
      ['观察原因',attentionReason(r)||'无额外观察项']]:[['操作','点击地图或机器人列表']]);
  }
  function roster(){
    const fragment=doc.createDocumentFragment();
    for(const r of filteredRobots(state.frame,state.filter,state.search)){
      const b=doc.createElement('button'),label=doc.createElement('span'),small=doc.createElement('small');
      b.type='button';b.classList.toggle('selected',state.selected===r.robot_id);label.textContent=`HR-${String(r.robot_id).padStart(3,'0')} · ${Math.round(r.battery)}%`;small.textContent=stageLabel(r.stage);b.append(label,small);
      b.addEventListener('click',()=>{state.selected=r.robot_id;details();roster();scheduleDraw();});fragment.append(b);
    }
    $('robotList').replaceChildren(fragment);
  }
  function renderFacts(){
    const f=state.frame;if(!f)return;
    const counts=f.counts||{},k=knowledgeFacts(f),meta=f.metadata||{},attention=f.robots.filter(attentionReason);
    text('robotCount',f.robots.length);text('workingCount',`移动 ${counts.moving??0} · 取放 ${counts.handling??0} · 充电 ${counts.charging??0}`);
    text('completedCount',f.metrics?.completed??f.completed);text('taskCountDetail',`全部 ${f.totalTasks??'—'} · 待处理 ${f.pending??'—'} · 未发布 ${f.unreleased??'—'}`);
    text('insideCount',f.inside);text('inventoryDetail',`货架占用 ${f.occupiedSlots?.length??'—'} · 入库暂存 ${f.bufferCounts?.inbound??'—'}`);
    text('attentionCount',attention.length);text('attentionDetail',`故障 / 阻塞 ${attention.filter(r=>/fault|blocked/.test(r.stage)).length} · 低SOC ${attention.filter(r=>!(/fault|blocked/.test(r.stage))).length}`);
    const labels={ready:'准备就绪 · 已暂停',running:'真实仿真运行中',paused:'真实仿真已暂停',complete:'运行已完成',failed:'运行失败 · 现场保留'};
    text('runStatus',labels[f.status]||(f.running?'真实仿真运行中':'真实仿真已暂停'));
    text('liveBadge',f.status==='running'?'LIVE':f.status==='complete'?'DONE':f.status==='failed'?'ERROR':'PAUSED');$('liveBadge').dataset.state=f.status||'paused';
    text('simClock',formatClock(f.elapsed));text('runId',f.runId);text('stageCounts',`移动 ${counts.moving??0} / 取放 ${counts.handling??0} / 等待 ${counts.queued??0} / 充电 ${counts.charging??0} / 待命 ${counts.standby??0}`);
    text('sourceLabel',`${meta.knowledge_mode??'—'} · 图谱周期 ${k.cadence??'—'}模拟秒 + 关键边界`);
    facts('knowledgeFacts',[['持久化seq',k.durable],['图谱已镜像seq',k.mirrored],['镜像滞后 / 批次',k.lag],
      ['图谱已提交模拟时刻',k.committedTime===null?'—':formatClock(k.committedTime)],['当前物理模拟时刻',formatClock(k.physicalTime)],
      ['状态提交周期',k.cadence===null?'—':`${k.cadence}模拟秒 + 关键边界`],['模式 / 存储',`${k.mode} / ${f.knowledge?.backend??'—'}`],['写入错误',k.error||'无']]);
    const dataset=state.datasets.find(d=>d.id===meta.dataset_id);
    text('datasetDetail',`当前运行：${dataset?.label||meta.dataset_id||'固定输入'} · CSV SHA ${meta.dataset_sha256?.slice(0,12)||'—'} · ${f.totalTasks??dataset?.task_count??'—'}项`);
    if(state.pendingDatasetId===null&&meta.dataset_id&&state.datasets.some(d=>d.id===meta.dataset_id))$('datasetSelect').value=meta.dataset_id;
    if($('latestQuery').checked)$('querySeq').value=String(k.mirrored||'');
    if(Number.isFinite(f.speed))$('speedSelect').value=String(f.speed);
    text('runMetadata',JSON.stringify({runId:f.runId,sourceKind:f.sourceKind,metadata:meta,knowledge:f.knowledge},null,2));
    $('emptyMap').hidden=true;details();roster();controls();
  }
  async function loadScene(){
    const epoch=state.epoch,response=await json('/api/scene');if(epoch!==state.epoch)return;
    const scene=response.scene||response;
    if(!(scene.spec?.width>0&&scene.spec?.depth>0)||!Array.isArray(scene.racks))throw new Error('真实场景协议无效');
    state.scene=scene;text('sceneSize',`${scene.spec.width} × ${scene.spec.depth}m · ${scene.racks.length}架 · ${scene.capacity}位 · ${scene.docks?.length??'—'}泊位`);
    const options=[new Option('全部层','all'),...Array.from({length:scene.spec.levels},(_,i)=>new Option(`第${i+1}层`,String(i+1)))];$('levelSelect').replaceChildren(...options);resize();
  }
  function apply(value){
    const next=normalizeFrame(value);if(!acceptsFrame(state.frame,next,state.retiredInstances))return false;
    const previousInstance=state.frame?.runtimeInstanceId;
    const restarted=previousInstance&&next.runtimeInstanceId&&previousInstance!==next.runtimeInstanceId;
    const changed=state.frame&&state.frame.runId!==next.runId;
    if(restarted)state.retiredInstances.add(previousInstance);
    if(restarted||changed)state.epoch++;
    state.frame=next;state.lastReceived=Date.now();
    if(changed||restarted){state.selected=null;state.pendingDatasetId=null;$('queryTask').value='';$('queryGoods').value='';loadScene().catch(e=>toast(e.message,true));}
    if(!next.robots.some(r=>r.robot_id===state.selected))state.selected=next.robots[0]?.robot_id??null;
    renderFacts();scheduleDraw();return true;
  }
  async function refresh(){
    if(state.polling)return;state.polling=true;const epoch=state.epoch;
    try{const value=await json('/api/snapshot');if(epoch===state.epoch)apply(value);}
    finally{state.polling=false;}
  }
  async function control(action,extra={}){
    let payload;try{payload=controlPayload(action,state.frame,extra);}catch(e){toast(e.message,true);return;}
    state.busy=true;controls();if(action==='new_run')state.epoch++;
    try{const value=await json('/api/control',payload);apply(value);toast(action==='step'?'已执行真实模拟单步':action==='new_run'?'已保留旧run并开启新run':'控制已由真实运行进程确认');}
    catch(e){toast(e.message,true);}
    finally{state.busy=false;controls();}
  }
  function socket(){
    clearTimeout(reconnect);if(state.socket&&state.socket.readyState<2)state.socket.close();
    const ws=new WebSocket(`${location.protocol==='https:'?'wss:':'ws:'}//${location.host}/ws/simulation`);state.socket=ws;
    ws.addEventListener('message',event=>{if(state.socket!==ws)return;try{if(apply(JSON.parse(event.data))){state.lastWS=Date.now();text('transportMode','WebSocket · 真实源');$('connectionDot').classList.remove('offline');}}catch(e){toast(e.message,true);}});
    ws.addEventListener('close',()=>{if(state.socket!==ws)return;$('connectionDot').classList.add('offline');text('transportMode','连接重试 · HTTP真实帧后备');reconnect=setTimeout(socket,2000);});
    ws.addEventListener('error',()=>ws.close());
  }
  const colors={moving:'#c9f46c',handling:'#68c7ff',charging:'#79b7e7',idle:'#8ca594',waiting:'#f0a65a',warning:'#ff8274'};
  function metrics(){return projectionMetrics(state.scene,state.camera,width,height);}
  function point(x,y,z=0){return project({x,y},metrics(),z);}
  function polygon(points,fill,stroke='#789d7a44',lineWidth=.6){ctx.beginPath();ctx.moveTo(points[0].x,points[0].y);for(const p of points.slice(1))ctx.lineTo(p.x,p.y);ctx.closePath();if(fill){ctx.fillStyle=fill;ctx.fill();}if(stroke){ctx.lineWidth=lineWidth;ctx.strokeStyle=stroke;ctx.stroke();}}
  function rect(r,fill,z=0){polygon([point(r.x,r.y,z),point(r.x+r.w,r.y,z),point(r.x+r.w,r.y+r.d,z),point(r.x,r.y+r.d,z)],fill);}
  function line(a,b,color='#577b6055',weight=.6){ctx.beginPath();ctx.moveTo(a.x,a.y);ctx.lineTo(b.x,b.y);ctx.strokeStyle=color;ctx.lineWidth=weight;ctx.stroke();}
  function label(value,x,y,z=0,color='#aac0aa'){const p=point(x,y,z);ctx.fillStyle=color;ctx.font='9px Consolas, monospace';ctx.textAlign='center';ctx.fillText(value,p.x,p.y);}
  function prism(r,h,fill='#4c6855',z=0){
    const b=[point(r.x,r.y,z),point(r.x+r.w,r.y,z),point(r.x+r.w,r.y+r.d,z),point(r.x,r.y+r.d,z)],t=[point(r.x,r.y,z+h),point(r.x+r.w,r.y,z+h),point(r.x+r.w,r.y+r.d,z+h),point(r.x,r.y+r.d,z+h)];
    if(state.camera.view!=='top'){
      const m=metrics(),depth=(x,y)=>(x-m.cx)*m.n+(y-m.cy)*m.c;
      const faces=[{p:[b[0],b[1],t[1],t[0]],d:depth(r.x+r.w/2,r.y)},{p:[b[1],b[2],t[2],t[1]],d:depth(r.x+r.w,r.y+r.d/2)},
        {p:[b[2],b[3],t[3],t[2]],d:depth(r.x+r.w/2,r.y+r.d)},{p:[b[3],b[0],t[0],t[3]],d:depth(r.x,r.y+r.d/2)}];
      faces.sort((a,b)=>a.d-b.d).forEach((f,i)=>polygon(f.p,i%2?'#304b39':fill));
    }
    polygon(t,fill);
  }
  function rack(r,boxes){
    const h=state.scene.spec.rackHeight||1.8;prism(r,h,'#47614f');
    for(let level=0;level<state.scene.spec.levels;level++)line(point(r.x,r.y,level*h/state.scene.spec.levels),point(r.x+r.w,r.y,level*h/state.scene.spec.levels),'#a4b49c66');
    for(const b of boxes)prism(b,b.h,'#a98752',b.z);
    label(r.id,r.x+r.w/2,r.y+r.d/2,h+.2,'#cad8bd');
  }
  function robot(r){
    const color=colors[robotCategory(r)],s=metrics().scale,p=point(r.x,r.y),chosen=r.robot_id===state.selected;
    if(state.envelope||chosen)polygon(Array.from({length:24},(_,i)=>point(r.x+Math.cos(i*Math.PI/12)*state.scene.spec.robotRadius,r.y+Math.sin(i*Math.PI/12)*state.scene.spec.robotRadius,.02)),`${color}16`,chosen?color:`${color}66`);
    ctx.fillStyle='#0007';ctx.beginPath();ctx.ellipse(p.x,p.y,Math.max(2,s*.35),Math.max(1,s*.14),0,0,Math.PI*2);ctx.fill();
    if(state.camera.view==='top'){ctx.beginPath();ctx.arc(p.x,p.y,Math.max(2,s*.27),0,Math.PI*2);ctx.fillStyle=color;ctx.fill();}
    else{const hip=point(r.x,r.y,.7),shoulder=point(r.x,r.y,1.25),head=point(r.x,r.y,1.57);ctx.lineWidth=Math.max(1,s*.11);ctx.lineCap='round';ctx.strokeStyle='#b8c7b7';ctx.beginPath();ctx.moveTo(p.x-s*.2,p.y);ctx.lineTo(hip.x-s*.1,hip.y);ctx.moveTo(p.x+s*.2,p.y);ctx.lineTo(hip.x+s*.1,hip.y);ctx.moveTo(shoulder.x-s*.2,shoulder.y);ctx.lineTo(hip.x-s*.35,hip.y);ctx.moveTo(shoulder.x+s*.2,shoulder.y);ctx.lineTo(hip.x+s*.35,hip.y);ctx.stroke();ctx.fillStyle='#b2c2ad';ctx.fillRect(shoulder.x-s*.2,shoulder.y,Math.max(2,s*.4),Math.max(2,hip.y-shoulder.y));ctx.fillStyle=color;ctx.fillRect(shoulder.x-s*.2,shoulder.y+1,Math.max(2,s*.4),Math.max(1,s*.1));ctx.beginPath();ctx.arc(head.x,head.y,Math.max(1.5,s*.15),0,Math.PI*2);ctx.fillStyle='#dbe6d0';ctx.fill();}
    if(r.carrying){ctx.fillStyle='#c29e63';ctx.fillRect(p.x-s*.22,p.y-s*.65,Math.max(2,s*.45),Math.max(2,s*.3));}
    if(chosen)label(`HR-${String(r.robot_id).padStart(3,'0')}`,r.x,r.y,2.15,color);
  }
  function draw(){
    drawPending=false;ctx.clearRect(0,0,width,height);if(!state.scene)return;
    const scene=state.scene,m=metrics();rect({x:0,y:0,w:scene.spec.width,d:scene.spec.depth},'#152219');
    for(let x=0;x<=scene.spec.width;x+=6)line(point(x,0),point(x,scene.spec.depth),'#3e5b462c');for(let y=0;y<=scene.spec.depth;y+=6)line(point(0,y),point(scene.spec.width,y),'#3e5b462c');
    for(const buffer of scene.buffers||[]){rect(buffer,buffer.id==='IB'?'#4b55303b':'#384b663b');label(buffer.id==='IB'?'INBOUND':'OUTBOUND',buffer.x+buffer.w/2,buffer.y+buffer.d/2);}
    for(const bay of scene.docks||[])rect(bay,'#29472e66');
    for(const station of scene.stations||[]){const p=point(station.x,station.y);ctx.fillStyle=station.kind==='inbound'?'#c9f46c':'#68c7ff';ctx.fillRect(p.x-2,p.y-2,4,4);label(station.id,station.x,station.y,.25,ctx.fillStyle);}
    const boxes=slotBoxes(scene,state.frame||{},state.level),byRack=new Map();for(const b of boxes){if(!byRack.has(b.rack_id))byRack.set(b.rack_id,[]);byRack.get(b.rack_id).push(b);}
    const depth=o=>(o.x-m.cx)*m.n+(o.y-m.cy)*m.c;
    const objects=[...scene.racks.map(r=>({x:r.x+r.w/2,y:r.y+r.d/2,draw:()=>rack(r,byRack.get(r.id)||[])})),...filteredRobots(state.frame,state.filter,state.search).map(r=>({x:r.x,y:r.y,draw:()=>robot(r)}))];
    objects.sort((a,b)=>depth(a)-depth(b)).forEach(o=>o.draw());
  }
  function scheduleDraw(){if(!drawPending){drawPending=true;requestAnimationFrame(draw);}}
  function resize(){const box=stage.getBoundingClientRect();width=Math.max(1,box.width);height=Math.max(1,box.height);const ratio=globalThis.devicePixelRatio||1;canvas.width=Math.round(width*ratio);canvas.height=Math.round(height*ratio);ctx.setTransform(ratio,0,0,ratio,0,0);scheduleDraw();}
  $('startButton').addEventListener('click',()=>control('start'));$('pauseButton').addEventListener('click',()=>control('pause'));$('stepButton').addEventListener('click',()=>control('step'));
  $('speedSelect').addEventListener('change',()=>control('speed',{value:Number($('speedSelect').value)}));
  $('newRunButton').addEventListener('click',()=>{const id=$('datasetSelect').value;if(!id)return toast('请选择服务端已验证的数据集',true);if(confirm('将保存当前run记录，并使用所选固定数据集开启新的独立run。继续？'))control('new_run',{dataset_id:id});});
  $('datasetSelect').addEventListener('change',()=>{state.pendingDatasetId=$('datasetSelect').value;});
  $('refreshButton').addEventListener('click',()=>refresh().catch(e=>toast(e.message,true)));
  $('robotSearch').addEventListener('input',()=>{state.search=$('robotSearch').value;roster();scheduleDraw();});
  for(const button of $('filterList').querySelectorAll('button'))button.addEventListener('click',()=>{state.filter=button.dataset.filter;for(const b of $('filterList').querySelectorAll('button'))b.classList.toggle('active',b===button);roster();scheduleDraw();});
  for(const [id,view] of [['viewIso','iso'],['viewTop','top']])$(id).addEventListener('click',()=>{state.camera.view=view;if(view==='top')state.camera.yaw=0;else{state.camera.yaw=Math.PI/4;state.camera.pitch=Math.PI/6;}$('viewIso').classList.toggle('active',view==='iso');$('viewTop').classList.toggle('active',view==='top');scheduleDraw();});
  $('levelSelect').addEventListener('change',()=>{state.level=$('levelSelect').value;scheduleDraw();});
  $('envelopeToggle').addEventListener('click',()=>{state.envelope=!state.envelope;$('envelopeToggle').setAttribute('aria-pressed',String(state.envelope));scheduleDraw();});
  $('resetView').addEventListener('click',()=>{state.camera=newCamera();$('viewIso').classList.add('active');$('viewTop').classList.remove('active');scheduleDraw();});
  for(const [id,delta] of [['zoomIn',-1],['zoomOut',1]])$(id).addEventListener('click',()=>{if(!state.scene)return;zoomCamera(state.camera,delta,{x:width/2,y:height/2},state.scene,width,height);scheduleDraw();});
  canvas.addEventListener('contextmenu',e=>e.preventDefault());
  canvas.addEventListener('pointerdown',e=>{if(![0,1,2].includes(e.button))return;e.preventDefault();drag={button:e.button,shift:e.shiftKey,x:e.clientX,y:e.clientY,startX:e.clientX,startY:e.clientY};canvas.setPointerCapture(e.pointerId);});
  canvas.addEventListener('pointermove',e=>{if(!drag)return;dragCamera(state.camera,drag.button,e.clientX-drag.x,e.clientY-drag.y,drag.shift);drag.x=e.clientX;drag.y=e.clientY;scheduleDraw();});
  canvas.addEventListener('pointerup',e=>{if(drag?.button===0&&!drag.shift&&Math.hypot(e.clientX-drag.startX,e.clientY-drag.startY)<4&&state.scene){const box=canvas.getBoundingClientRect(),mouse={x:e.clientX-box.left,y:e.clientY-box.top},m=metrics();const nearest=filteredRobots(state.frame,state.filter,state.search).map(r=>({r,d:Math.hypot(project(r,m).x-mouse.x,project(r,m).y-mouse.y)})).sort((a,b)=>a.d-b.d)[0];if(nearest?.d<18){state.selected=nearest.r.robot_id;details();roster();scheduleDraw();}}drag=null;});
  canvas.addEventListener('pointercancel',()=>{drag=null;});
  canvas.addEventListener('wheel',e=>{e.preventDefault();if(!state.scene)return;const b=canvas.getBoundingClientRect();zoomCamera(state.camera,e.deltaY,{x:e.clientX-b.left,y:e.clientY-b.top},state.scene,width,height);scheduleDraw();},{passive:false});
  $('fillSelected').addEventListener('click',()=>{const r=selected();if(!r)return;$('queryRobot').value=String(r.robot_id);$('queryTask').value=r.task_id||'';$('queryGoods').value=r.cargo_id||'';});
  $('latestQuery').addEventListener('change',()=>{$('querySeq').disabled=$('latestQuery').checked;if($('latestQuery').checked&&state.frame)$('querySeq').value=String(state.frame.knowledge?.mirrored_seq||'');});
  $('runQuery').addEventListener('click',async()=>{const frame=state.frame;if(!frame)return;try{const request=buildQuery($('queryKind').value,frame,{robotId:Number($('queryRobot').value),taskId:$('queryTask').value.trim(),goodsId:$('queryGoods').value.trim(),seq:$('latestQuery').checked?null:Number($('querySeq').value),variant:$('queryVariant').value});text('queryRequest',JSON.stringify(request,null,2));text('queryStatus',`查询 ${request.run_id} / 已提交seq ${request.seq}…`);$('runQuery').disabled=true;const value=await json('/api/knowledge/query',request);text('queryResult',JSON.stringify(value,null,2));text('queryStatus',`已返回 ${request.query_id} · cutoff seq=${request.seq}；与当前物理帧分开记录。`);}catch(e){text('queryStatus',e.message);text('queryResult',JSON.stringify({error:e.message},null,2));}finally{controls();}});
  $('exportButton').addEventListener('click',()=>{if(!state.frame)return;const url=URL.createObjectURL(new Blob(['\ufeff'+exportRows(state.frame)],{type:'text/csv;charset=utf-8'})),a=doc.createElement('a');a.href=url;a.download=`${state.frame.runId}-t${state.frame.tick}.csv`;a.click();setTimeout(()=>URL.revokeObjectURL(url),1000);});
  globalThis.addEventListener('resize',resize);
  setInterval(()=>{if(state.frame){const stamp=state.frame.capturedAtMs??state.frame.generatedAtMs;const age=Number.isFinite(stamp)?Math.max(0,Date.now()-stamp):Math.max(0,Date.now()-state.lastReceived);text('frameAge',`真实帧龄 ${Math.round(age)}ms`);}if(Date.now()-state.lastWS>1800)refresh().then(()=>{text('transportMode','HTTP · 真实源后备');}).catch(e=>{text('transportMode',e.message);$('connectionDot').classList.add('offline');});},1000);
  const labels={self:'机器人上下文',readiness:'可见性 / 就绪',task:'任务状态与协作',goods:'货物持有链',energy:'能量与充电',assistance:'协作观察',progress:'任务进展',memory:'机器人历史记忆',local_awareness:'区域态势',global:'全局态势',similar_history:'相似历史（向量）'};
  Promise.allSettled([loadScene(),refresh(),json('/api/health').then(v=>{state.health=v;}),json('/api/datasets').then(value=>{state.datasets=normalizeDatasets(value);$('datasetSelect').replaceChildren(...state.datasets.map(d=>new Option(`${d.label||d.id}${d.task_count!==undefined?` · ${d.task_count}项`:''}`,d.id)));if(value.selected_dataset_id)$('datasetSelect').value=value.selected_dataset_id;controls();if(state.frame)renderFacts();}),json('/api/knowledge/catalog').then(value=>{const queries=value.queries||{};const keys=Array.isArray(queries)?queries.map(q=>typeof q==='string'?q:q.query_id||q.id):Object.keys(queries);if(keys.length)$('queryKind').replaceChildren(...keys.map(key=>new Option(labels[key]||key,key)));})]).then(results=>{for(const result of results)if(result.status==='rejected')toast(result.reason.message,true);socket();resize();});
  return {state,apply,refresh,control};
}
if(typeof document!=='undefined'&&document.getElementById('liveCanvas'))startLiveConsole();
