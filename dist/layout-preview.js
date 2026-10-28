/* Reuses the existing app.js visual language: yaw/pitch Canvas projection, prisms,
   three-level racks and humanoid glyphs. This page never connects to /api or /ws. */
import {SPEC,createLayout,robotPositions,exampleRoutes,routeLength,contains} from './layout100-model.mjs';

const $=id=>document.getElementById(id);
const canvas=$('layoutCanvas'),stage=$('mapStage');
let ctx=canvas.getContext('2d'),labelScale=1;
const colors={lime:'#c9f46c',blue:'#68c7ff',orange:'#f0a65a',neutral:'#91a499',rack:'#597665',floor:'#18251c',grid:'#2b3c30'};
const state={view:'iso',zoom:1,panX:0,panY:0,yaw:-Math.PI/6,pitch:Math.PI/3.6,charging:'split',inbound:'edge',mode:'rotation',route:'inbound',selected:null,envelope:false};
let layout,robots,routes,width=1,height=1,metrics,queued=false,drag=null;
const clamp=(v,min,max)=>Math.max(min,Math.min(max,v));
const pad=(n,len=3)=>String(n).padStart(len,'0');
const runtimeMode=new URLSearchParams(location.search).has('run');
let runtimeFrame=null,runtimeOccupied=new Set();

function projectionMetrics(){
  if(state.view==='top'){
    const s=Math.min((width-90)/SPEC.width,(height-90)/SPEC.depth)*state.zoom;
    return {s,ox:(width-SPEC.width*s)/2+state.panX,oy:(height-SPEC.depth*s)/2+state.panY};
  }
  const c=Math.cos(state.yaw),n=Math.sin(state.yaw),sp=Math.sin(state.pitch),cp=Math.cos(state.pitch);
  const rw=Math.abs(SPEC.width*c)+Math.abs(SPEC.depth*n),rd=Math.abs(SPEC.width*n)+Math.abs(SPEC.depth*c);
  const s=Math.min((width-85)/rw,(height-105)/(rd*sp+3*cp))*state.zoom;
  return {s,c,n,sp,cp,ox:width/2+state.panX,oy:height/2+1.6*s+state.panY};
}
function project(x,y,z=0){
  const m=metrics;
  if(state.view==='top')return {x:m.ox+x*m.s,y:m.oy+y*m.s};
  const a=x-30,b=y-24;
  return {x:m.ox+(a*m.c-b*m.n)*m.s,y:m.oy+(a*m.n+b*m.c)*m.s*m.sp-z*m.s*m.cp};
}
function unproject(x,y){
  const m=metrics;
  if(state.view==='top')return {x:(x-m.ox)/m.s,y:(y-m.oy)/m.s};
  const u=(x-m.ox)/m.s,v=(y-m.oy)/(m.s*m.sp);
  return {x:30+u*m.c+v*m.n,y:24-u*m.n+v*m.c};
}
function depth(x,y){return state.view==='top'?y:(x-30)*metrics.n+(y-24)*metrics.c;}
function polygon(points,fill,stroke,lineWidth=1){
  ctx.beginPath();ctx.moveTo(points[0].x,points[0].y);for(const p of points.slice(1))ctx.lineTo(p.x,p.y);ctx.closePath();
  if(fill){ctx.fillStyle=fill;ctx.fill();}if(stroke){ctx.strokeStyle=stroke;ctx.lineWidth=lineWidth;ctx.stroke();}
}
function floorRect(r,fill,stroke,z=0){polygon([project(r.x,r.y,z),project(r.x+r.w,r.y,z),project(r.x+r.w,r.y+r.d,z),project(r.x,r.y+r.d,z)],fill,stroke,.7);}
function line(a,b,color,lineWidth=1,dash=[]){ctx.beginPath();ctx.moveTo(a.x,a.y);ctx.lineTo(b.x,b.y);ctx.strokeStyle=color;ctx.lineWidth=lineWidth;ctx.setLineDash(dash);ctx.stroke();ctx.setLineDash([]);}
function prism(x,y,w,d,h,palette,z=0){
  const b=[project(x,y,z),project(x+w,y,z),project(x+w,y+d,z),project(x,y+d,z)];
  if(state.view==='top'){polygon(b,palette.top,'#adc4ad24',.45);return;}
  const t=[project(x,y,z+h),project(x+w,y,z+h),project(x+w,y+d,z+h),project(x,y+d,z+h)];
  const faces=[{p:[b[0],b[1],t[1],t[0]],c:palette.left,k:depth(x+w/2,y)},{p:[b[1],b[2],t[2],t[1]],c:palette.right,k:depth(x+w,y+d/2)},{p:[b[2],b[3],t[3],t[2]],c:palette.left,k:depth(x+w/2,y+d)},{p:[b[3],b[0],t[0],t[3]],c:palette.right,k:depth(x,y+d/2)}];
  faces.sort((a,b)=>a.k-b.k).forEach(f=>polygon(f.p,f.c,'#adc4ad18',.4));polygon(t,palette.top,'#adc4ad35',.45);
}
function tag(text,x,y,{color='#c4d3c4',z=0,size=11}={}){
  const p=project(x,y,z);ctx.save();ctx.font=`${size*labelScale}px "Microsoft YaHei",sans-serif`;ctx.textAlign='center';ctx.textBaseline='middle';
  const w=ctx.measureText(text).width+13*labelScale;ctx.fillStyle='#0c160fe8';ctx.fillRect(p.x-w/2,p.y-10*labelScale,w,20*labelScale);ctx.fillStyle=color;ctx.fillText(text,p.x,p.y);ctx.restore();
}
function arrow(a,b,color){
  const pa=project(a.x,a.y,.04),pb=project(b.x,b.y,.04),ang=Math.atan2(pb.y-pa.y,pb.x-pa.x);
  line(pa,pb,color,.8);const n=4;line(pb,{x:pb.x-Math.cos(ang-.5)*n,y:pb.y-Math.sin(ang-.5)*n},color,.8);line(pb,{x:pb.x-Math.cos(ang+.5)*n,y:pb.y-Math.sin(ang+.5)*n},color,.8);
}
function drawFloor(){
  floorRect({x:0,y:0,w:60,d:48},colors.floor,'#73876b');
  for(let x=0;x<=60;x+=2)line(project(x,0),project(x,48),x%10===0?'#3a513b':'#293b2d',x%10===0?.65:.4);
  for(let y=0;y<=48;y+=2)line(project(0,y),project(60,y),y%10===0?'#3a513b':'#293b2d',y%10===0?.65:.4);
  for(const t of layout.trunks)floorRect(t,'#94b19a0b',null);
  for(const z of layout.zones){
    const color=colors[z.color]||colors.neutral;
    floorRect(z,`${color}0e`,`${color}60`);
  }
  for(const g of layout.groups){
    floorRect(g.approach,'#68c7ff08','#68c7ff35');
    const x=g.approach.x+g.approach.w/2;
    for(let y=g.y+2;y<g.y+g.count*1.2;y+=4)arrow({x,y:y+.7},{x,y:y-.7},'#68c7ff70');
  }
  for(const y of [14,26.4,39.2]){
    line(project(5,y),project(55,y),'#92a98545',.65,[7,9]);
    for(const x of [13,23,37,47]){arrow({x:x-.7,y:y-.6},{x:x+.7,y:y-.6},'#b1c7a260');arrow({x:x+.7,y:y+.6},{x:x-.7,y:y+.6},'#b1c7a260');}
  }
  for(const x of [6.8,30,53.2])line(project(x,12),project(x,41.1),'#92a98545',.65,[7,9]);
  for(const h of layout.homes){floorRect(h,'#9eb2a00a','#7b947c60');if(state.zoom>1.7)tag(h.id,h.point.x,h.point.y,{size:10});}
  for(const d of layout.docks){
    floorRect(d,'#68c7ff13','#68c7ff80');
    const chargerX=d.side==='west'?d.x+.08:d.x+d.w-.3;
    floorRect({x:chargerX,y:d.y+.2,w:.22,d:.8},'#68c7ff65','#68c7ff');
    if(state.zoom>1.8)tag(d.id,d.point.x,d.point.y,{color:colors.blue,size:10});
  }
  for(const s of layout.stations){
    const color=s.kind==='inbound'?colors.lime:colors.orange;
    floorRect({x:s.x-.65,y:s.y-.65,w:1.3,d:1.3},`${color}17`,`${color}a0`);
    if(state.view==='top'||state.zoom>1.6)tag(s.id.replace('IN-','I').replace('OUT-','O'),s.x,s.y,{color,size:10});
  }
  for(const b of layout.buffers)floorRect(b,b.id==='IB'?'#c9f46c0a':'#f0a65a0a','#84947760');
  const plate=state.inbound==='edge'?'沿边分布 / 8点':'角落集中 / 8点';
  tag(`收货 INBOUND · ${plate}`,16.4,47.8,{color:colors.lime});
  tag('发货 OUTBOUND · 8点',43.6,47.8,{color:colors.orange});
  line(project(7.8,49.5),project(52.2,49.5),'#869b76',.7);
  arrow({x:17,y:49.2},{x:17,y:47.1},colors.lime);arrow({x:43,y:47.1},{x:43,y:49.2},colors.orange);
}
function drawRack(r){
  const selected=state.selected?.id===r.id;
  if(state.view==='top'){
    floorRect(r,'#3d5745',selected?colors.lime:'#819b77');
    for(let i=1;i<8;i++)line(project(r.x+i*.6,r.y),project(r.x+i*.6,r.y+r.d),'#9cb18a60',.4);
    line(project(r.x,r.y+.6),project(r.x+r.w,r.y+.6),'#b2caa250',.4);
    if(metrics.s>9)tag(r.id,r.x+r.w/2,r.y+.6,{size:10,color:selected?colors.lime:'#b8ccac'});
    return;
  }
  for(let lev=0;lev<3;lev++){
    const z=.15+lev*.55;
    prism(r.x,r.y,r.w,r.d,.055,{top:'#465f45',left:'#263a2b',right:'#324b34'},z);
    for(let face=0;face<2;face++)for(let bay=0;bay<8;bay++){
      if(runtimeMode?!runtimeOccupied.has(`${r.id}-L${lev+1}-C${bay+1}-F${face+1}`):((r.column*17+r.row*29+lev*11+face*7+bay*3)%13)>6)continue;
      const k=(bay+r.row+lev)%4;
      const palettes=[{top:'#b99464',left:'#7e5b37',right:'#9b7549'},{top:'#829889',left:'#485e52',right:'#637c6a'},{top:'#789ea0',left:'#435f65',right:'#5b7e82'},{top:'#9b9c6e',left:'#686a42',right:'#848450'}];
      prism(r.x+bay*.6+.08,r.y+face*.6+.1,.44,.4,.34,palettes[k],z+.055);
    }
    for(const y of [r.y,r.y+r.d])line(project(r.x,y,z+.06),project(r.x+r.w,y,z+.06),selected?colors.lime:'#759475',.7);
  }
  for(const x of [r.x,r.x+r.w])for(const y of [r.y,r.y+r.d])line(project(x,y,0),project(x,y,1.8),selected?colors.lime:'#8ba183',1);
}
function drawCargoBuffer(b){
  const dx=b.w/8,dy=b.d/6;
  for(let r=0;r<6;r++)for(let c=0;c<8;c++){
    floorRect({x:b.x+c*dx+.07,y:b.y+r*dy+.06,w:dx-.14,d:dy-.12},null,'#9ea89040');
    if(runtimeMode?r*8+c<(runtimeFrame?.bufferCounts?.[b.id==='IB'?'inbound':'outbound']||0):(r*8+c)%3===0)prism(b.x+c*dx+.13,b.y+r*dy+.09,.4,.35,.3,{top:'#a18b59',left:'#635437',right:'#837043'});
  }
}
function drawRobot(robot){
  const p=project(robot.x,robot.y),s=metrics.s,color=colors[robot.status==='working'?'lime':robot.status==='charging'?'blue':robot.status==='waiting'?'orange':'neutral'];
  const selected=state.selected?.id===`HR-${pad(robot.id)}`;
  ctx.save();ctx.lineCap='round';
  ctx.fillStyle='#00000045';ctx.beginPath();ctx.ellipse(p.x,p.y,.38*s,.16*s,0,0,Math.PI*2);ctx.fill();
  if(state.envelope||selected){
    const points=Array.from({length:32},(_,i)=>project(robot.x+Math.cos(i*Math.PI/16)*SPEC.robotRadius,robot.y+Math.sin(i*Math.PI/16)*SPEC.robotRadius,.015));
    polygon(points,`${color}12`,`${color}95`,.75);
  }
  if(state.view==='top'){
    ctx.beginPath();ctx.arc(p.x,p.y,Math.max(2,.25*s),0,Math.PI*2);ctx.fillStyle=color;ctx.fill();
    ctx.strokeStyle='#e2ecda';ctx.lineWidth=.6;ctx.stroke();
  }else{
    const foot=project(robot.x,robot.y,.05),hip=project(robot.x,robot.y,.7),shoulder=project(robot.x,robot.y,1.22),head=project(robot.x,robot.y,1.55);
    ctx.strokeStyle='#c2d1c2';ctx.lineWidth=Math.max(1,s*.105);
    ctx.beginPath();ctx.moveTo(foot.x-s*.19,foot.y);ctx.lineTo(hip.x-s*.12,hip.y);ctx.moveTo(foot.x+s*.19,foot.y);ctx.lineTo(hip.x+s*.12,hip.y);ctx.stroke();
    const bodyW=s*.42,bodyH=Math.max(2,hip.y-shoulder.y);
    ctx.fillStyle='#b3c3b3';ctx.fillRect(shoulder.x-bodyW/2,shoulder.y,bodyW,bodyH);
    ctx.fillStyle=color;ctx.fillRect(shoulder.x-bodyW/2,shoulder.y+bodyH*.27,bodyW,Math.max(1,s*.08));
    ctx.strokeStyle='#bac9b8';ctx.beginPath();ctx.moveTo(shoulder.x-bodyW/2,shoulder.y+1);ctx.lineTo(shoulder.x-s*.34,hip.y-1);ctx.moveTo(shoulder.x+bodyW/2,shoulder.y+1);ctx.lineTo(shoulder.x+s*.34,hip.y-1);ctx.stroke();
    ctx.beginPath();ctx.arc(head.x,head.y,Math.max(1.6,s*.16),0,Math.PI*2);ctx.fillStyle='#dce7d4';ctx.fill();
    if(runtimeMode?robot.carrying:robot.status==='working'&&robot.id%3===0){ctx.fillStyle='#bf955a';ctx.fillRect(hip.x-s*.26,hip.y-s*.25,s*.52,s*.3);}
  }
  if(selected)tag(`HR-${pad(robot.id)}`,robot.x,robot.y,{z:2.3,color});
  ctx.restore();
}
function drawRoute(){
  const route=routes.find(r=>r.id===state.route);if(!route?.points)return;
  const points=route.points.map(p=>project(p.x,p.y,.06)),color=colors[route.color];
  ctx.save();ctx.beginPath();ctx.moveTo(points[0].x,points[0].y);for(const p of points.slice(1))ctx.lineTo(p.x,p.y);
  ctx.strokeStyle='#0a120a';ctx.lineWidth=4;ctx.stroke();ctx.strokeStyle=color;ctx.lineWidth=1.5;ctx.setLineDash([5,4]);ctx.stroke();ctx.setLineDash([]);
  for(const [i,label] of [[0,'起点'],[points.length-1,'终点']]){
    const p=points[i];ctx.beginPath();ctx.arc(p.x,p.y,3.5,0,Math.PI*2);ctx.fillStyle=color;ctx.fill();
    ctx.font='10px "Microsoft YaHei",sans-serif';ctx.fillText(label,p.x+6,p.y-6);
  }
  for(let i=1;i<route.points.length;i++){
    const a=route.points[i-1],b=route.points[i],len=Math.hypot(a.x-b.x,a.y-b.y);
    if(len>3){const t=.6,dx=(b.x-a.x)/len,dy=(b.y-a.y)/len,c={x:a.x+(b.x-a.x)*t,y:a.y+(b.y-a.y)*t};arrow({x:c.x-dx*.7,y:c.y-dy*.7},{x:c.x+dx*.7,y:c.y+dy*.7},color);}
  }
  ctx.restore();
}
function drawDimensions(){
  const a=project(0,-2.5),b=project(60,-2.5),c=project(-2.8,0),d=project(-2.8,48);
  line(a,b,'#9bb48a',.7);line(c,d,'#9bb48a',.7);
  for(const [x,y]of [[0,-3],[60,-3]])line(project(x,y),project(x,y+1),'#9bb48a',.7);
  for(const [x,y]of [[-3.3,0],[-3.3,48]])line(project(x,y),project(x+1,y),'#9bb48a',.7);
  tag('60 m',30,-2.5);tag('48 m',-2.8,24);
}
function drawLabels(){
  tag('初始 / 待命 · 100泊位',30,.15);
  tag('互助交接预留',4,6.1,{color:colors.blue,size:10});
  tag('维护 / 扩展预留',55.5,1.2,{size:10});
  for(const g of layout.groups){
    tag(`充电 ${g.id} · ${g.count}位`,g.side==='west'?2.8:57.2,g.y-1.2,{color:colors.blue});
  }
  if(state.view==='top'){
    for(const [text,x,y]of [['西北库区',18.4,14.6],['东北库区',41.6,14.6],['西南库区',18.4,27.2],['东南库区',41.6,27.2]])tag(text,x,y,{color:'#aebfaa',size:10});
    tag('中央主通道 · 4m',30,26.4,{size:11});
    tag('货架净巷道 · 2.4m',18.4,22,{size:10});
  }
  if(state.selected?.kind==='zone')floorRect(state.selected,null,colors[state.selected.color]||colors.lime,.02);
  if(state.selected?.kind==='dock')floorRect(state.selected,'#68c7ff20',colors.blue,.02);
}
function draw(){
  queued=false;metrics=projectionMetrics();ctx.clearRect(0,0,width,height);ctx.fillStyle='#0d1511';ctx.fillRect(0,0,width,height);drawFloor();drawRoute();
  const objects=[...layout.racks.map(r=>({k:depth(r.x+r.w/2,r.y+r.d/2),draw:()=>drawRack(r)})),...layout.buffers.map(b=>({k:depth(b.x+b.w/2,b.y+b.d/2),draw:()=>drawCargoBuffer(b)})),...robots.map(r=>({k:depth(r.x,r.y),draw:()=>drawRobot(r)}))];
  objects.sort((a,b)=>a.k-b.k).forEach(o=>o.draw());drawLabels();drawDimensions();
}
function requestDraw(){if(!queued){queued=true;requestAnimationFrame(draw);}}
function resize(){
  const b=stage.getBoundingClientRect(),dpr=Math.min(2,window.devicePixelRatio||1);width=b.width;height=b.height;
  canvas.width=Math.round(width*dpr);canvas.height=Math.round(height*dpr);ctx.setTransform(dpr,0,0,dpr,0,0);requestDraw();
}
function setSelection(title,text,facts,object=null){
  state.selected=object;$('selectionTitle').textContent=title;$('selectionText').textContent=text;
  $('selectionFacts').replaceChildren(...facts.map(([k,v])=>{const div=document.createElement('div'),dt=document.createElement('dt'),dd=document.createElement('dd');dt.textContent=k;dd.textContent=v;div.append(dt,dd);return div;}));requestDraw();
}
function selectObject(o){
  if(o.kind==='dock')setSelection(o.id,'一位只容纳一台机器人。蓝色细条是充电器示意，矩形是完整静态停靠包络，外侧另有2m进出带。',[['静态停靠','1.2 × 1.5m'],['单个位面积','1.8㎡'],['连同进出带分摊','4.2㎡'],['设备/姿态参数','待机型标定']],o);
  else if(o.kind==='rack')setSelection(`${o.id} / ${o.zone}库区`,'两侧均有外侧取放面。箱位不是可通行位置；上下架由机器人自身完成，货架高1.8m、层中心约0.4/0.95/1.5m。',[['占地','4.8 × 1.2m'],['单组容量','8列 × 双面 × 3层 = 48'],['附近净巷道','2.4m'],['三层可达性','须按机器人类型复核']],o);
  else if(o.kind==='zone')setSelection(o.name,o.detail,[['区域尺寸',`${o.w.toFixed(1)} × ${o.d.toFixed(1)}m`],['几何面积',`${(o.w*o.d).toFixed(1)}㎡`],['性质','设计预留 / 非运行读数']],o);
  else if(o.kind==='home')setSelection(o.id,'独立的初始/待命泊位，不带充电能力。停车区与充电区分开，100台初始化时各占不同位置。',[['泊位尺寸','1.2 × 1.5m'],['单个位面积','1.8㎡'],['停车区泊位','100个'],['出入通行带','2m']],o);
}
function refresh(){
  layout=createLayout(state);robots=robotPositions(layout,state.mode);routes=exampleRoutes(layout);state.selected=null;
  for(const [s,id]of [['working','workingCount'],['charging','chargingCount'],['standby','standbyCount']])$(id).textContent=robots.filter(r=>r.status===s).length;
  $('totalCount').textContent=robots.length;
  $('chargeSummary').textContent=state.charging==='split'?'12＋12 · 两处充电位置':'24位 · 西侧集中布置';
  updateRouteInfo();setSelection('整体布局','固定货架＋四个存储分区；收发货同侧分开，充电与待命分开。点击货架、充电位或区域查看尺寸。',[['主通道 / 中央纵通道','4m'],['货架间净巷道','2.4m'],['机器人占用包络','直径1.1m*']]);
}
function updateRouteInfo(){
  const r=routes.find(r=>r.id===state.route);
  $('routeInfo').textContent=r?(r.points?`${routeLength(r.points).toFixed(1)} m · ${r.targetId} · 静态避障`:'路径不可达'):'仅显示空间布局';
}
function changeZoom(amount,centerX=width/2,centerY=height/2){
  const before=projectionMetrics(),oldZoom=state.zoom;
  state.zoom=clamp(state.zoom*amount,.7,5);
  const ratio=state.zoom/oldZoom,after=projectionMetrics();
  state.panX+=centerX-(after.ox+(centerX-before.ox)*ratio);
  state.panY+=centerY-(after.oy+(centerY-before.oy)*ratio);
  requestDraw();
}
function reset(){state.zoom=1;state.panX=0;state.panY=0;state.yaw=-Math.PI/6;state.pitch=Math.PI/3.6;requestDraw();}
for(const b of document.querySelectorAll('[data-view]'))b.addEventListener('click',()=>{state.view=b.dataset.view;document.querySelectorAll('[data-view]').forEach(x=>x.setAttribute('aria-pressed',String(x===b)));reset();});
for(const b of document.querySelectorAll('[data-route]'))b.addEventListener('click',()=>{state.route=b.dataset.route;document.querySelectorAll('[data-route]').forEach(x=>x.setAttribute('aria-pressed',String(x===b)));updateRouteInfo();requestDraw();});
$('chargingLayout').addEventListener('change',e=>{state.charging=e.target.value;reset();refresh();});
$('inboundLayout').addEventListener('change',e=>{state.inbound=e.target.value;refresh();});
$('rosterMode').addEventListener('change',e=>{state.mode=e.target.value;robots=robotPositions(layout,state.mode);for(const [s,id]of [['working','workingCount'],['charging','chargingCount'],['standby','standbyCount']])$(id).textContent=robots.filter(r=>r.status===s).length;requestDraw();});
$('showEnvelope').addEventListener('change',e=>{state.envelope=e.target.checked;requestDraw();});
$('zoomOut').addEventListener('click',()=>changeZoom(1/1.3));
$('zoomIn').addEventListener('click',()=>changeZoom(1.3));
$('resetCamera').addEventListener('click',reset);
$('focusCharge').addEventListener('click',()=>{
  state.zoom=3;state.panX=0;state.panY=0;metrics=projectionMetrics();
  const g=layout.groups[0],p=project(g.x+1.75,g.y+Math.min(g.count,12)*.6);
  state.panX+=width/2-p.x;state.panY+=height/2-p.y;
  selectObject(layout.docks[4]);requestDraw();
});
canvas.addEventListener('wheel',e=>{
  e.preventDefault();if(!e.deltaY)return;
  const bounds=canvas.getBoundingClientRect();
  changeZoom(e.deltaY<0?1.08:.92,e.clientX-bounds.left,e.clientY-bounds.top);
},{passive:false});
canvas.addEventListener('pointerdown',e=>{
  if(e.button>2||drag)return;
  e.preventDefault();
  drag={x:e.clientX,y:e.clientY,px:state.panX,py:state.panY,yaw:state.yaw,pitch:state.pitch,button:e.button,moved:false,pan:e.button!==0||e.shiftKey||state.view==='top'};
  canvas.setPointerCapture(e.pointerId);
});
canvas.addEventListener('pointermove',e=>{
  if(!drag)return;const dx=e.clientX-drag.x,dy=e.clientY-drag.y;drag.moved ||= Math.hypot(dx,dy)>4;
  if(drag.pan){state.panX=drag.px+dx;state.panY=drag.py+dy;}else{state.yaw=drag.yaw+dx*.009;state.pitch=clamp(drag.pitch-dy*.006,.16,1.23);}requestDraw();
});
canvas.addEventListener('pointercancel',()=>{drag=null;});
canvas.addEventListener('pointerup',e=>{
  if(!drag)return;const click=!drag.moved&&drag.button===0&&e.button===0;drag=null;if(!click)return;
  const b=canvas.getBoundingClientRect(),x=e.clientX-b.left,y=e.clientY-b.top;
  const hits=robots.map(r=>{const p=project(r.x,r.y,state.view==='top'?0:.8);return {r,dist:Math.hypot(p.x-x,p.y-y)};}).filter(h=>h.dist<Math.max(6,metrics.s*.5)).sort((a,b)=>a.dist-b.dist);
  if(hits.length){const r=hits[0].r;
    if(runtimeMode){setSelection(`HR-${pad(r.id)}`,`执行状态：${r.stage}；这是运行采样，不是演示分布。`,[['坐标',`${r.x.toFixed(1)}, ${r.y.toFixed(1)}m`],['电量',`${r.battery.toFixed(1)}%`],['任务',r.task_id||'—'],['累计完成',String(r.completed)]],{id:`HR-${pad(r.id)}`,kind:'robot'});return;}
    setSelection(`HR-${pad(r.id)}`,`这是${r.status==='working'?'作业':r.status==='charging'?'充电':'待命'}位置示意，没有连接实时机器人状态。`,[['坐标',`${r.x.toFixed(1)}, ${r.y.toFixed(1)}m`],['示意位置',r.dock||r.home||'通道作业区'],['物理占用包络','直径1.1m'],['运行状态来源','离线布局示意']],{id:`HR-${pad(r.id)}`,kind:'robot'});return;}
  const w=unproject(x,y),o=[...layout.docks,...layout.racks,...layout.homes,...layout.zones].find(r=>contains(r,w.x,w.y));if(o)selectObject(o);
});
canvas.addEventListener('contextmenu',e=>e.preventDefault());
$('downloadDesign').addEventListener('click',e=>{
  e.preventDefault();const data={...layout,designOnly:true,notBackendCompatible:true,assumptions:{body:[.7,.7],planningRadius:.55,workHours:4,chargeHours:1,chargingPorts:24,capacityIncludesBuffers:true},roster:robots,staticRoutes:routes};
  const u=URL.createObjectURL(new Blob([JSON.stringify(data,null,2)],{type:'application/json'})),a=document.createElement('a');a.href=u;a.download=`warehouse100-${state.charging}-${state.inbound}.json`;a.click();setTimeout(()=>URL.revokeObjectURL(u),1000);
});
$('exportImage').addEventListener('click',()=>{
  const saved={ctx,width,height,labelScale,zoom:state.zoom,panX:state.panX,panY:state.panY};
  const image=document.createElement('canvas');image.width=1800;image.height=1500;
  ctx=image.getContext('2d');width=1800;height=1500;labelScale=1.55;state.zoom=.92;state.panX=0;state.panY=0;draw();
  ctx.fillStyle='#e5eee8';ctx.font='26px "Microsoft YaHei",sans-serif';ctx.fillText(runtimeMode?'均匀进出货 / 第一天运行采样':'100台机器人仓库 / 布局提案 01',45,52);
  ctx.fillStyle='#8d9d94';ctx.font='17px "Microsoft YaHei",sans-serif';ctx.fillText(`60×48m · 36组货架 / 1728箱位 · 100个待命泊位 · 24个充电位 · ${state.charging==='split'?'分散两组':'集中一组'}`,45,84);
  ctx.fillStyle='#f0a65a';ctx.fillText(runtimeMode?`运行采样回放 · ${runtimeFrame?.runId||'加载中'} · t=${runtimeFrame?.elapsed||0}s`:'设计预览 · 米制假设参数 · 非实时仿真 / 非性能验收',45,1450);
  ctx.fillStyle='#8d9d94';ctx.fillText(`${runtimeMode?'帧内状态':'示意分布'}：移动/取放 ${robots.filter(r=>r.status==='working').length} / 充电 ${robots.filter(r=>r.status==='charging').length} / 待命 ${robots.filter(r=>r.status==='standby').length}${runtimeMode?' / 预约等待 '+robots.filter(r=>r.status==='waiting').length:''}`,45,1478);
  $('exportedMap').src=image.toDataURL('image/png');
  ctx=saved.ctx;width=saved.width;height=saved.height;labelScale=saved.labelScale;state.zoom=saved.zoom;state.panX=saved.panX;state.panY=saved.panY;
  requestDraw();$('imageDialog').showModal();
});
$('closeImage').addEventListener('click',()=>$('imageDialog').close());
new ResizeObserver(resize).observe(stage);
if(runtimeMode){
  layout=createLayout(state);robots=[];routes=[];state.route='none';
  for(const id of ['workingCount','chargingCount','standbyCount','totalCount'])$(id).textContent='—';
  for(const id of ['chargingLayout','inboundLayout','rosterMode'])$(id).disabled=true;
  document.querySelector('.route-bar').hidden=true;
  import('./layout-run.js?v=1').then(m=>m.boot({applyFrame(frame){
    runtimeFrame=frame;runtimeOccupied=new Set(frame.occupiedSlots);
    robots=frame.robots.map(r=>({...r,id:r.robot_id,status:r.stage==='charging'?'charging':r.stage==='idle'?'standby':r.stage.includes('queue')?'waiting':'working'}));
    $('workingCount').textContent=frame.counts.moving+frame.counts.handling;$('chargingCount').textContent=frame.counts.charging;
    $('standbyCount').textContent=frame.counts.standby;$('totalCount').textContent=robots.length;
    requestDraw();
  }})).catch(e=>{$('selectionText').textContent='运行页面加载失败：'+e.message;});
}else refresh();
resize();
