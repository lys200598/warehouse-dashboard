/* Design-only metre-based scene. Deliberately separate from the live dashboard-unit model. */
export const SPEC = Object.freeze({
  version: 'warehouse100-layout-v1', width: 60, depth: 48, unit: 'm',
  robotCount: 100, robotBodyWidth: .7, robotBodyDepth: .7, robotRadius: .55,
  bayWidth: 1.2, bayDepth: 1.5, approachDepth: 2,
  rackWidth: 4.8, rackDepth: 1.2, rackHeight: 1.8,
  levels: 3, baysPerFace: 8, faces: 2, slotsPerRack: 48,
  inboundBuffer: 48, outboundBuffer: 48, gridStep: .2,
});
const rect = (id, x, y, w, d, kind, extra = {}) => ({id, x, y, w, d, kind, ...extra});
export const contains = (r, x, y, margin = 0) => x >= r.x-margin && x <= r.x+r.w+margin && y >= r.y-margin && y <= r.y+r.d+margin;
export function overlaps(a, b, epsilon = 1e-7) {
  return a.x < b.x+b.w-epsilon && a.x+a.w > b.x+epsilon && a.y < b.y+b.d-epsilon && a.y+a.d > b.y+epsilon;
}
export function discHitsRect(p, r, radius = SPEC.robotRadius) {
  const dx = p.x-Math.max(r.x, Math.min(p.x, r.x+r.w));
  const dy = p.y-Math.max(r.y, Math.min(p.y, r.y+r.d));
  return dx*dx+dy*dy < radius*radius-1e-8;
}
export function createLayout({charging = 'split', inbound = 'edge'} = {}) {
  if (!['split','single'].includes(charging) || !['edge','corner'].includes(inbound)) throw new Error('Unknown layout variant');
  const racks = [], homes = [], docks = [], stations = [];
  [8.8,16,23.2,32,39.2,46.4].forEach((x,c) => [16,19.6,23.2,28.4,32,35.6].forEach((y,r) => {
    racks.push(rect(`R${String(c*6+r+1).padStart(2,'0')}`,x,y,4.8,1.2,'rack',{
      capacity:48, zone:(c<3?'W':'E')+(r<3?'N':'S'), column:c, row:r,
    }));
  }));
  [1,4.5,6.4,9.9].forEach((y,r) => {
    for(let c=0;c<25;c++) homes.push(rect(`P${String(r*25+c+1).padStart(3,'0')}`,15+c*1.2,y,1.2,1.5,'home',{
      point:{x:15+c*1.2+.6,y:y+.75}, approachY:r%2===0?y+2.5:y-1,
    }));
  });
  const groups = charging==='split'
    ? [{id:'A',side:'west',x:1.3,y:16,count:12},{id:'B',side:'east',x:57.2,y:22.4,count:12}]
    : [{id:'A',side:'west',x:1.3,y:15,count:24}];
  for(const g of groups) {
    g.padArea=g.count*1.2*1.5;
    g.totalArea=g.count*1.2*(1.5+2);
    g.approach=rect(`CA-${g.id}`,g.side==='west'?g.x+1.5:g.x-2,g.y,2,g.count*1.2,'approach');
    for(let i=0;i<g.count;i++) docks.push(rect(`C-${g.id}${String(i+1).padStart(2,'0')}`,g.x,g.y+i*1.2,1.5,1.2,'dock',{
      group:g.id, side:g.side, point:{x:g.x+.75,y:g.y+i*1.2+.6},
      gate:{x:g.side==='west'?6.8:53.2,y:g.y+i*1.2+.6},
    }));
  }
  for(let i=0;i<8;i++) {
    stations.push({id:`IN-${i+1}`,kind:'inbound',x:inbound==='edge'?9.8+i*1.8:10.1+(i%4)*1.8,y:inbound==='edge'?42:42+Math.floor(i/4)*3.5});
    stations.push({id:`OUT-${i+1}`,kind:'outbound',x:37+i*1.8,y:42});
  }
  const buffers=[rect('IB',16.4,43.2,7,3.5,'buffer'),rect('OB',36.8,43.4,13.6,3,'buffer')];
  const zones=[
    rect('parking',15,1,30,10.4,'zone',{name:'初始 / 待命区',color:'neutral',detail:'100个独立泊位；4排×25位，成对朝向2m通行带。待命停车不等于占用充电位。'}),
    rect('inbound',8.8,41.2,15.2,6,'zone',{name:'收货 / 入库',color:'lime',detail:'8个机器人取货位置、48个暂存箱位。此处拿货后再去货架；不是到达任意墙边即可拿货。'}),
    rect('outbound',36,41.2,15.2,6,'zone',{name:'发货 / 出库',color:'orange',detail:'8个交货位置、48个暂存箱位。货物确认离仓后才释放全仓容量；不增加包装线或订单系统。'}),
    rect('handover',1,7,6,4,'zone',{name:'互助交接预留',color:'blue',detail:'预留两机器人临时交接的开放空间。不增加固定取放装置；任务、持货者与交接事件尚待后端实现。'}),
    rect('reserve',52,2,7,9,'zone',{name:'维护 / 扩展预留',color:'neutral',detail:'不计储货容量，不计额外机器人；暂不放置影响吞吐的有限服务装置。'}),
    ...groups.map(g=>rect(`charge-${g.id}`,g.side==='west'?g.x:g.x-2,g.y,3.5,g.count*1.2,'zone',{
      name:`充电区 ${g.id}`,color:'blue',detail:`${g.count}位：静态停靠${g.padArea.toFixed(1)}㎡＋2m进出带，共${g.totalArea.toFixed(1)}㎡。每位1.2×1.5m；示例参数，不是已标定机型。`,
    })),
  ];
  const trunks=[rect('north',4.8,12,50.4,4,'trunk'),rect('middle',4.8,24.4,50.4,4,'trunk'),rect('south',4.8,37.2,50.4,4,'trunk'),rect('west',4.8,12,4,29.2,'trunk'),rect('east',51.2,12,4,29.2,'trunk'),rect('spine',28,12,4,29.2,'trunk')];
  const layout={spec:SPEC,charging,inbound,racks,homes,docks,groups,stations,buffers,zones,trunks};
  layout.obstacles=[...racks,...homes,...docks,...buffers];
  layout.capacity=racks.reduce((n,r)=>n+r.capacity,0);
  return layout;
}
export function robotPositions(layout, mode='rotation') {
  if(!['rotation','initial','full'].includes(mode)) throw new Error('Unknown roster mode');
  if(mode==='initial') return layout.homes.map((h,i)=>({id:i+1,...h.point,status:'standby',home:h.id}));
  const result=[];
  const ys=[14,18.4,22,26.4,30.8,34.4,39.2], columns=mode==='full'?14:10;
  const xs=Array.from({length:columns},(_,i)=>10+i*(38/(columns-1)));
  for(const y of ys) for(const x of xs) result.push({id:result.length+1,x,y,status:'working'});
  if(mode==='full') {
    result.push({id:99,x:6.8,y:20,status:'working'},{id:100,x:53.2,y:20,status:'working'});
  } else {
    const occupied=layout.charging==='split'?[...layout.docks.slice(0,10),...layout.docks.slice(12,22)]:layout.docks.slice(0,20);
    occupied.forEach(d=>result.push({id:result.length+1,...d.point,status:'charging',dock:d.id}));
    layout.homes.slice(0,10).forEach(h=>result.push({id:result.length+1,...h.point,status:'standby',home:h.id}));
  }
  return result;
}
class MinHeap {
  constructor(){this.a=[];}
  push(item){const a=this.a;a.push(item);let i=a.length-1;while(i>0){const p=(i-1)>>1;if(a[p].f<=item.f)break;a[i]=a[p];i=p;}a[i]=item;}
  pop(){const a=this.a,top=a[0],last=a.pop();if(a.length){let i=0;while(i*2+1<a.length){let c=i*2+1;if(c+1<a.length&&a[c+1].f<a[c].f)c++;if(a[c].f>=last.f)break;a[i]=a[c];i=c;}a[i]=last;}return top;}
  get length(){return this.a.length;}
}
export function walkable(layout,p,excludeIds=[]) {
  const r=SPEC.robotRadius;
  if(p.x<r || p.y<r || p.x>SPEC.width-r || p.y>SPEC.depth-r)return false;
  return !layout.obstacles.some(o=>!excludeIds.includes(o.id)&&discHitsRect(p,o));
}
export function segmentClear(layout,a,b,excludeIds=[]) {
  const length=Math.hypot(a.x-b.x,a.y-b.y),n=Math.max(1,Math.ceil(length/.08));
  for(let i=0;i<=n;i++) if(!walkable(layout,{x:a.x+(b.x-a.x)*i/n,y:a.y+(b.y-a.y)*i/n},excludeIds))return false;
  return true;
}
export function findRoute(layout,start,end,excludeIds=[]) {
  if(!walkable(layout,start,excludeIds)||!walkable(layout,end,excludeIds))return null;
  const s=SPEC.gridStep,nx=Math.round(SPEC.width/s)+1,ny=Math.round(SPEC.depth/s)+1,size=nx*ny;
  const at=id=>({x:(id%nx)*s,y:Math.floor(id/nx)*s});
  const key=p=>Math.round(p.y/s)*nx+Math.round(p.x/s),from=key(start),to=key(end);
  const costs=new Float64Array(size);costs.fill(Infinity);costs[from]=0;
  const previous=new Int32Array(size);previous.fill(-1);
  const closed=new Uint8Array(size),allowed=new Int8Array(size),heap=new MinHeap();
  const dest=at(to),heuristic=id=>{const p=at(id);return Math.abs(p.x-dest.x)+Math.abs(p.y-dest.y);};
  const open=id=>{if(!allowed[id])allowed[id]=walkable(layout,at(id),excludeIds)?1:-1;return allowed[id]===1;};
  if(!open(from)||!open(to)||!segmentClear(layout,start,at(from),excludeIds)||!segmentClear(layout,at(to),end,excludeIds))return null;
  heap.push({id:from,f:heuristic(from)});
  while(heap.length){
    const {id}=heap.pop();if(closed[id])continue;
    if(id===to){
      const path=[];for(let k=id;k!==-1;k=previous[k])path.push(at(k));path.reverse();
      const simplified=[start];
      for(let i=1;i<path.length-1;i++){
        const a=path[i-1],b=path[i],c=path[i+1];
        if(Math.abs((b.x-a.x)*(c.y-b.y)-(b.y-a.y)*(c.x-b.x))>1e-8)simplified.push(b);
      }
      simplified.push(end);
      // Validate continuous connectors too; grid success alone is not clearance proof.
      if(!simplified.slice(1).every((p,i)=>segmentClear(layout,simplified[i],p,excludeIds)))return null;
      return simplified;
    }
    closed[id]=1;
    const x=id%nx,y=Math.floor(id/nx);
    for(const next of [x>0?id-1:-1,x<nx-1?id+1:-1,y>0?id-nx:-1,y<ny-1?id+nx:-1]){
      if(next<0||closed[next]||!open(next))continue;
      const g=costs[id]+s;if(g>=costs[next])continue;
      costs[next]=g;previous[next]=id;heap.push({id:next,f:g+heuristic(next)});
    }
  }
  return null;
}
export const routeLength=points=>points.slice(1).reduce((sum,p,i)=>sum+Math.hypot(p.x-points[i].x,p.y-points[i].y),0);
export function exampleRoutes(layout){
  const rack=layout.racks[9];
  const shelf={x:rack.x+2.4,y:rack.y+rack.d+.7};
  const source=layout.stations.find(s=>s.id==='IN-1');
  const output=layout.stations.find(s=>s.id==='OUT-8');
  const dock=layout.docks[layout.charging==='split'?15:3];
  return [
    {id:'inbound',name:'入库上架',color:'lime',from:source,to:shelf,targetId:rack.id,points:findRoute(layout,source,shelf)},
    {id:'outbound',name:'下架出库',color:'orange',from:shelf,to:output,targetId:'OUT-8',points:findRoute(layout,shelf,output)},
    {id:'charging',name:'空载回充',color:'blue',from:shelf,to:dock.point,targetId:dock.id,points:findRoute(layout,shelf,dock.point,[dock.id])},
  ];
}
