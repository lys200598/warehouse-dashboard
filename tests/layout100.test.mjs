import test from 'node:test';
import assert from 'node:assert/strict';
import {SPEC,createLayout,robotPositions,overlaps,discHitsRect,walkable,findRoute,segmentClear,exampleRoutes} from '../dist/layout100-model.mjs';

for(const charging of ['split','single'])for(const inbound of ['edge','corner']){
  const label=`${charging}/${inbound}`,l=createLayout({charging,inbound});
  test(`${label}: counts, real dimensions and no overlapping static objects`,()=>{
    assert.equal(l.racks.length,36);assert.equal(l.capacity,1728);
    assert.equal(l.homes.length,100);assert.equal(l.docks.length,24);
    assert.equal(l.stations.filter(s=>s.kind==='inbound').length,8);
    assert(Math.abs(l.groups.reduce((n,g)=>n+g.totalArea,0)-100.8)<1e-7);
    for(const o of l.obstacles){assert(o.x>=0&&o.y>=0&&o.x+o.w<=60&&o.y+o.d<=48,o.id);}
    for(let i=0;i<l.obstacles.length;i++)for(let j=i+1;j<l.obstacles.length;j++)assert(!overlaps(l.obstacles[i],l.obstacles[j]),`${l.obstacles[i].id}/${l.obstacles[j].id}`);
    for(const t of l.trunks)for(const o of l.obstacles)assert(!overlaps(t,o),`${t.id}/${o.id}`);
  });
  for(const mode of ['rotation','initial','full'])test(`${label}/${mode}: exactly 100 distinct collision-free display positions`,()=>{
    const ps=robotPositions(l,mode);assert.equal(ps.length,100);assert.equal(new Set(ps.map(p=>p.id)).size,100);
    for(const p of ps){assert(walkable(l,p,[p.home,p.dock].filter(Boolean)),JSON.stringify(p));}
    for(let i=0;i<ps.length;i++)for(let j=i+1;j<ps.length;j++)assert(Math.hypot(ps[i].x-ps[j].x,ps[i].y-ps[j].y)>=2*SPEC.robotRadius-1e-7,`${ps[i].id}/${ps[j].id}`);
  });
  test(`${label}: example routes have continuous static clearance`,()=>{
    for(const r of exampleRoutes(l)){
      assert(r.points,`unreachable: ${r.id}`);
      for(let i=1;i<r.points.length;i++)assert(segmentClear(l,r.points[i-1],r.points[i],r.id==='charging'?[r.targetId]:[]),r.id);
    }
  });
  test(`${label}: each home, dock and port has an unblocked ingress to the aisle network`,()=>{
    const centre={x:30,y:26.4};
    // Check the critical local connection, then representative graph connections.
    for(const h of l.homes){const a={x:h.point.x,y:h.approachY};assert(segmentClear(l,h.point,a,[h.id]),h.id);assert(walkable(l,a),h.id);}
    for(const d of l.docks)assert(segmentClear(l,d.point,d.gate,[d.id]),d.id);
    for(const s of l.stations)assert(findRoute(l,centre,s),s.id);
    for(const h of l.homes)assert(findRoute(l,centre,h.point,[h.id]),h.id);
    for(const d of l.docks)assert(findRoute(l,centre,d.point,[d.id]),d.id);
  });
  test(`${label}: both faces of every rack have reachable standing locations`,()=>{
    for(const r of l.racks)for(const y of [r.y-.7,r.y+r.d+.7]){
      assert(walkable(l,{x:r.x+r.w/2,y}),r.id);
      assert(findRoute(l,{x:30,y:26.4},{x:r.x+r.w/2,y}),r.id);
    }
  });
}
test('invalid variants rejected and no route through a rack',()=>{
  assert(SPEC.robotRadius>Math.hypot(SPEC.robotBodyWidth/2,SPEC.robotBodyDepth/2),'planning disc must contain body corners');
  assert.throws(()=>createLayout({charging:'random'}));
  const l=createLayout(),r=l.racks[0];
  assert(discHitsRect({x:r.x+1,y:r.y+.6},r));
  assert.equal(findRoute(l,{x:r.x+1,y:r.y+.6},{x:30,y:26.4}),null);
});
