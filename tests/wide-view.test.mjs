import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import test from 'node:test';

const source=readFileSync(new URL('../dist/wide.js',import.meta.url),'utf8');
const html=readFileSync(new URL('../dist/wide.html',import.meta.url),'utf8');
const view=await import('data:text/javascript;base64,'+Buffer.from(source).toString('base64'));
const spec={width:90,depth:76};

test('projection is parameterized and invertible on the 90x76 floor',()=>{
  for(const mode of ['iso','top']){
    const camera={...view.newCamera(),view:mode,panX:21,panY:-15,zoom:1.4};
    const m=view.projectionMetrics(spec,camera,1000,650);
    assert.equal(m.cx,45);assert.equal(m.cy,38);
    for(const p of [{x:0,y:0},{x:90,y:76},{x:45,y:38},{x:18.9,y:13.3}]){
      const q=view.unproject(view.project(p,m),m);
      assert(Math.abs(p.x-q.x)<1e-8);assert(Math.abs(p.y-q.y)<1e-8);
    }
  }
});

test('left drag rotates, right and middle drag only pan',()=>{
  const camera=view.newCamera(),original={...camera};
  assert.equal(view.dragCamera(camera,0,20,10),'orbit');
  assert.equal(camera.panX,0);assert.equal(camera.panY,0);
  assert.equal(camera.yaw,original.yaw+.18);assert.equal(camera.pitch,original.pitch-.06);
  for(const button of [1,2]){
    const c=view.newCamera(),before={...c};
    assert.equal(view.dragCamera(c,button,30,40),'pan');
    assert.equal(c.panX,30);assert.equal(c.panY,40);
    assert.equal(c.yaw,before.yaw);assert.equal(c.pitch,before.pitch);
  }
  const c=view.newCamera(),before={...c};assert.equal(view.dragCamera(c,3,20,20),null);assert.deepEqual(c,before);
});

test('wheel zoom anchors the world point under cursor and is bounded',()=>{
  for(const mode of ['iso','top']){
    const c={...view.newCamera(),view:mode,panX:10,panY:20},cursor={x:123,y:350};
    const before=view.unproject(cursor,view.projectionMetrics(spec,c,900,600)),yaw=c.yaw,pitch=c.pitch;
    view.zoomCamera(c,-120,cursor,spec,900,600);
    const after=view.unproject(cursor,view.projectionMetrics(spec,c,900,600));
    assert(Math.abs(before.x-after.x)<1e-9);assert(Math.abs(before.y-after.y)<1e-9);
    assert.equal(c.yaw,yaw);assert.equal(c.pitch,pitch);
    for(let i=0;i<100;i++)view.zoomCamera(c,-120,cursor,spec,900,600);
    assert.equal(c.zoom,5);
    for(let i=0;i<100;i++)view.zoomCamera(c,120,cursor,spec,900,600);
    assert.equal(c.zoom,.3);
  }
});

test('P home aliases are never rendered as 100 additional bays',()=>{
  const docks=Array.from({length:100},(_,i)=>({id:`C-${i}`,x:i,y:1,w:1,d:1}));
  const homes=docks.map((d,i)=>({id:`P${i}`,alias_of:d.id}));
  assert.equal(view.canonicalBays({docks,homes}).length,100);
  assert.equal(view.canonicalBays({docks:[...docks,{id:'other-name',alias_of:'C-0'}],homes}).length,100);
});

test('only actual frame robots are displayed; no synthesized population',()=>{
  assert.deepEqual(view.realRobots(null),[]);assert.deepEqual(view.realRobots({robots:[]}),[]);
  const robot={robot_id:1,x:12,y:20,carrying:true};
  assert.deepEqual(view.realRobots({robots:[robot,{robot_id:2,x:NaN,y:1},{robot_id:'3',x:1,y:1}]}),[robot]);
  assert.equal(view.robotColor({stage:'idle'}),'#91a499');
  assert.equal(view.robotColor({stage:'charging'}),'#68c7ff');
  assert.equal(view.robotColor({stage:'fault_wait'}),'#f0a65a');
});

test('knowledge queries are fixed endpoints with encoded IDs and cutoff',()=>{
  assert.equal(view.queryURL('robot','100',16),'/api/knowledge/robot?id=100&seq=16');
  assert.equal(view.queryURL('cooperation','T/测试',3),'/api/knowledge/cooperation?task=T%2F%E6%B5%8B%E8%AF%95&seq=3');
  assert.equal(view.queryURL('goods','G 1&x',4),'/api/knowledge/goods?id=G+1%26x&seq=4');
  for(const args of [['cypher','MATCH',1],['robot','101',1],['robot','0',1],['goods','',1],['goods','G',0],['goods','G',1.5]])assert.throws(()=>view.queryURL(...args));
});

test('clock supports cross-day replay without wrapping to day one',()=>{
  assert.equal(view.formatClock(0),'00:00:00');
  assert.equal(view.formatClock(86401),'第2天 00:00:01');
  assert.equal(view.formatClock(172800),'第3天 00:00:00');
});

test('page has replay, stable camera controls and real knowledge endpoints',()=>{
  for(const id of ['wideCanvas','runTimeline','playRun','viewTop','viewIso','resetView','robotSelect','querySeq','knowledgeResult','copyKnowledge'])assert.match(html,new RegExp(`id="${id}"`));
  assert.match(html,/左键旋转 · 右\/中键平移 · 滚轮缩放/);
  for(const endpoint of ['/api/scene','/api/status','/api/frame?second='])assert(source.includes(endpoint));
  assert(!source.includes('robotPositions('));assert(!source.includes('createLayout('));
  assert(source.includes('textContent'));assert(!source.includes('.innerHTML'));
  assert(source.includes('scene.spec.slotPitch'));
});
