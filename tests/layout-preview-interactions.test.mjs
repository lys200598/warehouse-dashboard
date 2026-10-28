import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {runInNewContext} from 'node:vm';
import test from 'node:test';

const source=readFileSync(new URL('../dist/layout-preview.js',import.meta.url),'utf8');
function controls(view='iso'){
  const state={view,zoom:1,panX:10,panY:20,yaw:-.5,pitch:.8};
  const listeners={},capture=[],selected=[];
  const canvas={addEventListener:(name,fn)=>{listeners[name]=fn;},setPointerCapture:id=>capture.push(id),getBoundingClientRect:()=>({left:20,top:40})};
  const c={state,canvas,drag:null,width:800,height:600,robots:[],layout:{docks:[],racks:[],homes:[],zones:[]},
    requestDraw:()=>{},clamp:(x,min,max)=>Math.max(min,Math.min(max,x)),unproject:()=>({x:0,y:0}),contains:()=>false,
    setSelection:(...v)=>selected.push(v),selectObject:(...v)=>selected.push(v)};
  c.projectionMetrics=()=>({ox:400+state.panX,oy:300+state.panY});
  const zoom=source.slice(source.indexOf('function changeZoom('),source.indexOf('function reset('));
  const events=source.slice(source.indexOf("canvas.addEventListener('wheel'"),source.indexOf("$('downloadDesign').addEventListener"));
  runInNewContext(zoom+'\n'+events,c);
  let prevented=0;
  const emit=(type,extra={})=>listeners[type]({clientX:100,clientY:100,button:0,pointerId:1,shiftKey:false,preventDefault(){prevented++;},...extra});
  return {state,emit,capture,selected,prevented:()=>prevented};
}

test('left drag orbits with the original dashboard direction and sensitivity',()=>{
  const h=controls();h.emit('pointerdown');h.emit('pointermove',{clientX:140,clientY:120});
  assert(Math.abs(h.state.yaw-(-.5+40*.009))<1e-9);assert(Math.abs(h.state.pitch-(.8-20*.006))<1e-9);
  assert.equal(h.state.panX,10);assert.equal(h.state.panY,20);
});
for(const [name,button]of [['right',2],['middle',1]])test(`${name} drag pans without orbiting`,()=>{
  const h=controls();h.emit('pointerdown',{button});h.emit('pointermove',{clientX:140,clientY:120});h.emit('pointerup',{button,clientX:140,clientY:120});
  assert.equal(h.state.panX,50);assert.equal(h.state.panY,40);assert.equal(h.state.yaw,-.5);assert.equal(h.state.pitch,.8);assert.equal(h.selected.length,0);
});
test('shift-left and top-view left drag pan',()=>{
  for(const view of ['iso','top']){
    const h=controls(view);h.emit('pointerdown',{shiftKey:view==='iso'});h.emit('pointermove',{clientX:135,clientY:125});
    assert.equal(h.state.panX,45);assert.equal(h.state.panY,45);assert.equal(h.state.yaw,-.5);
  }
});
test('wheel zoom keeps the world point under the cursor fixed',()=>{
  const h=controls(),x=200,y=220,oldOrigin={x:410,y:320},world={x:x-oldOrigin.x,y:y-oldOrigin.y};
  h.emit('wheel',{deltaY:-120,clientX:x+20,clientY:y+40});
  assert.equal(h.state.zoom,1.08);assert.equal(h.state.yaw,-.5);assert.equal(h.state.pitch,.8);
  assert(Math.abs(400+h.state.panX+world.x*h.state.zoom-x)<1e-9);
  assert(Math.abs(300+h.state.panY+world.y*h.state.zoom-y)<1e-9);
  h.emit('wheel',{deltaY:0});assert.equal(h.state.zoom,1.08);
});
test('right click does not select objects and suppresses the context menu',()=>{
  const h=controls();h.emit('pointerdown',{button:2});h.emit('pointerup',{button:2});h.emit('contextmenu');
  assert.equal(h.selected.length,0);assert.equal(h.prevented(),2);
});
test('unsupported buttons and cancelled drags do not move the camera',()=>{
  const h=controls();h.emit('pointerdown',{button:3});h.emit('pointermove',{clientX:200});assert.equal(h.capture.length,0);assert.equal(h.state.yaw,-.5);
  h.emit('pointerdown');h.emit('pointercancel');h.emit('pointermove',{clientX:200});assert.equal(h.state.yaw,-.5);
});
