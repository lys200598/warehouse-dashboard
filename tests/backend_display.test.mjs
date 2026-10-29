import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {runInNewContext} from 'node:vm';
import test from 'node:test';
const source=readFileSync(new URL('../dist/app.js',import.meta.url),'utf8');
function helpers(authoritative){
  const lines=source.split('\n').filter(line=>/^  const (taskCode|cargoCode) =/.test(line));
  return runInNewContext(lines.join('\n')+'\n({taskCode,cargoCode})',{backendAuthoritative:authoritative});
}
test('connected backend never fabricates absent task/cargo IDs',()=>{
  const h=helpers(true);
  assert.equal(h.taskCode({id:78,taskRound:0,taskId:null}),'—');
  assert.equal(h.cargoCode({id:78,taskRound:0,cargoId:null}),'—');
  assert.equal(h.taskCode({taskId:'TK-000006'}),'TK-000006');
});
test('offline demo retains its explicitly synthetic IDs',()=>{
  assert.match(helpers(false).taskCode({id:78,taskRound:0}),/^TK-/);
});

function originHelper(){
  const line=source.split('\n').find(line=>/^  const backendFrameOrigin =/.test(line));
  return runInNewContext(line+'\nbackendFrameOrigin');
}
test('consecutive frame begins at previous committed point, not partial display',()=>{
  const result=originHelper()({x:3,y:4,renderTargetX:5,renderTargetY:6},{x:7,y:8},true);
  assert.equal(result.x,5);assert.equal(result.y,6);
});
test('reconnect and skipped frames never invent a movement segment',()=>{
  const result=originHelper()({x:3,y:4,renderTargetX:5,renderTargetY:6},{x:7,y:8},false);
  assert.equal(result.x,7);assert.equal(result.y,8);
});
test('first real frame never animates from synthetic demo positions',()=>{
  const result=originHelper()(undefined,{x:7,y:8},false);
  assert.equal(result.x,7);assert.equal(result.y,8);
});
test('topbar exposes a replay link to the standalone playback page',()=>{
  const html=readFileSync(new URL('../dist/index.html',import.meta.url),'utf8');
  assert.match(html,/id="replayLink"[^>]*href="http:\/\/127\.0\.0\.1:14185\/"/);
});
