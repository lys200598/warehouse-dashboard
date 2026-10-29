import test from 'node:test';
import assert from 'node:assert/strict';
import {createFrameBuffer,bufferDelayFromQuery,DEFAULT_BUFFER_MS} from '../dist/frame-buffer.mjs';

const frame=(elapsed,extra={})=>({runId:'r1',runtimeInstanceId:'i1',status:'running',speed:1,elapsed,tick:elapsed,...extra});

test('running frames are held for the delay and released at simulation pace',()=>{
  const b=createFrameBuffer({delayMs:3000});
  assert.deepEqual(b.push(frame(10),0),[]);
  // a save stall: the next frame arrives 2.5 s late, then two arrive together
  assert.deepEqual(b.push(frame(11),2500),[]);
  assert.deepEqual(b.push(frame(12),2520),[]);
  assert.deepEqual(b.due(2999),[]);
  assert.deepEqual(b.due(3000).map(f=>f.elapsed),[10]);
  assert.deepEqual(b.due(4000).map(f=>f.elapsed),[11]);
  assert.deepEqual(b.due(5000).map(f=>f.elapsed),[12]);
});
test('pause, step and other non-running frames flush the buffer immediately',()=>{
  const b=createFrameBuffer({delayMs:3000});
  b.push(frame(10),0);b.push(frame(11),1000);
  const out=b.push(frame(11,{status:'paused',running:false}),1200);
  assert.deepEqual(out.map(f=>f.status),['running','running','paused']);
  assert.equal(b.size(),0);
});
test('run change, speed change and rewind re-anchor without replaying stale frames late',()=>{
  const b=createFrameBuffer({delayMs:3000});
  b.push(frame(10),0);
  assert.deepEqual(b.push(frame(0,{runId:'r2'}),500).map(f=>f.runId),['r1']);
  assert.deepEqual(b.push(frame(5,{runId:'r2',speed:20}),600).map(f=>f.elapsed),[0]);
  assert.deepEqual(b.due(3600).map(f=>f.elapsed),[5]);
});
test('a server that falls behind refills only half the buffer once',()=>{
  const b=createFrameBuffer({delayMs:3000});
  b.push(frame(10),0);b.due(3000);
  b.push(frame(11),9000);
  assert.deepEqual(b.due(10499),[]);
  assert.deepEqual(b.due(10500).map(f=>f.elapsed),[11]);
});
test('buffer delay comes from ?buffer= and can be disabled',()=>{
  assert.equal(bufferDelayFromQuery(''),DEFAULT_BUFFER_MS);
  assert.equal(bufferDelayFromQuery('?buffer=0'),0);
  assert.equal(bufferDelayFromQuery('?buffer=1500'),1500);
  assert.equal(bufferDelayFromQuery('?buffer=abc'),DEFAULT_BUFFER_MS);
  const b=createFrameBuffer({delayMs:0});
  assert.deepEqual(b.push(frame(1),0).map(f=>f.elapsed),[1]);
});
