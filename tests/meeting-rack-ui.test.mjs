import test from 'node:test';
import assert from 'node:assert/strict';
import {buildQuery} from '../dist/live-model.mjs';
const frame={runId:'observed-run',knowledge:{mirrored_seq:7}};
test('rack inventory binds actual run, historical cutoff and explicit rack ID',()=>{
  const request=buildQuery('rack_inventory',frame,{rackId:'R37',seq:3});
  assert.equal(request.rack_id,'R37');assert.equal(request.run_id,'observed-run');assert.equal(request.seq,3);
});
test('missing rack and unpublished cutoff are rejected without fabricating IDs',()=>{
  assert.throws(()=>buildQuery('rack_inventory',frame),/货架/);
  assert.throws(()=>buildQuery('rack_inventory',frame,{rackId:'R01',seq:8}),/已经镜像/);
});
