import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
const read=path=>readFileSync(new URL('../dist/'+path,import.meta.url),'utf8');
for(const mode of ['optimized','baseline']) {
  test(`v4 ${mode}: labeled replay and original knowledge service retained`,()=>{
    const html=read(`v4-${mode}/wide.html`);
    assert.match(html,/调度正式版 V4/);
    assert.match(html,/已完成运行的回放/);
    assert.match(html,/127\.0\.0\.1:14179/);
    assert.match(html,/<section hidden class="wide-query"/);
    assert.match(html,/左键旋转 · 右\/中键平移 · 滚轮缩放/);
    assert.equal(read(`v4-${mode}/wide.js`),read('wide.js'));
    assert.equal(read(`v4-${mode}/layout-preview.css`),read('layout-preview.css'));
  });
}
