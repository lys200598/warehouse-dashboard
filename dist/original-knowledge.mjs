import {normalizeDatasets,knowledgeFacts,buildQuery,formatClock} from './live-model.mjs';

// On-demand knowledge controls in the original dashboard. Rendering never queries ANN.
export function createKnowledgePanel({getFrame,getSelected,control,toast}) {
  const $=id=>document.getElementById(id);let datasets=[],pendingDataset=null,queryBusy=false,loaded=false;
  async function json(url,body){
    const r=await fetch(url,body===undefined?{cache:'no-store'}:{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});
    const v=await r.json();if(!r.ok)throw new Error(v.error||v.detail||`HTTP ${r.status}`);return v;
  }
  const labels={self:'机器人上下文',readiness:'可见性 / 就绪',task:'任务状态与协作',goods:'货物持有链',energy:'能量与充电',assistance:'协作观察',progress:'任务进展',memory:'机器人历史记忆',local_awareness:'区域态势',global:'全局态势',similar_history:'相似历史（向量）'};
  $('datasetSelect').addEventListener('change',()=>{pendingDataset=$('datasetSelect').value;});
  $('stepButton').addEventListener('click',()=>control('step'));
  $('checkpointButton').addEventListener('click',()=>control('checkpoint'));
  $('newRunButton').addEventListener('click',()=>{
    if(getFrame()?.running)return toast('请先暂停再另开新run','!');
    if(window.confirm('保留当前运行记录，另开所选数据集的新run？'))control('reset');
  });
  $('fillSelected').addEventListener('click',()=>{const r=getSelected();if(!r)return;
    $('queryRobot').value=r.id;$('queryTask').value=r.taskId||'';$('queryGoods').value=r.cargoId||'';});
  $('latestQuery').addEventListener('change',()=>{$('querySeq').disabled=$('latestQuery').checked;});
  $('runQuery').addEventListener('click',async()=>{
    const frame=getFrame();if(!frame||queryBusy)return;
    queryBusy=true;$('runQuery').disabled=true;
    try{
      const request=buildQuery($('queryKind').value,frame,{robotId:Number($('queryRobot').value),taskId:$('queryTask').value.trim(),goodsId:$('queryGoods').value.trim(),seq:$('latestQuery').checked?null:Number($('querySeq').value),variant:$('queryVariant').value});
      $('queryRequest').textContent=JSON.stringify(request,null,2);$('queryStatus').textContent=`查询 ${request.run_id} / seq ${request.seq}…`;
      const result=await json('/api/knowledge/query',request);
      $('queryResult').textContent=JSON.stringify(result,null,2);$('queryStatus').textContent=`已返回 ${request.query_id} · run ${request.run_id} · seq ${request.seq}`;
      $('queryOutput').open=true;
    }catch(e){$('queryStatus').textContent=e.message;$('queryResult').textContent=JSON.stringify({error:e.message},null,2);}
    finally{queryBusy=false;apply(getFrame());}
  });
  async function load(){
    const results=await Promise.allSettled([
      json('/api/datasets').then(v=>{datasets=normalizeDatasets(v);$('datasetSelect').replaceChildren(...datasets.map(d=>new Option(`${d.label} · ${d.task_count??'—'}项`,d.id)));apply(getFrame());}),
      json('/api/knowledge/catalog').then(v=>{const q=v.queries||{};const keys=Array.isArray(q)?q.map(x=>typeof x==='string'?x:x.query_id||x.id):Object.keys(q);$('queryKind').replaceChildren(...keys.map(k=>new Option(labels[k]||k,k)));})]);
    for(const r of results)if(r.status==='rejected')toast(r.reason.message,'!');
  }
  function apply(f){
    if(!f)return;
    if(!loaded){loaded=true;load();}
    $('platformKnowledge').hidden=false;
    const k=knowledgeFacts(f),meta=f.metadata||{};
    if(pendingDataset===null&&datasets.some(d=>d.id===meta.dataset_id))$('datasetSelect').value=meta.dataset_id;
    const dataset=datasets.find(d=>d.id===meta.dataset_id);
    $('datasetDetail').textContent=`当前：${dataset?.label||meta.dataset_id} · ${f.totalTasks}项 · 未发布 ${f.unreleased} · 仓内 ${f.inside}`;
    $('knowledgeSummary').textContent=`持久化 ${k.durable} / 图谱 ${k.mirrored} / 滞后 ${k.lag}批 · 物理 ${formatClock(f.elapsed)} / 图谱 ${formatClock(k.committedTime)}${k.error?` · 写入错误 ${k.error}`:''}`;
    $('runIdentity').textContent=f.runId;
    $('runMetadata').textContent=JSON.stringify({runId:f.runId,sourceKind:f.sourceKind,metadata:meta,knowledge:f.knowledge},null,2);
    if($('latestQuery').checked)$('querySeq').value=k.mirrored;
    $('stepButton').disabled=!['paused','ready'].includes(f.status);
    $('checkpointButton').disabled=f.status==='failed';
    $('newRunButton').disabled=f.running||!datasets.length;
    $('runQuery').disabled=queryBusy||!(k.mirrored>0);
    $('sceneVersion').textContent=f.specVersion||'V5';
  }
  return {apply,duration:id=>datasets.find(d=>d.id===id)?.duration_seconds};
}
