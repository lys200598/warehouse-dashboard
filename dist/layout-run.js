// Read-only playback of real committed pilot output; no fabricated roster fallback.
export async function boot({applyFrame}){
  const $=id=>document.getElementById(id);
  const stylesheet=document.createElement('link');stylesheet.rel='stylesheet';stylesheet.href='./layout-run.css?v=1';document.head.append(stylesheet);
  document.title='均匀进出货 · 第1天运行回放';document.querySelector('h1').textContent='均匀进出货 · 第1天试运行';
  document.querySelector('.proposal-tag').textContent='运行采样回放 · Local / 非Ray联合验收';
  document.querySelector('.map-note').innerHTML='RUN PLAYBACK <span>真实运行采样 / 每10秒一帧</span>';
  document.querySelector('.control-section .eyebrow').textContent='RECORDED RUN';
  document.querySelector('.control-section h2').textContent='运行配置固定，布局不随回放改变';
  document.querySelector('.control-section .fine-print').textContent='台数、电量和库存来自试验输出。移动/取放、充电、待命和排队分别计数；本轮是单进程协调100台模型。';
  const roster=document.getElementById('rosterMode');roster.replaceChildren(new Option('由运行采样驱动，不使用示意分布','recorded'));
  document.querySelector('label[for="rosterMode"]').textContent='机器人状态来源';
  document.querySelector('.population').setAttribute('aria-label','运行采样机器人数量');
  document.querySelector('.population>span:first-child').firstChild.textContent='移动/取放 ';
  const legend=document.querySelector('.legend');legend.children[0].lastChild.textContent='移动/取放';legend.children[3].lastChild.textContent='预约等待';
  document.querySelector('.population').insertAdjacentHTML('beforeend','<span>排队 <b id="queuedCount">—</b></span>');
  document.querySelector('.population').style.flexWrap='wrap';
  document.querySelector('.design-details').style.display='none';
  const panel=document.createElement('section');panel.className='runtime-panel';panel.innerHTML=`
    <div class="runtime-metrics"><span>已接收入库 <b id="receivedValue">—</b></span><span>已确认出库 <b id="shippedValue">—</b></span><span>已完成任务 <b id="completedValue">—</b> / <span id="totalTasksValue">—</span></span><span>仓内实物 <b id="insideValue">—</b> / 1728</span><span>待处理 <b id="backlogValue">—</b></span><span>尚未发布 <b id="unreleasedValue">—</b></span><span>历史最低电量 <b id="minSocValue">—</b></span></div>
    <div class="runtime-timebar"><button id="playRun" type="button">播放回放</button><button id="latestRun" type="button">最新记录</button><label>倍速 <select id="replaySpeed"><option value="20">20×</option><option value="60">60×</option><option value="300" selected>300×</option><option value="900">900×</option></select></label><output id="runClock">00:00:00</output><a href="./day1-knowledge.html" target="_blank" rel="noopener">图谱查询 ↗</a></div>
    <input id="runTimeline" type="range" min="0" max="86400" value="0" step="10" aria-label="运行回放时刻">
    <p id="runStatus">正在读取独立试验输出…</p>`;
  document.querySelector('.overview').after(panel);
  let latest=0,cursor=0,playing=false,busy=false,status=null;
  const clock=s=>[Math.floor(s/3600),Math.floor(s/60)%60,s%60].map(n=>String(n).padStart(2,'0')).join(':');
  async function readStatus(){
    const r=await fetch('/api/day/status',{cache:'no-store'});if(!r.ok)throw new Error('试验状态不可用');status=await r.json();latest=status.latest_second;$('runTimeline').max=latest;
    const state=status.results?.status||'running';
    $('runStatus').textContent=`${status.run_id} · ${state} · 最新 ${clock(latest)} · 图谱提交 ${status.knowledge.mirrored_seq}/${status.knowledge.durable_seq} · 图谱全状态采样5分钟，业务事件全量保留`;
  }
  async function show(second){
    if(busy)return;busy=true;
    try{
      const response=await fetch('/api/day/frame?second='+Math.round(second),{cache:'no-store'});if(!response.ok)throw new Error('运行帧不可用');const f=await response.json();
      cursor=f.elapsed;applyFrame(f);$('runTimeline').value=cursor;$('runClock').textContent=clock(cursor);
      $('receivedValue').textContent=f.metrics.received;$('shippedValue').textContent=f.metrics.shipped;$('completedValue').textContent=f.metrics.completed;
      $('insideValue').textContent=f.inside;$('backlogValue').textContent=f.pending;$('minSocValue').textContent=f.metrics.min_soc.toFixed(1)+'%';$('queuedCount').textContent=f.counts.queued;
      $('totalTasksValue').textContent=f.totalTasks;$('unreleasedValue').textContent=f.unreleased;
      $('selectionTitle').textContent='实际运行 / '+clock(cursor);
      $('selectionText').textContent=`移动${f.counts.moving}台、取放${f.counts.handling}台、充电${f.counts.charging}台、排队${f.counts.queued}台、待命${f.counts.standby}台。点击机器人查看电量和任务。`;
    }catch(e){playing=false;$('runStatus').textContent=e.message;}
    finally{busy=false;}
  }
  $('runTimeline').addEventListener('input',()=>{playing=false;$('playRun').textContent='播放回放';show(Number($('runTimeline').value));});
  $('playRun').addEventListener('click',()=>{playing=!playing;$('playRun').textContent=playing?'暂停回放':'播放回放';});
  $('latestRun').addEventListener('click',async()=>{playing=false;await readStatus();show(latest);});
  await readStatus();await show(Math.min(latest,21600));
  setInterval(async()=>{
    if(!playing||busy)return;
    if(cursor>=latest){await readStatus();if(cursor>=latest){playing=false;$('playRun').textContent='播放回放';return;}}
    await show(Math.min(latest,cursor+Number($('replaySpeed').value)/2));
  },500);
  setInterval(()=>readStatus().catch(e=>{$('runStatus').textContent=e.message;}),10000);
}
