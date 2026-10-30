# 17692 同步图谱全天运行长尾诊断

### 任务背景

日志与下表统一使用UTC保留原始时间；北京时间为UTC+8。

只读分析 wz-test-day-20261030-final 在第9264步停止的原因，核对写入长尾、日志与计时口径；不运行压测。本诊断与后续授权的8GiB页缓存维护分别记录。

### 关键结论

> 重要混杂：本场加速运行中途加入了同run百万历史前缀与100独立进程联合压测，不能把全场或最长66秒作为独占稳态负载性能。晚期长尾仍需单独解释。

- 已记录9264个返回成功的 sim.step()，其中write_ms>5秒有23次；最长在tick1712，为65.906秒。
- 最终已返回的tick9264：write=14.071秒、wait_step=78.025毫秒。下一次step等待Step9264时2秒超时；该失败调用没有timing行。不能把此前65.906秒actor批写直接当成这次2秒超时的原因。
- write_ms是聚合区间：actor批写/收集/重试，加全局记录构造与提交等；没有独立actor SQL、RPC、HTTP入队和服务落盘计时。wait_step_ms对应当前派单输入的前一返回步骤，是另一边界。现有证据不能精确把长尾拆给其中某一个内部环节。
- 超5秒写入的tick%4分布为{1: 1, 0: 22}；每4步有全体AgentState历史写入，分布可检验其相关性，但不能单凭相关性认定根因。
- 原始debug日志有8.639/14.232/18.937秒的checkpoint，其后多为1.5~2秒；需与下表的墙钟范围比较。checkpoint发生并不证明它单独造成写入停顿。
- 此运行期间未采集GC详细日志，也没有旧服务活动的持续历史采样；debug中未见本窗口明确的VM pause/OOM行，不能据此排除GC，也不能把旧服务认定为并发写入原因。维护时的一次旧服务快照不能证明整场运行无活动。
- 另未保留逐tick绝对UTC、每actor/全局落盘span、历史iostat或持续锁样本；因此无法排除或定量归因I/O、GC、锁排队及进程调度。
- 当前库文件与索引约4.81GiB，页缓存原为1GiB；内存当前available约136.7GiB，调整页缓存到8GiB有容量依据，仍不等于已证实页缓存是根因。

### 执行过的操作

- 读取tick-timing.jsonl、report.json、progress.json及诊断时源码快照。源码副本是诊断时版本，若事故后已有修改，不能将它作为事故时逐行执行证明。
- 保存debug运行时间窗、GC/heap关键词与checkpoint原始抽样（debug-gc-heap.log是宽关键词匹配，不等于存在GC采样）；读取/proc/meminfo、Java命令行、目录配置和库文件尺寸。
- 核对ordinary基准取消后的cleanup=0及当前没有本任务生产者；旧服务只读观察，没有停止旧服务。
- 本目录environment.json、store-files.json、tick-samples.jsonl、debug-*.log、analysis.json保留证据。

### 修改内容

未修改产品代码。另一次已授权维护仅调整17692页缓存；维护前后配置、PID、库运行ID、内存与文件尺寸在pagecache-maintenance-*目录独立记录。

### 后续建议

1. 保留生产memory派单与异步有界图谱写入；将KG同步一致读作为显式可选模式，区分页面物理tick和图谱完整Step。
2. 对KG模式在发布步骤时加入明确的全局持久化确认；不要用放宽2秒等待来掩盖尚未提交完整Step的发布问题。
3. 下一次短测增加actor_batch_ms/global_enqueue_ms/global_step_wait_ms及各span绝对UTC，记录重试次数；只需短程相关测试，不必重跑全天来定位。
4. 页缓存8GiB维护后先做异步短测，观察真实入队/落盘滞后与主循环耗时；没有新证据不继续盲调heap或停止旧服务。

### 计时与原始抽样

计时文件出生时间：2026-10-30T10:55:02+00:00（精度1秒）；已返回step总计3971.880秒；到最后mtime的未计时差3.823秒。以下是下界估计，不是逐tick实测UTC；在墙钟未跳变的假设下，每行真实时刻可能最多晚约4.823秒。

|tick|write秒|total秒|wait Step毫秒|tick%4|估计UTC开始下界|估计UTC结束下界|可能重叠的checkpoint开始UTC|
|---:|---:|---:|---:|---:|---|---|---|
|1|11.105|11.715|283.625|1|2026-10-30T10:55:02+00:00|2026-10-30T10:55:13.714943+00:00|无|
|40|6.841|7.268|384.635|0|2026-10-30T10:55:43.615114+00:00|2026-10-30T10:55:50.883537+00:00|无|
|208|8.173|8.328|98.259|0|2026-10-30T10:57:16.394235+00:00|2026-10-30T10:57:24.722289+00:00|无|
|212|5.323|5.501|133.290|0|2026-10-30T10:57:26.190099+00:00|2026-10-30T10:57:31.690856+00:00|无|
|696|5.328|5.564|141.749|0|2026-10-30T11:00:47.438341+00:00|2026-10-30T11:00:53.001868+00:00|无|
|1188|9.743|9.958|157.870|0|2026-10-30T11:04:25.064653+00:00|2026-10-30T11:04:35.022549+00:00|2026-10-30T11:04:26.641000+00:00|
|1660|5.189|5.324|67.601|0|2026-10-30T11:07:42.623126+00:00|2026-10-30T11:07:47.946813+00:00|无|
|1708|20.069|20.176|58.686|0|2026-10-30T11:08:07.244400+00:00|2026-10-30T11:08:27.420403+00:00|无|
|1712|65.906|66.083|127.679|0|2026-10-30T11:08:30.300033+00:00|2026-10-30T11:09:36.382896+00:00|无|
|1716|6.259|6.659|350.379|0|2026-10-30T11:09:37.524186+00:00|2026-10-30T11:09:44.183124+00:00|无|
|1720|13.089|13.350|177.220|0|2026-10-30T11:09:45.272549+00:00|2026-10-30T11:09:58.622858+00:00|无|
|1736|7.411|7.517|55.269|0|2026-10-30T11:10:08.616068+00:00|2026-10-30T11:10:16.133557+00:00|无|
|1832|12.550|12.641|38.861|0|2026-10-30T11:11:04.843019+00:00|2026-10-30T11:11:17.483975+00:00|2026-10-30T11:11:05.374000+00:00|
|1968|9.399|9.468|15.042|0|2026-10-30T11:12:15.293204+00:00|2026-10-30T11:12:24.760847+00:00|无|
|2056|7.797|7.933|77.045|0|2026-10-30T11:13:04.093979+00:00|2026-10-30T11:13:12.026776+00:00|无|
|2084|5.571|5.748|109.559|0|2026-10-30T11:13:43.033962+00:00|2026-10-30T11:13:48.781724+00:00|无|
|2176|6.827|6.977|76.607|0|2026-10-30T11:14:39.459203+00:00|2026-10-30T11:14:46.436003+00:00|无|
|2480|10.650|10.865|148.787|0|2026-10-30T11:16:39.544014+00:00|2026-10-30T11:16:50.408649+00:00|2026-10-30T11:16:39.616000+00:00|
|9248|24.850|25.491|580.109|0|2026-10-30T11:59:36.356936+00:00|2026-10-30T12:00:01.848164+00:00|无|
|9252|15.235|15.664|344.606|0|2026-10-30T12:00:05.374167+00:00|2026-10-30T12:00:21.038353+00:00|无|
|9256|18.397|20.026|1544.157|0|2026-10-30T12:00:26.366073+00:00|2026-10-30T12:00:46.392226+00:00|无|
|9260|8.884|9.192|249.915|0|2026-10-30T12:00:48.033267+00:00|2026-10-30T12:00:57.225226+00:00|无|
|9264|14.071|14.229|78.025|0|2026-10-30T12:00:59.650494+00:00|2026-10-30T12:01:13.879747+00:00|无|

运行期 pause/OOM关键词抽样：

```
```

checkpoint完成日志：

```
4936: 2026-10-30 10:54:14.625+0000 INFO  [o.n.k.i.t.l.c.CheckPointerImpl] [neo4j/9ae801ab] Checkpoint triggered by "Scheduled checkpoint for every 100000 log chunks threshold" @ txId: 1250487, append index: 1250487 checkpoint completed in 740ms. Checkpoint flushed 1518 pages (1% of total available pages), in 1470 IOs. Checkpoint performed with IO limit: unlimited, paused in total 0 times( 0 millis).
5054: 2026-10-30 10:59:36.607+0000 INFO  [o.n.k.i.t.l.c.CheckPointerImpl] [neo4j/9ae801ab] Checkpoint triggered by "Scheduled checkpoint for every 100000 log chunks threshold" @ txId: 1350981, append index: 1350981 checkpoint completed in 1s 972ms. Checkpoint flushed 4053 pages (3% of total available pages), in 3976 IOs. Checkpoint performed with IO limit: unlimited, paused in total 0 times( 0 millis).
5087: 2026-10-30 11:04:35.282+0000 INFO  [o.n.k.i.t.l.c.CheckPointerImpl] [neo4j/9ae801ab] Checkpoint triggered by "Scheduled checkpoint for every 100000 log chunks threshold" @ txId: 1452263, append index: 1452263 checkpoint completed in 8s 639ms. Checkpoint flushed 912 pages (0% of total available pages), in 904 IOs. Checkpoint performed with IO limit: unlimited, paused in total 0 times( 0 millis).
5114: 2026-10-30 11:11:19.607+0000 INFO  [o.n.k.i.t.l.c.CheckPointerImpl] [neo4j/9ae801ab] Checkpoint triggered by "Scheduled checkpoint for every 100000 log chunks threshold" @ txId: 1552861, append index: 1552861 checkpoint completed in 14s 232ms. Checkpoint flushed 69740 pages (53% of total available pages), in 69551 IOs. Checkpoint performed with IO limit: unlimited, paused in total 0 times( 0 millis).
5141: 2026-10-30 11:16:58.554+0000 INFO  [o.n.k.i.t.l.c.CheckPointerImpl] [neo4j/9ae801ab] Checkpoint triggered by "Scheduled checkpoint for every 100000 log chunks threshold" @ txId: 1655284, append index: 1655284 checkpoint completed in 18s 937ms. Checkpoint flushed 113059 pages (86% of total available pages), in 112832 IOs. Checkpoint performed with IO limit: unlimited, paused in total 0 times( 0 millis).
5151: 2026-10-30 11:20:40.406+0000 INFO  [o.n.k.i.t.l.c.CheckPointerImpl] [neo4j/9ae801ab] Checkpoint triggered by "Scheduled checkpoint for every 100000 log chunks threshold" @ txId: 1759484, append index: 1759484 checkpoint completed in 1s 767ms. Checkpoint flushed 5229 pages (4% of total available pages), in 5196 IOs. Checkpoint performed with IO limit: unlimited, paused in total 0 times( 0 millis).
5163: 2026-10-30 11:24:42.105+0000 INFO  [o.n.k.i.t.l.c.CheckPointerImpl] [neo4j/9ae801ab] Checkpoint triggered by "Scheduled checkpoint for every 100000 log chunks threshold" @ txId: 1860763, append index: 1860763 checkpoint completed in 1s 691ms. Checkpoint flushed 5212 pages (3% of total available pages), in 5181 IOs. Checkpoint performed with IO limit: unlimited, paused in total 0 times( 0 millis).
5172: 2026-10-30 11:28:43.562+0000 INFO  [o.n.k.i.t.l.c.CheckPointerImpl] [neo4j/9ae801ab] Checkpoint triggered by "Scheduled checkpoint for every 100000 log chunks threshold" @ txId: 1963693, append index: 1963693 checkpoint completed in 1s 450ms. Checkpoint flushed 5464 pages (4% of total available pages), in 5418 IOs. Checkpoint performed with IO limit: unlimited, paused in total 0 times( 0 millis).
5176: 2026-10-30 11:32:55.129+0000 INFO  [o.n.k.i.t.l.c.CheckPointerImpl] [neo4j/9ae801ab] Checkpoint triggered by "Scheduled checkpoint for every 100000 log chunks threshold" @ txId: 2067169, append index: 2067169 checkpoint completed in 1s 557ms. Checkpoint flushed 5279 pages (4% of total available pages), in 5244 IOs. Checkpoint performed with IO limit: unlimited, paused in total 0 times( 0 millis).
5183: 2026-10-30 11:37:07.011+0000 INFO  [o.n.k.i.t.l.c.CheckPointerImpl] [neo4j/9ae801ab] Checkpoint triggered by "Scheduled checkpoint for every 100000 log chunks threshold" @ txId: 2170298, append index: 2170267 checkpoint completed in 1s 875ms. Checkpoint flushed 5338 pages (4% of total available pages), in 5307 IOs. Checkpoint performed with IO limit: unlimited, paused in total 0 times( 0 millis).
5188: 2026-10-30 11:41:18.817+0000 INFO  [o.n.k.i.t.l.c.CheckPointerImpl] [neo4j/9ae801ab] Checkpoint triggered by "Scheduled checkpoint for every 100000 log chunks threshold" @ txId: 2270433, append index: 2270433 checkpoint completed in 1s 794ms. Checkpoint flushed 5293 pages (4% of total available pages), in 5272 IOs. Checkpoint performed with IO limit: unlimited, paused in total 0 times( 0 millis).
5198: 2026-10-30 11:45:20.457+0000 INFO  [o.n.k.i.t.l.c.CheckPointerImpl] [neo4j/9ae801ab] Checkpoint triggered by "Scheduled checkpoint for every 100000 log chunks threshold" @ txId: 2370579, append index: 2370579 checkpoint completed in 1s 632ms. Checkpoint flushed 5182 pages (3% of total available pages), in 5158 IOs. Checkpoint performed with IO limit: unlimited, paused in total 0 times( 0 millis).
5205: 2026-10-30 11:49:32.008+0000 INFO  [o.n.k.i.t.l.c.CheckPointerImpl] [neo4j/9ae801ab] Checkpoint triggered by "Scheduled checkpoint for every 100000 log chunks threshold" @ txId: 2471489, append index: 2471489 checkpoint completed in 1s 534ms. Checkpoint flushed 5411 pages (4% of total available pages), in 5382 IOs. Checkpoint performed with IO limit: unlimited, paused in total 0 times( 0 millis).
5217: 2026-10-30 11:53:43.713+0000 INFO  [o.n.k.i.t.l.c.CheckPointerImpl] [neo4j/9ae801ab] Checkpoint triggered by "Scheduled checkpoint for every 100000 log chunks threshold" @ txId: 2573282, append index: 2573282 checkpoint completed in 1s 692ms. Checkpoint flushed 5372 pages (4% of total available pages), in 5332 IOs. Checkpoint performed with IO limit: unlimited, paused in total 0 times( 0 millis).
5222: 2026-10-30 11:57:55.420+0000 INFO  [o.n.k.i.t.l.c.CheckPointerImpl] [neo4j/9ae801ab] Checkpoint triggered by "Scheduled checkpoint for every 100000 log chunks threshold" @ txId: 2676542, append index: 2676542 checkpoint completed in 1s 696ms. Checkpoint flushed 5455 pages (4% of total available pages), in 5405 IOs. Checkpoint performed with IO limit: unlimited, paused in total 0 times( 0 millis).
```

### 对照结论

- 23个超5秒步骤中，仅1188、1832、2480在保守时间范围内可能与checkpoint重叠。相关性不等于因果。
- 最长的tick1712（65.906秒）及tick9248（24.850秒）均未与记录的checkpoint区间重叠，因此不能用checkpoint统一解释所有长尾。
- 22/23发生在4步历史采样边界（另1个是初始化tick1）。这支持优先对actor历史写入/Run关联锁排队做span拆分；尚不能区分数据库SQL、Ray RPC调度、重试及存储I/O的各自份额。
- 旧服务只在原heap维护时2026-10-30 09:53 UTC有前后快照（maintenance-17692/20261030T095320Z-report.json），本次事故窗口没有持续历史活动采样，不归因旧服务。

### 已授权维护结果

- 用户随后授权只对17692调整页缓存；维护前已确认普通基准cleanup=0，且本任务生产者与专用writer停止。
- 同home正常stop/start：PID 3895698 → 446833；其余服务未停止，影响是17692连接短暂中断。
- SHOW SETTINGS实际生效值：heap initial 1.00GiB、max 4.00GiB；pagecache 8.00GiB。
- neo4j/system均online，维护前后C Run集合0个、WHRun集合19个，逐ID一致；未删除或移动数据库。
- 配置备份：/data/workspace/wz/warehouse-wheel-agents-20261029/graph-test/neo4j/conf/neo4j.conf.20261030T122122Z.before-pagecache-8g。前后配置、内存、文件尺寸、PID/命令行和服务只读快照：/tmp/wz-kg-evidence/stall-diagnosis/pagecache-maintenance-20261030T122122Z/report.json。
- 该维护是基于4.81GiB存储与充足可用内存的容量调整，不能据此宣称原长尾根因已证明或性能已改善。

### 共享关系端点与锁：已证据和推测边界

- `record_robot_snapshot`（当前源码153行；采样判断169行、调用181行）每4步让100个actor写历史。
- `_WRITE_AGENT_STATES`（372–378行）只是 `MATCH` 同一个Run，并没有100份 `MERGE/SET Run` 属性；但每个新AgentState会 `MERGE (state)-[:IN_RUN]->(run)`，共享Run的关系端点/关系组可能产生写锁排队。
- `_WRITE_EXECUTIONS`（380–386行）、`_WRITE_EXECUTION_STATES`（388–395行）和 `_WRITE_EVENTS`（403–407行）也向同一个Run建立关系。Execution按任务ID作用域，通常各任务不同；同任务交接/事件可能共享Execution，不能说100 actor都更新同一个Execution。
- `_WRITE_RUN_AND_ROBOTS`（363–366行）的 `MERGE/SET Run` 属于旧 `record_snapshot` 中央写入路径；当前RobotActor调用的是 `record_robot_snapshot`，不能混淆两者。
- 另一次live（wz-test-live-neo4j-032e8e4a）在 2026-10-30T10:07:34.646000+00:00 的原始样本中，84个事务对同一个运行中事务显示Blocked，查询前缀均为AgentState写入，等待/事务时间约0.2–0.3秒；随后样本blocked=0。这证明该查询曾有actor间锁排队。
- 此样本不在全天事故窗口，且首样本未采集resourceInformation，不能证明锁资源就是Run，更不能证明tick1712/9264的长尾由它导致。全天没有事故实时锁数据。原始样本已复制为prior-live-lock-sample.json、prior-live-no-lock-sample.json。
- 本次未修改lys的schema、关系或写入代码，不据推测重构。优先保留memory异步写入与8GiB缓存对照。

### 后续单次按需锁采样（已准备，未自动启动）

若下一轮UTC写时延日志再次出现大于5秒的写入，可在另一个终端显式执行一次：

```bash
cd /data/workspace/wz/warehouse-integration-on-lys
bash ops/kg_dev_env.sh python /tmp/wz-kg-evidence/stall-diagnosis/sample_locks_once.py --run-id RUN_ID --tick TICK --phase actor_graph_write --reason write_over_5s
```

该脚本只向17692执行一次 `SHOW TRANSACTIONS YIELD *`，保存query前120字符、query/transaction/connection/client标识、elapsed/wait、锁数量/资源与状态；不PROFILE、不写数据库、不持续轮询、不启动负载。run/tick为调用者标注，只有服务器暴露并匹配运行参数时才归属到某run，避免凭查询文本猜运行。
输出在本目录lock-samples/。脚本仅做语法编译校验，没有在新短测上执行采样。

### 人为联合压测/百万前缀混杂因素（补充只读核对）

来源：joint-c-day-fullscale.log、同目录acceptance.json/round文件元数据，以及其它joint原始日志。原始复制与文件时间保存在本目录joint-confound/。

|事件|UTC原始时间/保守边界|北京时间|
|---|---|---|
|最长tick1712整步66.083秒（write65.906秒）|开始约11:08:30.300–11:08:35.123，结束约11:09:36.383–11:09:41.206|19:08:30–19:09:41|
|fullscale日志出生|2026-10-30T11:08:59+00:00（原始纳秒时间见元数据）|19:08:59|
|fullscale报告生成标记（位于填数前）|2026-10-30T11:08:59.947216+00:00|19:08:59|
|已填完百万前缀后的graph观测|2026-10-30T11:11:58.993000+00:00；同run节点1112494、latest_step=1919|19:11:58.993|
|最后一轮查询后观测|2026-10-30T11:12:59.940000+00:00|约19:13|
|前缀清理完成的保守上界（cleanup后写acceptance文件）|2026-10-30T11:14:48.833530+00:00；cleanup remaining=0|19:14:48.833|
|晚期tick9248长尾|开始约11:59:36.357–11:59:41.180，结束约12:00:01.848–12:00:06.672|约19:59:36–20:00:07|

- fullscale明确作用于同一 `wz-test-day-20261030-final`，确实插入100万AgentState历史前缀，并在三轮100独立进程查询结束后清理到0。
- tick1712长步在fullscale启动前至少约24.8秒已经开始；它后段与填数任务窗口重叠。因此不能说整个66秒的初始原因是填数，也不能把这条长尾当作独占稳定负载下的表现。
- 原日志每10万行打印进度但没有逐批UTC。只能将填数阶段限定在报告生成标记到填完后graph观测之间；无法精确证明哪一条CREATE批次对应哪一段停顿。时间重叠不是因果证据。
- 清理发生在最后一轮之后、acceptance文件写入之前；原始日志明确 `[prefix cleanup] remaining=0`。报告写出位于cleanup finally之后，可保守认为11:14:48.833之前已清理完。不是将最终文件mtime误当成DELETE实际起止时刻。
- 晚期tick9248开始距前缀清理完成上界至少约44.8分钟；所有现有joint原始文件也没有晚于11:14:49的前缀作业记录。因此晚期没有已知正在进行的填数/批量删除，但不能证明不存在未记录活动，也不能排除早先填删带来的间接缓存/存储影响。
- 另一次joint-c-day-direct在约11:02–11:05期间作用同run，prefix_nodes=0但有100进程查询。这也意味着较早一段全天计时受到人为并发查询压力，不能笼统标为独占全天稳定性测试。
- 此混杂不否定联合验收本身的百万节点/100并发/同时写入指标；需要把联合压力窗口与独占稳定性结论分开。

全run失败清理也须区分：失败检查点mtime为2026-10-30T12:01:17.967097+00:00，runner在捕获失败、写完该检查点之后才进入finally清理整个run；tick9248的保守结束上界为2026-10-30T12:00:06.671515+00:00，早于失败检查点。因此后续整run删除及其12:02左右的事务日志活动也不能作为已经完成的tick9248长尾的直接原因。此处是程序顺序和文件mtime提供的下界，不是每条DELETE的实测span。

### 8GiB页缓存后的memory+异步图谱新观测（普通完整基准）

这一轮来自冻结副本`/data/workspace/wz/warehouse-kg-async-20261030`，run为`wz-test-kg-af94168d3bae`。只读分析已完成的878–900帧，未修改冻结源码/测试/配置，也没有再做SHOW采样。

|图谱帧tick|真实记录UTC开始→完成（秒精度）|actor图谱批写/回执/重试 ms|HTTP提交+持久Step确认 ms|100 actor上下文 ms|
|---:|---|---:|---:|---:|
|878|2026-10-30T12:55:51Z → 2026-10-30T12:55:55Z|253.846|4430.509|0.000|
|879|2026-10-30T12:55:55Z → 2026-10-30T12:56:06Z|6390.307|4844.777|0.000|
|880|2026-10-30T12:56:06Z → 2026-10-30T12:57:30Z|83769.562|115.371|96.954|
|881|2026-10-30T12:57:30Z → 2026-10-30T12:57:31Z|72.018|109.498|0.000|
|882|2026-10-30T12:57:31Z → 2026-10-30T12:57:31Z|76.436|110.245|0.000|
|883|2026-10-30T12:57:31Z → 2026-10-30T12:57:31Z|70.121|108.192|0.000|
|884|2026-10-30T12:57:31Z → 2026-10-30T12:57:34Z|2886.430|110.501|0.000|
|885|2026-10-30T12:57:34Z → 2026-10-30T12:57:34Z|59.545|109.045|0.000|
|886|2026-10-30T12:57:34Z → 2026-10-30T12:57:34Z|63.311|109.596|0.000|
|887|2026-10-30T12:57:34Z → 2026-10-30T12:57:35Z|61.855|111.117|0.000|
|888|2026-10-30T12:57:35Z → 2026-10-30T12:57:36Z|1616.067|113.907|0.000|
|889|2026-10-30T12:57:36Z → 2026-10-30T12:57:37Z|178.009|110.763|0.000|
|890|2026-10-30T12:57:37Z → 2026-10-30T12:57:37Z|55.185|109.584|129.727|
|891|2026-10-30T12:57:37Z → 2026-10-30T12:57:37Z|72.615|108.789|0.000|
|892|2026-10-30T12:57:37Z → 2026-10-30T12:57:39Z|1706.217|109.918|0.000|
|893|2026-10-30T12:57:39Z → 2026-10-30T12:57:39Z|73.656|110.358|0.000|
|894|2026-10-30T12:57:39Z → 2026-10-30T12:57:39Z|186.535|109.426|0.000|
|895|2026-10-30T12:57:39Z → 2026-10-30T12:57:40Z|64.214|110.172|0.000|
|896|2026-10-30T12:57:40Z → 2026-10-30T12:57:41Z|1365.206|110.484|0.000|
|897|2026-10-30T12:57:41Z → 2026-10-30T12:57:41Z|109.822|113.114|0.000|
|898|2026-10-30T12:57:41Z → 2026-10-30T12:57:41Z|56.479|110.364|0.000|
|899|2026-10-30T12:57:41Z → 2026-10-30T12:57:42Z|50.827|113.998|0.000|
|900|2026-10-30T12:57:42Z → 2026-10-30T12:57:43Z|1291.239|110.270|135.546|

- tick880发生于UTC12:56:06–12:57:30（北京时间20:56:06–20:57:30），write_ms=83,769.562、wait_step_ms=115.371、context_ms=96.954。83.8秒主要位于100 actor图谱RPC派发、等待回执及重试阶段；不能归给约97ms上下文或约115ms全局确认。
- 冻结源码`ray_actor.py:342–356`将write_ms限定在write_graph_all、ray.get和失败重试；`365–397`的wait_step_ms包含客户端submit/flush及Run.latest_step读取确认；`399–410`才是上下文查询。因此878的4.431s和879的4.845s wait也不能直接全部叫作Neo4j写Step SQL耗时，HTTP/客户端与确认轮询在同一计时区间。
- UTC12:55:30–12:58:59原始debug窗口没有checkpoint或明确VM pause/OOM记录；最近此前checkpoint为12:53:00.860–12:53:02.043（1.181秒），与880不重叠。GC详细日志仍未采集，因此不能据无日志排除GC。
- 第一份事务快照在12:58:52才取得，已晚于880结束约82秒；当时writer.latest_tick/Run.latest_step=1023、队列0、错误0、进程仍在推进，没有持续Blocked。它不能反映880进行中的锁现场。
- 同时取得的主机IO PSI full为avg10=1.13%、avg60=11.69%、avg300=17.94%，memory PSI avg10=0；这些是恢复后的主机指数衰减统计，只能提示此前存在IO压力，不能把主机IO认定为880长尾的根因。
- 明确结论：8GiB页缓存后仍出现83.8秒写入长尾，不能宣称已解决卡顿。由于模式/实现和负载也改变，旧66秒与新83.8秒不是单变量A/B，不能反向声称8GiB导致变慢。
- 本目录pagecache8g-memoryasync-observation/保存23帧原始抽样、阶段代码行和SHA、debug窗口、恢复后事务/水位及压力数据。图谱worker完成帧时间不能替代主进程物理推进时间。

### 有界触发式只读观察器（为下一次长尾保留现场）

按后续授权，独立观察器已启动。每秒只读基准status和当前run的图谱日志mtime；运行中、场景结果尚不存在且超过5秒无完成帧时，才执行一次SHOW事务/锁、Run/Robot水位、writer health和主机压力快照。相同停滞仅采一次，至少30秒冷却，最多10次；基准结束/runner退出自动退出。不会PROFILE，不修改数据或配置，也不发起额外负载。
状态为active-observer.json，证据在observer-20261030T130908Z/。run/tick标注不是实际事务归属证明。此机制不能追溯880，若下一次未命中也不能捏造现场结论。

原始证据根目录：`/tmp/wz-kg-evidence/stall-diagnosis/`；上述相对文件名均相对于该目录。
