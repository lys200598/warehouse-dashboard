# 每机器人独立进程的共享知识写查

## 1. 运行链路

本轮完整联合回归使用新run，覆盖100进程正常运行、失败闭锁及关闭。服务journal的幂等/待提交重放与轮式物理checkpoint分别有测试；100个进程跨重启的完整会话身份、旧HTTP证据与checkpoint联合恢复需单独验收，操作时保留旧run并另开新run，避免把局部恢复测试扩展为整套恢复已通过。

```text
真实中央仿真与权威图谱事务成功提交
  → 该批次每台机器人的真实状态，分别送入其独立 OS 进程
  → 机器人进程决定是否有状态变化、低电边界或周期采样需求
  → 自己通过 HTTP 提交观察与本地事件
  → 共享服务持久 journal → 有界队列批量写入 Neo4j → 成功 ACK
  → 机器人进程按需查询自己的历史和当前任务上下文
  → 共享服务查询 Neo4j，校验与已 ACK 的观察和 canonical seq 一致
  → 机器人进程核对身份与内容，将可用上下文交给中央协调器
```

机器人进程各自拥有独立 PID、专属 IPC 观测通道和 HTTP credential。只有共享服务持有数据库连接；子进程启动后移除继承的 NEO4J_* 环境变量。HTTP 仅监听127.0.0.1。

中央继续负责交通、任务分配、货权、货位占用等需要原子协调的事实。机器人上报自己的观察和本地事件，不能直接写“货权已经转移”等权威业务事件。机器人查询结果目前标记 `advisory_available`、`action_applied:false`；`AgentFleet.latest_contexts` 可被调度读取。既有中央历史授权闭环仍由原 KnowledgeSimulation 负责，不能把新增进程的只读上下文误写成新的控制闭环。

## 2. 文件与接入点

|文件|职责|
|---|---|
|`backend/agent_runtime/service.py`|身份绑定、HTTP端点、SQLite持久journal、批量Neo4j写入、查询与服务端计时|
|`backend/agent_runtime/fleet.py`|每机器人独立spawn进程、状态采样、自己的HTTP写查、进程监控和失败闭锁|
|`backend/agent_runtime/integration.py`|`agent_mirror_class(BaseMirror, ...)`，在原始图谱事务提交成功后触发真实观测发布|
|`ops/run_agent_simulation.py`|复用原knowledge_runtime.runner，支持原始场景、数据集、干扰、恢复与图谱审计|
|`ops/agent_process_smoke.py`|1—100真实进程的协议/图谱验收；明确区分合成fixture与实际canonical batch|
|`tests/test_agent_runtime.py`|HTTP身份、幂等、队列批写、序号/水位、超时、恢复、真实进程和故障测试|

主进程接入接口：

```python
fleet = AgentFleet(run_id=run_id, robot_ids=range(1, 101),
                   backend=Neo4jBackend.from_env(), output_dir=output/'agents')
# 仅在原canonical图谱提交已经成功后调用；不在部分物理step中调用。
rows = fleet.publish(sim.robots, sim.elapsed, sim.knowledge_seq)
context = fleet.latest_contexts.get(robot_id)
# rows记录query是否发生、源水位和context SHA；context仍仅是可用建议。
fleet.close()
```

批量runner接入：

```bash
bash run_in_lade.sh python -m ops.run_agent_simulation \
  --scene data/layouts/warehouse100-wide-v2.json \
  --dataset data/datasets/my-input --output data/outputs/my-agent-run \
  --run-id my-agent-run --knowledge-backend neo4j --knowledge-mode rules \
  --commit-seconds 30 --agent-query-seconds 30 --agent-observation-seconds 30
```

现有统一入口的额外集成由主代理负责。此处的专用runner无需覆盖旧服务。`required`模式仍要求真实Neo4j历史授权，不能用journal冒充。

## 3. 事实模型与一致性

追加两个独立标签：

- `WHAgentObservation`：run_id、robot_id、agent_seq、committed_seq、sim_time、payload_json、payload_sha256。
- `WHAgentRuntimeEvent`：observation_started、stage_changed、task_changed、low_energy_observed四类自身事件，通过FROM_OBSERVATION关联观察。

观察以 `(run,robot,agent_seq)`唯一键幂等写入，并通过OBSERVED_BY关联原WHRobot、IN_RUN关联原WHRun。写入前检查原canonical WHRun/WHRobot/WHAgentState已经存在、水位已提交、观察state逐字段与该canonical state相同。禁止凭空创建权威run或robot来凑规模。

查询先读取已ACK的机器人观察，再复用FragmentQueryService.robot_context读取该canonical seq下机器人及其任务。查询返回两部分：进程自报历史 `observations` 和权威 `canonical_context`。不做ANN，也不把这条轻量路径当成完整向量检索压测。

写入顺序为：SQLite FULL同步提交 → 图谱批事务 → 标记mirrored → ACK。若图谱提交失败，pending观察保留在journal；重新构造相同run/robot/backend identity的服务时先重放pending，服务就绪后再启动进程。相同agent_seq和不同内容会拒绝；序号跳跃、时间/源水位倒退会拒绝。可重放的source batch可能被再次观察，新增观察记录保留其canonical_seq，不伪装成新的物理动作。

## 4. 查询频率与背压

默认30模拟秒观察/查询周期，阶段、任务、携货、故障或低电跨阈值变化可提前触发。坐标每秒变化不会触发每秒查询。每次查询需要新观察时，先写入ACK再查；未变化且未到周期时不发HTTP。

服务默认写队列512条、每批最多100条、最多等待5毫秒聚合。HTTP执行线程上限128，超过上限返回503。每机器人进程同一时刻只有一个请求在途；本轮失败不会无限重试。超时后写入可能已经提交，客户端不能把超时当作成功；通过journal及agent_seq进行审计或恢复。

子进程死亡、写查错误、回包身份/摘要不一致均使AgentFleet闭锁并抛异常。集成runner沿用现有mirror.last_error门禁，在下一物理step之前失败退出；不会静默假装100进程仍工作。

## 5. HTTP 与压测接口

- `POST /api/agent/observation`：`{run_id,robot_id,seq,sim_time,committed_seq,state,events}`。
- `POST /api/agent/query`：`{run_id,robot_id,at_seq?,limit?}`，limit默认8，上限100。
- `Authorization: Bearer <本机器人credential>`，服务端将token绑定robot_id，不能借JSON字段改成其他机器人。
- `X-Probe-ID`用于逐请求匹配压测。凭证不落证据日志；外部压测凭证通过内存或环境变量传递。

`agent-http.jsonl`记录服务端perf_counter_ns开始/完成区间、完整body发送是否成功、HTTP状态、PID和request_id。关闭后写入`.status.json`记录完整性和drain状态。新增的observe/write响应包含排队加提交时长，查询响应包含backend_query_ms和service_ms。服务器区间才用于真实API并发峰值，不能用100进程数量替代同时在途100请求。

## 6. 已验证范围

- 本机11项测试通过，包含真实loopback HTTP和2个spawn进程；没有用mock宣称Neo4j通过。
- 2026-10-29本机100进程协议smoke通过：100个不同PID均独立写入后查询；不变样本不重复查询；杀死1号机器人后闭锁。最终预算/追踪版本耗时8.06秒含启动，100写、100查，聚合为46批（最大批9），查询队列拒绝0。输出位于`data/local-check/agent-smoke-100-20261029-v3/`，backend明确标记journal-only-not-neo4j。
- 首次100进程测试暴露Windows WaitForMultipleObjects最多63句柄限制；改为分组非阻塞等待后通过。失败目录保留。
- 真实Neo4j运行和统一入口端到端验收由主代理继续执行；上述协议成功不能替代图谱验收、百万前缀或100同时在途资格。

- 最终本机冷启动协议时延未通过500ms：写均值2466ms、P99 3303ms、最大3333ms，100/100超500；查询均值919ms、P99 1398ms、最大1406ms，92/100超500。协议数据不含Neo4j，这些延迟不能归咎图谱；服务器持久进程热身后需独立测量，保留全部慢样本。
- 读并发默认限制16条，独立writer保留池容量余量；排队超时1秒显式503。query_queue_ms与backend_query_ms分开记录，每个子进程独立HTTP JSONL记录完整客户端计时、状态及失败。


### 真实图谱首轮修复记录

服务器首轮独立100进程运行在elapsed=0失败，官方Neo4j driver明确报告：`Query object is only supported for session.run`。问题在新增Agent观察事务把Query包装传给了tx.run。已将托管事务内所有tx.run改为原始Cypher字符串，通过`@neo4j.unit_of_work(timeout=...)`给整个事务配置超时，继续使用session.execute_write。补充API契约回归测试，并增加环境可选实库测试：设置`WAREHOUSE_AGENT_TEST_BATCH`指向真实canonical batch及Neo4j凭证。该测试向指定run追加一个自身观察并读取权威任务上下文，不创建虚假的权威run/robot。

修复后本机13项测试中12项通过、1项实库测试因无凭证明确跳过。首轮失败目录保留，新源码与新run单独重测；本机协议通过不覆盖该真实图谱问题。

### 真实图谱第二轮与传输门限修复

第二轮100个机器人全部写入成功，93个查询成功，7个客户端收到零body的503。服务端追踪只有193条200、无503，且追踪关闭状态为complete=false。它对应HTTP Server在连接清理线程达到128门限时直接返回的传输层503；不是查询并发门限，也没有证据归因Neo4j锁。对该失败run使用冻结v2源码、绕过ensure_schema，在1/16/100线程各两轮执行600次只读Neo4j查询，全部成功；诊断产物保存在服务器diagnostics/read-only-v1。

修复版本将HTTP追踪移到有界8192条后台sink，保持128连接门限；响应处理线程不再等待同步文件I/O。传输层拒绝也记录显式原因并保留负资格。处理异常记录类型、Neo4j错误code和文件/函数/行号栈帧；不记录异常args、locals或密钥。周期采样和客户端完整请求仍不变，没有过滤首轮慢样本。

每机器人新增`agent-0001-responses.jsonl.gz`，持久文件只打开一次，每请求追加一个完整gzip member，包含完整request/response、SHA、HTTP时间与状态。JSON编码/压缩/落盘发生在HTTP计时结束之后。正常完成或故障终止前已经写完的gzip member可离线解压核验。`agent-closed-status.json`补充记录关闭后0存活进程、HTTP线程和writer线程状态，原agent-final-status保留关闭前含义。

当前14项测试：13通过，1实库可选测试因本机无凭证跳过。异步observer版本100本机协议smoke保持128门限通过，图谱新版本需主线新run验收；真实第二轮失败和全部慢请求继续保留。

### 跨Python与关闭失败保护

v4窄修复统一Py3.10的FuturesTimeoutError为builtin TimeoutError；镜像最终状态文件I/O失败时仍finally关闭fleet与base driver；HTTP observer关闭异常时仍finally关闭共享writer。所有资源清理后再尽力写关闭证据。Agent模块17项测试中16通过、1实库可选跳过，另12项离线审计测试通过；真实v4回归由主线冻结后执行，不能用v3结果替代v4来源。
