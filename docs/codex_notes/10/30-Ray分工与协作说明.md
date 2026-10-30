# Ray 分工与协作说明

日期：2026-10-30
对象：前端布局 / 调度协作者；后端 / 知识图谱侧

## 1. 当前版本实际做到哪一步

当前 4173 的 Ray 版不是“机器人进程已经独立”，而是：

- 100 个 Ray actor 已存在；
- 每个 actor 执行本机器人每 tick 的**运动段计算**；主进程把结果应用到 `self.robots`；
- 加 `--robot-graph` 后，每个 actor 用自己的 Neo4j writer 写自己的
  `Robot / AgentState / Execution / RuntimeEvent` 子图；
- 共享 `Run` 节点由主进程启动时创建一次。

主进程目前仍持有并维护：

- 机器人位置、电量、stage/task/cargo；
- `self.jobs`、任务完成/phase/parts；
- 库存、货位、SIPP 交通预留；
- 换电/充电/能量准入；
- 派单、逐步推进、checkpoint、页面汇总；
- journal/RealMirror 全局写入。

所以“Ray 只做了图谱写入”这个说法从职责上看基本成立；更精确地说，Ray 现在做的是**运动段计算 + 每机器人图谱写入**，还没有接管机器人状态。

## 2. 目标分工

### 2.1 前端布局 / 调度协作者

负责：

- 前端布局、地图/任务/事件展示和交互；
- 调度/派单策略、交通/任务循环；
- 把主进程的“维护位置”剥离给 Ray：
  - `RobotActor` 持有位置、电量、任务队列、当前 job；
  - 主进程只派单、等待本步返回、汇总；
  - actor 在配送途中可以接收新任务；
- 为自己的前端/Ray 测试使用独立端口，不占用共享服务。

### 2.2 后端 / 知识图谱侧（当前这边负责）

负责：

- Neo4j 静态图谱、运行态图谱 schema；
- 每机器人 actor 的 graph writer 接口和幂等写入；
- 共享 `Run`、`Robot/AgentState/Execution/RuntimeEvent` 的字段/关系契约；
- 4173 `/api/kg/warehouse/*` 和 kg.html；
- journal/checkpoint、回放服务、Neo4j 查询验证；
- 图谱性能、重复写入、写失败隔离。

### 2.3 共同遵守

- V5 调度语义（派单、交通、库存、换电、能量）不能因为 actor 化而悄悄改变；
- actor 协议、事件字段、runId、robotId 变更要同时通知双方；
- 不写共享 Neo4j 的测试数据；不停止共享 4173/14185/26380/47687；
- 任何“主进程不再维护机器人状态”的改动，必须保留 local/legacy 回退路径和对照测试。

## 3. 需要前端 / 调度协作者先实现的部分

### 3.1 有状态 RobotActor

建议接口：

```python
actor.snapshot()                 # 返回本机器人状态给主进程派单
actor.assign(job)                # 主进程派单
actor.step(now, dispatch=None)   # 推进一个 tick，返回状态+事件
actor.checkpoint()
actor.restore(state)
```

Actor 至少要持有：

- `robot_id / robot_type_key`
- `x / y / stage`
- `battery`
- `task_queue`
- 当前 `job / parts / phase_index`
- 本机器人事件 outbox

### 3.2 主进程收缩

- 主进程不再直接改 `robot['x']/['y']/['battery']/['task_id']`；
- 派单前向 actor 请求 snapshot；
- 每步调用 actor.step，汇总返回；
- 只保留任务表、交通/库存全局约束、页面推送和持久化边界。

### 3.3 途中插单

- 任务进入 actor 本地队列；
- actor 在当前配送结束后/安全点选择下一任务；
- 主进程只决定“可派哪些任务”，不直接改 actor 队列。

### 3.4 每步循环

```text
主进程派单
  -> 所有 RobotActor 并行 step 一小段仿真时间
  -> 主进程汇总状态/事件
  -> 页面推送 + 下一轮派单
```

## 4. Actor 与图谱的接口边界

知识图谱侧当前提供：

- 每个 actor 自己的 `write_graph(snapshot)`；
- 每个 actor 只写自己的 Robot/State/Execution/Event；
- 共享 Run 由主进程创建；
- 失败只记录 `robotGraphErrors`，不阻塞仿真。

需要前端/调度侧保证：

- snapshot 中包含 `robot_id / x / y / stage / battery / task_id / cargo_id`；
- 事件包含 `event_seq / kind / tick / robot_id / task_id / goods_id` 等现有字段；
- 每个 runId/robotId 稳定，不在运行中换 ID；
- actor 重启/恢复时不要重复写入不同 Robot 节点。

### 4.1 全局记录写入进程：普通子进程已实现

结论：**已经按普通进程实现：`GlobalWriterProcess` 是一个独立 child process + 有界 `multiprocessing.Queue`，不是 Ray actor，不需要单独 Web 服务/端口。**

Ray 不是硬要求：同样的“单独进程 + 有界队列 + 单 writer”也可以用 `multiprocessing.Process`/`Queue` 或 Unix socket 实现。当前项目已经有 100 个 Ray actor 和 Ray head，但全局 writer 仍然选择普通进程，原因是它只需要一个写队列，不需要 actor RPC。

- child process 独立于主进程，由 `WarehouseLiveRuntime` 启动/关闭；
- 如果要求跨机器、独立重启或故障隔离，再把它提升为独立常驻服务；当前单机阶段不需要；
- 主进程不等待 Neo4j；队列满时只丢中间 stat，不阻塞物理 tick。

当前职责：

| 写入方 | 负责写的图 |
|---|---|
| 每个 `RobotActor` | 自己的 `Robot / AgentState / Execution / RuntimeEvent` |
| `GlobalWriterProcess` | `RunStat`、Run 全局字段、全局 `RuntimeEvent`（robot_id 为空） |
| 主进程 | 不直接写全局 Neo4j；`--global-writer` 时把已提交 tick 放入有界队列 |

接口：

```text
main  --(runId, tick, committed stats, global events)-->  multiprocessing.Queue
GlobalWriterProcess  --batch MERGE-->  Neo4j (Run / RunStat / global RuntimeEvent)
GlobalWriterProcess  --health: errors/dropped/lastTick-->  /api/snapshot runtime.globalWriter
```

约束：

- `Run` 只创建一次；每个 `RobotActor` 不重复写 `Run`；
- global writer 的写入按 `runId + tick` 幂等，可重试；
- 主进程只保留有界队列和背压，不在其中同步等待 Neo4j；
- checkpoint/flush 时等待 global writer 追平到已提交 tick；
- writer 失败只影响图谱完整性，不应悄悄改变 V5 物理/调度结果。

当前实现：

- 新增 `backend/global_writer.py`；
- `--global-writer --global-writer-queue 256` 启动普通 child process；
- 写 `RunStat`（每个提交 tick 一个节点）、`Run` 全局字段、全局 `RuntimeEvent`；
- `/api/snapshot` 的 `runtime.globalWriter` 显示 `processAlive/errors/dropped/lastTick/queueSize`；
- 测试实例中 `workerProcessCount=2`（主进程 + writer），writer `errors=0`、`dropped=0`；
- `KG_STATE_SOURCE=neo4j` 仍未接。

## 5. 独立测试端口

前端/调度协作者自测时使用：

```text
Ray head: 27380
worker: 27400–27599
页面: 14590
输出: /tmp/wh-frontend-runs
```

不要占用共享 `26380 / 4173 / 14185 / 47687`；不要执行全局 `ray stop`。

详细命令见《启动与图谱数据操作手册》0.5 节。

## 6. 当前实现状态

| 图中的要求 | 当前 | 归属 |
|---|---|---|
| 100 个 Ray actor | ✅ | 已完成 |
| 每机器人写自己图谱 | ✅ | 后端/KG |
| 主进程只派单/汇总 | ✗ | 前端/调度 |
| actor 维护位置/电量/任务队列 | ✗ | 前端/调度 |
| 配送途中接收新任务 | ✗ | 前端/调度 |
| 独立全局记录进程 | ✅ | 后端/KG；普通子进程已实现 RunStat/Assist/global events |
| `KG_STATE_SOURCE=neo4j` | ✗ | 后端/KG |

## 7. 交接顺序

1. 前端/调度先实现有状态 `RobotActor`，保留当前 `MotionActor` 作为回退。
2. 主进程逐步改为读 actor snapshot，不再写位置。
3. 双方一起跑 local/Ray 对照，确认 V5 调度结果不变。
4. 全局 writer 已用普通进程接上；下一步接 `KG_STATE_SOURCE=neo4j`。
5. 最后再接 live frames 录制。

当前分支：`dev_261028-layout-migration`
最近提交：`7d86f44`（Ray actor + 每机器人图谱）、`a8d90fb`（启动手册/分工文档）
