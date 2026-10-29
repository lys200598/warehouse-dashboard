# Ray 架构满足度评估（主进程只派单 / 机器人进程写图谱）

日期：2026-10-29
对照图：网页 → 主进程（派单/汇总）↔ N 个无人机进程；全局记录进程；Neo4j 知识图谱。
当前实现：`backend/ray_medium/` + `ops/warehouse_live.py --ray`。

## 任务背景

用户要求按图实现：

1. 主进程只负责派单和返回；
2. 每个机器人进程维护自己的位置、电量、任务队列；
3. 每个机器人进程写自己的图谱记录；
4. 另有全局记录进程写全局统计/跨机协作；
5. 主进程通过 `KG_STATE_SOURCE=neo4j` 读取调度状态；
6. 每步循环：派单 → 所有机器人并行推进一小段仿真 → 汇总 → 下一步。

本次只评估当前实现是否满足，不先改代码。

## 关键结论

- **当前不满足第 1 条。** 主进程仍是 V5 中央协调器：持有全部 `self.robots`、`self.jobs`、任务/库存/交通预留/电量/换电状态，并负责派单、推进、汇总和页面推送。Ray 只是中央 `step()` 里的一个运动计算接缝。
- **当前不满足第 2 条。** `MotionActor` 是无状态函数：只接收 `(part, at)` 返回 `(a, b)`，不持有位置、电量或任务队列；机器人状态仍在主进程。
- **当前不满足第 3 条。** 机器人 actor 不写图谱；图谱由中央 `WarehouseLiveRuntime` 的 journal/RealMirror 统一写，medium live 当前默认还是 journal，不是 Neo4j。
- **当前不满足第 4 条。** 没有独立的全局记录进程；全局写入和页面 snapshot 都由主进程完成，也没有 `KG_STATE_SOURCE=neo4j` 的调度读取路径。
- 当前满足的只是“**有 100 个 Ray actor**”的形式，以及“V5 调度结果不变”。5 tick local/Ray 对比 0 差异，证明的是 V5 行为没变，不是架构已经分布式化。
- 按这张图，下一步必须把机器人状态和写图谱职责从主进程迁到 `RobotActor`，并把主进程收缩为派单/汇总器；否则 actor 数量只是执行层的并行化，不是图上的架构。

## 执行过的操作

- 阅读 `backend/ray_medium/actors.py`：确认 `MotionActor.motion(part, at)` 无状态。
- 阅读 `backend/ray_medium/sim.py`：确认 `RayWideMixin` 只挂 `motion_executor`，V5 `WideSimulation` 仍是主状态机。
- 阅读 `backend/scheduling_v3/sim.py` 的 `step()`：确认主进程负责 `release_and_dispatch`、逐机器人状态更新、battery/charge、marker、metrics、审计。
- 阅读 `ops/warehouse_live_runtime.py`：确认 journal/RealMirror 由主进程统一写；`--ray` 只影响运动执行。
- 运行中的 4173 Ray 版：`backend=warehouse-real-scale-v1-ray`、`rayActorCount=100`，但状态仍集中。

## 修改内容

本次未改代码。评估结论针对当前未提交的 Ray 版实现。

## 逐项对照

| 图中的职责 | 当前实现 | 是否满足 |
|---|---|---|
| 主进程只派单、等待、汇总、推页面 | V5 主进程同时做派单、物理推进、电量、库存、交通、图谱 | 否 |
| 每个机器人进程维护位置/电量/任务队列 | 100 个无状态 `MotionActor`，只算运动段 | 否 |
| 每个机器人进程写自己的事件/状态 | actor 无写图谱逻辑 | 否 |
| 独立全局记录进程 | 无；主进程 RealMirror/journal | 否 |
| 主进程从 Neo4j 读调度状态 | 无 `KG_STATE_SOURCE=neo4j` 路径 | 否 |
| 每步循环：派单 → 并行推进 → 汇总 | 中央 step 内先派单，再批量调用 actor，再由中央应用结果 | 部分；主进程仍承担推进和汇总 |
| 120 页面/120 布局 | 已满足 | 是 |
| V5 调度语义保留 | 已满足（当前实现刻意保留） | 是 |

## 目标改造方案

### 1. `RobotActor` 持有机器人本地状态

每个 actor 维护：

- `robot_id/type`
- `x/y/stage`
- `battery`
- `task_queue`
- 当前 `job/parts/phase_index`
- 本地事件序列/outbox

接口建议：

```python
actor.snapshot()
actor.assign(job)          # 主进程派单
actor.step(now, dispatch)  # 推进一个 tick，返回状态+事件
actor.checkpoint()
actor.restore(state)
```

主进程不再直接改 `robot['x']/battery/task_queue`，只发派单和收结果。

### 2. 主进程收缩为 V5 Dispatcher

保留 V5 的全局派单/交通/库存语义，但只保留“调度需要”的全局状态：

- 任务表、当前派单；
- SIPP/交通预留；
- 货位/库存的全局约束；
- 机器人 actor 快照的只读视图（派单时查询）；
- 页面 snapshot 汇总。

机器人位置/电量/队列以 actor 为准；主进程不再逐机器人执行 V5 物理更新。

### 3. 每机器人写图谱 + 全局记录进程

- `RobotActor` 用本地 outbox 按 tick 批量写 `Robot/AgentState/Event/Execution`；写失败不阻塞推进，重试/幂等。
- 新增 `GlobalWriterActor`：只写全局统计、跨机协助、批次汇总；从主进程/Neo4j 读 `KG_STATE_SOURCE=neo4j`。
- 100 个 actor 不要各开 100 条 Neo4j 长连接；用写池/批量提交，避免压垮 47687/17690。
- 主进程派单前通过 `KG_STATE_SOURCE=neo4j` 读取必要的调度状态；本地 actor 快照仍用于实时推进。

### 4. checkpoint/restore

- 主进程保存派单/任务表/预留；
- 每个 actor 保存自己的机器人状态；
- 恢复时先恢复主进程，再逐个 actor restore，最后校准一次全局一致性；
- 需要保证失败边界不会出现“主进程有任务、actor 没状态”。

## 风险与工作量

- 这是对 V5 单体 `WideSimulation` 的实质重构，不是加 `--ray` 参数能完成的。
- 最大风险：V5 的库存/交通/换电/能量逻辑大量直接访问 `self.robots`；把这些状态迁到 actor 后，派单、审计、checkpoint 都要改。
- 100 个 actor 各写 Neo4j 会产生连接/事务压力，必须先设计批量写和幂等 outbox。
- 当前 4173 的 `--ray` 只能作为“Ray 执行层 PoC”，不能宣称已经符合图上的分布式架构。

## 后续建议

1. 先做 `RobotActor` 状态化 + 单机器人 `step`，用 1 台机器人跑通“actor 持有状态 + 返回事件”。
2. 再把主进程 `WideSimulation` 的逐机器人更新替换为 actor 调用，保留 V5 派单/预留为全局服务。
3. 加 `GlobalWriterActor` 和 `KG_STATE_SOURCE=neo4j` 的只读调度状态路径。
4. 做 100 actor 性能/写入压力测试，再决定是否把 Neo4j 写入设为 medium Ray 默认。
