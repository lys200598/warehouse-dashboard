# 普通进程全局 writer

日期：2026-10-30

## 任务背景

按目标架构给 4173 V5 运行增加“全局记录写入进程”。要求用普通进程实现，不局限于 Ray actor；主进程只把已提交的全局记录放入有界队列，不等待 Neo4j。

## 关键结论

- 新增 `backend/global_writer.py`：`GlobalWriterProcess` 是一个普通 `multiprocessing.Process`（spawn），不是 Ray actor，不新增端口。
- 主进程 `WarehouseLiveRuntime._persist` 把每次提交的全局统计/全局事件放入有界 `multiprocessing.Queue`；队列满只丢中间 stat，不阻塞物理 tick。
- writer 写：
  - `Run` 全局字段/汇总；
  - 每个提交 tick 一个 `RunStat`；
  - `robot_id` 为空的全局 `RuntimeEvent`（跳过 `initial_stock`，避免初始化刷 thousands 条）。
- 每个 Ray actor 继续只写自己的 `Robot/AgentState/Execution/RuntimeEvent`；全局 writer 不重复写这些机器人子图。
- `--global-writer --global-writer-queue 256` 启用；`/api/snapshot` 的 `runtime.globalWriter` 显示 `processAlive/errors/dropped/lastTick/queueSize`，`workerProcessCount` 变为主进程 + writer = 2。
- writer 失败/队列满只影响全局图谱完整性，不改变 V5 物理/调度结果。
- P0 已补：`RunStat(id)`/`Assist(id)` 唯一约束；`RunStat.commit_seq` 与 durable/mirrored/generated/writer 错误字段；`Run` 的 dataset/scene/tasks/config identity 和 `USES_DATASET`；`Assist` 节点及 donor/receiver/task 关系；`run_stats/global_events/assists` 查询 API。
- kg.html 已增加“全局统计与跨机协助”面板，读取上述三个 API；`dist/medium/kg.html` 同步。
- 已增加 `DispatchState`：每提交 tick 写 active jobs、pending、ports、robots/tasks 摘要和 reservations 摘要；约束/索引和 `/dispatch` API 已加；`KG_STATE_SOURCE=neo4j` 读取路径仍未接。
- 100 个 robot actor 与 global writer 并发写 `Run` 关系时出现 Neo4j deadlock；global writer 对 TransientError/Deadlock 重试 10 次，actor graph writer 重试 5 次，并减少 Robot 的 `IN_RUN` 关系重复写入。当前 4173 运行约 20 tick，`globalWriter.errors=0`。
- 旧 1728 静态图谱已删除，medium 静态图谱已导入；当前 Neo4j 只有 medium Dataset（capacity 2304）和 medium Warehouse。运行期 `Run.dataset_id` 使用 canonical medium id，`USES_DATASET` 已建立。历史 1728 Run/AgentState 仍可能保留，未做全量清理。

## 全局记录应写什么

### 必须写（当前/近期）

| 内容 | 字段/节点 | 当前状态 |
|---|---|---|
| Run 生命周期与身份 | `Run{run_id}`：backend、robot_count、max_active、dataset/scene/tasks/config、created/updated、status、updated_tick | 已写 |
| 每提交 tick 的全局 KPI | `RunStat`：commit_seq、tick、elapsed、status、counts、tasks、metrics、inside | 已写 |
| 任务进度汇总 | `RunStat.tasks`：completed/waiting/carrying 等状态计数 | 已写 |
| 机器人阶段汇总 | `RunStat.counts`：moving/handling/charging/standby | 已写 |
| 全局指标 | `RunStat.metrics`：完成数、等待时间、最大活跃、库存等标量 | 已写 |
| 跨机协助汇总 | `RunStat.assists` + `Assist`：donor/receiver/task/status | 已写 |
| 全局事件 | `RuntimeEvent{robot_id=null}`：assist、run 状态、全局异常等 | 已写骨架；目前跳过 initial_stock |
| 提交/一致性元数据 | commit_seq、durable seq、mirrored seq、writer errors/dropped | 已写 RunStat |
| 调度状态 | `DispatchState`：active jobs、pending、ports、robots/tasks 摘要、reservations 摘要 | 已写；主进程读取未接 |

### 建议补充（下一步）

| 内容 | 节点/关系 | 用途 |
|---|---|---|
| 关键全局事件 | `RuntimeEvent`：run_started/paused/completed/failed、congestion、port_pressure、audit_failure | 页面/复盘追溯 |
| 能量/换电汇总 | `RunStat`：平均电量、低电数、充电队列、swap 站队列 | 全局运行健康 |
| 数据质量/审计 | `RunStat`：motion_audit、separation、late tasks、dropped writes | 验收与告警 |

### 不应由全局 writer 写

- 单机器人实时位置/电量/队列：由每个 `RobotActor` 写自己的 `AgentState`；
- 单机器人任务执行/事件：由 actor 写 `Execution/RuntimeEvent`；
- 不要重复写 100 个 `Run` 或每个 actor 的 Robot 节点；
- 不要全局 writer 每 tick 写全量机器人快照，否则图谱爆炸。

## 知识图谱结构现状

已写入 Neo4j，但“有数据”不等于“有查询结构”：

| 结构 | 现状 | 是否被 kg.html/查询层使用 |
|---|---|---|
| `Run` | 原有标签，writer 更新 backend/robot_count/max_active/status/updated_tick/global_stats_json | 部分：health/runId 使用；global_stats_json 未展示 |
| `RunStat` | writer 新增标签，每提交 tick 一个；有 `(run_id,tick)` 索引 | **否**：没有 `RUNTIME_SCHEMA` 约束、没有 `Neo4jWarehouseGraph` 方法、没有 API/kg 页面 |
| `RuntimeEvent`（全局） | writer 写 `robot_id=null` 的全局事件，`IN_RUN` | **否**：现有 runtime-trace 主要查 AgentState/机器人事件 |
| `Assist` | **没有独立节点**；只在 `RunStat.assists_json` | 否 |
| `DispatchState` | **没有** | 否；`KG_STATE_SOURCE=neo4j` 未接 |
| `Robot/AgentState/Execution` | 每个 Ray actor 写自己的子图 | 是：kg.html 的运行态/任务追溯已使用 |
| 静态图 `Dataset/Task/Rack/Location/Goods` | 原有导入结构 | 是 |

结论：

- **写入层：有。** `Run`、`RunStat`、全局 `RuntimeEvent` 都已经写入 Neo4j。
- **结构/查询层：还没有完整对应。** `RunStat` 是 writer 侧临时新增标签，尚未进入 `RUNTIME_SCHEMA` 约束，也没有 `Neo4jWarehouseGraph.run_stats()`/API/kg.html 面板；`Assist`、`DispatchState` 还没有节点结构。
- 当前 kg.html 能查的是静态任务图、Robot/AgentState/Execution/机器人事件；**查不到 RunStat 和全局统计**。

下一步如果要让全局记录真正“可查”：

1. 把 `RunStat` 加入 `RUNTIME_SCHEMA`，补唯一约束/索引；
2. 给 `Neo4jWarehouseGraph` 增加 `run_stats(run_id)`、`global_events(run_id)`；
3. 给 `ops/warehouse_live.py` 增加 `/api/kg/warehouse/runs/{run}/stats`；
4. 在 kg.html 加“全局统计”面板；
5. 跨机协助建 `Assist` 节点，调度状态建 `DispatchState` 后再接 `KG_STATE_SOURCE`。

## 执行过的操作

- 阅读 `ops/warehouse_live_runtime.py` 的 `_persist`、`_publish`、`_close`。
- 新增 `backend/global_writer.py`：记录构建 + Neo4j 写入 + 子进程循环 + 有界队列。
- 修改 `ops/warehouse_live_runtime.py`：
  - `--global-writer` 参数、identity、首次 persist 启动 writer；
  - 提交时 `_record_global`；
  - `_publish` 增加 writer health/process count；
  - `_close` 关闭子进程。
- 修改 `ops/warehouse_live.py`：`--global-writer`、`--global-writer-queue`。
- 修改 `ops/start_runtime.sh` / `ops/start_medium.sh`：`--global-writer` / `GLOBAL_WRITER=1`、Neo4j 预检。
- 新增 `tests/test_global_writer.py`：记录构建、过滤 initial_stock/机器人事件。
- 在 14584 以无 Ray 模式跑 medium19 + global writer，验证：
  - `workerProcessCount=2`，主进程 PID 与 writer PID 不同；
  - writer `errors=0`、`dropped=0`、`lastTick=5`；
  - Neo4j `RunStat` 3 个（tick 0/5/10），Run.updated_tick=10；
  - 停止主进程后 writer 子进程退出。
- 17 项相关测试通过。

## 修改内容

- `backend/global_writer.py`：新增普通进程 writer。
- `ops/warehouse_live_runtime.py`：队列接入、生命周期、health、process count。
- `ops/warehouse_live.py`：CLI 参数。
- `ops/start_runtime.sh` / `ops/start_medium.sh`：启动入口与 Neo4j 预检。
- `tests/test_global_writer.py`：单元测试。
- `README.md` / 启动手册 / 分工文档：补充 `--global-writer` 说明。

## 当前验证

```text
test runId: wh-live-20261030T013257Z-9ea64a90
runtime.workerProcessCount=2
runtime.globalWriter.processAlive=true
runtime.globalWriter.errors=0
runtime.globalWriter.dropped=0
RunStat: 3 (tick 0/5/10)
Run.updated_tick=10
```

P0 修复后的新验证 run：

```text
runId: wh-live-20261030T020545Z-381123ec
RunStat.commit_seq: 1,2,3,4（tick 0/0/5/...，同 tick 不覆盖）
RunStats 唯一约束: warehouse_runtime_runstat_id
Assist 唯一约束: warehouse_runtime_assist_id
Run.dataset_id=medium19, scene/tasks/config identity 已写
GET /api/kg/warehouse/runs/<run>/stats -> 200
GET /api/kg/warehouse/runs/<run>/assists -> 200（空）
```

当前 4173 Ray+graph+global-writer 长跑：

```text
runId: wh-live-20261030T023207Z-904fc9b0
tick: 55
workerProcessCount: 2
globalWriter.errors: 0
globalWriter.dropped: 0
DispatchState commit_seq: 13
GET /dispatch -> 200
```

## 后续建议

- 把跨机协助事件明确划为全局事件，验证 `RuntimeEvent` 的 assist 记录。
- 增加 `KG_STATE_SOURCE=neo4j` 后，让主进程派单可读全局状态。
- 需要跨机/独立重启时，再把 `GlobalWriterProcess` 升级为独立常驻服务；当前单机普通进程足够。
- 当前 4173 仍按旧进程运行，未重启加载 `--global-writer`；测试实例 14584 已停止。
