# Ray 执行层现状与恢复方案（供讨论）

日期：2026-10-29。本文只描述现状和可选方案，`wz/warehouse-design` 分支不实施 Ray 恢复。Ray 改造由lys负责，本分支保持一份不含 Ray 改动的版本供他合并。

## 1. 原始设计：中央仲裁＋Ray Actor 执行

原仓库在 `backend/runtime.py` 定义了机器人执行接口 `RobotRuntime`：

| 方法 | 作用 |
|---|---|
| `replace` / `snapshots` | 装入、导出全部机器人状态 |
| `assign` / `reroute_many` / `admit_pickups` | 中央下发任务、路径和取货许可 |
| `propose_all` | 每个机器人根据自己的状态提出本步动作 |
| `commit_all` | 中央裁决后，机器人提交被接受的动作 |

它有两个实现：`LocalRobotRuntime`（同进程）和 `RayRobotRuntime`（每台机器人一个 Ray Actor）。`backend/server.py` 用 `--backend ray --ray-address ...` 选用 Ray。部署初期的 14173/14174 服务就是这样运行的，现在仍在 PRO6000 上（`--backend ray`）。

一句话概括：**中央负责仲裁和冲突判定，100 个 Ray Actor 各自保存并推进自己的机器人状态。**

## 2. 当前实时引擎：中央物理＋独立 HTTP 读写进程

当前 14192 至 14199 所用的实时服务入口是 `ops/warehouse_live.py` → `ops/warehouse_live_runtime.py`。它加载 `backend.knowledge_runtime.sim`，背后是 V2→V3→Wide→V4→V5→RealScale→Knowledge→Wheeled 这条调度仿真继承链。

- 这条链**没有调用 `RobotRuntime`**。机器人位置、电量、货权都由中央进程直接计算（帧里标为 `coordination: central-physics-independent-robot-http-io`）。
- `backend/agent_runtime/fleet.py` 的 `AgentFleet` 为每台机器人启动一个 `multiprocessing` 进程。这些进程只通过 HTTP 上报观测、查询知识，不推进机器人运动。
- 因此，"100 个独立进程"指的是 100 个知识读写进程，并不等于 100 个执行 Actor。

偏离的形成过程：新引擎是在原代码旁边另建的，旧的 Ray 路径没有被删除，只是新路径没有接上它。

## 3. 恢复方案（待讨论）

### 方案 A：新引擎通过 `RobotRuntime` 推进机器人

每个 tick 改为：中央规划 → `propose_all` → 中央裁决 → `commit_all`。Ray Actor 重新保存机器人状态，读写进程可以并入 Actor 或保留为 Actor 的一部分。

- 优点：回到原架构，Local/Ray 两种后端可切换，便于对照。
- 需要先确认：V5 调度的预约、路径段、电量和货权状态能否拆分到 propose/commit 的粒度；哪些状态必须留在中央（例如货位预约、交通预约表）。
- 风险：每个 tick 增加 100 次 Actor 往返，需要实测对出帧间隔的影响（当前每步平均约 84ms，保存约 1.07 秒）。

### 方案 B：保留中央物理，仅把 `AgentFleet` 进程改为 Ray Actor

- 优点：改动集中在 `backend/agent_runtime/`，接口不变。
- 不足：执行仍是集中的，只是进程管理换成 Ray，与原设计意图仍有差距。

### 两种方案都需要约定的事项

1. Ray 集群的使用方式：同一集群按 namespace 和 Actor 名前缀区分使用者，或者分别启动两个集群。
2. 回退开关：保留 `local` 后端，测试和对照实验可以不依赖 Ray。
3. 验收口径：100 个 Actor 真实在线，单步、保存和查询的耗时与现在的 Local 版逐项对比，同一数据集、同一配置。

## 4. 对本分支的约束

- `wz/warehouse-design` 及后续集成分支不修改 `backend/runtime.py` 和 `backend/agent_runtime/` 的执行方式。
- 本分支的仿真修复只限显示、节奏和观测，提交说明中写明不涉及 Ray。
- 如果我们自己的压测需要 Ray 版本，另开分支实验，不并入供lys合并的干净版本。
