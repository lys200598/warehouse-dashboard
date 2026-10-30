# 当前分支 vs wz/integration-on-lys 功能对比

日期：2026-10-30
当前分支：`dev_lys_261030` @ `2956e3e`
协作者分支：`wz/integration-on-lys` @ `e30af3a`
共同祖先：`a8d90fb`

## 结论

- 两个分支**不是简单的谁包含谁**：
  - 当前分支只比共同祖先多 1 个提交：`2956e3e`（全局 writer / DispatchState / 120 单一基准）；
  - 协作者分支比共同祖先多 **31 个提交、246 个文件**，包含前端重构、100 个 agent 进程、查询/资源图/压测工具等。
- 协作者分支包含我们较早的 `a8d90fb`（120 布局、回放、Ray motion actors、per-actor graph writer、启动脚本），但**不包含**最新 `2956e3e` 的 global writer、RunStat/Assist/DispatchState、120 静态图谱切换。
- 当前分支**不包含**协作者新增的统一前端、agent_runtime、query_views、resource_graph、free_ports、压测/验收工具和 seven-patterns 数据。
- 两边都有“100 个 Ray motion actor”和“每 actor 写自己图谱”，但两边都还没有实现 `docs/design/ROBOT_ACTOR_DISPATCH_SPLIT_20261030.md` 里那种“有状态机器人 Ray 进程 + 主进程只派单”的最终结构；目标分支该设计文档自己标注为“待对齐，尚未实施”。

## 功能对比

| 功能/范围 | 当前 `dev_lys_261030` | `wz/integration-on-lys` |
|---|---|---|
| 120×90/48 架/2304 货位布局 | ✅ | ✅（来自共同祖先） |
| 4173 medium Ray 实时页 | ✅ 独立 `dist/medium` | 改为统一 `dist/live.html`/`live.js`，删除 `dist/medium` |
| 100 Ray motion actors | ✅ | ✅（合并了 a8d90fb） |
| 每 actor 写自身 Robot/AgentState/Event | ✅（含 deadlock 重试） | ✅（合并了 a8d90fb，但没有当前分支的重试优化） |
| 全局 writer（普通进程） | ✅ RunStat/Assist/DispatchState/global event | ❌ 没有 `backend/global_writer.py` |
| RunStat/Assist/DispatchState 结构 | ✅ 约束、字段、API、kg 面板 | ❌ 没有当前 2956e3e 的结构 |
| 120 静态图谱/旧 1728 清理 | ✅ 当前 Neo4j 只有 medium Dataset/Warehouse | ❌ 未包含 2956e3e 的切换 |
| canonical Dataset + USES_DATASET | ✅ | ❌ 仍是我们旧的 alias 方案 |
| 统一前端组件 | ❌ 仍是 `dist/medium` 副本 | ✅ `dashboard-view.mjs`、`motion-view.mjs`、`frame-buffer.mjs`、`live.html/js`，删除 legacy-console 显示模式 |
| 100 个独立 HTTP agent 进程 | ❌ | ✅ `backend/agent_runtime/`（100 个 multiprocessing HTTP 读写，A 套图） |
| 查询目录/视图/事件计数 | 基础 kg API | ✅ `query_catalog`、`query_views`、查询加速/压测 |
| 资源图/货架资源/混合车队 | 基础 V5 | ✅ `resource_graph`、mixed fleet、rack resources |
| 自由工位/端口背压 | 基础 medium 配置 | ✅ free ports、port relief、logical boundaries |
| 压测/联合验收工具 | ❌ | ✅ `pressure_gui`、`load_harness`、`joint_acceptance`、`quick_load` 等 |
| seven-patterns 七天数据 | ❌ | ✅ 50k 任务参考数据 |
| activation limit / dispatch golden | 未单独做 | ✅ `test_dispatch_golden`、medium19 1780 tick 基准、activation seam |
| 有状态 Ray RobotActor（主进程只派单） | ❌ 只有设计/评估 | ❌ 只有设计文档，代码尚未实施 |

## 两边各自独有

### 当前分支独有

- `backend/global_writer.py` 普通进程 writer：RunStat、Assist、DispatchState、全局 RuntimeEvent；
- `DispatchState` 约束/API/kg.html 全局统计面板；
- Neo4j RunStat/Assist/DispatchState 字段、deadlock 重试；
- 120 静态图谱导入、旧 1728 静态数据清理、canonical Dataset；
- 当前分支的启动手册/README/四服务脚本更新。

### 协作者分支独有

- 统一前端 live 页面（`dist/live.html/js`、`dist/dashboard-view.mjs`、`motion-view.mjs`、`frame-buffer.mjs`）；
- 删除 `dist/medium` 和 legacy-console 显示模式；
- `backend/agent_runtime` 100 个 HTTP 图 I/O 进程；
- `query_catalog`、`query_views`、查询加速与联合验收；
- `resource_graph`、mixed fleet、free ports/port relief；
- 压测/压力 GUI/load harness；
- seven-patterns 50k 任务数据；
- activation limit seam、dispatch golden 测试；
- Ray 机器人进程/只派单主进程设计文档（尚未实施）。

## 合并影响

如果直接合并两个分支，主要冲突点：

1. **`dist/medium` 被协作者删除**，而当前分支在 `dist/medium/kg.html` 增加了全局面板；需要把我们的全局面板迁移到统一 `dist/kg.html`，或保留 `dist/medium` 作为兼容入口。
2. **`ops/warehouse_live.py` 默认 `--dist`**：当前分支默认 `dist/medium`；协作者默认 `dist`，并改成统一 live 前端。
3. **`ops/warehouse_live_runtime.py`**：协作者加了 live motion view/catch-up 逻辑；当前分支加了 global writer 生命周期，需要合并成一套。
4. **`config/live-wheeled-datasets.json`**：当前加了 `graph_dataset_id` canonical；协作者可能有自己的 `live-datasets` 配置，需要统一。
5. **`dist/kg.html`**：当前全局面板与协作者 kg.html 改动需要手工合并。
6. **测试/文档**：协作者有大量新测试和设计文档；当前全局 writer 测试需要保留并接入新前端。

## 建议

- 不要直接把当前分支覆盖到协作者分支上；用 merge/cherry-pick：
  1. 以 `wz/integration-on-lys` 为前端/查询/agent 基线；
  2. 把 `2956e3e` 的 global writer、DispatchState、RunStat/Assist、120 canonical Dataset 作为独立补丁接入；
  3. 前端从 `dist/medium` 迁移到统一 `dist/live.html`，kg.html 只保留一份；
  4. 以协作者的 `dispatch-golden` 为回归基准，验证全局 writer 和 Ray 运动拆分不改变调度结果。
- 当前 4173 的 global writer/DispatchState 已验证；协作者分支还没有这些能力，因此合并时不能假设目标分支已包含当前最新提交。
