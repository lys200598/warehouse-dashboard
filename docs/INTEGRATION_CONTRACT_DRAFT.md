# 双方对接接口约定（草案 v0）

> **已被取代（2026-10-30）**：本草案写于分工确定之前。分工、机器人与全局写入的分界、图谱标签和提交点，以 [`docs/design/ROBOT_ACTOR_DISPATCH_SPLIT_20261030.md`](design/ROBOT_ACTOR_DISPATCH_SPLIT_20261030.md) 为准；本文仅作历史参考，不再更新。

日期：2026-10-29。这是 wz 一方先整理的草案，尚未与李姚舜确认；字段均取自当前代码和样例数据。目的：约定双方以哪些文件和接口为边界，边界内各自修改，改动边界时先通知对方。

## 1. 分工

| 部分 | 负责人 | 说明 |
|---|---|---|
| 数据集生成、知识图谱模型与查询 | 李姚舜 | 标签、主键、关系、查询包 |
| Ray 执行层 | 李姚舜 | 见 `docs/RAY_EXECUTION_GAP_20261029.md` |
| 仓库布局、调度仿真、实时页面、压测工具 | wz | 地图、交通、电量、显示、压测 |

## 2. 地图（布局）

- 文件：`data/layouts/<名称>.json` 或 `data/layouts/expansion-candidates/<名称>/layout.json`，同目录的 `task-locations.csv` 列出全部可用作任务端点的位置，`manifest.json` 记录场景 SHA256（`scene_sha256`）。
- 货位编号：`R<货架>-L<层>-C<列>-F<面>`，例如 `R15-L1-C4-F2`。层从 1 开始，面为 1 或 2。
- 进出口：具体工位 `IN-<n>` / `OUT-<n>`；自由进出口模式下也可以写通用端点 `INBOUND` / `OUTBOUND`，由仿真选择具体工位。
- 规则：地图一旦用于生成数据，其 `scene_sha256` 写进数据集 manifest；改地图即换新文件名或新版本，不原地覆盖。

## 3. 任务数据集

- `tasks.csv` 七个字段，顺序固定：
  `task_id,task_type,goods_id,release_time,due_time,source_id,target_id`
  - `task_type`：`INBOUND` 或 `OUTBOUND`
  - 时间：带时区的 ISO 8601，例如 `2026-10-28T00:00:01+08:00`
  - `source_id` / `target_id`：第 2 节中的货位编号或进出口
- 同目录还有：`initial_inventory.csv`（初始库存）、`manifest.json`、`validation.json`、`SHA256SUMS.json`。
- `manifest.json` 必含 `scene_sha256`、`start`、`end`、`duration_seconds`、`capacity`、`seed`、`version`；仿真启动时校验它与地图一致，不一致就拒绝运行。
- 实时服务通过注册表（例如 `config/live-wheeled-datasets.json`）按 `dataset_id` 找到这些文件；新增数据集只需加一条注册项。

## 4. 实时服务接口（仿真 → 页面、压测）

| 接口 | 用途 |
|---|---|
| `GET /api/scene`、`GET /api/scene-point` | 当前运行的地图，与帧同源 |
| `GET /api/snapshot`、`WS /ws/simulation` | 实时帧 |
| `GET /api/health`、`GET /api/datasets` | 状态、可选数据集 |
| `POST /api/control` | `start`/`pause`/`step`/`speed`/`checkpoint`/`new_run`，带 `expected_run_id` |
| `GET /api/knowledge/status`、`/catalog`，`POST /api/knowledge/query` | 知识状态与查询 |

实时帧的关键字段：`runId`、`runtimeInstanceId`、`tick`、`elapsed`、`status`、`speed`、`robots`、`occupiedSlots`、`knowledge`（含 `durable_seq`、`mirrored_seq`）、`metadata`（含 `scene_sha256`、`dataset_sha256`）。每台机器人：`robot_id`、`robot_type_key`、`x`、`y`、`stage`、`battery`、`carrying`、`task_id`、`cargo_id`、`location_id`、`dock_id`，以及显示用的 `motion`（剩余规划路径与上一秒执行折线）和 `work`（任务、货物、货位、进度）。

约定：

1. **坐标同源**：机器人坐标、路径点、货位位置都用同一次运行的场景坐标（米）。页面不对机器人坐标做缩放或换算；地图和帧的 `scene_sha256` 不一致时，页面应拒绝显示。
2. **查询提交点**：查询只落在帧里的 `mirrored_seq` 上，即已完整写入图谱的提交点。
3. **新增字段可以，删改字段先通知**：页面和压测工具会忽略未知字段。

## 5. 知识图谱

- 写入：仿真每次保存产生一个提交（`knowledge_seq`），先写 SQLite 日志，再镜像到 Neo4j；图谱中 `WHRun.committed_seq` 表示已镜像的提交。
- 标签、主键和关系以李姚舜的图谱设计为准（参见 `docs/RACK_RESOURCE_GRAPH_GUIDE.md`）。改动标签、主键或关系时，需要同步修改查询包、机器人读写接口和压测验真器，具体对照 `docs/LOAD_TEST_COOKBOOK.md` 的兼容表。

## 6. 改动边界时的流程

1. 在 PR 或提交说明里写明改了哪一节的约定。
2. 地图、数据集换新版本时，同时更新 manifest 的 SHA256。
3. 对方合并前，先跑 19 任务样例，确认库存、货物流转、图谱写查都正确。
