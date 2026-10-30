# 仓储机器人与知识图谱项目交接说明（面向后续大模型/协作者）

> 本文是“先读什么、系统现在是什么、不要误改什么”的项目交接入口。开始编码前先读本文、`AGENTS.md` 和 `启动与图谱数据操作手册.md`；不要仅根据页面外观推断数据来源。

## 1. 用户最终目标

用户希望完成一个仓储机器人系统的知识图谱闭环，参考 `KG_Embodied` 和 LaDe 的设计思路。目标不是只展示一个动画页面，而是具备以下能力：

1. 前端展示约 100 个机器人并发搬运、状态、负载、任务和仓库布局；
2. 生成可复现的测试业务数据集，并能扩展为百万级知识图谱数据；
3. 把仓库/货架/货位/货物/订单任务等静态事实写入 Neo4j；
4. 让机器人执行任务时持续产生位置、阶段、取货、放货、充电、完成等运行态图谱；
5. 能从图谱工作台查询任务全链路、机器人运行轨迹和运行事件；
6. 最终面向“百万级图谱、100 智能体并发查询低延迟”的后续优化目标。性能压测和最终优化不属于当前代码交接阶段。

当前优先级是：**数据集 → 静态图谱 → 4173 实时执行/写图 → 图谱查询** 的可见闭环。不要为了保留旧演示动画而回退 CSV 数据源或机器人自主写图逻辑。

## 2. 必须先理解的四个运行面

项目有多个页面和运行路径，它们相关但不等价。

|运行面|入口/端口|数据来源|用途|与 Neo4j 的关系|
|---|---|---|---|---|
|静态数据包|`data/generated/warehouse100-day1/`|生成器输出 CSV|业务输入：任务、库存、位置|可一次性导入静态图谱|
|V5 仿真与回放|`backend.warehouse run` / `14183`|CSV 数据包|离线、可复现的调度与历史帧回放|可写运行态图谱|
|4173 实时控制台|`backend.server` / `4173`|可选 CSV 数据包，默认演示任务|实时页面、Ray Actor 派单和搬运|机器人 Actor 自主写运行态图谱|
|图谱工作台|`4173/kg.html`|项目专用 Neo4j|静态图谱、任务、机器人、运行态追溯|只读查询，不负责写入|

### 最重要的边界

- **14183 是 V5 历史回放。**它读已完成运行生成的 `frames.sqlite`，不是实时服务。
- **4173 是实时服务。**当前可读取同一类 CSV 数据包，但其运动/交通内核仍是 `SimulationCoordinator`，不是 V5 的逐帧回放实现。
- 两者都能使用数据集和写 Neo4j，但其 `runId` 不相同，绝不能混用追溯结果。
- 4173 的可视仓库使用协作者的扩展布局渲染；实时控制内核将业务位置投影到紧凑控制地图。页面上的“CSV DATASET”表示任务源已是 CSV，**不表示轨迹已经与 V5 完全一致**。

## 3. 建议阅读顺序

1. `AGENTS.md`：本项目的工作规则。多文件/长任务需要写 `docs/codex_notes/<MM>/` 总结；优先用 `./run_in_lade.sh python`，但该脚本依赖缺失时可使用 `./.venv/bin/python` 并说明原因。
2. 本文：系统边界和当前完成度。
3. `启动与图谱数据操作手册.md`：首次环境、Neo4j、数据生成、静态导入、Ray 与 4173 的完整操作命令。
4. `docs/graph-contract-v1.md`：静态图谱合同和节点/关系语义。
5. `backend/server.py`：4173 服务、HTTP/WebSocket、配置和 Neo4j 接口。
6. `backend/simulation.py`：4173 主协调器；这里是派单、任务回执、库存投影和交通决策权威。
7. `backend/runtime.py`：Local/Ray 运行时和每个 Ray Robot Actor。
8. `backend/domain.py`：单个机器人的状态机，支持 `INBOUND` / `OUTBOUND`。
9. `backend/dataset.py`：CSV 数据包读取与字段适配。
10. `backend/warehouse_runtime_graph.py`、`backend/warehouse_graph.py`、`backend/warehouse_graph_query.py`：运行态写入、静态图谱导入和查询。
11. `dist/app.js`、`dist/kg.html`：4173 主页面与知识图谱工作台。
12. `backend/warehouse/`、`backend/scheduling_v5/`：V5 仿真与回放路径；修改前先明确是否真的需要影响 V5。

## 4. 数据、图谱与仿真的正确关系

### 4.1 CSV 数据集不是仿真结果

数据生成脚本先单独创建测试业务输入。典型包目录为：

```text
data/generated/warehouse100-day1/
├── manifest.json
├── initial_inventory.csv
├── tasks.csv
└── validation.json
```

`tasks.csv` 使用 Warehouse100 七字段业务格式：

```text
task_id,task_type,goods_id,release_time,due_time,source_id,target_id
T-000001,OUTBOUND,INITIAL-00001,...,R01-L1-C1-F1,OUT-1
T-000005,INBOUND,GOODS-20261027-000001,...,IN-1,R01-L1-C1-D1
```

- `INBOUND`：货物从 `IN-n` 入库台进入，送往目标货位；
- `OUTBOUND`：货物从源货位取出，送往 `OUT-n` 出库台；
- `initial_inventory.csv` 给出开始时已有货物及其货位；
- `release_time` 定义任务在仿真时间中的放行顺序；不是页面随机生成任务。

### 4.2 静态图谱

脚本 `scripts/import_warehouse_neo4j.py` 将数据包写入 Neo4j。核心节点包括：

|节点|含义|
|---|---|
|`Dataset`|一次数据包版本与来源|
|`Warehouse` / `Zone`|仓库与分区|
|`Rack` / `Location`|货架和货位/逻辑入出库边界|
|`Goods`|初始或入库货物|
|`WarehouseTask`|CSV 中的 `T-*` 任务|

核心关系包括 `IN_ZONE`、`HAS_RACK`、`HAS_LOCATION`、`STORED_AT`、`INCLUDES_TASK`、`FROM`、`TO`、`CONCERNS`。具体合同以 `docs/graph-contract-v1.md` 为准。

### 4.3 运行态图谱

运行时写入以下节点：

|节点|含义|
|---|---|
|`Run`|一次 V5 或 4173 运行批次|
|`Robot`|运行批次内某台机器人|
|`AgentState`|某个 tick 的机器人状态采样|
|`Execution`|一个 `T-*` 或其他任务在该 Run 中的实际执行|
|`RuntimeEvent`|派单、取货、放货、阻塞、完成、充电等事件|

主要关系：`Robot-[:IN_RUN]->Run`、`AgentState-[:OF_ROBOT]->Robot`、`Execution-[:ASSIGNED_TO]->Robot`、`Execution-[:EXECUTES]->WarehouseTask`、`RuntimeEvent-[:ABOUT_ROBOT]->Robot`、`RuntimeEvent-[:ABOUT_EXECUTION]->Execution`。

静态 `WarehouseTask` 关联成功的前提是：4173 启动时 `WAREHOUSE_NEO4J_PACKAGE_DIR` 与 `--simulation-dataset-dir` 指向同一个已静态导入的数据包，且实时任务仍保留原始 `T-*` ID。

## 5. 已完成内容

### 5.1 数据与静态图谱

- 已有 Warehouse100 测试数据生成器：`scripts/generate_warehouse100_dataset.py`。
- 已有 100 机器人、日级任务量数据包和百万运行态样本生成器：`scripts/generate_million_runtime_dataset.py`。
- 已有静态图谱导入和只读查询接口。
- 图谱支持仓库、货架、货位、任务、货物、订单/任务来源与目标等关系。

### 5.2 布局与前端

- 已合入协作者布局能力和均衡扩展布局：`data/layouts/expansion-candidates/balanced/`。
- 4173 页面能渲染扩展仓库布局、货架、货位、工位和机器人状态。
- 前端保留了旧控制坐标到新布局的映射层；它是兼容层，不是 V5 坐标权威。
- 4173 在 CSV 模式下会显示 `CSV DATASET` 与 `CSV 测试数据集`，任务列表应出现 `T-*`，而不是仅有 `TK-*`。

### 5.3 4173 + Ray + 图谱实时写入

- 4173 可通过 `--backend ray` 运行 100 个 Ray Robot Actor。
- 主进程（`SimulationCoordinator`）负责：任务放行、派单、交通裁决、货位状态权威更新、回执汇总、WebSocket 推送。
- 单个 Robot Actor 负责：保存自己的状态机、执行移动/取放、在接收任务和提交每 tick 状态时写自己的 Neo4j 图谱事实。
- 默认运行态图谱写入模式为 `robot-actors`；旧中央批量写入模式仅作为 `--warehouse-neo4j-write-mode central` 的兼容选项。
- 主进程只会一次性创建运行态图谱约束/索引，不写运行态节点和关系。
- Neo4j 写入失败不会停止机器人搬运，错误随 Actor 回执返回；可从 `/api/health` 的 `graphWriterErrors` 和 `runtimeGraph.lastError` 检查。

### 5.4 4173 CSV 接入

- 新参数：`--simulation-dataset-dir <数据包目录>`，或环境变量 `WAREHOUSE_SIMULATION_DATASET_DIR`。
- `backend.server` 加载 `tasks.csv` 和相邻的 `initial_inventory.csv`。
- 4173 以数据集的释放时间放行任务，使用真实 `T-*`、货物 ID、入/出库类型和货位 ID。
- `INBOUND` 任务执行“入库台 → 目标货位”；`OUTBOUND` 执行“源货位 → 出库台”。
- 为兼容已有 4173 紧凑控制地图，物理货位会按确定规则投影到其控制货位；原始数据集 location ID 始终保存在任务和 Neo4j 中。

### 5.5 验证状态

当前已经通过的关键单元测试包括：

```bash
./.venv/bin/python -m unittest \
  tests.test_live_dataset_source \
  tests.test_robot_actor_graph \
  tests.test_simulation \
  tests.test_warehouse_runtime_graph -v
```

最近结果为 15 项通过。真实 Ray + Neo4j 联调依赖当前机器实际服务，不要把单元测试通过误报成已完成压力测试。

## 6. 未完成内容与推荐下一步

### 6.1 最重要的未完成项：统一 V5 和 4173 运行内核

当前已经做到“4173 使用真实 CSV 任务”，但还没有做到“4173 的每一步状态就是 V5 的每一步状态”。差异如下：

|能力|4173 当前实现|V5 当前实现|后续工作|
|---|---|---|---|
|任务输入|CSV，已接入|CSV，已接入|保持同一数据包合同|
|机器人状态机|实时控制台状态机|V5 调度状态机|统一或增加 V5 实时适配器|
|路径/交通|紧凑控制地图模型|V5 仓库布局模型|让 4173 WebSocket 推送 V5 提交帧|
|坐标|前端映射到扩展布局|V5 原生布局坐标|移除映射层，直接使用 V5 坐标|
|回放|实时页面不保存 V5 帧|`frames.sqlite` 历史回放|保留 14183 作为历史复盘|

推荐实现方向：不要再复制一套任务逻辑；增加 `backend.server` 的 V5 实时适配模式，在 V5 的稳定提交点同时完成：更新 WebSocket 快照、写 `frames.sqlite`、写 Neo4j。完成后 4173 与 14183 可使用同一个 `runId`，但这是单独的大改动。

### 6.2 图谱查询体验

- `kg.html` 已支持查询，但应进一步自动带入当前 4173 `runId`，减少用户不知道输入什么的情况。
- 任务追溯应优先展示：静态任务事实 → 分配机器人 → 运行事件 → 当前/最终状态。
- 可以增加数据库运行批次选择器，但不要把查询页面改成新的布局页面。

### 6.3 性能与规模

- 百万 `AgentState` 数据可生成，但尚未在目标硬件做 100 Agent 并发查询小于 500ms 的完整压测验收。
- 后续优化应使用索引、分区/时间窗、批量写入、连接池、查询限行与真实并发负载测试；不要仅用浏览器动画帧率作为性能结论。

### 6.4 其他协作者负责的区域

- 仓库布局、机器人策略、充电策略的设计权以协作者分支/设计文档为准。
- 本分支重点是知识图谱、数据接入、运行态读写、查询与 4173 对接。
- 合并时先保护 `backend/runtime.py`、`backend/server.py`、`backend/simulation.py`、`backend/domain.py` 的当前接口；不要用协作者较旧版本整体覆盖这些文件。

## 7. 关键启动与验证方式

完整命令以 `启动与图谱数据操作手册.md` 为准。以下是当前实时闭环的最小结构。

### 7.1 项目专用 Neo4j

- HTTP：`127.0.0.1:47474`
- Bolt：`127.0.0.1:47687`
- 数据目录：`.runtime/warehouse-neo4j-5.26.32/`
- 默认示例密码：`test123456`（仅本地开发示例，生产环境不可沿用）

Neo4j 已运行时不需要为重启 4173 而重启它。

### 7.2 独立 Ray Head

同一 Linux 用户可能有其他协作者的 Ray。**禁止把 `ray stop --force` 当作本项目停机方式**，它会扫描同一用户的全部 Ray 进程。项目推荐独立端口与临时目录：

```bash
mkdir -p /tmp/wh-ray
./.venv/bin/ray start --head \
  --node-ip-address=127.0.0.1 --port=26380 --num-cpus=12 \
  --temp-dir=/tmp/wh-ray \
  --min-worker-port=26400 --max-worker-port=26599 \
  --dashboard-port=18265 --ray-client-server-port=20001 \
  --disable-usage-stats
```

- Ray Unix socket 对路径长度敏感，因此不要把 `--temp-dir` 设为较长的项目路径；使用 `/tmp/wh-ray`。
- 不要把 GCS 端口设在 Ray worker 端口区间内；当前推荐 GCS 为 `26380`、worker 为 `26400-26599`。
- 看到其他目录的 `zombie` Ray 进程时不要 kill；僵尸进程不可直接终止，且它们不是端口占用的活动服务。

### 7.3 4173 CSV + Actor 自主写图

至少需要这些环境变量：

```bash
export WAREHOUSE_NEO4J_URI='bolt://127.0.0.1:47687'
export WAREHOUSE_NEO4J_USER='neo4j'
export WAREHOUSE_NEO4J_PASSWORD='test123456'
export WAREHOUSE_NEO4J_DATABASE='neo4j'
export WAREHOUSE_NEO4J_PACKAGE_DIR='data/generated/warehouse100-day1'
export WAREHOUSE_SIMULATION_DATASET_DIR='data/generated/warehouse100-day1'
export WAREHOUSE_NEO4J_WRITE_ENABLED='1'
export WAREHOUSE_NEO4J_WRITE_MODE='robot-actors'
export RAY_AUTH_MODE='disabled'
```

服务关键参数：

```bash
CHECKPOINT="data/checkpoints/console-csv-ray-$(date +%Y%m%d-%H%M%S).json"

./.venv/bin/python -m backend.server \
  --backend ray --ray-address='127.0.0.1:26380' --ray-actor-cpus=0.1 \
  --robot-count=100 --max-active-robots=100 \
  --simulation-dataset-dir="$WAREHOUSE_SIMULATION_DATASET_DIR" \
  --checkpoint-path="$CHECKPOINT" --port=4173
```

启动后检查：

```bash
curl http://127.0.0.1:4173/api/health
```

应重点确认：

```json
{
  "backend": "ray",
  "taskSource": "csv-dataset",
  "runtimeGraph": {
    "enabled": true,
    "writeMode": "robot-actors",
    "writerOwner": "ray-robot-actors"
  },
  "graphWriterErrors": 0
}
```

`runId` 是本次 4173 运行的图谱追溯输入。打开 `http://127.0.0.1:4173/` 应看到 `CSV DATASET` 和 `T-*` 任务；打开 `http://127.0.0.1:4173/kg.html` 用该 `runId` 查询运行态。

## 8. 修改代码时的约束

1. 不要删改用户已有、与当前任务无关的工作区改动；工作区可能是 dirty 状态。
2. 只用 `apply_patch` 编辑文件；不要用重定向或 `cat` 覆盖项目文件。
3. 不要以 `git reset --hard`、`git checkout --`、全局 `ray stop --force` 处理问题。
4. 不要启动两个服务使用相同的 checkpoint 路径；每次新运行生成新的 checkpoint 文件名。旧 checkpoint 需加 `--resume`，且配置必须兼容。
5. 不要将 4173 的 `runId` 填到 V5 运行的回放/追溯结论中，反之亦然。
6. 改图谱模型时，同步审查：静态导入、运行态 writer、查询 API、`kg.html` 和图谱合同文档。
7. 改 CSV 格式时，优先改生成器和 `backend/dataset.py`，再同步 V5、4173 和导入器；不要只在前端硬编码适配。
8. 完成多文件工作后运行针对性测试、`git diff --check`，并按 `AGENTS.md` 写开发总结。

## 9. 当前重要文件速查

|文件|职责|
|---|---|
|`backend/server.py`|4173 服务、CLI、Ray/Neo4j/CSV 配置、HTTP/WebSocket|
|`backend/simulation.py`|4173 主协调器、CSV 放行/派单、库存投影、交通和回执|
|`backend/runtime.py`|Local/Ray Runtime、Actor 私有 Neo4j writer 调用|
|`backend/domain.py`|机器人状态机、入库/出库阶段|
|`backend/dataset.py`|任务包和库存 CSV 读取|
|`backend/warehouse_runtime_graph.py`|运行态节点、关系、索引和批量写入合同|
|`backend/warehouse_graph.py`|静态图谱导入|
|`backend/warehouse_graph_query.py`|图谱查询与追溯 API|
|`backend/warehouse/`|V5 命令行、运行、回放服务|
|`dist/app.js`|4173 可视化和 WebSocket 快照消费|
|`dist/kg.html`|知识图谱工作台页面|
|`scripts/generate_warehouse100_dataset.py`|测试业务数据包生成|
|`scripts/import_warehouse_neo4j.py`|静态 Neo4j 图谱导入|
|`启动与图谱数据操作手册.md`|可直接执行的运行/排障手册|

## 10. 给后续大模型的第一轮检查清单

在回答“系统是否已完成”或继续开发前，先逐项确认：

- 当前分支、未提交修改和协作者分支分别是什么；
- Neo4j 是否是本项目专用端口 `47474/47687`；
- 4173 是否在 `ray` 后端、`csv-dataset` 任务源和 `robot-actors` 写入模式下运行；
- 页面显示的是实时 4173 还是 14183 V5 回放；
- 用于查询的 `runId` 来自哪个运行面；
- 静态数据包是否已导入 Neo4j；
- 当前问题是数据源、调度、可视化、图谱写入还是查询层，不要跨层盲改。

只有这些边界清楚后，才能判断下一步是继续统一 V5 与 4173、改图谱查询，还是做规模与性能优化。
