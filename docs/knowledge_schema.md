# Warehouse 时序知识闭环 v1

## 范围

沿用现有入库上架、规则派单、交通仲裁和 Local/Ray 运行时；不增加补货、LLM、能耗准入或三维物理仿真。

## 提交与恢复契约

1. 主进程完成 `propose → resolve → commit` 后生成一个知识提交。
2. SQLite WAL 日志以一个本地事务写入不可变批次和最新完整恢复 checkpoint，`synchronous=FULL`。
3. Neo4j 是派生读取模型，每批在一个图事务内写入实体、版本、事件与 WHRun 水位。
4. 两种存储不是分布式原子事务。`durable_seq` 表示日志提交位置，`mirrored_seq` 表示已确认的图谱位置；数据库不可用时保留日志并暴露延迟。
5. checkpoint 文件是带 SHA256 的 v2 envelope；读取器仍接受旧版裸 JSON。恢复优先使用同 run 日志中较新的完整 checkpoint，不会为了恢复先创建并写入一个额外空 run。
6. 图水位回退时可从保留日志重放；图水位超过当前日志、批次哈希冲突、提交序号不连续会拒绝。重复一致提交不会重复创建历史节点。
7. 在线默认 journal 模式；`--knowledge-backend neo4j` 开启图写入。首次启用 Neo4j 需要数据库在线；此后暂时断连不会丢弃已进入日志的提交。

## 时间和身份

- `run_id` 隔离每次实验。所有图标签使用 `WH` 前缀，避免与 LaDe 图混淆。
- `tick` 是逻辑仿真步；`sim_time` 是累计仿真秒；`seq` 是每次提交的单调序号。
- 暂停、继续、优先任务等可以发生于同一个 tick，仍获得不同 seq。
- `event_seq` 在 run 内单调增长；`event_id = run_id:event_seq`。完整事件不依赖前端最近 100 条的环形列表。
- `as_of_tick` 选择该 tick 结束时最后一次已落库提交；精确区分同 tick 控制操作使用 `as_of_seq`。历史状态和事件不得越过该位置。
- 坐标仍是现有 dashboard 单位，没有未经标定就宣称真实米制。

## 图模型

| 节点 | 身份/版本 | 内容 |
|---|---|---|
| WHRun | run_id | 初始配置、来源、源码指纹、已提交 seq/tick |
| WHZone / WHRack / WHSlot | run + 业务 ID | 仓储结构、格位最新占用与预约 |
| WHRobot | run + robot_id | 机器人身份与类型；历史位置从状态节点读取 |
| WHTask / WHCargo | run + 业务 ID | 任务当前属性、货物身份 |
| WHStep | run + seq | 提交位置、tick、仿真秒、完成数、批次校验和；新提交还含运行/倍率/流量字段 |
| WHAgentState | run + seq + robot_id | 每个提交下的机器人状态；JSON payload 保留路径等嵌套字段 |
| WHTaskState / WHSlotState | run + seq + 业务 ID | 只在业务状态变化时新增的版本 |
| WHEvent | event_id | 派单、取货、上架、阻塞、充电和控制等结构化过程事实 |

关系包含：`IN_RUN`、`IN_ZONE`、`IN_RACK`、`IN_STEP`、`OF_ROBOT`、`OF_TASK`、`OF_SLOT`、`EXECUTED_BY`、`TARGETS`、`MOVES`、`ABOUT_ROBOT`、`ABOUT_TASK`。

实体的最新属性不用于回答过去的状态；任务、格位查询取截止 seq 之前的最后一个版本。AgentState 不是完整恢复包：随机数、全局队列等精确恢复状态保存在日志 checkpoint 中。

## 查询目录 warehouse-v1

- `context`：同一截止位置的机器人状态、最近事件和活动任务，单次材料化读取事务。
- `robot_history`：截止位置以前最多 200 个状态；向前翻页可将 as_of_seq 设置为上一页最小 seq 减一。
- `task`、`slot`：对应实体在截止位置的最后一个版本。
- `events`：按 robot_id / task_id 过滤的过程事件。
- `step`：提交步骤摘要。

接口：`GET /api/knowledge/status`、`GET /api/knowledge/templates`、`POST /api/knowledge/query`。只接受参数化目录查询，不接受任意 Cypher；每次查询有事务超时和返回条数上限。图写入延迟时只读已提交图水位，不使用未落库状态伪装成最新结果。

```json
{"query_id":"context","run_id":"warehouse-...","robot_id":95,"as_of_tick":100}
```

## 命令入口

在 `/data/workspace/wz/warehouse-dashboard` 使用 `./run_in_lade.sh python ...`，环境设置 `NEO4J_URI`、`NEO4J_USER`、`NEO4J_PASSWORD`，密码不写入源码或报告。

- `python -m backend.server --knowledge-backend neo4j --journal-path ... --checkpoint-path ...`：网页服务；已有 checkpoint 必须显式 `--resume`。
- `python -m backend.experiment --backend local|ray --steps ... --max-wall-seconds ... --journal-path ... --checkpoint-path ... --output ...`：有界、无浏览器实验；Local 明确标记离线生成。
- `python -m backend.replay --journal-path ... --run-id ...`：只重放知识日志，不运行机器人。
- `python -m backend.benchmark --run-id ... --clients 100 --rounds 3 --output-dir ...`：完整上下文并发测试，默认要求至少 100 万 AgentState；保留 warmup、逐请求 JSONL、环境、索引、分位数和失败数。

规模生成的批量 flush 只是降低图事务频率，SQLite 仍逐提交持久化；中间阶段图水位可能落后。默认在线模式每次提交尝试同步落图。

## 验收边界

100 个真实 Actor 运行、百万状态节点和 100 路并发请求分别记录。线程池中的 100 个身份不是 100 台物理设备，也不证明 100 个 Actor 在自主查询。当前压测是 Pro6000 同机 Bolt、warm-cache 的完整上下文读取，不是端侧网络、冷启动、向量跨模态或第三方认证。

不支持从任意图快照精确恢复整个物理仿真，不提供完整历史画面重建或主进程自动 supervisor；恢复接口与查询目录是后续扩展的基础。
