# KG_Embodied 迁移审计与实施建议

### 任务背景

审阅相邻项目 `/home/lys/workspace/projects/KG_Embodied`，结合本项目内的《LaDe项目架构与跨项目复用指南》，识别为人形机器人仓储系统补齐前端展示、100 个智能体并行工作、百万级知识图谱，以及 100 智能体并发查询小于 500ms 所需迁移的能力。此次只做审计与方案，不复制代码、不改动现有业务实现。

### 关键结论

- 当前项目已经实现了最重要的执行底座：100 个 `Ray` actor、统一仿真时钟、提案/交通仲裁/提交屏障、WebSocket 实时快照、断点恢复和 Canvas 仓库看板。它不是从零开始的项目。
- 已确认“100 个智能体同时工作”在本任务中指 100 个智能体并发查询知识图谱，而非 100 台机器人同时执行搬运；因此当前 `max_active_robots=20` 不构成该性能指标的阻塞项。机器人执行并行与图查询并发须分别度量。
- 应迁移 KG_Embodied 的架构与接口，不应直接搬运其配送业务、无人机电量模型、区域 depot、任务数据适配和 Canvas 页面。KG_Embodied 本身是无人机配送仿真，并不提供人形机器人运动学、抓取、避障或真实控制代码。
- 第一优先级是新增可替换的知识层：内存事件实现用于开发，Neo4j 实现用于实验；以 `Run` 隔离数据，写入 `Robot`、`WarehouseTask`、`Rack/Zone`、`Event`、`Step`、`RobotState` 六类时序事实，并通过领域无关的 `KnowledgeStore` 接口接入 `SimulationCoordinator`。
- 第二优先级是从真实仓储事件或可复现合成数据批量导入百万级图数据，并在导入前建立与查询模板匹配的复合索引。不能靠运行前端仿真慢慢积累百万节点，也不能只按节点数宣称“百万级”；验收需明确节点、关系、run 规模和 schema 状态。
- 第三优先级是迁移查询模板注册、参数校验、并发压测、分阶段计时和 JSONL 结果证据；手写通用 Cypher 接口只可用于管理员调试，不能直接向浏览器开放。
- 已保存的 KG_Embodied 干净基准中，100 条并发查询全部成功，p95 为 294.416ms、p99 为 302.609ms、wall time 为 329.268ms；它表明目标可达，但只是 `agent-state` 一类特定查询的历史结果，不能作为本项目的验收结果。
- KG_Embodied 的另一组 12 类上下文查询说明风险：无属性索引时百万 `AgentState` 标签扫描会达到秒级；建立索引后该组端到端 p95 为 301.065ms、最大 653.286ms，且 i2 查询存在语法错误。因此本项目要以“指定查询包的成功率、p95、p99、冷/热缓存、并发读写状态”作为验收标准，而不是只看平均值。

### 执行过的操作

- 阅读当前项目的 [跨项目复用指南](../../../26-LaDe项目架构与跨项目复用指南.md)、`README.md`、`backend/domain.py`、`backend/runtime.py`、`backend/simulation.py`、`backend/traffic.py`、`backend/server.py`、`backend/checkpoint.py`、`dist/index.html`、`dist/app.js` 和现有测试。
- 阅读 KG_Embodied 的 `README.md`、`项目目标.txt`、`HANDOFF.md`、`simulation/knowledge.py`、`simulation/graph_query.py`、百万数据构建/并发查询脚本及性能证据文档。
- 运行 `./.venv/bin/python -m unittest discover -s tests -v`；8 项测试均通过。
- 未连接 Neo4j、未启动 Ray、未执行数据导入，因而没有把历史基准误当作本机实测结果。

### 修改内容

- 新增本审计文档；未改动业务、前端、依赖或数据库。

### 后续建议

按以下顺序实施，且每阶段完成后再进入下一阶段：

1. **固化业务契约和验收查询包。** 定义机器人、任务、货架、区域、事件、tick 的稳定字段；以“100 个不同机器人 ID 同时发起图查询”为并发口径。先选出 5--10 条真实决策查询，例如单机器人最近状态、当前任务链、邻近机器人资源、区域拥堵、时间窗事件和 step 汇总。
2. **接入知识写入。** 新建 `backend/knowledge.py`，实现 `InMemoryKnowledgeStore` 与可选 `Neo4jKnowledgeStore`；在 `SimulationCoordinator.step_once()` 的事件处理和 tick 结束处批量写入。写入必须位于协调器，而不是 actor 内，以保留单一事实来源；以队列/批量提交隔离图数据库背压，避免同步写库拖慢仿真 tick。
3. **设计仓储时序图 schema 与索引。** 推荐：`Run`、`Robot`、`WarehouseTask`、`Rack`、`Zone`、`Event`、`Step`、`RobotState`，并通过 `IN_RUN`、`OF_ROBOT`、`IN_STEP`、`ABOUT_TASK`、`AT_RACK`、`ASSIGNED` 等关系表达事实。至少根据最终查询计划验证 `RobotState(run_id, robot_id, step_index)`、`RobotState(run_id, zone_id, step_index)`、`RobotState(run_id, step_index)`、`Event(run_id, robot_id, sim_time)`、`Event(run_id, zone_id, sim_time)` 和 `Step(run_id, step_index)`；不要只创建 lookup index。
4. **生成百万级独立基准库。** 迁移 KG_Embodied 的“先 dry-run 估算、显式 apply、分批 UNWIND、创建 schema、导入后计数核验”模式；数据应来自仓储任务/事件的确定性生成器或脱敏真实数据，独立于实时 run。记录节点、关系、各标签数、磁盘占用和导入版本。
5. **迁移查询服务与压测。** 提供仅允许命名模板的 `/api/kg/query/{template}`，参数化 Cypher、限制返回行数/超时；迁移 100 agent、连接池、逐请求 JSONL、p50/p95/p99、服务端/客户端分阶段计时、成功率和 QPS 的压测能力。查询日志须异步或可关闭，避免日志 I/O 伪造查询瓶颈。
6. **扩展看板而非替换它。** 保留当前 WebSocket + Canvas 渲染、插值与选中机器人面板；新增“KG 状态/查询延迟/图谱规模/查询结果”侧栏和按 tick 回放。若“人形机器人”要求真实三维外观、关节姿态或动作，另行引入模型资产和 3D 引擎；KG_Embodied 的二维无人机页面不能解决这一项。
7. **用目标环境验收。** 以独立 Neo4j 实例、百万级基准数据、100 个不同 robot ID、连接池不少于并发数、无并发导入为基线，分别测冷/热缓存与 1/10/50/100 并发。建议门槛：100% 成功、每个必选模板 p95 < 500ms，另报告 p99、最大值、wall/QPS 和系统 CPU/内存/页缓存；增加写入负载后的独立对照。

### 功能映射

| 能力 | 当前项目 | KG_Embodied 可迁移内容 | 建议 |
| --- | --- | --- | --- |
| 100 智能体执行 | 已有 Ray actor、统一 barrier、恢复 | actor payload/snapshot、中央协调者模式 | 保留现有实现；先压测 100 活跃台的 tick 延迟 |
| 任务协同 | 有基本派单和交通仲裁 | 策略/执行/存储解耦、协助事件建模 | 仅迁移边界设计；按仓储任务重写协助逻辑 |
| 人形机器人展示 | 已有 Canvas 仓库、机器人详情、实时 WebSocket | 帧历史、回放游标、事件/状态映射 | 扩展为时序回放和 KG 面板；无人机 UI 不直接迁移 |
| 时序知识图谱 | 仅内存 `events` 和 checkpoint | 内存/Neo4j 双后端、run 隔离、实体/事件/step/state 模型 | 必须迁移并重命名领域模型 |
| 百万级数据 | 无导入/规模工具 | batch `UNWIND`、dry-run、schema、计数核验 | 必须迁移模式，重写为仓储数据生成器 |
| 图查询 API | 无 | 只读图查询、上下文模板 | 迁移模板注册与权限边界，不开放任意 Cypher |
| 并发性能证据 | 无 | 连接池、100 worker、百分位数、分阶段计时 | 必须迁移；以本项目查询包重新实测 |
| LLM 决策 | 无 | 区域摘要与规则回退 | 非本轮关键路径；待 KG 查询 SLA 稳定后再接入 |

