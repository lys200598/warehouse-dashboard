# 100机器人仓库与共享时序知识实验平台

## 先看这里：当前推荐版本与历史版本

| | 当前推荐（新实验、新数据都用它） | 历史对照（保留，不要用来生成新数据） |
|---|---|---|
| 地图 | 120×90米、48架、2304货位：`data/layouts/expansion-candidates/medium/layout.json` | 90×76米、36架、1728货位：`data/layouts/warehouse100-wide-v2.json` |
| 运行方式 | `python -m backend.warehouse live ... --mobility wheeled --robot-agents` | 不加这两个参数时的默认 legacy 入口 |
| 说明文档 | [轮式扩仓对接总览](docs/WAREHOUSE_WHEELED_HANDOFF_20261029.md) | [旧版对接说明](docs/LYS_DATASET_HANDOFF.md)、[旧实时控制台指南](docs/LIVE_CONSOLE_GUIDE.md) |

与李姚舜分支的合并：使用 `wz/integration-on-lys` 分支，接口边界见[对接约定草案](docs/INTEGRATION_CONTRACT_DRAFT.md)；Ray 执行层现状见 [Ray 差异说明](docs/RAY_EXECUTION_GAP_20261029.md)。

旧版说明：本分支最初提供90×76米仓库、36架1728货位、100个充电兼待命泊位、入库出库仿真、低电接力、时序图谱写入和查询。默认入口 **`python -m backend.warehouse`** 仍是这一版，由中央V5协调器推进100个仿真机器人，保留能量、交通、库存和恢复门禁。

## 会议后增量

**2026-10-29轮式扩仓与独立进程交付**：[对接总览](docs/WAREHOUSE_WHEELED_HANDOFF_20261029.md)说明120×90米、48架2304位、1.5m/s、有限换电与自由进出货口；[机器人进程读写](docs/ROBOT_AGENT_IO_GUIDE.md)说明每台机器人独立HTTP写查；[压测指南](docs/LOAD_TEST_GUIDE.md)及[百万查询A/B](docs/QUERY_AB_REGRESSION_20261029.md)提供真实进程、冷热/错峰/突发及完整源验真。默认legacy入口保留，新run显式选择 `--mobility wheeled --robot-agents`。完整七天参考数据已在 [`data/examples/seven-patterns-20261028/`](data/examples/seven-patterns-20261028/README.md)。

货架资源与历史库存、三型号能力门禁、独立扩仓候选和运力对照见 [2026-10-28会议交付](docs/MEETING_DELIVERY_20261028.md)。原正式地图与全X3配置保持；候选的完成、晚点和假设速度分别报告。

入库口背压、期限与取货节拍优化见 [2026-10-29晚点优化](docs/LATENESS_OPTIMIZATION_20261029.md)：保留默认策略，另提供显式启用的疏口候选；全部对照、剩余晚点和出库侧代价分别记录。

## 阅读路线

- **网页压测控制台**：`python pressure_test.py --gui`，打开本机14595端口，按[使用说明](docs/PRESSURE_GUI_GUIDE.md)选择参数、看图表及历史；与原仓库网页独立。
- **不想拼压测命令**：在项目目录执行 `bash pressure_test.sh`，按[新手向导](docs/PRESSURE_TEST_QUICKSTART.md)输入地址、选择方案，自动生成中文报告；已有Python环境可运行 `python pressure_test.py`。
- **第一次接触本项目**：[从仓库设计到实际运行](docs/WAREHOUSE_WHEELED_HANDOFF_20261029.md)、[压测一步一步操作与API改动兼容表](docs/LOAD_TEST_COOKBOOK.md)
- **整体实现与运行**：[平台指南](docs/WAREHOUSE_PLATFORM_GUIDE.md)
- **开始使用实时界面**：当前版本见[轮式扩仓对接总览](docs/WAREHOUSE_WHEELED_HANDOFF_20261029.md)；[实时控制台指南](docs/LIVE_CONSOLE_GUIDE.md)描述的是旧版14186页面
- **联合验收压测**：`python -m ops.joint_acceptance --url http://127.0.0.1:14192`，固定3轮×100客户端，每轮记录节点数和写入水位；满足资格后才按平均<500ms判定，详见脚本说明
- **数据生成同学 lys**：[对接说明](docs/LYS_DATASET_HANDOFF.md)、[地图与输入契约](docs/dataset-contract/README.md)
- **布局与调度**：[仓库设计指南](docs/WAREHOUSE_DESIGN_GUIDE.md)
- **仓库查询与KG对应**：[查询指南](docs/WAREHOUSE_QUERY_GUIDE.md)
- **历史控制**：[实际控制链](docs/HISTORY_RUNTIME.md)
- **连续多日生成**：[生成规则](docs/MULTIDAY_DATASET.md)
- **向量用途、算法与验证**：[检索指南](docs/VECTOR_RETRIEVAL_GUIDE.md)
- **实际结果与待验收边界**：[完整周回归记录](docs/PLATFORM_REGRESSION_20261028.md)
- **原实时控制台与Ray**：[保留入口](docs/LEGACY_CONTROL_PANEL.md)

## 最小运行

使用Python3.10以上环境。平台安装`requirements-platform.txt`；原FastAPI/Ray控制台依赖保留在`requirements.txt`。Pro6000可用已有环境；`./run_in_lade.sh`支持`LADE_ENV_DIR`指定环境目录。

```bash
python -m backend.warehouse status
python -m backend.warehouse validate --dataset data/examples/warehouse-v5
python -m backend.warehouse run --mode rules --knowledge-backend journal \
  --dataset data/examples/warehouse-v5 --output data/runs/my-smoke \
  --run-id my-smoke --frames
python -m backend.warehouse serve --output data/runs/my-smoke --port 14183
```

输出目录、run-id使用新的唯一值，同一次暂停运行用`--resume`继续。网页读取实际保存帧，保留相机交互。

实时控制使用`python -m backend.warehouse live`，部署入口为`http://127.0.0.1:14186/`；它直接运行V5真实协调器并持续写图，支持固定数据集、开始/暂停/单步、墙钟倍速和按需知识查询。原`serve`继续专用于已保存帧回放。

## 真实图谱与历史控制

配置已有Neo4j的`NEO4J_URI`、`NEO4J_USER`、`NEO4J_PASSWORD`，可选`NEO4J_DATABASE`，然后：

```bash
python -m backend.warehouse run --dataset data/examples/warehouse-v5 \
  --output data/runs/my-history --run-id my-history --frames
python -m backend.warehouse query --catalog
python -m backend.warehouse query --request-json my-query.json
```

run默认`--mode required --knowledge-backend neo4j --commit-seconds 5`，历史参与接力规划与实际交接前授权；图谱不一致或读失败会停止并保留现场。`--mode rules`为明确的规则对照。旧冻结版本及输出保留；跨源码/配置/模式恢复会拒绝，避免混用。

## 连续多日输入

```bash
python -m backend.warehouse generate --kind multiday --days 7 \
  --seed 20261028 --output data/datasets/my-seven-days
python -m backend.warehouse validate --dataset data/datasets/my-seven-days
```

默认七种代表性日共50,112任务，跨日库存与货物身份连续。固定CSV无人为日数上限；`--max-tasks`是显式条数预算。电量、故障和受控低电扰动放在运行配置。

长运行可显式设`--commit-seconds 300`：全部业务事件保留，100机器人状态按此间隔和关键协作边界提交。5秒与300秒结果分别计量。大输入设置合适的`--max-wall-seconds`和存储预算，长任务不默认保存巨大网页帧。

原Ray控制台保留；新版中央执行器的100机器人实体、100客户端查询、100独立Actor分别记录。LLM不在本分支交付范围。运行与性能结论均应关联相应输入、源码及验收记录。

## 逻辑入出库边界与自由工位

[自由边界指南](docs/FREE_BOUNDARIES_GUIDE_20261029.md)说明七字段中使用`INBOUND / OUTBOUND`、由仿真选择实际工位及记录计划/实际关系的方法。现有8IN/8OUT、每IN六件和物理约束保持；当前1728高峰自由口场景全部按时完成。旧固定口数据和运行继续保留，其他日型与查询性能分别验收。

## 查询增量摘要与原生入口

[查询部署指南](docs/QUERY_TAIL_DEPLOYMENT_20261029.md)说明单Runtime、8个query-only进程、原子发布点和原生HTTP路由。当前S9动态124131完整响应源验真通过，完整HTTP P99约303ms，仍保留24条超500ms；固定百万前缀与动态规模资格分别报告。输入、源码、SHA及对接说明见[本轮总结](docs/codex_notes/10/29-自由进出货口与查询长尾优化.md)。
