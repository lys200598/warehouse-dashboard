# 100机器人仓库与共享时序知识实验平台

本分支提供90×76米仓库、36架1728货位、100个充电兼待命泊位、入库出库仿真、低电接力、时序图谱写入和查询。默认入口为 **`python -m backend.warehouse`**，中央V5协调器推进100个真实仿真机器人，保留能量、交通、库存和恢复门禁。

## 会议后增量

货架资源与历史库存、三型号能力门禁、独立扩仓候选和运力对照见 [2026-10-28会议交付](docs/MEETING_DELIVERY_20261028.md)。原正式地图与全X3配置保持；候选的完成、晚点和假设速度分别报告。

## 阅读路线

- **整体实现与运行**：[平台指南](docs/WAREHOUSE_PLATFORM_GUIDE.md)
- **开始使用实时界面**：[实时控制台指南](docs/LIVE_CONSOLE_GUIDE.md)
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
