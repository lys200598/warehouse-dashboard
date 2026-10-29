# 100机器人仓库与共享时序知识实验平台

本分支当前主布局为120×90米仓库、48架2304货位、100个泊位；同时保留90×76米/36架/1728货位的旧基线。默认入口为 **`python -m backend.warehouse`**，V5中央协调器推进100个真实仿真机器人，保留能量、交通、库存和恢复门禁；Ray模式只是把每机器人运动段/图谱写入交给100个actor。

## 阅读路线

- **整体实现与运行**：[平台指南](docs/WAREHOUSE_PLATFORM_GUIDE.md)
- **数据生成同学 lys**：[对接说明](docs/LYS_DATASET_HANDOFF.md)、[地图与输入契约](docs/dataset-contract/README.md)
- **布局与调度**：[仓库设计指南](docs/WAREHOUSE_DESIGN_GUIDE.md)
- **仓库查询与KG对应**：[查询指南](docs/WAREHOUSE_QUERY_GUIDE.md)
- **历史控制**：[实际控制链](docs/HISTORY_RUNTIME.md)
- **连续多日生成**：[生成规则](docs/MULTIDAY_DATASET.md)
- **向量用途、算法与验证**：[检索指南](docs/VECTOR_RETRIEVAL_GUIDE.md)
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

## 120×90 中型布局与轮式运行

`wz/warehouse-design` 的 medium 候选（120×90m、48架、2304货位、100泊位）已迁入本分支的 `data/layouts/expansion-candidates/medium/`，配套 `data/examples/wheeled-medium-*`、`config/wheeled-medium-100.json` 和 `backend/wheeled/`。

```bash
# 与 4173 并存：在 14583 启动 120 布局实时页
bash ops/start_medium.sh

# 或把 4173 本身切到 120 布局（会先停止当前 4173）
bash ops/start_runtime.sh --restart

# 120 布局 + 100 个 Ray 运动 Actor（V5 派单/库存/交通/换电仍在中央）
# 先启动独立 Ray head；Neo4j 仅做图谱/机器人写图谱时需要
bash ops/start_ray.sh
bash ops/start_neo4j.sh   # 可选
RAY=1 bash ops/start_medium.sh
# 或切换 4173 为 Ray 版
bash ops/start_runtime.sh --restart --ray

# 让每个 Ray actor 写自己的 Robot/AgentState/Event 子图（需 Neo4j 密码）
ROBOT_GRAPH=1 RAY=1 bash ops/start_medium.sh
bash ops/start_runtime.sh --restart --ray --robot-graph

# 批量短跑（必须带 medium 的完整配置）
python -m backend.warehouse run \
  --scene data/layouts/expansion-candidates/medium/layout.json \
  --dataset data/examples/wheeled-medium-sample19 \
  --config-json config/wheeled-medium-100.json \
  --mobility wheeled --mode rules --knowledge-backend journal \
  --output data/local-runs/medium-smoke --run-id medium-smoke \
  --until 30 --frames
```

当前 4173 已经是 120×90 medium 的 V5 Ray 实时页；14185 只读回放已落盘的批量 frames，14183 保留旧 V5 单 run 回放。三者不要混用 runId。medium 的 0 晚点结论只适用于其 1728 任务峰值输入，不能外推到任意负载。

Ray 版是同一个 V5 medium 运行：派单、库存、SIPP 交通预留、换电和知识写入仍由中央协调器负责；100 个 Ray Actor（`--ray-actor-cpus 0.1`）执行每台机器人每个 tick 的运动段。加 `--robot-graph` 后，每个 actor 用本地 Neo4j writer 只写自己的 `Robot/AgentState/Execution/RuntimeEvent` 子图；共享 `Run` 节点由主进程启动时创建一次，避免 100 个 actor 争抢同一个 Run 写锁。它不能替换成 legacy `backend.server` 的 32×22 Ray 控制台。

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
