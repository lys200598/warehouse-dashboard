# 仓储图谱 CSV 生成器

### 任务背景

参考 KG_Embodied 中吉林配送 CSV 的“一行一任务、附带执行区域、执行体、时序和坐标”形式，为仓储人形机器人项目编写确定性合成数据生成器。生成的数据用于后续知识图谱批量导入，不直接写 Neo4j。

### 关键结论

- 新增 `scripts/generate_warehouse_dataset.py`，默认生成 10,000 条仓储上架任务 CSV，默认路径为 `data/generated/warehouse_tasks.csv`。
- 每行使用仓储语义：任务、分区、仓库、机器人、货架/格位、货物、优先级、四个状态时间和起点/目标坐标；不使用配送员或经纬度字段。
- 使用固定 seed 可复现；支持 `--count`、`--robot-count`、`--day`、`--output`、`--overwrite` 和 `--dry-run`。
- 图规模估算沿用吉林构建器的 `Task + 2 Event` 基础模型：333,334 条任务预计产生 1,000,003 个基础节点，尚不包含 Robot、Rack、Zone 或多 Step RobotState。
- 生成器不会构成实际知识图谱，也没有执行数据库导入；CSV 是后续 Neo4j 批量导入的输入。

### 执行过的操作

- 对照吉林 `build_neo4j_scale_dataset.py` 的 CSV 批量构建和规模估算设计字段。
- 新增生成器及单元测试。
- 运行 `./.venv/bin/python -m unittest discover -s tests -v`，14 项通过。
- 运行 `./.venv/bin/python scripts/generate_warehouse_dataset.py --count 333334 --dry-run`，确认预计 1,000,003 个基础节点；未生成大文件。

### 修改内容

- `scripts/generate_warehouse_dataset.py`：流式、确定性的仓储任务 CSV 生成器。
- `tests/test_generate_warehouse_dataset.py`：字段、可复现性、写文件和规模估算测试。
- 本文档：字段语义与使用说明。

### 后续建议

- 小规模样例：`./.venv/bin/python scripts/generate_warehouse_dataset.py --count 1000 --output data/generated/warehouse_tasks_1k.csv`。
- 百万基础节点输入：先使用 `--count 333334 --dry-run` 核对，再显式生成 CSV；生成后由独立 Neo4j 导入器创建 Task、Event、Robot、Rack、Zone 等节点关系。
- 在接入真实 WMS 或传感器前，保持该合成数据与真实来源分目录、分 run_id 管理。
