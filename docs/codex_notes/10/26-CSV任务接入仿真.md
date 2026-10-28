# CSV 任务接入仿真

### 任务背景

将新生成的仓储任务 CSV 接入现有 `SimulationCoordinator`，使前端可通过已有 WebSocket 展示 CSV 驱动的机器人任务、货架目标和事件，而不是只展示随机生成任务。

### 关键结论

- 新增 `backend/dataset.py`，校验仓储 CSV 必填列、`putaway` 任务类型、唯一任务 ID、时间格式及正整数货架/格位字段。
- 新增 `--dataset-path` 启动参数；不传该参数时，随机任务生成流程保持不变。
- CSV 按 `release_time` 排序，以首条记录为仿真时间原点；任务到期后进入队列，并由现有调度器选择可执行机器人。
- CSV 的 `robot_id` 是优先分配提示而不是强制指派；目标格位不可用时会回退到机器人能力范围内的空格位，避免初始库存状态导致任务无法展示。
- snapshot 新增 `dataset.enabled/total/released/queued`，供后端 API、日志或将来的明确前端需求使用；本次不改变既有前端页面。
- 当前仓库只有 15 组货架、540 个格位，上架任务完成后格位会占满；因此 10,000 行 CSV 可作为图谱输入和回放任务源，但当前仿真状态机不能无库存周转地完成全部 10,000 个上架任务。

### 执行过的操作

- 新增 `backend/dataset.py`、`tests/test_dataset.py`。
- 修改 `backend/simulation.py`，接入 CSV 加载、按时间释放、分配、checkpoint 保存/恢复和 snapshot 数据源状态。
- 修改 `backend/server.py`，新增 `--dataset-path` 配置。
- 运行 `./.venv/bin/python -m unittest discover -s tests -v`：16 项通过。
- 运行 Python 编译、`backend.server --help` 和 `git diff --check`：均通过。

### 修改内容

- `backend/dataset.py`：CSV 解析与校验。
- `backend/simulation.py`：CSV 驱动任务队列。
- `backend/server.py`：数据集路径配置。
- `tests/test_dataset.py`：CSV 时间顺序与仿真分配测试。

### 后续建议

- 先生成小型样例：`./.venv/bin/python scripts/generate_warehouse_dataset.py --count 100 --output data/generated/warehouse_tasks_100.csv`。
- 使用数据集启动：`./.venv/bin/python -m backend.server --backend local --robot-count 100 --max-active-robots 100 --dataset-path data/generated/warehouse_tasks_100.csv --port 4173`。
- 如需完整回放万级任务，增加出库/库存周转状态机，或将 CSV 主要用于 Neo4j 图谱导入而非当前上架仿真执行。
- 刷新页面后应仍访问 `http://127.0.0.1:4173`；CSV 数据源状态可通过后端 snapshot/API 核验，不在既有页面增加标识。
