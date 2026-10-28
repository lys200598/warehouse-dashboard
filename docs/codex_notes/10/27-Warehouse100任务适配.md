### 任务背景

运行态图谱已经成功出现节点后，需要让仿真能够读取 Warehouse100 的七字段 `INBOUND/OUTBOUND` 任务数据，而非仅支持旧版 `putaway` CSV；这样执行记录的任务 ID 才能与静态图谱任务 ID 保持一致。

### 关键结论

- `backend/dataset.py` 现在会自动识别七字段 Warehouse100 CSV，保留任务 ID、货物 ID、源/目标位置及 ISO 时间，不破坏旧 CSV 兼容路径。
- `INBOUND` 在仿真中选择空货位；`OUTBOUND` 选择已占用货位，完成后释放该货位及其货物占用。
- 已保留原始 `task_type`，后续运行态 `Execution.task_id` 可与静态 `WarehouseTask.task_id` 对齐。
- 当前二维仿真地图尚未替换为 60×48m Warehouse100 布局：目标位置会映射到当前可执行的货位。因此本次完成的是任务语义和库存生命周期适配，不是完整路径规划迁移。

### 执行过的操作

- 阅读现有任务解析、调度和完成事件处理逻辑。
- 修改 `backend/dataset.py`，增加 Warehouse100 七字段解析分支。
- 修改 `backend/simulation.py`，按入库/出库方向选择空位或占用位，并在出库完成后释放货位。
- 增加七字段测试并运行 `./.venv/bin/python -m unittest discover -s tests -v`；25 项通过，`git diff --check` 通过。

### 修改内容

- `backend/dataset.py`：增加 `_load_warehouse100_tasks`。
- `backend/simulation.py`：增加 `OUTBOUND` 的库存释放语义。
- `tests/test_dataset.py`：验证当前 6,912 条 Warehouse100 任务能够读取。

### 后续建议

使用 Warehouse100 数据包启动时传入其任务文件：

```bash
export WAREHOUSE_NEO4J_PASSWORD='你的Neo4j密码'
export WAREHOUSE_NEO4J_WRITE_ENABLED=1
./.venv/bin/python -m backend.server --backend local --max-active-robots 100 \
  --dataset-path data/generated/warehouse100-day1/tasks.csv
```

下一步应将 `Execution` 以 `EXECUTES` 关系关联到 Neo4j 中对应的静态 `WarehouseTask`，并将仿真地图/路径规划迁移到 Warehouse100 的 60×48m 布局后，再进行 100 并发查询和百万级图谱压测。
