## 任务背景

知识图谱工作台此前只显示完整 Step 的 tick，用户无法判断该 tick 对应哪一段仿真时间。本次仅完善 KG 页面与其只读查询的数据表达。

## 关键结论

- tick 被明确标记为离散调度步编号，不再暗示 tick 等于秒数。
- 完整 Step 现在与仿真经过时间并列显示，格式为 `仿真 HH:MM:SS`。
- 旧图谱中的 Step 没有 `elapsed` 时，查询自动使用同 tick 的 `RunStat.elapsed` 回退。
- 后续全局记录写入会把 `elapsed` 直接固化到完整 Step，减少查询时的补充匹配。
- 本次不修改 4173 页面、仿真推进速度、Neo4j 历史数据或数据集。

## 执行过的操作

- 阅读 `backend/global_writer.py` 中 RunStat、Step 的写入顺序与字段。
- 阅读 `backend/warehouse_graph_query.py` 中完整 Step 查询。
- 调整 `dist/kg.html` 中侧栏完整 Step、图谱锚点和时间上下文的展示文案。
- 执行 `git diff --check` 静态检查。

## 修改内容

- `backend/global_writer.py`：写入完整 Step 时同时写入全局记录中的 `elapsed`。
- `backend/warehouse_graph_query.py`：完整 Step 查询返回 `elapsed`；旧节点缺该字段时从同 tick 的 RunStat 回退读取。
- `dist/kg.html`：新增仿真时间格式化和 tick 语义说明，在完整 Step、当前选择和调度锚点中同步展示。

## 后续建议

- 重启提供 KG 接口的 4173 服务后，图谱页面可读取新的 `elapsed` 字段；旧 Run 在存在 RunStat 时无需重新导入。
- 如某个极早期 Run 同时没有 Step.elapsed 和 RunStat，则页面会显示“仿真时间未记录”，避免错误换算。
