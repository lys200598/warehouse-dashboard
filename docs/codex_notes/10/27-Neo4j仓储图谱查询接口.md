### 任务背景

在已完成 Warehouse100 静态知识图谱导入脚本的基础上，开始接入后端查询能力。要求不改动当前前端，不把内存仿真演示数据误认为 Neo4j 持久化数据，并为后续 100 个机器人并发查询和运行态事件写入打基础。

### 关键结论

- 新增的持久化图谱接口位于 `/api/kg/warehouse/*`，原有 `/api/kg/*` 内存演示接口保持不变。
- Neo4j 驱动为惰性创建：服务启动时不会连接数据库；只有收到仓储图谱请求后才连接。
- 未设置 `WAREHOUSE_NEO4J_PASSWORD` 时，健康检查返回 `not_configured`，读接口返回 HTTP 503，不会静默回退到模拟数据。
- 查询均有明确范围：汇总只返回数据集/布局计数，任务接口以单个 `task_id` 查询并在 Cypher 中限制为 `LIMIT 1`，不会把整张图返回给调用方。
- 同步 Neo4j 驱动调用通过 `asyncio.to_thread` 运行，不占用 FastAPI 的事件循环，从而不阻塞仿真 WebSocket 事件循环。
- 此阶段只有静态图谱查询；机器人实际分配、状态和执行事件尚未写入 Neo4j，不能将查询结果解释为机器人实时状态。

### 执行过的操作

- 阅读当前 `backend/server.py`、`backend/knowledge.py` 与现有接口测试，确认内存图谱与持久化图谱的边界。
- 新增 `backend/warehouse_graph_query.py`，实现 Neo4j 健康检查、数据集/布局汇总和单任务子图查询。
- 修改 `backend/server.py`，增加专用 Neo4j 配置及三个只读 API。
- 新增 `tests/test_warehouse_graph_query.py`，使用假驱动验证不配置密码时不连接、汇总参数绑定以及任务查询范围。
- 执行 `./.venv/bin/python -m unittest discover -s tests -v`（22 项通过）、`./.venv/bin/python -m compileall -q backend scripts` 与 `git diff --check`。项目不存在 `run_in_lade.sh`，因此未能使用该项目约定的包装命令。

### 修改内容

- `backend/warehouse_graph_query.py`：持久化 Warehouse100 图谱的只读适配器和错误分类。
- `backend/server.py`：新增下列端点：
  - `GET /api/kg/warehouse/health`
  - `GET /api/kg/warehouse/summary?dataset_id=...`
  - `GET /api/kg/warehouse/tasks/{task_id}/subgraph?dataset_id=...`
- `tests/test_warehouse_graph_query.py`：查询边界与无网络离线测试。

### 后续建议

先导入静态数据包，再以环境变量提供密码后启动服务；密码不应写进源代码、命令历史或配置文件：

```bash
export WAREHOUSE_NEO4J_PASSWORD='你的Neo4j密码'
./.venv/bin/python -m backend.server --backend local --max-active-robots 100
```

随后验证：

```bash
curl http://127.0.0.1:4173/api/kg/warehouse/health
curl http://127.0.0.1:4173/api/kg/warehouse/summary
curl http://127.0.0.1:4173/api/kg/warehouse/tasks/T-000001/subgraph
```

确认静态查询正确后，下一项开发是定义 `Robot`、`Execution`、`AgentState`、`Event` 的运行态图模型，并以批量异步写入记录任务分配、取货、完成和异常事件；随后再做 100 并发查询及百万级图谱压测。
