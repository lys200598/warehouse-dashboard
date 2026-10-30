### 任务背景

在服务器 `/data/workspace/wz/warehouse-integration-on-lys` 接续图谱驱动派单，并推进上下文查询、C 套联合验收、旧路径停用、恢复、全天数据和前端。服务器为权威工作区，本地旧镜像未覆盖；仅同步报告。功能分支 `wz/kg-driven-dispatch` 基于 `18bae53`，已合并 lys 更新 `1f21f0b`，不推送。

### 关键结论

- 已合并 `origin/dev_lys`（包括 `cca56ac`、`de9424e`），合并无冲突；合并后的快速测试通过。
- Neo4j 模式每步收齐 100 台机器人完整 19 字段状态写入回执，提交 Step，然后在派单前等待 `Run.latest_step` 并读取精确 tick 的 payload；失败暂停，不退回内存。
- 负责人变更由主进程事件生成，重试使用稳定 change_id；非 5 秒保存点 dispatch 为 `{}`。
- 全局记录客户端支持最多 128 条有序批量和固定重试载荷。Neo4j 的逐步读屏障意味着通常是单条 records 批；不能累积未来步骤后才提交当前派单所需的 Step。
- 真实测试发现并修复 lys 负责人变更查询缺少 `WITH` 的 Cypher 语法错误；Step 发布调整到全局事实写完之后。
- Actor 五包上下文默认每10步并发查询，结果只观测不接派单。真实100个Ray进程完成200次请求，错误0，均值约259ms（含冷启动）；逐机器人/包分布已有JSON。
- 最终整合回归91项符合预期：89通过、1个明确原有KeyError('robots')、1个门控跳过；该Ray恢复另行真实通过26.383秒。前端42测试通过；真实运动235–263窗口20秒内P95渲染16.7ms、最大33.3ms、>100ms及JS异常0。

真实接力全基准1989步通过，1990个Step各100回执、owner=receiver=6、清理0。冻结源码400步实时对照严格通过（包括源码identity哈希）：KG139.70秒、memory145.95秒，latest400/401条Step/各100回执/接收方6，两run残留0。

最终C联合验收通过：三轮同run节点1,113,057/1,113,399/1,114,757，100独立进程且并发峰100；每轮10秒均值46.63/53.42/50.58ms，P99为112.86/88.60/72.79ms，最大208.33/157.92/151.94ms，超过500ms均0；查询窗口内实际新Step为6/21/26。生产者为真正Neo4j派单模式，Ray32、heap4GiB，另有actor每10步100台背景查询。独占临时百万前缀已清0，所有旧失败尝试保留且不计通过。

全天数据已生成（86400秒、1728任务、每小时72），`wz-test-day-20261030-final` 正在后台运行，输出 `/tmp/wz-kg-evidence/full-day-run-20261030-final`，后台完成前不得称全天通过。普通/重推完整Neo4j基准也不能用短程替代；其状态见最终报告。

经用户授权，17692 heap从1GiB扩为4GiB，数据库/运行ID/旧服务状态复验保持。原地恢复未来任务绑定问题已修复；本地和真实Ray继续等价通过；同run图谱超前检查点时拒绝倒带。

### 执行过的操作

- 检查本地及服务器 `AGENTS.md`；服务器仓库没有 `.agents` 目录。服务器初始工作区干净，HEAD 为 `18bae53`。
- 原 origin SSH fetch 因服务器 GitHub host key 未配置失败；改用同一仓库 HTTPS fetch 成功，未修改 remote。
- `git switch -c wz/kg-driven-dispatch 18bae53`、`git merge --no-edit origin/dev_lys`。
- Python 通过 `LADE_ENV_DIR=/data/workspace/wz/warehouse-deploy/.venv bash run_in_lade.sh python ...` 执行；开发验证脚本 `ops/kg_dev_env.sh` 设置相同环境。
- 已执行快速、派单、本地 split、启动接线、live runtime、global writer/client 和 runtime graph 测试；环境集成测试由 `tests/test_robot_process_kg.py` 控制。
- 性能脚本 `ops/kg_probe.py` 记录推进、写入、等待 Step、读取、派单及总耗时；`ops/kg_ray_cluster.py` 调整本任务 Ray CPU 配额。
- 测试运行均使用 `wz-test-` ID，并通过全局服务 DELETE 接口删除后检查残留。
- 并行5个子任务；最终使用 `ops/kg_regression.py` 汇总，只有用户注明的精确 `KeyError('robots')` 归为旧错，其他错误仍失败。
- 实际 `EXPLAIN` 证实复合索引全序读取后直接Limit，消除self/fleet历史排序；正式查询不使用PROFILE。
- 维护脚本备份conf并仅stop/start 17692同home，核对前后库、运行ID、旧页面；详见 `30-17692堆内存维护.md`。

### 修改内容

- `backend/robot_actor/kg_state.py`：只读图谱客户端，2 秒截止时间，100 条状态及字段/编号验证。
- `backend/robot_actor/local.py`：派单前原位替换机器人字典；每帧输出状态来源。
- `backend/robot_actor/ray_actor.py`：完整状态发布、同步回执、逐步全局记录、低电量等步间变更同步、失败暂停、五阶段计时和资源关闭。
- **lys 文件 `backend/warehouse_runtime_graph.py`**：复用原 Robot 写入语句增加完整当前状态及精确 tick 索引；保留 AgentState 历史采样。
- **lys 文件 `backend/global_writer.py`**：负责人变更构造、dispatch 节流、最后发布 Step；修复负责人来源关系查询的缺失 WITH。
- **lys 文件 `backend/warehouse_graph_query.py`**：集中C上下文，完整Step/时间窗口约束，复合索引有序读取。
- **lys 文件 `backend/global_writer_api.py`**：确认删除接口按1000节点提交，避免百万级单事务耗尽内存。
- `backend/global_record_client.py`：有界有序批提交和固定重试批。
- `ops/warehouse_live.py`、`ops/warehouse_live_runtime.py`：参数依赖校验、逐步记录接入、运行/帧元数据、actor 与客户端生命周期。
- `tests/test_kg_state.py`、`tests/test_global_record_client.py`、`tests/test_robot_process_kg.py`：错误边界、批量重试及真实三场景基准。
- `ops/kg_*.py`、`ops/kg_*.sh`：可复现的短程、性能、真实服务与实时验证脚本。
- `backend/wide/sim.py`：恢复前重建不可变任务底表，清除检查点后遗留的未来绑定。
- `backend/agent_runtime/`、live参数：A镜像默认关闭并保留显式开关，旧required模式需显式开关；旧 `--robot-agents` 弃用。
- `backend/query_c/`：复用lys查询、独立客户端与逐机器人/包耗时分布；`dist/` 增加模式/Step/错误及旧A区停用提示。
- 设计说明、决定表、启动手册、最终验收报告和各子任务总结同步更新。

### 后续建议

- 完成后由 wz 核对提交、测试证据及未运行验收项，再决定合并推送。
- 不修改旧基准来适配实现；发现差异先修复链路。
- 全天后台试验完成前仅报告进行中；查看progress/report及每2小时检查点，结束或失败清理后停止本任务自己的writer。
- medium19不能证明100台同时业务搬运；普通/重推完整Neo4j基准、全天完成情况分别列出，不能由100个查询进程替代。
- 从图谱快照恢复待wz与lys确认；库存/任务表入图读取、上下文参与派单和LLM继续暂缓。
