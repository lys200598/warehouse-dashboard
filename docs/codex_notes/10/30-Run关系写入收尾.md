### 任务背景

核对机器人当前状态和执行记录是否仍反复写 IN_RUN，并将临时属性改为中性名称。

### 关键结论

- 机器人当前状态已通过注册缓存选择纯更新语句，每个 writer、run、robot 首次注册才 MERGE IN_RUN；没有每步重复注册。新增测试覆盖历史采样步、普通步及换 run。
- actor 的执行记录确实仍重复 MERGE IN_RUN，已改为唯一 Execution 首次创建时建边，更新和重放不再执行关系 MERGE。全局服务派单/交接时维护负责人的路径不在本次改动内。
- 三类历史记录及执行记录统一使用临时属性 __in_run_new；使用输入行编号区分同批重复 ID，并在创建行同一事务内移除。
- 所有 IN_RUN 关系保留，current_robot_id 仍由全局记录服务维护。已有的外部缺边数据修复不在此次范围。
- 快速测试、完整相关回归和真实图谱短探针通过；回归保留 lys 已有 KeyError('robots') 为预期失败，另1项门控跳过，其余通过；同批重复历史/执行记录、三次重放、标记无残留与 API 删除零残留通过。
- 没有新增性能对照，因此此前 82ms 不能作为这次提交的实测数据；修复版完整图谱基准和全天实验仍未补跑，未合并推送。

### 执行过的操作

先只读核对 3d2b585 和干净工作区，阅读 record_robot_snapshot、record_robot_current_state 与 Cypher 常量。修改后先跑 tests.test_robot_process_quick 和注册缓存小测，再跑 ops/kg_regression.py，最后启动自己的14591写入服务运行真实重复写与4步图谱/HTTP探针，完成后停止服务。

原始日志：/tmp/wz-kg-evidence/run-links-followup-20261030/。

### 修改内容

- lys 的 backend/warehouse_runtime_graph.py：中性临时属性；Execution 的首次创建关系策略。
- tests/test_warehouse_runtime_graph.py：当前状态只注册一次且按 run 重新注册；旧测试改按写入常量定位调用，避免依赖 SQL 文本形式。
- tests/test_robot_process_kg.py：中性标记检查；同批重复 Execution 和跨调用重放。

### 后续建议

下一轮若测性能，对照单独记录本次版本，不能沿用旧测量。完整图谱基准及长时间观察通过后再决定合并推送；无需变更查询或删除接口。
