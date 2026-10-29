### 任务背景

为统一仓库查询的真实读取与联合回归提供独立后验工具。测量阶段完整保存请求和响应，全部读取结束后再从原SQLite日志/RealOracle构造预期，区分服务耗时和验真耗时。

### 关键结论

- 新增`ops/verify_catalog_responses.py`，不连接图数据库，不调用catalog及其聚合函数构造预期。每条请求核对完整返回SHA后，再逐字段验证源事实，指纹一致不自动判正确。
- 覆盖self/readiness、任务与货权全链、memory状态/事件、energy物理泊位、assistance候选与排序、progress反馈、global/local任务分区和Step完成数。
- business-exact用独立17维标量特征和余弦分数计算完整合法候选域、预算、排序与topK，不接受未来、自身机器人或复制补数。旧full-ann明确提示使用原冻结query_v2验真器，不混入业务检索认证。
- 保存每条缺失、重复、失败、来源不匹配、字段错误以及孤立响应。未知字段、bool/int类型混淆、JSON重复键与非有限数字不会静默通过。
- 32项目录与验真器专项测试通过；其中12项验证器测试包含原始SQLite日志、RealOracle构建及手工self/goods完整响应的CLI集成正例，确认货权时间的JSON类型不会被SQLite REAL转换误报。
- 不规则排列的任务分组行可按zone/status规范化，行数及每个字段全部核对；浮点余弦分数和几何距离只有明确小容差。

### 执行过的操作

- 阅读RealOracle的原始来源与索引结构，独立从源批次重建Step及100个泊位状态，从状态和事件表重建其余事实。
- 执行`python -m unittest tests.test_catalog_verifier tests.test_query_catalog -v`，32项通过，耗时2.689秒。日志位于`research/kg-warehouse-scope-audit-20261028/catalog-and-verifier-tests.log`。
- 在真实SQLite单元夹具上构建只读Oracle并验证2份手工完整响应，0错误；该例验证工具IO/来源接线，单独于物理仿真与Neo4j压力验收。

### 修改内容

- 新增`ops/verify_catalog_responses.py`。
- 新增`tests/test_catalog_verifier.py`。
- 在`docs/WAREHOUSE_QUERY_GUIDE.md`增加后验工具命令、请求/响应记录格式和验证范围。
- 后端及冻结生产源码保持不变。

### 后续建议

- 使用原始请求/响应记录运行工具，并把报告、源码SHA、原日志SHA、Oracle前缀及包文件SHA纳入独立联合审计。
- 完整ANN沿原验真器验证，若正式功能集包含它，应保存其独立质量契约报告。
- `goods`仅货物编号返回cutoff与批次SHA，没有暴露图head；需要完整公共head证据的联合用例提供task_id，使用合作上下文形态。
