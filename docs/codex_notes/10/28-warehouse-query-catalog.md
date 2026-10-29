### 任务背景

按KG_Embodied参考版本的56个查询目的，整理仓库所需的标准知识包，供统一CLI调用。保留成熟规则与物理规划，不增加LLM，不修改旧查询及已冻结实验。

### 关键结论

- 新增`QueryCatalogService`，提供11个明确ID：self、readiness、task、goods、energy、assistance、progress、memory、local_awareness、global、similar_history。
- self/readiness直接复用单机器人轻量接口，不读取全fleet、充电位或全部任务；较重的全局/区域观察明确供中央协调共享使用。
- 指定cutoff、批次SHA和run创建版本绑定知识包中的多语句读取；事件/状态窗口及任务预算超限显式拒绝，不用截断历史冒充完整上下文。
- 全局任务聚合逐任务实体进行复合索引最新版本seek，并与Step完成数核对；使用过去版本，保留当前实体与历史真值的区别。
- 帮手输出为空闲、空载、最低SOC和欧氏半径候选事实。原规划器继续验证负载能耗、回充余量、真实路线、交通预约和实时货权。
- 相似历史默认使用向量研究子任务提供的17维`business-exact`有界精确检索；原32维完整ANN由显式`variant=full-ann`保留。两者不用于货权授权。
- 56项映射逐条区分covered、warehouse-adapted、planner-owned和not-applicable，保留不同仓库/无人机业务含义。它不声称执行了56个相同SQL，也不要求每机器人每tick发送56次网络请求。
- 20项本地独立固定事实/负例测试通过。真实Neo4j编译、来源Oracle、性能及联合运行由主流程单独验证；本轮子任务未访问服务器。

### 执行过的操作

- 阅读KG原56项元数据和查询分组，核对仓库现有schema、索引、原轻量查询、货权与协作历史、V5任务状态及充电位写入格式。
- 运行`python -m unittest tests.test_query_catalog -v`：20项通过，0.245秒。
- 测试覆盖机器人/任务/货物、协作候选、窗口反馈、全局负载、全56映射、业务检索委托、错误身份/未来/摘要/预算/字段、run重建、历史完整性、充电位别名和线程隔离计量。

### 修改内容

- 新增`backend/query_catalog/__init__.py`和`service.py`，没有修改旧后端模块。
- 新增`backend/query_catalog/kg56_mapping.json`，发布时随源码携带。
- 新增`tests/test_query_catalog.py`。
- 新增`docs/WAREHOUSE_QUERY_GUIDE.md`，含统一CLI示例、参数、返回含义、时间/区域/预算约定和56项对照表。

### 后续建议

- 在独立图上通过统一CLI对11类查询逐一编译/验真，再固定源码身份进入联合回归。
- 全局/区域查询按中央调度用途共享，避免把重包复制成100个机器人每帧的常规读取。
- 协作历史包继续由已有`knowledge_runtime`门禁消费，新的只读目录本身不自动变成已执行控制。
- 性能统计区分catalog直接语句和委托接口；总服务时间单独完整计量。
