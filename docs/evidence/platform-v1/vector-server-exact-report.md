# 服务端有界精确TopK：独立ops原型

## 结论

将17维精确排序从客户端移到Cypher，可以明显减少候选跨Bolt传输和Python逐行物化。**保留为实验入口，本轮没有修改backend、默认查询路径或正在运行的七天冻结源码。**

两轮各12个固定case，AB/BA交错：每轮24条完整请求，共48条。全部结果与原始来源的独立标量精确TopK逐项核对，源key的顺序严格一致、分数差≤1e-12、物理字段和payload SHA相同。两轮预算超额拒绝均通过，Oracle文件SHA和图head前后保持一致。

|计时分母|客户端精确|服务端精确|
|---|---:|---:|
|两轮每方法24次平均|230.153ms|146.898ms|
|两轮最大|309.628ms|1121.801ms|
|两轮>500ms|0|1|
|最后重复轮每方法12次平均|234.995ms|106.632ms|
|最后重复轮最大|309.628ms|148.871ms|

最后重复轮约2.20倍速度，整体包括首次新Cypher冷请求约1.57倍。首冷1121.801ms全部保留，不能宣称所有请求达标；未只展示暖缓存最后一轮。12个case/方法样本很少，用于确认改进方向，不作尾延迟、100客户端或长期稳定性认证。

## 做了什么

1. 保留与已发布17D接口相同的header/查询状态读取、run、cutoff、地图几何、query payload SHA、历史窗口和排除整个查询机器人的条件。
2. 在索引支持的历史范围内最多取`max_candidates+1`个节点，先检查预算；超额不做完整候选域排序，明确拒绝。没有把截断候选称为精确全集。
3. 在Cypher构造同一17D人工物理特征，用`reduce`计算原始点积和范数。使用原始余弦分数，避开Neo4j内置向量函数可能返回映射分数与不同舍入口径的问题。
4. 数据库按`score DESC, seq, robot_id`保留TopK，只返回5个候选和候选数量；此前客户端需跨Bolt接收约4059行再精确排序。最早case有198个候选，其余为4059。
5. 结果接收后验证来源身份、未来/self/重复、不足K、分数和完整JSON物化。所有这些都计入完整请求耗时；原始来源的独立复核随后执行并单列。

未使用APOC，未创建索引，未写入图，未缓存动态结果，未安装共享依赖。仍使用既有CPython3.10 venv及Rust Neo4j驱动路径。

## 来源与固定case

run为`wh-real-day-source-20261028`，只读来源为W10000的百万实际状态Oracle，图17688。12个预先固定case：350/9、576/88、6000/66、10000/100、22/1、456/58、1130/79、2239/56、3348/34、6004/3、7783/45、8892/22。

每个请求的候选域与旧客户端精确接口相同，先排除最近20个提交和查询机器人的整条轨迹，再按60提交回看范围取完整候选。独立复核从Oracle逐行构造相同特征，用Python标量`math.fsum`余弦求完整TopK。验证逻辑允许1e-12数值等价边界，但本次48条均达到更严格的源key顺序完全相同。

## 负结果和保留项

- `server-exact-probe-01`首版使用Cypher不接受的`1.`字面量而失败，修为`1.0`后重新以新目录执行，首版日志和源码保留。
- `server-exact-probe-02`完成12对正确结果，第一条新Cypher有1121.8ms冷编译/执行耗时。
- 最终脚本收紧了query bool类型和源SHA检查，`server-exact-probe-03`按相同case重新通过，4项独立边界测试通过。
- 当前ops原型的返回结构与正式查询服务仍有差别，不能未经适配直接替换默认接口。采用前应保持字段契约、统一日志、初始化/就绪语义，并补并发与持续写入回归。
- 后续可以显式计量模板初始化与服务ready之后的请求，但首冷不得从现有结果中删除。是否推广需由统一版本回归结果决定。

## 运行及证据

```bash
./run_in_lade.sh python ops/probe_business_exact_server.py \
  --oracle-index /absolute/path/to/verified-oracle.sqlite \
  --output /absolute/path/to/new-probe
```

环境变量使用已有Neo4j只读连接配置；工具不把凭据写入报告。

- 最终原型：`warehouse-dashboard/ops/probe_business_exact_server.py`。
- 边界测试：`warehouse-dashboard/ops/test_probe_business_exact_server.py`，4项通过。
- 机器摘要：[vector-server-exact-summary.json](vector-server-exact-summary.json)。
- 完整证据包：`server-exact-evidence.zip`，62,402字节，10个数据文件的清单SHA与ZIP CRC通过；加清单本身共11个ZIP条目。
- 包SHA256：`d90c965c611301dd1e8698c62d3f8fecb0761ea9a510790dba9aa8086f41e796`。
- 服务器原始记录：`/data/workspace/wz/warehouse-vector-selection-20261028/server-exact-probe-*`。

只新增ops入口、测试和报告，七天运行使用的冻结后端和默认业务查询保持原有字节。
