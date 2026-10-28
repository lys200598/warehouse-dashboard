# 独立派生汇总侧车：完整API只读原型

本原型沿原始journal连续构建一个独立SQLite汇总，实测降低global/local_awareness的完整API开销。**生产backend、冻结源码、图谱、默认查询和正式联合负载保持原样；当前尚未接入live writer。** 数值与来源见[紧凑JSON](catalog-summary-sidecar-summary.json)。

## 1. 做了什么

`ops/probe_catalog_summary_sidecar.py`从原始压缩批次逐批读取任务变化，核对连续seq、run、原始批次SHA、scene/config身份，再维护：

- `latest_task`：最新任务投影及其来源seq。
- `summary`：每个seq的区域/状态/逾期计数、sim_time、Step.completed、来源批次SHA和内容校验。
- 到期堆：模拟时钟推进时更新逾期计数，覆盖“任务没有新变化但deadline已过去”的情况；旧堆项按版本作废。

生成返回完全来自原journal，未使用RealOracle或验真器生成生产结果。派生库用临时文件构建、完整检查后发布到新路径；原文件保持只读。

外部`SidecarCatalog`仅覆盖原服务的`_tasks`。完整API仍执行原fleet、事件窗口、JSON编解码，并**每次从图谱完整count当前任务实体数**，继续执行max_tasks预算、run创建身份、批次SHA和Step.completed检查。缺失准确seq、破损内容、错误身份和不完整计数明确拒绝。

原始完整无过滤聚合以run、创建版本、scene/config身份、seq和批次SHA命名；区域、窗口等完整API参数仍由原catalog处理，实体预算每次重新读取。未来若缓存完整API返回，键还须包含全部查询参数，避免把不同zone、limit或窗口混用。

## 2. 真实成本与完整分母

构建输入为固定HEAD877的原始快照，查询截止点固定876。v2连续处理877批，建库**6.810秒**，派生SQLite **5382144字节**；逐批处理平均7.531ms、最大49.820ms。构建、源SHA和发布成本单列，不放入服务计时。Reader初始化也在请求之前，原实验未单独计时；表中数据属于已建立派生库的服务路径。

global/local_awareness各4对AB/BA；每类baseline4条、sidecar4条，共16条完整API请求。请求全部保留真实起止和完整返回，包含各方法首次测量；未人为清空数据库/OS缓存。

|完整API|原版均值ms|侧车均值ms|侧车最大ms|侧车超过500ms|均值提速|
|---|---:|---:|---:|---:|---:|
|global|925.773|154.570|233.884|0/4|5.99倍|
|local_awareness|953.011|144.931|151.970|0/4|6.58倍|

首次侧车global请求：v1为201.951ms，v2为112.192ms；均保留在各自原始记录中。v1也保存了另16条完整请求，其global/local侧车均值165.434/146.384ms。v2补齐了latest-view中仅由时钟触发的逾期字段持久化，正式原型汇总采用v2，两个版本分别保存。

没有额外施加100个global并发，以避免与正式七日写者及读写混合阶段争用资源。本次8条侧车样本均小于500ms；100并发能力、冷机器启动和任意请求的最坏耗时仍需单独测量。

## 3. 独立来源检查

两版各16条完整返回均由原`ops/verify_catalog_responses.py`及独立源Oracle逐字段检查：**v1为16/16、v2为16/16，均0错误**。验真在查询结束后进行，不混入服务计时。

首轮使用旧HEAD877快照验后续响应时，连baseline一起被严格拒绝：图谱响应实际观察到的head已超过877。保留该失败文件，随后对实际producer journal执行新的SQLite在线backup，得到HEAD1967，覆盖两版响应最大观察水位1940/1959。新快照的历史876前缀与原Oracle逐批SHA一致。验证器未放宽规则；派生构建仍绑定旧877快照，新快照仅负责覆盖完整响应中的真实观察head。

构建源SHA：`375dd3d52c84f2cd20f7149e97b06699f202afa880b9ffa02c152020af7c5002`。

验真源SHA：`b04bcf9dc1bbc12e433da87df395d66b716c37070f50c700b29a17cf24676470`。

v2派生库SHA：`35c6f46c6b28662c17b756eb8b23466caba56e2551f08e187829793a11e3fc1f`。

9项测试在Windows和Pro6000 Linux通过，覆盖期限跨越、旧堆项、完成任务、区域变化、时钟回退、run重建、缺失seq、破损checksum、Step.completed及实体预算。

## 4. 怎样继续接入真正latest

下一阶段需要把派生视图变成可恢复、连续追赶的发布链：

1. 原始journal先durable提交；派生视图按连续seq和原SHA执行幂等事务，成功后才发布自己的available水位。
2. 只有图谱和侧车都能提供同一创建身份、配置及准确seq时，才提供该共同点。图谱已前进而侧车未准备好时，应明确等待、拒绝或采用有界的原查询回退；不能静默把旧统计当latest。
3. 恢复时核对原批次链、应用水位和身份，重建到期堆；scene/config变化采用明确的新身份或版本边界。当前原型对这些变化直接拒绝。
4. 单独开展故障恢复、缺失/损坏、run重建、参数隔离、写入期间100客户端并发及冷准备阶段测试，通过后再考虑切默认。

因此，当前已验证的是派生汇总的来源正确性和固定点完整API收益，发布同步与live接入保留为后续工作。

## 5. 产物与归属

代码为项目自行编写的Python/SQLite增量汇总与Cypher预算检查，使用已有索引和原catalog；未复制闭源算法、未写图或改schema。

服务器：`/data/workspace/wz/warehouse-platform-joint-20261028/queries/summary-sidecar-v2`，旧v1和原始失败材料继续保留。原始证据ZIP含23个文件加哈希清单，103815字节，SHA256：`5faa8f4175a02f4e7e1b486dd4dca9d6f91ebc90d36c78aa01447ed22ddb762d`。凭据、private env和大数据库均未打包。

本地证据：`research/platform-joint-20261028/summary-sidecar-prototype-v2/`。Git仅提交ops、测试、本文及紧凑JSON。
