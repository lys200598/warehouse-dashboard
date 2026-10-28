# 百万历史＋真实业务＋结构向量联合测试 v2

本工具是**实验负载与正确性核验器**，不是调度器。实测结果必须由服务器原始报告补充，单测/mock不构成性能达标。

## 1. 数据口径

- 原始业务 run：真实宽图任务运行的 `WHRobot/WHAgentState/WHTaskState/WHGoodsState/WHEvent`。不修改它的节点、提交水位或历史。
- 独立 archive run：100机器人×10,000个合成采样序号＝**1,000,000个 WHAgentState**，另含100机器人、10,000个 `WHStep`、3,000,000条状态关联（机器人、step、真实来源状态）。不能把这些节点解释为百万任务。
- 每条状态都保留 `source_run/source_seq/source_key/source_payload_sha256` 与 `DERIVED_FROM`，payload来自真实业务状态。对真实提交均匀重采样以放大规模，重复来源明确标记为 `synthetic_resampled_history`。
- archive 的 seq/tick 只是合成采样序号，不是新机器人执行、时长或新的业务事实；原始业务时间存在来源状态中。archive不作为派单权威。
- 每条百万状态附32维归一化结构向量：位置、电量、载货、阶段、机器人身份与低幅度时间/身份特征。**不是文字／图像语义嵌入，也不证明跨模态检索能力。**

向量索引 `whscale_archive_vector_v2` 建在 `WHScaleArchive.embedding`，关闭量化以便严格独立cosine分数核验，附加实验标签与原业务隔离。多个探针archive可能共享此索引；实际查询仍按run过滤，但筛选发生在ANN候选生成之后，必须保留该限制。

## 2. 每个完整查询实际做什么

1. 读取真实业务run在固定提交的机器人＋任务上下文（复用 `WideQueries.robot_context`，并发期不漂移截止点）。
2. 对百万archive按 `run_id,robot_id,seq` 复合索引执行截止点历史范围查询，返回最近5条；并非只取一个已知key来回避历史索引。
3. 从图中读取该archive查询点的32维向量。
4. 调用真实 Neo4j `db.index.vector.queryNodes` 获取100个ANN候选，再按同run与 `seq<=cutoff` 过滤，返回前5条。它**不是保证精确的时间过滤top-k**；空候选计为失败，自身命中率单独保留，不能隐藏召回失败。
5. 完整JSON物化，并用独立的来源payload、确定性采样公式、向量公式与cosine公式核验结果。计时包含这些步骤、连接排队和核验成本。

### 查询路径窄优化（保留基线）

`benchmark --query-mode baseline` 仍为默认，保留首次实测的5次串行Bolt读取（截止水位、业务上下文、历史范围、查询向量、ANN）。`--query-mode batched` 将同样字段和固定截止点收成单条Cypher子查询，正常路径只有1次Bolt；并未减少并发、查询字段、历史行数、向量维度或oracle。

batched模式若全库ANN100经时间过滤后为空，再真实查询ANN1000；仍空则通过 `WHAgentState(run_id,seq)` 索引对所有合格历史做 `vector.similarity.cosine` 排序top5。仅兜底路径是精确合格集top5，正常ANN路径仍非精确top-k。每条数据库读取15秒超时，超时/空结果仍计失败，不用自身点或缓存冒充结果。逐请求记录 `retrieval_path/bolt_statements`，逐轮统计各路径数量，保留优化前目录和失败，不原地覆盖。

真实性能改善必须比较新旧完整三轮报告；20项本机工具测试只覆盖实现/路由/错误传播，不是Neo4j延迟证据。

Oracle只用来源模板检查数据库实际结果；不会用模板返回结果冒充数据库读取。使用 `--business-output` 时，先由 `backend.wide.verify.load_source` 验证checkpoint及完整journal，再独立重放所有任务版本并逐提交比较机器人＋完整task payload／None；不传时明确记录为数据库来源历史oracle而非独立journal。真实业务货权/交接完整性仍应额外用 `backend.wide.verify` 对checkpoint＋journal＋Neo4j复核。

## 3. 并发与写入观察

默认每轮100线程通过Barrier同步开始，每客户端连续5次请求，共3轮、1500次完整API调用。没有丢弃首轮；预处理计数及索引检查可能预热缓存，不称冷启动。

可选 `--observe-writer-run` 每0.2秒读取另一个真实runner的提交水位。报告逐轮请求窗口、水位和窗口内部观察样本；只有每一轮内部明确观察到水位增加，才标记 `concurrent_observed_writer_commits=true`。写入来源是否为真正运行中的仿真，须结合root保留的runner进程、日志、journal证据证明；观察器本身不生成、重放或伪造任务。

这是**同库、不同run的知识读写压力**，不是同一个业务run累计百万真实历史，也不是100实体设备、100并行机器人执行进程或指定端侧验收。即使全请求低于500ms，`full_task2_acceptance`仍为false。

## 4. 复现命令

仅连接本次独立Neo4j实验实例，由root设置 `NEO4J_URI/USER/PASSWORD/DATABASE`。不要向承载旧服务的1GB heap实例加载百万向量。命令不打印环境秘密。

```bash
./run_in_lade.sh python -m backend.scale_benchmark preload \
  --business-run wh-wide-v1-cooperation-hour \
  --archive-run wh-scale-v2-million --steps 10000 \
  --output-dir data/scale-preload --max-wall-seconds 1200

# 与真实wide-day runner写入同一个独立数据库期间执行：
./run_in_lade.sh python -m backend.scale_benchmark benchmark \
  --business-run wh-wide-v1-cooperation-hour \
  --archive-run wh-scale-v2-million \
  --observe-writer-run wh-compare-v2-wide-day \
  --business-output data/seed-cooperation \
  --clients 100 --rounds 3 --samples 5 \
  --query-mode batched \
  --output-dir data/scale-query
```

先以唯一probe run、`--steps 10`验证真实Neo4j语法；probe保留，不覆盖。正式输出目录必须不存在。中断时archive保留，可显式 `--resume`、相同来源/contract、**新输出目录**恢复；不会清库或删除旧节点。

默认每事务20采样序号（2000状态），单事务120秒上限；生成写入循环有1200秒墙钟停止阈值，向量索引等待另有600秒上限，root宜再包外层总超时。不大于2,000,000状态；完整数据、索引和事务日志预计数GB量级，实际磁盘/内存以服务器监控为准，不宣称精确容量或完成时长。

## 5. 证据与失败保留

- preload：`contract.json`、逐批 `preload.jsonl`、计数/索引/原业务水位核验 `report.json`。
- benchmark：逐请求 `queries.jsonl`、`writer-observations.json`、逐轮mean/P95/P99/max/超500ms数/吞吐/错误/自命中数及SHA-256 `report.json`。
- 所有请求（含失败）的延迟都进入报告统计，不删慢请求，不用平均数替代尾延迟。
- 失败输出 `failure.json`，既有数据/输出不覆盖。

## 6. 参考

使用Neo4j官方向量索引与 `db.index.vector.queryNodes` 接口，不导入第三方调度代码。[Neo4j Vector indexes](https://neo4j.com/docs/cypher-manual/current/indexes/search-indexes/vector-indexes/)。本实现使用Neo4j 5.x的LIST向量与固定32维cosine索引；实际版本、ONLINE状态由报告读取，不靠文档推断运行环境。
