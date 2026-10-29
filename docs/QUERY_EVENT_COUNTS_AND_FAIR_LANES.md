# 全局事件统计复用与轻重查询隔离

## 1. 优化针对什么工作

`global`和`local_awareness`查询的响应包含最近事件的各类数量，例如`picked: 42`、`shipped: 40`。原实现每次拉取窗口内全部事件payload、解码并校验，最后只保留kind计数。100个客户端反复查询同一提交点时，会重复进行相同的读取、传输和解码。

本轮增加显式可选的`EventCountCatalog`。第一次查询仍走完整原始验证；后续对同一不可变窗口复用已验证的计数。响应字段、事件窗口、event_limit和查询点语义保持一致。`memory`、`progress`、`assistance`仍返回它们原本需要的完整事件，不通过计数替换。

## 2. 如何保证查询点正确

每次计数访问，包括缓存命中，都调用原`AcceleratedCatalog._fence`：

1. 读取当前run的真实head与created_at、source_run、metadata、config。
2. 用明确完整key读取请求Step，以及必要时的head Step。
3. 验证run、seq、created_at、checksum和源身份。
4. 与本次fleet的token、step、metadata和config核对。

缓存key包含`run, seq, start, checksum, created_at, robot_filter, task_filter`。因此不同run、同名run重建、不同提交点、窗口与过滤条件都隔离。head推进不会替换历史窗口，也不会使输出越过请求cutoff。

冷填充调用原`_events`，保留原始排序、来源、payload过滤、effective time和limit+1截断校验。无效、截断或读取异常不填缓存。热命中重新检查调用者当前event_limit与窗口最大有效时间，较小预算仍会得到原`IncompleteContext`错误。

计数缓存默认最多32项、4MiB。字节预算包含计数、key以及metadata/config/step身份内容；超大的单项正常返回但不缓存。固定16条带锁避免同key并发冷请求重复拉取，锁数量不随请求数增长。调用者拿到私有计数字典，不能改写内部缓存。

缓存以原图谱事实在commit之后不可变为前提。源文件、图谱schema或同commit历史payload的非法原地修改应通过独立来源审计发现；计数缓存不会在每次命中时重新扫描全部payload来检测这种违反不可变契约的改写。

## 3. 怎样启用与回退

代码使用：

```python
QueryCatalogService.from_env(timeout=15, pool_size=1,
    summary_root=producer_root, summary_cache_root=view_cache_root,
    cache_fleet=True, cache_event_counts=True)
```

默认`cache_event_counts=False`沿用原版。`ops/warehouse_query_sidecar.py`新增`--event-count-cache`，只在新隔离服务上显式启用。去掉该参数即可回到原事件读取路径。原冻结源码和服务不受本地新增文件影响。

缓存命中仍多做fresh fence的小查询，属于谨慎的首版实现。是否真正降低尾延迟，需要在同一百万前缀、相同请求集上A/B测量；本地单元测试只证明逻辑等价和约束。

## 4. 轻重查询为什么分队列

例如同一sidecar总共3个查询worker，3条global请求已经执行或排队。随后一条只读自己状态的`self`请求也需要等重请求结束。

显式设置`--query-workers 3 --heavy-workers 1`后：

- `self/readiness`使用2个轻量worker。
- 其余查询使用1个重查询worker。
- 总线程和最大Neo4j查询连接仍为3，保留原来的资源预算。
- 每条lane默认最多排队128条，默认排队超时15秒。满队列和超时返回503，响应中标记`X-Warehouse-Admission-Rejected: 1`，health保留reject/timeout计数。
- 队列等待仍计入服务耗时，并通过Queue-Ms观察。压测器必须把拒绝纳入失败分母。
- 客户端取消请求后，执行中的底层查询不重试；其slot在底层future结束后才释放，避免仍在运行的查询被当作空闲资源。

`--heavy-workers 0`为默认值，继续使用原共享lane；`--max-lane-queue`和`--queue-timeout`只在显式开启隔离lane时生效。轻重隔离有吞吐权衡：重lane可能排队更久，需要同时报告两类查询和错误数量，不能只展示轻查询改善。

## 5. 测试与真实验收

新增12项事件统计测试覆盖冷/热整包响应等价、fresh head、两个run同checksum和creation隔离、重建、过滤/空窗、limit截断、坏payload和future time、错误不缓存、字节/LRU上限、16并发single-flight及完整事件查询保持原路径。

原20项catalog、4项fleet acceleration及11项双阶段fence测试通过。新增4项HTTP轻重lane测试覆盖重查询堵塞时轻查询继续执行、队列满拒绝、排队超时与pending清理、默认共享模式和参数校验；本机bundled Python缺少FastAPI，HTTP测试需在具备项目依赖的运行环境执行，未以skip计为通过。

真实A/B需要分别比较默认共享、只启事件统计复用、只启轻重lane、两者组合，并保留完整请求响应与服务端区间。查询字段与窗口保持原样，不为提高速度缩小工作量。最终是否默认启用以实际收益与正确性审计决定。
