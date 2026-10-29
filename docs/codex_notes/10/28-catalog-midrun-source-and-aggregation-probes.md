# 11类仓库查询：中途快照验真与聚合热点诊断

### 任务背景

在七日联合运行继续推进期间，先检查全部11类查询的真实功能，提前发现重查询长尾。使用当前冻结生产源码，只读图谱；查询后通过SQLite在线backup创建独立快照，来源验真单独执行。

### 关键结论

- 11类查询全部成功，并通过独立journal/RealOracle完整字段核验：11条、0错误、源文件和请求/完整响应未变化。
- 实际run为 `wh-platform-week-required-20261028`；一次读取已提交水位876并固定为本轮historical cutoff。SQLite在线快照水位877，覆盖全部请求。
- 单机器人self/readiness分别4.00/1.08ms；task/goods为41.23/5.45ms；energy、assistance、progress、memory分别125.66、229.93、189.52、219.62ms；business-exact相似历史309.68ms。
- local_awareness780.37ms、global652.23ms。热点均为完整任务汇总SQL，占718.78/611.15ms；本轮对应fleet和events读取各约6—13ms。全部冷样本和完整返回保留。
- 两个外部只读候选均保持实体预算、同一截止点和全部任务状态，聚合结果逐项与独立源Oracle一致。四对AB/BA各8次请求，原型各4次；PROFILE调用单列。
- 有序索引流式候选平均368.97ms，对照563.99ms，下降34.58%；仅前置货位聚合候选447.16ms，对照616.92ms，下降27.52%。各自第一个原型请求仍为538.56/519.23ms，均未获得“所有请求小于500ms”的证明。
- 原型继续留在ops诊断层，生产查询、正式联合实验源码和默认入口保持原样。本次快照诊断单独说明覆盖范围。

### 执行过的操作

1. `ops/catalog_snapshot_diagnostic.py`依次调用全部11类真实查询，完整保存参数、响应、纳秒起止和每条实际SQL/驱动物化耗时。历史窗口`start_seq=873`，任务ID从实际WHEvent选取，limit5、event_limit10000、history_limit1000。
2. 对活跃journal执行`sqlite3.Connection.backup()`，得到23MB独立快照。未直接复制活跃SQLite文件。
3. 使用`ops/verify_catalog_responses.py`和独立RealOracle逐项验真。快照SHA为`375dd3d52c84f2cd20f7149e97b06699f202afa880b9ffa02c152020af7c5002`。
4. `ops/probe_catalog_task_counts.py`对原聚合、有序索引流和前置货位聚合执行只读AB/BA及单独PROFILE。所有返回均通过完整源聚合对照。
5. 新增辅助测试6项，全部通过。初版原型在AB请求前因过强实体数量guard退出，日志和工具版本保留；随后按真实historical语义改为检查完整当前实体预算、独立核验截止点counts。

### 修改内容

新增：

- `ops/catalog_snapshot_diagnostic.py`：11查询完整诊断、外部SQL计时、SQLite在线快照。
- `ops/probe_catalog_task_counts.py`：两个可选择的只读聚合候选、原查询AB/BA、完整返回、独立源计数和真实PROFILE计划。
- `tests/test_catalog_snapshot_ops.py`：计数、期限边界、已完成任务、未知区域、顺序及候选范围检查。

生产`backend/`保持原字节，所有服务器诊断工具均放在独立`tools-*`目录中。

### 后续建议

PROFILE显示，原查询20667次货位→货架关联被前置聚合降至1738次；相应关联DB hits从518779降至44760。有序流为保持严格实体域额外检查WHTask成员关系，产生267404 hits。两个候选的取舍应在完整输入与并发混合负载中继续比较。

业务侧可按知识提交点复用只读全局汇总，使100个机器人共享同一水位的统计结果；缓存键须同时绑定run创建版本、seq和查询参数，保留正确的历史点和实体预算。冷计划、全量返回编码及共同读写时的长尾仍应单独记录。

本地完整白名单证据包：`C:/E/projects/warehouse/research/platform-joint-20261028/catalog-midrun-and-probes-v1/catalog-midrun-and-probes-v1.zip`，85857字节、23个原始文件加哈希清单，SHA256为`fd185e0980513a82d83f63385dd29bb7fd5be39f2c12c40c719e8ddde0753e5c`。快照、Oracle和全部原始文件留在服务器独立查询目录。

服务器目录：`/data/workspace/wz/warehouse-platform-joint-20261028/queries/catalog-midrun-v1`、`task-counts-stream-v2`、`task-counts-preaggregate-v3`。
