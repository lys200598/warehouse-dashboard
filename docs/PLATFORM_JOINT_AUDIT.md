# 联合实验的独立组合审计

`ops/audit_platform_joint.py`只读冻结源码、输入、原始日志、最终结果、完整查询响应和写者观察记录。它把已完成的单项核对通过文件身份组合起来，并单独核实相同run在查询期间有真实水位推进。

运行方法：

```bash
python ops/audit_platform_joint.py \
  --manifest /absolute/path/to/joint-manifest.json \
  --output /absolute/path/to/new-joint-audit.json
```

输出已存在时拒绝覆盖。所有相对路径从manifest所在目录解析。审计不修改图、日志或检查点，也不接触数据库密码。

## 1. manifest接口

```json
{
  "version": "warehouse-platform-joint-v1",
  "source": {"root": "code", "manifest": "code/frozen-files.json"},
  "dataset": {"directory": "inputs/day", "scene": "code/data/layouts/warehouse100-wide-v2.json"},
  "run": {
    "directory": "runs/history", "run_id": "unique-run-id", "mode": "required",
    "require_complete": true, "require_zero_late": true, "require_handoffs": true
  },
  "control_audit": "control-source-audit.json",
  "queries": {
    "requests": "queries/request-index.jsonl",
    "responses": "queries/complete-responses.jsonl.gz",
    "verification": "queries/source-verification.json",
    "expected_clients": 100, "require_peak": true,
    "require_latest": true, "require_writer_overlap": true
  },
  "writer_observations": "queries/writer-head-observations.jsonl",
  "stages": [
    {"name": "run", "exit_code": 0, "start_ns": 100, "end_ns": 200,
     "log": "logs/run.log", "log_sha256": "actual SHA256"}
  ]
}
```

`source.manifest`使用`{"files":{"relative/path":"sha256"}}`结构。脚本核对所有列明文件实际字节，拒绝逃出源码根目录的路径。`stages`填写所有实际执行阶段，记录真实起止、退出码和日志摘要；示例中的数字与摘要只是接口示意。

## 2. 查询索引和完整返回

每个已实际发出的请求保存一条索引，失败请求同样保存真实时间：

```json
{"request_id":"r1-c1-s1","round":1,"client_id":1,"run_id":"unique-run-id","seq":122,"point_kind":"latest","start_ns":1000000,"end_ns":2000000,"ok":true,"response_sha256":"canonical full-result SHA256"}
```

- `request_id`全局唯一；每个client在同一轮中的请求按自身实际时间串行。
- `start_ns/end_ns`为同一wall-clock的真实纳秒时间。API在途峰值从实际区间扫线计算。
- `point_kind`明确区分`latest`与`historical`，每条返回仍绑定请求水位。
- 完整响应文件每条是`{"request_id":"...","result":{...}}`，可用gzip。结果含`run_id`、`seq`、`graph_committed_seq`、`batch_checksum`等公共来源字段；原始业务字段完整保留。
- 响应摘要使用UTF-8、`ensure_ascii=False,sort_keys=True,separators=(',',':'),allow_nan=False`的完整JSON计算，不能只对少数字段或状态码计算。

`source-verification.json`来自独立的逐条来源检查，至少包含：

```json
{
  "passed": true,
  "request_count": 3000,
  "source_journal_sha256": "actual final journal SHA256",
  "file_hashes": {"requests": "actual request-index SHA256", "responses": "actual complete-response file SHA256"}
}
```

组合审计检查每个完整响应摘要、水位和源批次，检查全部响应与请求一一对应，并将独立核验结果绑定到当前原始文件。逐字段业务真值核对由各查询的独立来源验真器完成，组合报告保留其原报告文件SHA。

当前`QueryCatalogService`有明确的封装profile：outer提供`run_id/seq/query_id`；self/readiness/task、带task_id的goods及similar_history使用`result.context`中的head/checksum；fleet类使用`result.point`。审计按query_id明确提取，并核对任何inner run/seq与outer一致，完整响应仍按原字节内容规范化计算指纹。goods仅给goods_id时没有返回observed head，严格联合测试应同时提供task_id；该形式的普通功能测试另列，审计不会凭最终HEAD为它补造观测水位。

## 3. 实际写入重叠怎样记录

观察器从图谱实际读取当前run已提交水位，每次读取后记录：

```json
{"observed_at_ns":1500000,"run_id":"unique-run-id","committed_seq":122}
```

观察时钟与请求时钟一致；水位单调、在原始journal前缀内。严格联合测试要求每轮查询真实区间内至少观察到两次不同水位，证明同一run的图写入在该查询窗中推进。开始/结束两个外部静态快照不自动满足这项检查。

标为`latest`的请求，其截止水位不能落后于该请求开始前最近一次实际图观察水位。该检查验证可观察到的新鲜度下界；实际“在同一个数据库读事务中取最新提交点”的语义由查询实现负责。`historical`请求继续使用显式截止时间重建过去。

短查询窗口若太快，观察器可能未捕获到写入推进；报告将保留缺少证据的结论。需要预先安排足够的真实读写重叠窗口，保留全部请求，避免事后丢弃失败或冷请求。

## 4. 源与控制链核对

- 原始journal前缀连续，每批SHA、run、seq、配置和元信息一致；每批含声明数量的全部不同机器人状态。
- 最终检查点的SHA envelope、日志HEAD、图镜像水位与results一致。
- 完整运行核对全部输入已发布并完成；窗口实验可显式关闭`require_complete`，报告仍保留其实际覆盖范围。
- required模式要求既有`audit-knowledge-runtime.py`的逐决策来源/授权/实际动作审计通过，并核对该报告绑定当前journal/results的SHA。
- 源事件中的实际交接、知识授权及已应用动作数量与控制链审计及results一致。

审计输出分列`same_run_live_writer_overlap_each_round`、`latest_cutoff_observed_fresh`、每轮实际API峰值及全量请求耗时。关闭某项要求用于单项实验时，相应qualification仍如实记录；它不转变为该项联合验收通过。

源码、数据、图谱和控制完整性、服务层100API并发、100机器人同时业务活动、100个Ray进程及端侧硬件分别验收。组合脚本不从API并发数推导其他数量，也不把有限样本最大值当作硬实时上界。
