# 仓库知识查询压测指南

日常操作使用[一条命令的压测向导](PRESSURE_TEST_QUICKSTART.md)：自动取运行编号和提交点、执行原工具并生成中文结果。本页是详细测量参考。

第一次使用请先看[压测操作手册](LOAD_TEST_COOKBOOK.md)：包含可独立运行的教学例子、真实接口参数来源、各工具的完整命令、依赖及修改API后的兼容清单。本页继续作为计时、观测和资格字段的详细参考。

## 1. 这套入口做什么

`python -m ops.load_harness run`向已有HTTP服务发出可复现的请求，保存每条请求的输入、完整JSON响应和计时。`analyze`在负载结束后做统计与验真。两个步骤分开，避免把逐条校验和压缩解码挤进被测查询时间。

实现使用Python标准库，Windows和Linux都通过`multiprocessing.spawn`创建独立OS进程。默认100客户端、100进程。每个机器人同一时刻最多有一个请求在途。`--processes 8 --clients 100`可作为资源开销对照，报告会保留真实PID数量。

相关文件：

|文件|作用|
|---|---|
|`ops/load_harness.py`|负载、原始记录、离线分析统一入口|
|`ops/load_probe.py`|外层ASGI真实服务端请求区间观测|
|`ops/load_validation.py`|离线目录响应结构、run/seq、机器人身份及载荷SHA校验|
|`config/load-light-live.json`|每机器人读取自己的轻量上下文|
|`config/load-heavy-live.json`|单独测全局任务和资源窗口|
|`config/load-agent-query.json`|每机器人凭独立身份读取自身历史事实|
|`tests/test_load_harness.py`|计时/峰值/错误保留/真实spawn HTTP等回归测试|

## 2. 推荐测量矩阵

每组使用新的输出目录和唯一session。保持原始输入、查询语义和服务配置不变；改动配置后使用新的实验身份。

1. 静态读：固定真实run、固定已提交seq，轻量查询单独跑。
2. 重查询：相同run和seq，global与向量分别跑，重查询不要混入每台机器人每帧必查的工作负载。
3. 生产按需：100机器人每5秒最多查一次，错峰；必要时结合事件触发的真实查询日志。
4. 读写共存：由独立真实仿真/AgentFleet持续产生事实，查询服务公开已提交水位，压测查询。压测器不会伪造物理观测。
5. 极端突发：100进程闭环、无思考时间，记录队列饱和、错误与尾延迟。
6. 固定到达率：例如全局20、50、100请求/秒，逐级增加到排队、丢到达或延迟明显增加。
7. 冷/热：第一次访问和预热后分别记录。`--phase cold`只记录操作者声明，脚本不重启数据库、不清缓存。冷查询的定义与预热操作要写进实验说明。

负载示例是合成的测量方案；实际按需查询频率应结合仿真真实消费链单列。8进程×多线程和100独立进程是两个不同拓扑，报告会区分。

## 3. 运行例子

Linux建议通过仓库环境包装器运行，下面`RUN`、`PORT`、新输出目录由操作者填写。不要用别人的正在运行服务做写入试验。

### 3.1 闭环突发

```bash
bash run_in_lade.sh python -m ops.load_harness run \
  --base-url http://127.0.0.1:PORT \
  --workload config/load-light-live.json \
  --run-id RUN --session light-burst01 \
  --clients 100 --processes 100 --mode closed --duration 30 \
  --output outputs/load/light-burst01
```

100客户端会同时释放首个请求，之后每人等上次请求返回再发下一次。100进程就绪和100请求到达服务器分别记录。只有服务端区间重叠达到100，才能报告“真实API峰值100”。

### 3.2 错峰按需

```bash
bash run_in_lade.sh python -m ops.load_harness run \
  --base-url http://127.0.0.1:PORT \
  --workload config/load-light-live.json \
  --run-id RUN --session light-periodic01 \
  --clients 100 --processes 100 --mode periodic --interval 5 --duration 60 \
  --output outputs/load/light-periodic01
```

每个机器人间隔5秒，初次请求按机器人编号均匀错开；全局目标约20请求/秒。请求太慢时，该机器人错过的到达记录为`load_generator_busy`，不会无限排队或补发成为另一种突发负载。

### 3.3 固定到达率

```bash
bash run_in_lade.sh python -m ops.load_harness run \
  --base-url http://127.0.0.1:PORT \
  --workload config/load-light-live.json \
  --run-id RUN --session light-rate50 \
  --clients 100 --processes 100 --mode open --rate 50 --duration 60 \
  --output outputs/load/light-rate50
```

全局计划到达率50次/秒。每机器人维持一个在途请求的上限，忙碌期间的计划到达不会隐藏，计入丢到达数量。计划到达量、实际发出量、成功量和吞吐分别统计，避免闭环压测的自限速掩盖饱和。

### 3.4 固定提交点、百万数据及ANN

实时网关在服务内选择当前提交点；查询sidecar通常要求明确`seq`。可复制工作负载JSON到独立实验目录，给body加上真实固定`seq`，同时保留文件SHA。`run_id`和`seq`必须来自同一个已有run，不能从最终计数倒推请求当时的数据量。

向量查询应独立定义case，使用已确认的真实task_id/goods_id与检索参数：

```json
{
  "version": "warehouse-load-v1",
  "cases": [{
    "name": "ann-existing-point", "query_class": "vector", "kind": "read",
    "path": "/api/knowledge/query",
    "body": {"query_id": "similar_history", "variant": "full-ann",
      "run_id": "{{run_id}}", "seq": 123,
      "robot_id": "{{client_id}}", "task_id": "REPLACE_REAL_TASK",
      "goods_id": "REPLACE_REAL_GOODS", "limit": 5}
  }]
}
```

上面的`123`和ID只是格式示例，运行前必须替换成真实数据。测试ANN、业务精确检索和轻量上下文时要保持各自语义，分别报告质量和速度。

### 3.5 Agent自身查询与写入

Agent服务使用`/api/agent/query`，每台机器人有自己的Bearer凭证。以安全方式在进程环境设置`WAREHOUSE_AGENT_AUTH_1`至`WAREHOUSE_AGENT_AUTH_100`，值为完整`Bearer ...`；使用`config/load-agent-query.json`。脚本不保存请求头或token值，若服务误回显已知token也会脱敏。env名称会保留在工作负载说明中。

写入应首先使用真实AgentFleet把每个机器人实际观测写入服务，产生服务日志和水位推进。若额外测试合成写入吞吐，应使用独立run和数据库、明确合成事实身份，并在case中声明`kind: write`及CLI加`--allow-writes`。脚本默认拒绝已声明写入，并拒绝把`/api/control`作为查询负载。

## 4. 计时怎样划分

- `elapsed_ms`：发起HTTP请求之前到完整响应字节读完；包含连接、传输、服务器排队和处理。超时与连接失败同样保留经过时间。
- `service_ms`：服务器提供的`X-Warehouse-Service-Ms`原值。部分入口包含自身排队，需按入口解释。
- `queue_ms`：`X-Warehouse-Queue-Ms`原值；缺失记null，不拿客户端时间减法冒充队列时间。
- `upstream_ms`：网关提供的上游耗时；缺失记null。
- `parse_ms`：完整字节到手之后JSON解码的时间，不计入网络请求时间。
- 原始SHA计算、JSONL写盘和离线验真在计时区间外；仍会消耗压测机资源。需要同时观察压测机CPU/内存，必要时对照进程拓扑。脚本不将压测机耗时归为数据库计算。
- 所有同机区间使用`perf_counter_ns`；跨主机不能直接比较单调时钟绝对值。

请求完成后原始证据按每客户端独立gzip保存，避免100进程竞争同一日志锁。分析逐条读完整JSON后只保留轻量指标，减少内存开销。

## 5. 真实API并发观测

在新隔离ASGI服务外层包装：

```python
from ops.load_probe import LoadProbeMiddleware

probe = LoadProbeMiddleware(app,
    output="NEW_EVIDENCE/server-ingress.jsonl",
    source_identity={"run_id": "RUN", "source_manifest_sha256": "VERIFIED_SHA"})
try:
    uvicorn.run(probe, host="127.0.0.1", port=PORT)
finally:
    probe.close()
```

每个ASGI进程用独立文件。观测器只记录请求ID、时刻、状态、路径、PID，不读取token和请求正文。请求区间结束于最后响应body成功发送，异常退出另外标记。日志通过有界队列写盘；丢观测或写盘错误会使status文件`complete=false`。

分析时只合并同一台服务器、同一入口层的多个worker，不能把网关与sidecar两层区间相加。所有请求都由`X-Probe-ID`关联。缺少服务端trace、未关闭观测器、重复ID、遗漏或跨层重复，`server_peak_qualified`保留false。生产服务可以继续运行，此时可提供未封口的阶段报告，但不能把尚未完整收集的日志说成完整并发验收。

## 6. 离线分析和完整验真

```bash
bash run_in_lade.sh python -m ops.load_harness analyze \
  --input outputs/load/light-burst01 \
  --output outputs/load/light-burst01-analysis.json \
  --server-trace NEW_EVIDENCE/server-ingress.jsonl \
  --require-run RUN \
  --validator ops.load_validation:validate_catalog
```

输出包含每类查询的均值、P50/P95/P99、最大值、超过500ms数量，以及成功、HTTP错误、连接/超时错误、解析错误、发起器丢到达。成功样本延迟和所有已发出请求的延迟分开。

`all_requests_under_500ms`仅描述这次实际样本：运行完整、每个配置客户端都有请求、没有错误或丢到达，且全部观察到的耗时不超过500ms。它不保证未来所有请求的最坏耗时。分析命令不达此条件时退出2，报告仍完整保存。

`load_validation.validate_catalog`检查run、请求点、机器人身份及自洽载荷SHA。完整JSON与源checkpoint/event/frame的逐字段验真需要项目的独立源审计器，不能用自洽SHA替代。可用`--validator your_module:verify`运行受信的离线校验函数，输入是完整单条记录，返回True/None为通过，返回其他结果或抛异常为失败。该函数只在分析阶段调用。

百万资格默认false，需要另附同run、逐请求cutoff的前缀规模证据；最终超过百万、多个run加总、仅数据库全库规模均不能自动通过。真实控制是否被API在途区间覆盖、历史知识是否实际改变动作也由独立控制链审计确认。

## 7. 安全退出与复现

报告同时保留 `network_*`（开始发送至完整body接收）和 `complete_http_json`（再计入客户端JSON解析）。`all_requests_under_500ms`使用完整HTTP+JSON时间；离线源验真、SHA和日志压缩分别计时，不加到网络时延。旧版本原件保留其原度量，不跨版本混用。

输出目录、报告和trace都拒绝覆盖。每轮保存脚本SHA、工作负载SHA、种子、参数、每进程PID、退出码和原始记录SHA。Ctrl+C设置停止事件，等待单次请求超时后清理；超出排空期限仅终止本次创建的子进程，报告中保留其PID与非完整状态。脚本不会重启被测服务、修改数据库、清缓存或自动重试失败请求。

可通过`--identity path/to/provenance.json`同时保存源清单、数据集SHA、schema版本、生产者身份和水位证明文件的内容与SHA；脚本将其标为“已采集输入”，独立审计后才能升级为验收结论。分析报告同时列出最慢10个请求ID及其queue/service/upstream时间，原始请求全集继续保留，便于按ID关联服务日志定位热点。

Windows执行可直接调用配置好的Python；Linux使用`bash run_in_lade.sh python ...`。新增测试使用本机loopback测试服务器，不连接Neo4j，不证明真实百万图谱已达标。实际服务器联合回归需要单列结果和资格。

2026-10-29本机回归：9项默认测试通过，1项100进程测试默认跳过；显式设置`WAREHOUSE_TEST_100=1`后，100个真实OS客户端、400次HTTP请求全部通过。该测试使用独立测试服务器，只验证压测器拓扑和请求完整性，不代表生产服务峰值100或百万图谱性能通过。
