# 查询长尾优化与进程隔离

## 任务背景

机器人执行、图谱持续写入和查询会共享计算资源。本轮分别测量查询算法、HTTP排队、客户端处理和离线验真，先消除重复计算，再验证实时运行下的长尾。旧服务14186/14188、固定口输入、冻结源及失败请求全部保留。

## 关键结论

- 查询`global / local_awareness`改用增量摘要。原journal和Neo4j继续保存真实状态；摘要按run、创建版本、seq和checksum建立可重建的派生视图。
- 每次查询继续读取Neo4j新鲜围栏。历史seq读取对应历史计数，不混入未来状态；已发布帧要求journal、Neo4j、摘要均完成。
- 当前S9固定百万前缀：1,652,135图节点、507,300条32D真实状态。每类521条，global均值/P99/最大110.30/240.10/601.35ms（1条冷慢）；local111.75/275.95/340.17，self18.38/53.18/58.46；self真实峰不足100保留负资格。全部2084完整响应源验真通过，严格全500ms/各类峰100仍未通过。
- 完整ANN采用现有HNSW；当前S9均值178.16、P99 412.85、最大457.30ms，521条超500为0，四轮真实峰108/124/119/114。此前V2的分散客户端对照最大318.08ms及集中客户端慢样本均保留其自身范围。Flat32穷举更慢，保留为研究对照。
- 实时旧集成存在排队：S2真实峰100，20,056条中1条HTTP400，API最大10,240ms。S3标量快照降低不必要复制；S4增加到16查询线程，仍有明显执行前等待。每轮原始负结果独立保留。
- 当前S9实时19任务压力：124131完整响应全部源验真、0错误，完整HTTP均值80.18/P99302.58/最大712.77ms，24条超500；原生入口保留少量长尾，未取得全样本500ms及同run百万持续写联合资格。原始结果与SHA见[本轮总结](codex_notes/10/29-自由进出货口与查询长尾优化.md)。

## 执行过的操作

### 1. 摘要如何减少重复工作

假设一个提交新增20个任务、完成10个任务。publisher消费该批新增内容并推进期限计数，保存这个seq的摘要。之后100台机器人查询时，各自校验真实图谱点并读取已生成的摘要，无须反复扫描全周任务。

原journal始终只读；派生SQLite使用单独文件。缺批、checksum不符、创建版本变化、数量超预算或摘要损坏都会报错。查询不会返回残缺数据冒充成功。

### 2. 实时网关的轻量截止点

`WarehouseLiveRuntime.query_point()`在锁内只取`run_id/seq`。地图、100机器人完整帧和UI字段留给原快照接口。浏览器仍使用原index页面、原控制与WebSocket接口。

### 3. S5的进程与连接

```text
100客户端 → 原live网关（24准入，全部等待计入耗时）
               ├─ query-only 14200：3线程，各1个Neo4j连接
               ├─ query-only 14201：3线程，各1个Neo4j连接
               ├─ ...
               └─ query-only 14207：3线程，各1个Neo4j连接

仿真主进程：一个物理世界、一个写入器、历史决策provider
```

API最多24连接，加provider最多4和writer1，共29。网关选择当前run/seq，按最少在途数分配端点，长连接每端点最多3条。子服务返回完整JSON字节，网关原字节转发。连接等待、准入等待、查询、完整响应发送都在正式测量区间中；没有自动重试或结果缓存。

只读子服务不创建Runtime。直接给live启动八个uvicorn worker会复制世界，不适用于此设计。

### 4. 两阶段围栏（S7起）

每个需要fleet/摘要的请求固定执行两阶段图读取：A观察新鲜提交头H；B在新只读事务中使用明确参数读取请求点S和预算点H。B核对run、创建版本、source、完整metadata/config及两个Step的identity/checksum，要求`S <= H <= H2`。任务数量预算使用A实际观察的H。缺点、头回退或版本变更直接失败，无重试或旧摘要降级。

S7的600次真实global查询全部通过，其中4次覆盖了B期间头继续前进的`H2 > H`；S8仍出现过一次预算点H缺失。当前S9改为Python完整key逐点probe：S=H复用单点共2读，S不同H则共3读。2436次真实global全部完整通过，482次覆盖两点，14次覆盖解析期间头继续前进。此前缺点根因保持未决，不能将一轮未复现宣称永久修复，也不能直接归为官方文档描述的索引扫描缺读。

### 5. 原生网关与原子发布点（S8起，S9当前候选）

S5/S6的真实executor只需约7ms，主要等待位于收到完整body之后。增加客户端进程没有改善这段等待，因此进一步将高频HTTP转发从仿真进程拆出：

```text
原URI /api/knowledge/query → 独立Nginx → 八个query-only进程（各3查询线程）
原页面 / 控制 / WebSocket  → 独立Nginx → 唯一Runtime（私有14190）
```

Runtime显式启用`query_point_path`后，在journal、Neo4j、摘要处于同一个正seq且无错误时，原子发布约333字节的SHA envelope；同一提交的其他tick不会重复写。侧车每请求只读取这个小文件，验证owner、版本和checksum，再补默认run/seq。跨run/未来点400，缺失或损坏源点503，跨Origin403；均无旧点fallback。

Nginx配置使用`least_conn`、keepalive、明确关闭重试和缓存，HTTP可自然在途100；查询执行仍受八个3线程上限控制，Neo4j连接预算保持29。原生反代只运行独立prefix、loopback端口、用户目录日志和临时文件，不注册系统服务。配置模板为`config/query-gateway-nginx.conf.example`。[上游/keepalive文档](https://nginx.org/en/docs/http/ngx_http_upstream_module.html)、[反代文档](https://nginx.org/en/docs/http/ngx_http_proxy_module.html)。

主要比较完整客户端HTTP至JSON解析的时延，包含Nginx。Nginx原始request_time与八侧车ASGI分别报告，后者称为内部查询耗时，不与旧Python网关的ASGI区间直接计算端到端改善率。最终S8实测资格见本轮总结。

### 6. 启动与回退

先冻结源并准备SHA清单，格式为`{"files":{"backend/...py":"sha256","ops/...py":"sha256"}}`。清单必须覆盖实际执行的sidecar文件。选定独立端口、实际Neo4j URI和同一个producer输出根；私密认证用环境变量，不写入Git。本轮原始运行配置另存为`config/free-ports-balanced-100.json`，与完成1728任务0晚点的配置字节一致。

复现本轮Git源码时，完整清单已随证据交付；将`docs/evidence/query-tail-v2/source-s9-backend-manifest.json`按原字节复制到源根的`source-manifest.json`，再使用下方完整清单。若已修改代码或配置，应创建新冻结版本、重新测试并生成新清单，保留旧文件。

仿真创建初始提交后，以实际run ID分别启动八个子服务。以下展示一个端口，其余使用14201—14207：

```bash
bash run_in_lade.sh python ops/warehouse_query_sidecar.py \
  --source-root SOURCE \
  --source-manifest SOURCE/docs/evidence/query-tail-v2/source-s9-manifest.json \
  --summary-root RUN_ROOT --warm-run-id ACTUAL_RUN_ID \
  --live-query-point RUN_ROOT/live-query-point.json \
  --expected-runtime-instance ACTUAL_RUNTIME_INSTANCE \
  --expected-neo4j-uri bolt://127.0.0.1:17691 \
  --port 14200 --query-workers 3
```

`/api/health`报告`kind=query_only`、`runtime_created=false`、实际来源清单SHA、线程数和单列的启动准备耗时。唯一Runtime通过原统一`python -m backend.warehouse live`启动，私有端口14190，额外指定`--query-point-file RUN_ROOT/live-query-point.json`、`--query-acceleration summary`及原registry/source/output参数。构造完成时打印真实run与owner，亦可从私有`/api/snapshot`读取；子服务使用这个实际ID。发布点路径首次构造时必须不存在且直属output root，保留原点文件或为新实验使用新output目录。

私有live同时配置`--query-workers 24`和八个`--query-upstream`，使其备用查询路径也复用这24条查询连接，不另建4个本地查询连接池。常规公开查询由原生路由直接到侧车。子服务就绪后，从独立prefix启动Nginx，模板已将14189公开入口与14190唯一owner分开：

```bash
mkdir -p OWNED_PREFIX/logs
NGINX_BINARY -t -p OWNED_PREFIX/ -c SOURCE/config/query-gateway-nginx.conf.example
NGINX_BINARY -p OWNED_PREFIX/ -c SOURCE/config/query-gateway-nginx.conf.example -g 'daemon off;'
```

S5—S7对照使用的Python网关可以继续通过`--query-workers 24`和八个`--query-upstream`启用。原始实验完整启动与控制顺序保存在独立wrapper，查询回放需使用新输出目录，不能覆盖旧原件。

网关CLI参数只接受显式loopback HTTP源，禁止其他主机、路径、凭据、重复地址和重定向。依赖增加`httpx>=0.27,<1`，默认本地查询路径仍为4线程。

回退时停止本轮明确拥有的网关/子服务，改用原冻结目录和无`--query-upstream`的本地查询入口；默认fixed工位模式继续可用。先归档原始journal/checkpoint/results和SHA，再结束owned实验。旧服务和数据库不需重启。

## 修改内容

- `backend/query_views/*`：精确增量摘要、版本围栏、不可变fleet片段。
- `ops/warehouse_live_runtime.py`：持久化成功后推进摘要，发布标量查询点。
- `ops/warehouse_live.py`：显式摘要开关、1—24查询线程、可选异步查询上游。
- `ops/warehouse_query_proxy.py`：loopback验证、最少在途选择、完整字节转发。
- `ops/warehouse_query_sidecar.py`：独立query-only进程、严格参数/来源校验、错误传播和排队计时。
- `config/query-gateway-nginx.conf.example`：原生query-only路由与原页面、控制、WebSocket路由分离。
- 新worker/sidecar/proxy契约测试共20项；另外保留旧HTTP测试和完整生产回归。确定性单测中的同步门仅用于验证并发上限，性能测量没有人工等待门。

## 后续建议

1. 按本轮总结分别判断物理、图谱正确性、静态百万性能、实时持续负载，保留各自资格。
2. 实际机器人按状态变化与控制需要发查询；持续100客户端全速请求作为压力上界独立报告。
3. 新run继续由网关提供实际截止点，子服务可按新run派生摘要；新数据与地图仍需执行原validate和库存约束检查。
