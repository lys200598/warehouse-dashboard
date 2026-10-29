# 仓库知识实验平台：统一入口与模块对接

这份指南面向会写Python、初次接触本项目的同学。平台把任务文件送入100机器人仓库仿真，保存实际任务、货物、状态与协作历史，再通过知识查询支持复盘、接力决策和性能实验。

先阅读本文了解模块与使用顺序；仓库几何和调度细节见[仓库设计指南](WAREHOUSE_DESIGN_GUIDE.md)，数据生成对接见[给lys的数据交接说明](LYS_DATASET_HANDOFF.md)。

## 1. 一次实验怎样走完

```text
任务CSV、初始库存、manifest、地图
                │ 校验身份、位置、时间、参考库存
                ▼
统一运行入口 backend.warehouse
                │
                ├─ V5：派单、能量准入、交通预约、取放、充电
                ├─ required：接力规划/交接前读取共享历史并授权
                └─ rules：保留相同物理规则，作为知识控制的对照
                ▼
SQLite journal + checkpoint ──事务提交──→ Neo4j
                │                         │
                │                         ├─ 明确对象的图/时序查询
                │                         ├─ 仓库决策查询包
                │                         └─ 向量召回及图谱关系验证
                └─ 实际frames → 浏览器回放
```

仿真协调器维护当前物理状态，负责提交任务、库存、路线和货权变化。图谱保存已经提交的共享知识；查询通过run编号和知识水位选择同一个可见时点。当前实现使用规则协调器，不调用LLM。

### 运行中的三个时间

- `sim_time`：仓库内事件发生的模拟时间，例如600秒报告低电。
- `event_seq`：本run中的事件发射序号，适合追溯动作因果。
- `seq/commit_seq`：知识提交水位，表示该批数据已经可查询。

例如：600秒发生的货物接力预约，随后和这一批状态一起写入水位122。查询`seq=122`时，只使用截至122已经提交的知识；之后产生的事件继续留在后续提交中。

## 2. 代码放在哪里

|目录/模块|负责的内容|优先阅读|
|---|---|---|
|`data/layouts/`|实际仓库地图、资源位置与容量|`warehouse100-wide-v2.json`|
|`docs/dataset-contract/`|给数据生成侧的合同、位置表、独立校验器和小样例|`README.md`、`contract/data-contract.json`|
|`backend/wide/`|参数化仓库、输入校验、连续多日任务生成|`data.py`、`multiday.py`|
|`backend/scheduling_v5/`|选定调度策略，继承已有能量、交通和恢复规则|`sim.py`、`matching.py`、`invariants.py`|
|`backend/real_scale/`|完整机器人状态写入、结构向量和来源核对|`writer.py`、`oracle.py`|
|`backend/query_v2/`|轻量上下文和完整ANN上下文、查询计时|`service.py`、`service_tail.py`|
|`backend/knowledge_runtime/`|历史提供者、决策授权、实际接力与恢复|`provider.py`、`policy.py`、`sim.py`|
|`backend/warehouse/`|本轮统一命令入口|`python -m backend.warehouse --help`|
|`ops/`、`scripts/`|版本、批运行、演示和工程操作|随具体实验选择|
|`tests/`|格式、库存、交通、恢复、查询和检索测试|按模块运行|

查询目录及向量选型随本轮独立模块提供各自的说明，分别见[仓库查询指南](WAREHOUSE_QUERY_GUIDE.md)和[向量检索指南](VECTOR_RETRIEVAL_GUIDE.md)。

## 3. 统一命令入口

从仓库根目录执行。服务器可先载入既有Python环境；`run_in_lade.sh`会优先使用`LADE_ENV_DIR`指定的环境。

```bash
export LADE_ENV_DIR=/data/workspace/wz/warehouse-deploy/.venv
bash run_in_lade.sh python -m backend.warehouse --help
bash run_in_lade.sh python -m backend.warehouse status
```

统一入口分为八类操作：

|子命令|用途|
|---|---|
|`validate`|核对地图和固定任务目录|
|`generate`|生成单类负载或连续多日典型负载|
|`run`|推进仿真、写入日志/图谱并保存恢复状态|
|`query`|调用标准仓库知识查询|
|`serve`|展示指定运行保存的实际帧|
|`live`|真实实时控制台：固定数据、单写者运行、图谱更新与查询|
|`status`|查看选定模块、版本、身份及使用入口|
|`retrieval-eval`|执行向量检索的质量/性能对照|

各子命令的完整参数以`python -m backend.warehouse <子命令> --help`为准。下面先使用最小闭环，不要求安装或启动LLM。

### 运行环境与性能复现

统一入口的常规依赖在`requirements-platform.txt`；原FastAPI/Ray控制台继续使用原`requirements.txt`。本轮Pro6000生产查询使用CPython3.10.12、Neo4j驱动API6.4.1及`neo4j-rust-ext`6.4.1.0。测试时通过独立`PYTHONPATH`目录复用加速驱动，共享虚拟环境保持原样。

新机器可在自己的独立环境中选用`requirements-platform-performance.txt`复现驱动版本；它作为普通平台依赖文件的替代项。安装后的业务导入仍为`import neo4j`。Rust扩展是Neo4j官方提供的加速方案，依赖及安装关系见[官方性能指南](https://neo4j.com/docs/python-manual/current/performance/)与[官方扩展仓库](https://github.com/neo4j/neo4j-python-driver-rust-ext)。新机器的实际性能仍需要重新测量。

```bash
python3 -m venv .venv-platform
.venv-platform/bin/python -m pip install -r requirements-platform-performance.txt
export LADE_ENV_DIR="$PWD/.venv-platform"
bash run_in_lade.sh python -m backend.warehouse status
```

连接数也要符合数据库的处理预算。本轮私有Neo4j的Bolt处理线程上限为32；100客户端压力由8个进程共享每进程3条连接，另留观察器和写者连接。客户端等待连接的时间完整计入API耗时。最初放大连接池时出现的31条`NoThreadsAvailable`失败请求保留在诊断记录中；后续轮次在明确的连接预算下重新执行。100客户端数量与24条业务Bolt连接分别记录，避免通过无限连接把数据库线程耗尽。

### 3.1 先校验19条任务的小样例

```bash
bash run_in_lade.sh python -m backend.warehouse validate \
  --scene data/layouts/warehouse100-wide-v2.json \
  --dataset data/examples/warehouse-v5
```

这个样例用于快速确认格式、地图和流程。完整数据生成同学的输入可以替换`--dataset`路径。

### 3.2 运行规则对照并保存回放

每次新实验使用一个尚不存在的output和唯一run编号。

```bash
bash run_in_lade.sh python -m backend.warehouse run \
  --scene data/layouts/warehouse100-wide-v2.json \
  --dataset data/examples/warehouse-v5 \
  --output data/local-runs/platform-sample-rules \
  --run-id platform-sample-rules \
  --mode rules --knowledge-backend journal \
  --frames --max-wall-seconds 1800
```

`journal`模式将提交写入本地SQLite，适合不依赖数据库的格式和物理流程检查。`rules`保留V5能量、交通与货权规则，关闭历史授权，是同输入知识对照的基线。

### 3.3 查看实际回放

```bash
bash run_in_lade.sh python -m backend.warehouse serve \
  --output data/local-runs/platform-sample-rules --port 14183
```

浏览器访问`http://127.0.0.1:14183/`。远程服务器需要相应SSH端口转发。页面播放保存的实际frames，运行前需使用`--frames`。长数据的帧数量、日志和检查点应结合磁盘预算安排。

### 3.4 运行历史参与的实验

准备一个独立Neo4j数据库，使用环境变量载入连接地址、用户名和密码，再运行：

```bash
bash run_in_lade.sh python -m backend.warehouse run \
  --scene data/layouts/warehouse100-wide-v2.json \
  --dataset data/examples/warehouse-v5 \
  --output data/local-runs/platform-sample-history \
  --run-id platform-sample-history \
  --mode required --knowledge-backend neo4j \
  --commit-seconds 5 --max-wall-seconds 1800
```

有意测试低电或故障时，通过独立的`--disturbances-json`输入。该文件与任务CSV分开，以便在同一业务输入上比较策略。

`required`在接力规划与实际交接前查询该任务的货物历史和协作预约，核对来源、水位和当前货权，再提交原物理动作。普通移动保持原规则；必要历史读取失败时保留现场并明确失败，避免静默执行未经授权的交接。详细机制见[历史控制说明](HISTORY_RUNTIME.md)。

### 3.5 查询和检索评估

```bash
bash run_in_lade.sh python -m backend.warehouse query --help
bash run_in_lade.sh python -m backend.warehouse retrieval-eval --help
```

先按[查询指南](WAREHOUSE_QUERY_GUIDE.md)选择明确对象、时间水位和查询包，再调用入口。向量检索用于寻找特征相似的历史状态；按货物编号追踪持有者直接走货权与时序查询。两类读取分别记录正确性、平均、P99、最大值和超标数量。

## 4. 多日输入与采样预算

固定数据集支持任意合法时长，生成工作的任务条数预算由`--max-tasks`显式控制。当前任务文件仍一次加载；运行时继续检查容量和实际依赖。

```bash
bash run_in_lade.sh python -m backend.warehouse generate \
  --kind multiday --scene data/layouts/warehouse100-wide-v2.json \
  --output data/datasets/my-seven-days --days 7 --seed 20261028
```

默认七类日是均匀、固定峰值、局部热点、批次脉冲、热点迁移、进出潮汐、混合高负载与恢复。日期切换只改变负载，不重置货物、库存和在途任务。生成规则、期限公式和可复现参数见[多日数据说明](MULTIDAY_DATASET.md)。

**任务数量、事件数量与状态采样数量分别统计。** 100台机器人按5模拟秒采样，七天约产生12,096,000条机器人状态；若明确改为300秒周期采样，七天约201,600条，另加关键边界提交。业务事件继续保留其实际时间和事件序号。密集采样、稀疏周期采样各自使用自己的源码/配置身份和结果，查询可见性以提交水位为准。

在长数据上，接力授权前应发布一致的关键边界状态，使该次决策使用新鲜的已提交知识。周期采样间隔与请求响应耗时分别计量；500ms查询耗时无法表达某个状态距当前模拟时间有多旧。

## 5. 原入口怎样保留

- `backend.server`继续保留原FastAPI、WebSocket与local/Ray控制台，方便对照和逐步集成。
- `ops/warehouse_v5_release.py`及版本runner继续保留对应发布的显式入口。
- 新统一入口选择当前知识实验工作流，使数据、物理运行、查询与检索更容易连起来使用。
- 当前V5知识实验由中央进程维护100个机器人状态；独立Ray Actor数量、同时业务作业数量和并发API数量分别记录。对Ray执行的验收沿相应入口进行。

原网页或旧冻结结果用于其原来的版本。新运行使用新的run编号、output和源码身份，避免旧输出被更新后误认为仍可直接比较。

## 6. 查看哪些输出

|输出|回答的问题|
|---|---|
|`results.json`|完成、迟到、流转、安全、协作及本轮验收范围|
|`journal.sqlite`|原始知识提交及已确认恢复状态|
|`checkpoint.json`|源码、输入和配置匹配时从哪里继续|
|`frames.sqlite`（启用时）|每个保存时点实际绘制的机器人与仓库状态|
|查询原始请求/完整响应|每次请求的对象、截止水位、字段、真实起止与错误|
|来源审计报告|返回事实和实际动作是否能回到同一原始提交|
|联合实验manifest|源码、输入、命令、阶段退出码、覆盖范围及各产物身份|

新运行默认拒绝覆盖非空output。恢复使用`--resume`，并要求原源码、模式、配置、输入和扰动身份保持一致。源码更新后保留旧冻结目录供旧检查点恢复，再用新目录开启新实验。

## 7. 交付前怎样验收

1. 标明测试提交、源码清单及数据/地图SHA。
2. 同一数据先校验，再运行；核对所有已发布任务、库存、位置引用和期限。
3. 使用实际低电/故障与交接，逐条核对授权、预约、货权转移和动作记录。
4. 查询固定已提交截止水位；每种查询有独立预期、边界与来源核对。
5. 最新读写混合实验记录每个请求真实起止，以及相同run在期间的图水位推进。
6. 暂停恢复后继续原输入，确认扰动和交接各执行一次。
7. 性能记录平均、分位数、最大值、全部错误和超标请求；完整ANN与轻量业务接口分开。

上轮已保存的基础证据见[历史控制说明第6—7节](HISTORY_RUNTIME.md#6-已经实际验证的结果)。本轮集成版本的实际结果、提交和覆盖范围由以下交付记录统一填写，原结果继续保留自己的身份。

### 本轮交付记录

当前实现集成已通过实际Git工作树完整回归：643项，636通过、7项可选集成跳过；前端75项通过。实验源码包SHA256为`6f049ddbfaf16575ad2867ec0fa0fff427a447df3f11589a5361304d3531fc0a`，其中108份后端文件与实现提交`21a8271`逐字节一致。后续实时功能增加`live`CLI转发及独立ops adapter，旧实验继续从原冻结目录运行与恢复。

七天300秒采样基线的规则/历史模式均50,112任务完成、0迟到、14次低电接力，最终物理状态一致；28次历史授权的来源与实际动作逐条通过审计。60,240次轻量请求和18次功能请求的完整来源验真通过。该run实际994,184节点；第四轮100-client实际API峰值61，严格组合审计与百万资格继续保留未通过。新120秒采样规模回归已独立启动，保持同输入和物理策略。分轮性能、已测提速原型及后续门槛见[完整回归记录](PLATFORM_REGRESSION_20261028.md)。

实时交互入口现为14186，加载相同七天CSV并使用独立17690数据库；当前批量规模回归使用独立run和数据库。真实浏览器已验证单步、运行、暂停及图谱查询。操作与数据选择见[实时控制台指南](LIVE_CONSOLE_GUIDE.md)。14183/14184回放与旧14182/V5等服务保留。原Git提交`52c9206...`可用于回看原仓库交付，旧运行应由其原冻结源恢复。
