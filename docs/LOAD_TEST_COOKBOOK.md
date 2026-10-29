# 压测操作手册：从零到真实实验

**需要在页面上配置与看图：使用[独立压测控制台](PRESSURE_GUI_GUIDE.md)，`python pressure_test.py --gui`。** 它调用同一套测量工具，不改变判据。

**只想马上检查服务：先看[一条命令的压测向导](PRESSURE_TEST_QUICKSTART.md)，执行 `bash pressure_test.sh` 并按菜单选择。** 本页保留高级配置、各工具独立用法和接口适配细节；普通使用无需手工拼接这些长命令。

面向第一次接触仓库项目的同学。建议按第2节教学例子 → 第3节真实查询 → 第8节修改兼容顺序阅读。没有要求你把全部脚本跑一遍。

## 1. 先选择工具，不要一次全跑

**压测**是按规定频率发请求，检查系统承受多少压力、回复多慢、失败多少。**验真/审计**是检查回复与原始运行事实是否一致。后者通常在压测结束后进行，耗时不算进生产查询延迟。

|文件|一句话用途|是否独立命令|依赖|
|---|---|---|---|
|`ops/load_harness.py`|向已有HTTP接口发请求、保存响应、离线统计|是，`run`和`analyze`|Python标准库；网络可达，服务返回完整JSON对象|
|`ops/load_probe.py`|在服务器入口记录实际同时在途请求|否，是ASGI中间件|ASGI服务，例如FastAPI/Uvicorn；第2节演示如何包装|
|`ops/load_validation.py`|核对标准catalog包的身份、字段、SHA|否，分析器通过函数名加载|本项目catalog响应契约；第3节给出调用方式|
|`ops/run_query_ab.py`|同一个封存百万run对比旧/新查询配置|是|Linux、Neo4j驱动、FastAPI/Uvicorn、Nginx、冻结源码、journal、oracle|
|`ops/query_ab_validation.py`|让离线真值读取少做重复计算|否，是A/B工具的辅助类|原JournalTruth格式；以`--cache-truth`启用|
|`ops/audit_robot_agent_io.py`|核验100机器人实际写入、读回、身份和退出证据|是|完整已结束Agent运行目录；默认需Neo4j和对应schema|
|`ops/audit_wheeled_capacity.py`|核验地图运力对照是否完成、库存/电池是否一致|是|原矩阵plan、对应冻结源码、输入、checkpoint、journal与frames|
|`ops/audit_wheeled_day_window.py`|核验已完成的24小时窗口及未到期任务|是|24小时窗口原件、匹配源码/输入、退出码文件；当前含medium固定断言|
|`ops/verify_catalog_responses.py`|将目录查询完整响应与原journal独立对照|是|项目专用request/response记录、原journal；可生成独立oracle|

术语：

- **API**：其他程序调用的接口，例如 `POST /api/knowledge/query`。
- **workload（负载JSON）**：告诉发起器“请求哪个接口、带什么参数、各类请求占比”。
- **run_id**：一次仿真的编号；**seq**：这次运行的知识提交批次编号。
- **journal.sqlite**：原运行逐批保存的事实；**oracle.sqlite**：根据原记录构建、供验真使用的独立索引。它不是另一个生产数据库。
- **冷/热**：首次读取和已经加载过相应缓存后的读取。仅给命令写 `--phase warm`不会自动预热。
- **A/B**：保持数据和请求相同，比较两套实现/配置。
- **ASGI/中间件**：ASGI是Python网页应用与服务器之间的调用约定；中间件是在请求进入、回复发出时加一层处理。本工具只加计时和记录，不改查询答案。
- **Nginx/sidecar**：Nginx是接收和转发HTTP请求的服务器；这里sidecar是独立的查询服务进程，把读取工作与仿真进程分开。

## 2. 第一次练习：不连图谱也能学会发起器和观测器

### 2.1 环境与目录

以下例子在Linux/WSL的Bash、仓库根目录执行。需要Python 3.10以上；教学服务器只额外需要Uvicorn。项目已有环境可跳过创建步骤；新机器执行：

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install 'uvicorn[standard]'
export LADE_ENV_DIR="$PWD/.venv"
```

以下用 `bash run_in_lade.sh python ...` 选择环境。如果直接运行已激活环境的 `python ...`，参数相同。Windows使用PowerShell时可直接用Python命令，但本文的export、变量和反斜线换行属于Bash；最方便的是SSH到Pro6000后执行。

### 2.2 终端A：启动个人教学接口

```bash
WORK="$PWD/data/local-load/demo-$(date +%Y%m%d-%H%M%S)-$$"
mkdir -p "$WORK"
printf '%s\n' "$WORK"
bash run_in_lade.sh python docs/examples/load_probe_demo.py \
  --port 14590 --output "$WORK/server.jsonl"
```

程序保持前台运行。它是本仓库附带的教学服务，回复含 `demo:true`，没有Neo4j和仿真。`server.jsonl`记录真实服务端请求区间。端口被占用时选择一个新端口，同时改下一步URL；不要结束已有仓库服务。

### 2.3 终端B：先用4个客户端测8次

仍在仓库根目录，设置同一个环境。把终端A打印的完整目录填入WORK；这里这一个参数来自上一步，不需要猜run编号或数据库：

```bash
export LADE_ENV_DIR="$PWD/.venv"  # 已有环境则用你的实际环境路径
WORK='/填写终端A打印的完整目录'
bash run_in_lade.sh python -m ops.load_harness run \
  --base-url http://127.0.0.1:14590 \
  --workload docs/examples/load-demo.json \
  --run-id tutorial --session demo-eight \
  --clients 4 --processes 4 --requests 2 --duration 15 --mode closed \
  --output "$WORK/client"
```

预期每客户端2次，共8次。`run`结束仅表示完成发起和保存，延迟达标由`analyze`判断。`--requests`是每客户端上限，duration是另一停止边界；短duration或故障可能使实际数量更少，须看报告。

返回终端A按Ctrl+C，等进程退出后再分析。该动作只停止自己的教学服务器，不停止图谱或仓库。

```bash
bash run_in_lade.sh python -m ops.load_harness analyze \
  --input "$WORK/client" --output "$WORK/analysis.json" \
  --require-run tutorial --server-trace "$WORK/server.jsonl"
```

输出：

```text
WORK/
├── server.jsonl                 # 服务端进入/结束区间
├── server.jsonl.status.json     # 观测日志是否排空、是否丢记录
├── client/identity.json         # 脚本、负载、参数等身份
├── client/run.json              # 客户端启动和完成状态
├── client/client-0001.jsonl.gz  # 此客户端的完整请求、响应、计时
├── client/...                   # 其他客户端
└── analysis.json                # 统计和资格说明
```

`actual_server_api_peak`是实测并发，未必等于4；`server_peak_qualified=true`表示区间证据完整，不表示并发恰好达到了目标。教学服务结果只能验证工具链，不作为图谱性能结果。

### 2.4 观测器在真实服务中怎么接

`load_probe.py`没有可独立运行的CLI。假设已有FastAPI实例叫app：

```python
from ops.load_probe import LoadProbeMiddleware
import uvicorn
probe = LoadProbeMiddleware(app, output="data/new-evidence/server.jsonl",
                            paths=["/api/knowledge/query"])
try:
    uvicorn.run(probe, host="127.0.0.1", port=14591, workers=1)
finally:
    probe.close()
```

要用新文件路径。多worker每个进程分别写一个文件；分析时把多个文件跟在 `--server-trace` 后。包装真实app须在你自己的实验服务入口修改，不向正在使用的旧服务强塞观测器。第2.2节的教学脚本就是这段模式的完整可运行版本。

`paths`要覆盖实际请求路径，客户端的 `X-Probe-ID`头必须原样到达被观察入口。只合并同主机、同一入口层的区间；不能把网关和后端两层算成两次请求。没有trace仍可统计客户端延迟，但服务端并发资格为false。

## 3. 测真实仓库查询：从哪里拿URL、run和seq

### 3.1 先获取一次有效查询

前置条件：已有 **Neo4j模式** 的实时服务；journal演示模式没有可用图谱查询。启动方式见[交付说明第6节](WAREHOUSE_WHEELED_HANDOFF_20261029.md#6-第一次运行先跑小样例再接数据库)。

Pro6000既有部署示例，SSH后在服务器执行；若使用自己的实例，将BASE换成自己的地址：

```bash
BASE=http://127.0.0.1:14192
HEALTH_DIR="$PWD/data/local-load/health-$(date +%Y%m%d-%H%M%S)-$$"
mkdir -p "$HEALTH_DIR"
curl --fail --silent --show-error "$BASE/api/health" > "$HEALTH_DIR/health.json"
RUN=$(python -c 'import json,sys; print(json.load(open(sys.argv[1]))["run_id"])' "$HEALTH_DIR/health.json")
SEQ=$(python -c 'import json,sys; print(json.load(open(sys.argv[1]))["runtime"]["knowledge"]["mirrored_seq"])' "$HEALTH_DIR/health.json")
printf 'RUN=%s SEQ=%s\n' "$RUN" "$SEQ"
curl --fail --silent --show-error "$BASE/api/knowledge/query" \
  -H 'Content-Type: application/json' \
  -d "{\"query_id\":\"self\",\"run_id\":\"$RUN\",\"seq\":$SEQ,\"robot_id\":1}"
```

先确认回复的run、seq和机器人1正确。实时页面切换数据会更换run，此时重新读取health，不继续用旧RUN。seq为0或接口503时先检查图谱水位，不盲发100次。

上面14192是既有部署示例，不是克隆项目后自动存在的服务。health响应字段属于当前仓库API；若合并时改了它，获取RUN/SEQ的两行也要适配。Python变量提取需在已激活环境中执行。

### 3.2 轻查询：10秒错峰示例

```bash
WORK="$PWD/data/local-load/self-$(date +%Y%m%d-%H%M%S)-$$"
mkdir -p "$WORK"
bash run_in_lade.sh python -m ops.load_harness run \
  --base-url "$BASE" --workload config/load-light-live.json \
  --run-id "$RUN" --session self-periodic \
  --clients 100 --processes 100 --mode periodic --interval 5 --duration 10 \
  --output "$WORK/client"
bash run_in_lade.sh python -m ops.load_harness analyze \
  --input "$WORK/client" --output "$WORK/analysis.json" \
  --require-run "$RUN" --validator ops.load_validation:validate_catalog
```

这里每机器人5秒最多一次，初次错峰，总目标约20次/秒。已有配置不指定seq，由实时服务选择当前已提交点；查询sidecar通常必须明确seq。没有新服务端trace，这个例子只报告客户端耗时和结构校验，不能称“真实API峰100”或“完整来源验真”。

`load_validation.py`由 `--validator 模块:函数` 调用，不能直接运行该文件当作验真命令。它检验标准catalog身份、响应结构与自身SHA一致，不从原journal重建真值。

### 3.3 重查询：先少量，再测突发

在同一终端沿用BASE、RUN，先发1次global确认窗口没有超预算；正式压力选择自己的空闲测试服务并记录背景负载：

```bash
curl --fail --silent --show-error "$BASE/api/knowledge/query" \
  -H 'Content-Type: application/json' \
  -d "{\"query_id\":\"global\",\"run_id\":\"$RUN\",\"window_steps\":60,\"event_limit\":10000}"
WORK="$PWD/data/local-load/heavy-$(date +%Y%m%d-%H%M%S)-$$"
mkdir -p "$WORK"
bash run_in_lade.sh python -m ops.load_harness run \
  --base-url "$BASE" --workload config/load-heavy-live.json \
  --run-id "$RUN" --session heavy-burst \
  --clients 100 --processes 100 --mode closed --requests 2 --duration 60 \
  --phase warm --output "$WORK/client"
bash run_in_lade.sh python -m ops.load_harness analyze \
  --input "$WORK/client" --output "$WORK/analysis.json" \
  --require-run "$RUN" --validator ops.load_validation:validate_catalog
```

最多200次重查询。上面已经人为发过global，不能再把整轮称为“全冷”；这也不保证所有worker都已热。重查询与self分别统计。仅标签warm不产生预热，更不改变任何慢样本。

### 3.4 固定到达率与静态历史点

- 将run参数换为 `--mode open --rate 50 --duration 30`：每秒计划50次到达。发起器忙会记录 `load_generator_busy`；丢到达也计入失败。
- `closed`完成一个再发一个，服务慢时自动降速；不能用它证明外部持续50次/秒下无排队。
- 测固定历史点时，复制负载到新输出目录，给请求body加上从同run读取的明确seq。保存这份JSON，不能事后用最终节点数推断测试时已达百万。
- `--base-url`只填协议＋主机＋端口，不能加 `/api`路径、用户名或密码；具体接口写在workload的path。

### 3.5 机器人私有查询不是14192上的同一条接口

`config/load-agent-query.json`测的是Agent服务的 `/api/agent/query`，需要该服务实际监听地址及每机器人凭证。普通实时网页端口不能据此推断Agent端口。

凭证保存在运行中的 `fleet.service.tokens`，地址在 `fleet.http.endpoint`，当前没有供普通用户调用的CLI凭证导出接口。知道run_id或Neo4j密码都不能代替机器人凭证。常规使用直接跑 `--robot-agents` 让真实机器人发请求，再用第5节审计，无需导出token。

开发者若要在自己创建的Fleet中额外测私有查询，可在**已成功publish且Fleet仍存活**时，以子进程环境传递：

```python
# 位于自己创建并持有fleet的运行程序中；这段不是独立终端命令。
import os, subprocess, sys
child_env = dict(os.environ)
for rid, token in fleet.service.tokens.items():
    child_env[f"WAREHOUSE_AGENT_AUTH_{rid}"] = "Bearer " + token
subprocess.run([
    sys.executable, "-m", "ops.load_harness", "run",
    "--base-url", fleet.http.endpoint,
    "--workload", "config/load-agent-query.json",
    "--run-id", fleet.service.run_id, "--session", "agent-own-check",
    "--clients", "100", "--processes", "100", "--mode", "periodic",
    "--interval", "5", "--duration", "10", "--output", str(new_output_dir)
], env=child_env, check=True)
```

`new_output_dir`由此调用程序设置为新的输出目录。仅给这一次子进程传环境，不打印token、不写进workload。该测试需100机器人均已有观测，`analyze`可以带 `--require-run`，但不能套catalog校验器，因为Agent返回结构不同。其独立来源验证由真实Agent运行审计完成。

## 4. 百万静态A/B：run_query_ab.py的真实服务器例子

这是高级实验编排器，会在新端口启动8个查询进程和独立Nginx，并发起负载、测后验真、清理它创建的进程。它复用已有图数据，不启动业务仿真，不为你生成百万数据，不安装Neo4j。

前提：Linux；环境已有Neo4j驱动、FastAPI/Uvicorn；所选run已结束且journal、oracle、图谱head匹配；同run已经真实百万；源目录有匹配的 `source-manifest.json`。这些原件不包含在Git的小样例里。

下面是**Pro6000现有归档的重测命令**，路径来自已交付ab-v2的launch/identity记录；其他电脑不能原样使用。它可能消耗较多查询资源，应在自己的实验时段运行，不在本文修订时自动执行：

```bash
ROOT=/data/workspace/wz/warehouse-wheel-agents-20261029
TOOLS=/data/workspace/wz/warehouse-git-delivery-20261028
OUT="$ROOT/docs-ab-$(date +%Y%m%d-%H%M%S)-$$"
cd "$TOOLS"
export LADE_ENV_DIR=/data/workspace/wz/warehouse-deploy/.venv
bash run_in_lade.sh python ops/run_query_ab.py \
  --source "$ROOT/source-integrated-v2" --output "$OUT" \
  --env /data/workspace/wz/warehouse-platform-density120-20261028/private-neo4j.env \
  --journal-root /data/workspace/wz/warehouse-query-tail-v2-20261029/journals \
  --oracle /data/workspace/wz/warehouse-query-tail-v2-20261029/outputs/oracle.sqlite \
  --harness "$TOOLS/ops/load_harness.py" \
  --nginx /data/workspace/wz/warehouse-free-ports-20261029/native-nginx-deb-v1/extracted/usr/sbin/nginx \
  --run-id wh-density120-week-required-20261028 --seq 5073 \
  --gateway-port 14599 --sidecar-port 14600 \
  --candidate-heavy-workers 2 --fully-warm-heavy --cache-truth
```

先检查14599、14600—14607没有被占用；工具也会拒绝已占端口，不会替你关闭服务。`--env`是已有权限的凭证文件，不要cat或提交到Git。`--source`是被测实现目录，`--harness`是发起工具，二者身份分别保存。这里比较旧归档实现的配置，不能用来声称测试了后来修改的API。

参数作用：

- `--journal-root`下必须有 `RUN/journal.sqlite`，不是直接传单个SQLite文件。
- `--oracle`是同run、足够前缀的已有只读真值索引。
- `--candidate-heavy-workers 2`：每查询进程总3worker中2个处理重查询，剩1个轻查询。
- `--fully-warm-heavy`：另测明确预热后的重查询，并保留预热请求和worker证明。
- `--cache-truth`：使用 `query_ab_validation.py` 减少离线真值重复读取，不改变生产查询结果或抽样数量。该辅助文件没有独立命令。

结果看 `$OUT/final.json`、各阶段 `analysis.json` 和 `full-source-validation.json`。`complete=true`表示实验与来源检查完整，**不保证全部请求低于500ms**；同时查看每阶段的超时数、`all_requests_under_500ms`、实际API峰。它对固定封存数据有效，持续写查需另做联合实验。

## 5. 机器人读写审计：audit_robot_agent_io.py

适用：已结束且保存完整100机器人进程记录的run。输入是运行目录，不是网页URL，也不是单个results文件。检查原HTTP全集、观测journal、源journal、实际图谱及进程关闭记录。

**自己新跑的例子**：按交付说明6.3运行后，在同一环境和终端中保留CASE变量，执行：

```bash
# 仍保留原来的NEO4J_*环境，指向该次run实际写入的库。
bash run_in_lade.sh python -m ops.audit_robot_agent_io \
  --run "data/local-runs/$CASE" \
  --output "data/local-runs/$CASE-agent-audit.json"
```

`--output`请使用新文件名，当前审计器使用write_text，可覆盖同名报告。保留旧报告方便比对。

**Pro6000已有完整高峰归档**（约44,216条完整HTTP，验真可能数分钟）：

```bash
ROOT=/data/workspace/wz/warehouse-wheel-agents-20261029
AUDIT_OUT="$ROOT/agent-peak-doc-audit-$(date +%Y%m%d-%H%M%S)-$$.json"
set -a
source "$ROOT/graph-test/neo4j.env"  # 仅在有权限的Pro6000归档环境使用，不输出内容
set +a
bash run_in_lade.sh python -m ops.audit_robot_agent_io \
  --run "$ROOT/agent-peak-v1" --output "$AUDIT_OUT"
```

图谱凭证必须指向该run原库。默认做图谱核对；`--no-graph`只用于明确的离线排查，报告会标 `graph_checked=false`，不能算完整图谱验真通过。当前脚本要求100个独立PID、各机器人都有写查、运行已完整结束；少量机器人样例或仍在运行的live不能拿它作通过判据。

结果分开看：`passed`是完整性/来源检查，`statistics`是延迟，`all_requests_under500ms`同时包含写入和查询。原高峰查询全部低于500ms、写有67条超标，因此可能出现 `passed=true` 但全请求500ms资格为false。

## 6. 仓库运力审计：audit_wheeled_capacity.py

输入是**已完成的对照实验矩阵目录**：`plan.json`列出各case的地图、数据集、配置和输出名；各case下面保存results、checkpoint、journal、frames。它不会替你生成plan、补造记录或重新跑仿真。

真实归档例子，使用当时匹配的冻结源码；不能把今天的新源码传入昨天的结果：

```bash
ROOT=/data/workspace/wz/warehouse-wheel-agents-20261029
OUT="$ROOT/capacity-doc-audit-$(date +%Y%m%d-%H%M%S)-$$.json"
bash run_in_lade.sh python -m ops.audit_wheeled_capacity \
  --matrix "$ROOT/capacity-v1" --source "$ROOT/source-wheel-v1" --output "$OUT"
```

它主要依赖文件，不需要Neo4j或HTTP服务。检查：输入身份、任务完成、箱货唯一归属、电池守恒、采样时刻机器人是否越界/碰撞。稀疏画面不能证明两次采样之间绝对无碰撞。

必须检查每个case：`status=complete`、`all_input_completed=true`、`accepted_final_state_audit=true`。没有results时会返回 `pending-no-final-results`；当前CLI只有 `audit-failed` 导致非零，因此**退出0仍可能含pending**。`late`也要单看，审计完成不等于零晚点。

该工具含100机器人、SOC范围等当前模型断言；改成200机器人或不同能量底线，应版本化调整断言并对新实验验收，保留旧审计器供旧结果复现。

## 7. 24小时窗口审计：audit_wheeled_day_window.py

用于“恰好运行一天，然后停下看结果”，未来几天任务还没释放是正常情况。该工具当前明确要求86400秒、`wheeled-medium-100.json`配置、100机器人与有限电池模型；不是任意时长通用审计。

真实归档例子：

```bash
ROOT=/data/workspace/wz/warehouse-wheel-agents-20261029
OUT="$ROOT/day1-doc-audit-$(date +%Y%m%d-%H%M%S)-$$.json"
bash run_in_lade.sh python -m ops.audit_wheeled_day_window \
  --source "$ROOT/source-integrated-v4" --run "$ROOT/day1-energy-v1" \
  --scene "$ROOT/source-integrated-v4/data/layouts/expansion-candidates/medium/layout.json" \
  --dataset "$ROOT/seven-medium-logical" \
  --exit-file "$ROOT/logs/day1-energy-v1.exit" --output "$OUT"
```

`--dataset`必须是该次运行实际使用的目录，不能拿Git里的旧七天数据替换；上面的实际路径见本次文档验证记录。`--exit-file`是原生产进程自然退出时写下的退出码，不能为了通过手工创建一个0。缺失原文件时保留为未验证。

报告重点：released（已释放）、completed（已完成）、unfinished_not_yet_due（在途未到期）、unfinished_overdue（已过期未完成）、future_or_boundary_release（未来或边界任务）。原例6912释放、6902完成、10在途未到期，与“全七天50112任务完成”属于不同结论。

### 补充：完整catalog验真脚本怎么用

`ops/verify_catalog_responses.py`读取的是专用请求/响应文件，**不能把load_harness目录或gzip直接当作两个参数传入**。原格式见[标准查询指南第8节](WAREHOUSE_QUERY_GUIDE.md#8-保存响应后的独立来源验真)。日常A/B已内置完整验真，直接用第4节即可，不必再手工转换。

附带的 `docs/examples/capture_catalog_query.py` 可以从当前实时API保存一条真实self请求，直接生成该格式。先让**自己的**实时run保持暂停/ready，采集和验真期间不继续运行或切换数据，以免源文件正在变化。在服务所在的同一台服务器执行：

```bash
CASE_DIR="$PWD/data/local-load/capture-$(date +%Y%m%d-%H%M%S)-$$"
bash run_in_lade.sh python docs/examples/capture_catalog_query.py \
  --base-url "$BASE" --output "$CASE_DIR"
JOURNAL=$(python -c 'import json,sys; print(json.load(open(sys.argv[1]))["journal"])' "$CASE_DIR/identity.json")
bash run_in_lade.sh python ops/verify_catalog_responses.py \
  --journal "$JOURNAL" \
  --requests "$CASE_DIR/requests.jsonl" --responses "$CASE_DIR/responses.jsonl" \
  --oracle-dir "$CASE_DIR/oracle" --output "$CASE_DIR/verification.json"
```

capture脚本不推进仿真，只发一次读取；它依赖当前health的run、水位和output字段。源journal路径来自服务返回，跨电脑使用时需要取得同一原件，不能猜另一个文件代替；路径为空时说明接口契约变了，先适配采集器。完整验真会在新oracle目录构建真值，可能耗时。`full-ann`有独立旧验真契约，此验证器会明确拒绝该变体；不能用普通业务相似检索的验真结果替代ANN召回验收。

## 8. 改代码以后，哪些还能用

### 8.1 依赖层级与兼容矩阵

|你的改动|HTTP发起/计时|额外要改哪里|
|---|---|---|
|换主机或端口|可复用|改`--base-url`；BASE只能是origin|
|改GET路径或GET查询参数|通常可复用|改workload.path；观测器paths也要改|
|把POST查询路径改成`/api/kg/query`|**当前不能只改JSON**|`read_workload()`只认两条现有查询POST路径；需审查并扩展只读白名单，或在服务端保留兼容旧路由；不要谎报kind=write绕过|
|加请求字段、改字段名|可复用发起器|改workload.body；`{{client_id}}`等模板仍可用；更新校验器/独立真值映射|
|认证换成Bearer或其他请求头|请求头认证可配|用headers_env读环境变量；现有工具不执行登录流程、刷新token或浏览器Cookie会话|
|返回JSON字段或嵌套结构变化|能发请求并计时|响应必须完整JSON对象；改load_validation、require-run读取位置及领域验真，不把坏答案当成功|
|返回顶层数组、二进制、SSE或WebSocket|当前不满足分析契约|编写对应协议/解析适配；长连接推流不能按一次完整JSON查询统计|
|仅修改Neo4j内部索引，业务结果不变|通常可复用|重新做同输入验真与性能对照，保留版本和背景负载|
|图谱标签/主键/关系改变|发起器本身无直接依赖|更新生产Cypher、Agent适配器、查询provider、独立oracle/审计中的图读取；不能只改生产端|
|journal/checkpoint/事件/日志改格式|通用HTTP仍可测|更新对应完整验真器与恢复逻辑；日志request_id等字段变动会影响观测资格|
|换地图/数据但格式不变|发起器可复用|新manifest/run；使用有效对象ID；位置/数量/型号能力重新校验，旧性能结果不自动延续|
|改机器人数量、窗口长度或电池规则|发起器clients可配|检查服务是否仍写死100；Agent/容量/24h审计有固定模型断言，需相应新版本|
|改JSON排序/浮点表示等hash规则|时间仍可测|同步核对双方canonical/hash与独立验真；不要修改旧原件的指纹|

`--allow-writes`只允许你对确实声明为write的端点做写测试，不会把新路径变成已审查的只读接口。`/api/control`始终被拒绝，不能用压测切换数据或反复开始/停止仿真。

headers_env配置例子：

```json
{"version":"warehouse-load-v1","cases":[{
  "name":"self-auth","kind":"read","query_class":"light",
  "path":"/api/knowledge/query","headers_env":{"Authorization":"MY_QUERY_AUTH"},
  "body":{"query_id":"self","run_id":"{{run_id}}","robot_id":"{{client_id}}"}
}]}
```

将完整Bearer值安全放环境变量 `MY_QUERY_AUTH`。不支持在JSON中写明文headers；记录会脱敏凭证，仍应检查自己新增的日志没有另行打印。

### 8.2 我和李姚舜改接口时的最小流程

1. 保存一条旧接口完整请求与响应，写清字段、单位、缺失值、run/seq和版本。
2. 优先保留旧路由兼容，或明确更新POST只读白名单，负载JSON和probe.paths保持一致。
3. 先curl一条，检查HTTP状态与业务内容；再用1客户端、少量请求。
4. 改响应结构时更新结构校验器；改业务含义时更新独立源真值，不通过删断言适配。
5. 新输出目录跑10客户端，再上100客户端；轻重、冷热、读写分别保存。
6. 同时看错误/丢到达、最大值与源验真，不能只抄均值。旧失败和旧结果保留。

## 9. 如何读结果和排错

- `mean_ms`是平均；P99是99%样本不超过的值；`max_ms`是本次最慢；`over_500ms`是超过500ms的数量。
- `complete_http_json`包含HTTP完整接收和客户端JSON解析。`network_*`到接收完整body为止。日志、SHA、离线验真另计，但仍会消耗客户端资源。
- 缺少Queue-Ms/Service-Ms响应头时记null，不能用两个时间相减伪造数据库执行耗时。
- load_harness analyze退出2：本轮全请求500ms条件没过，也可能有错误/丢到达/校验失败；先看报告。run退出0不代表达标。
- 缺少服务端trace：客户端延迟仍可读，真实API并发资格为false。服务器进程没退出、trace尚未封口：继续保留阶段状态，不冒称完整通过。
- 404：端口/路径错；400：字段、run、seq或预算不合法；401/403：凭证/身份错误；503：查询未启用、服务失败或队列拒绝；逐条看error，不忽略失败样本。
- `IncompleteContext`：窗口超出当前预算，先确定研究需要的完整窗口并显式设预算，保留原失败，不能偷偷截断结果来让测试通过。
- `frozen source mismatch`：源码与该次冻结清单不同。旧实验用原冻结树；新实现用新身份和新实验，不篡改旧manifest强行复用结果。

百万资格、实际在途并发、写查共存、控制消费和500ms目标各自独立。工具不会根据最终总节点数自动宣称每条早期查询都发生在百万规模下。

## 10. 这些例子本次验证到了哪一步

2026-10-29在Pro6000的新目录 `warehouse-doc-review-20261029-v1` 实跑：

- 教学服务：4进程闭环8请求、100进程闭环200请求、周期40请求、固定到达率20请求，268次全部成功，服务端trace封口完整。
- 100进程教学组实际在途峰只有22。这个结果恰好说明“100个客户端”和“100个请求同时在途”需要分开报告。
- 对既有ready实时run只发3次真实只读请求：2次通过catalog结构校验，另1次经capture格式保存后通过独立journal/oracle完整验真。
- 第6.2节同款离线19任务命令完整跑通，19完成、0晚点；它未启图谱、未启动独立Agent进程。
- 检查了文档相对文件链接、Bash代码块语法和8个CLI帮助入口。
- 没有重新跑百万A/B、完整高峰Agent审计或24小时审计；这些高级例子的参数与已有launch、plan、原件SHA和已通过报告核对。不能把帮助命令通过称为大型回归重新通过。

摘要和原件SHA见[文档验证记录](evidence/doc-review-v1/summary.json)。当前复查只启动并关闭自己的教学服务，没有重启原实时服务、数据库或冻结源码。
