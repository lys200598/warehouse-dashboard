# 轮式仓库交付与协作指南（新同学从这里开始）

这份文档面向第一次接触项目的计算机专业学生。读完应能回答：仓库长什么样、数据怎样交给仿真、机器人怎样工作、知识图谱保存什么、怎样启动、怎样测试，以及修改代码后要检查哪里。

## 1. 先用一分钟了解这个项目

我们在电脑里模拟一个仓库：货物到入库口，机器人把它送到货架；收到出库任务后，机器人从货架取货送到出库口。机器人会耗电、充电或换电，也可能把任务和货物交给其他机器人。

**输入**是仓库地图、任务CSV、初始库存CSV和机器人配置；**输出**是运行结果、可回放画面，以及写入Neo4j的历史状态和事件。Neo4j是一种图数据库，用“节点＋关系”表示机器人、货物、任务及它们的联系。

```text
地图＋任务＋初始库存＋机器人配置
                 ↓
中央仿真/调度进程：分任务、规划路线、避免碰撞、确认货物归属
                 ↓
100台仿真机器人工作 → 各机器人进程上报观测、查询自己的上下文
                 ↓
共享知识服务 → Neo4j保存历史 → 按需查询 → 调度参考或历史授权
                 ↓
原实时网页显示当前运行；已保存画面可另做回放
```

本轮主要交付仓库、运输与补能模型、机器人读写接口、压测及验真工具。李姚舜继续负责新数据集和双方知识图谱设计的整合。LLM不属于这次需要实现的内容。

### 按你要做的事找入口

|你想做什么|先看哪里|
|---|---|
|看仓库尺寸、机器人和充电位|本页第2、4节|
|生成符合仓库的数据|本页第3节，再看[数据对接](LYS_DATASET_HANDOFF.md)|
|安装并跑起来|本页第6节|
|第一次使用压测工具|[压测操作手册：从零到真实实验](LOAD_TEST_COOKBOOK.md)|
|改了API，判断压测还能不能用|[兼容清单](LOAD_TEST_COOKBOOK.md#8-改代码以后哪些还能用)|
|查看每个脚本的命令、依赖和结果|[脚本菜单](LOAD_TEST_COOKBOOK.md#1-先选择工具不要一次全跑)|
|查某台机器人或某货架的信息|[标准查询说明](WAREHOUSE_QUERY_GUIDE.md)、[货架库存](RACK_RESOURCE_GRAPH_GUIDE.md)|
|核查已经测过的结果|本页第8节，详细原始记录在链接的回归报告里|

## 2. 仓库长什么样，为什么推荐这一版

推荐地图是 [`data/layouts/expansion-candidates/medium/layout.json`](../data/layouts/expansion-candidates/medium/layout.json)。medium只是这版地图的目录名，表示“中型候选”。

|内容|早期100台地图|中间balanced版本|本轮推荐medium|
|---|---:|---:|---:|
|长×宽|90×76米|100×90米|120×90米|
|面积|6,840平方米|9,000平方米|10,800平方米|
|货架数|36|40|48|
|货位数|1,728|1,920|2,304|

- 每架3层、2面、每面8列，共48个货位。机器人能否取某层，受型号和手臂能力限制。
- 普通巷道宽5米，主通道宽8米。机器人机体、货架和停靠机器人参与碰撞检查。
- 西侧8个入库工位，东侧8个出库工位。
- 南北两侧共100个充电兼待命泊位，即可以停车的位置。P和C编号是同一物理位置的两种引用，不是200个位置。
- 空泊位允许通行；有机器人停靠时，需要避开机器人。

地图保留原36架的货架/货位编号，增加12架；旧编号对应的坐标发生了变化。数据文件需要声明自己配对的地图版本，不能仅靠“编号还存在”判断兼容。

我们也测试过150×100米、60架版本：同一任务期限下出现明显晚点，所以本轮推荐120×90米。扩大地图会增加运输距离，需要和速度、补能、到货强度一起评估。

同一小时1,728任务、覆盖48架的测试中，1.5米/秒换电组全部完成、0晚点，平均任务流转408.6秒。这里“流转”指从任务释放到完成；“0晚点”指完成时没有超过任务原截止时间。这个结果适用于该测试输入，其他负载需要另测。详见[容量审计](WHEELED_CAPACITY_AUDIT_20261029.md)。

## 3. 李姚舜的数据怎样交给仿真

### 一个数据目录包含什么

```text
我的数据集/
├── tasks.csv               # 每行一个入库或出库任务
├── initial_inventory.csv   # 开始时有哪些货、各在哪个货位
└── manifest.json           # 地图版本、文件校验值等身份信息
```

`manifest`可以理解为数据的说明和校验清单；SHA-256是文件内容的“指纹”，用于检查拿到的是不是同一份文件。任务文件继续采用七字段：

```csv
task_id,task_type,goods_id,release_time,due_time,source_id,target_id
T-IN-1,INBOUND,G-1,2026-10-29T08:00:00+08:00,2026-10-29T08:30:00+08:00,INBOUND,R01-L1-C1-F1
T-OUT-1,OUTBOUND,G-1,2026-10-29T10:00:00+08:00,2026-10-29T10:30:00+08:00,R01-L1-C1-F1,OUTBOUND
```

两行的意思是：8点货物G-1到仓，要求8点半前放入指定货位；10点把同一件货从该货位送到出库区，要求10点半前完成。`+08:00`表示北京时间的时区。

### 自由口如何使用

`INBOUND`、`OUTBOUND`代表入库/出库区域，数据生成者不用先选IN-1或OUT-3。仿真根据可用资源选择具体工位，并记录此次选择。已写明具体工位的旧数据继续支持。

这改变了入口选择方式，仓容与工位限制保留：8个入库口、8个出库口，每入库口6件暂存额度，货位和货物仍需要预留并保持唯一归属。启用自由口的配置为 `port_selection_policy=balanced`，推荐轮式配置已设置。细节见[自由口说明](FREE_BOUNDARIES_GUIDE_20261029.md)。

### 新数据对接检查表

1. 使用medium目录下的 `layout.json` 和 `task-locations.csv`，更新manifest中的地图版本与SHA。
2. 明确2,304格里哪些有初始货物、哪些参与任务。热点按新坐标生成。
3. 使用七字段CSV和带时区时间，保持货物ID唯一、先入后出依赖及仓容约束。
4. 若按运输距离设置截止时间，在生成时写明参考速度和时间裕量。
5. 机器人电量、型号、换电和故障放运行配置，不要求任务CSV为每台车预先安排充电。

已提交的[七类波动参考数据](../data/examples/seven-patterns-20261028/README.md)共50,112任务、约5.62MB，包含不同日期的典型到货波动。它配对旧36架地图和具体工位，适合参考生成方法或做旧对照；迁移到新图要重新校验坐标、容量和期限。固定数据集时长没有七天上限；默认七天只是样例。见[多日生成说明](MULTIDAY_DATASET.md)。

## 4. 机器人怎样移动和补能

配置文件是 [`config/wheeled-medium-100.json`](../config/wheeled-medium-100.json)：

- 100台混合型号机器人，轮式移动速度1.5米/秒；型号能力约束继续保留。
- 12个换电站，每站4块备用电池、4条离线充电通道，共48备用电池、48离线通道。
- 全系统电池为100块装车电池＋48块备用电池，每块有独立身份，交换前后数量守恒。
- 换电服务时间180秒，进站和离站也要走真实路径并避碰。
- 没有可换的满电电池或没有安全进站路径时，回退到普通充电。

1.5米/秒参考了[欣奕华SIBOT12公开满载速度](https://www.sineva.com/product/detail/?cid=123&id=4851)，这里使用的是轮式运输能力假设；移动能耗尚未按某台实机的电池容量与功率标定。能量模型见[换电指南](WHEELED_ENERGY_SWAP_GUIDE.md)。

24小时均匀日测试中，25次换电完成、178次回退普通充电。主要现象是部分换电位置也被用作待命停车位，即使还有满电备用电池，机器人也可能进不去。普通充电兜底已起作用，换电服务位与停车位分离仍是后续布局优化项。备电数量的分析见[电池供给敏感性](BATTERY_SUPPLY_SENSITIVITY_20261029.md)，实际电池柜面积与障碍几何仍需单独补齐。

## 5. 图谱和每个机器人进程如何分工

一个“进程”是操作系统独立运行的一份程序。本轮有100个机器人I/O进程：各自提交观测、等待写入成功确认，再查询自己的历史和任务。I/O在这里指读写通信；路线与运动仍由中央进程协调。

|谁|负责什么|
|---|---|
|中央仿真/调度|任务分配、交通预约、运动、货物唯一归属、实际交接|
|机器人进程|上报自己的状态变化，查询已提交的自身与任务上下文|
|共享知识服务|身份校验、合并写入、控制排队、调用Neo4j|
|Neo4j|保存机器人、任务、货物、货架及它们随时间变化的事实|

每台机器人有专属身份凭证，只有共享服务持有数据库密码。机器人能写自己的观测，不能自行宣布“别人的货已转给我”。新观察节点为 `WHAgentObservation`，本地事件为 `WHAgentRuntimeEvent`。

两个名词后面查结果时会用到：

- **run_id**：某次独立运行的编号。例如同一数据跑两次，使用两个run_id，历史不会混起来。
- **seq**：这次运行第几批已经保存的知识。查seq=100，表示读取第100批所能看到的事实；不保证等于第100秒。`mirrored_seq`表示Neo4j实际已写到第几批。

机器人新接口返回的上下文目前作为可用建议。原中央required模式会在低电接力规划、交接前读取历史事实并授权实际动作。统一目录还有其他知识包可按需调用，不能仅凭“接口存在”认为每台机器人已经自动使用全套查询。详见[机器人接口与证据](ROBOT_AGENT_IO_GUIDE.md)。

### 与李姚舜的图谱如何合并

货架资源模块采用 `Warehouse → Rack → Location`（仓库→货架→货位）和 `Dataset → WarehouseTask → Goods`（数据集→任务→货物），关联原有实际状态。查询 `rack_inventory` 可得到指定货架在指定seq的货物占用。

机器人读写适配器当前使用 `WHRun / WHRobot / WHAgentState` 标签。若合并分支改成 `Run / Robot / AgentState`，需要修改 `backend/agent_runtime/service.py` 的 `Neo4jBackend.write/query` 和上下文读取器中的标签、主键与字段映射，并重跑来源验真。仅改名称无法自动兼容；关系映射见[货架资源说明](RACK_RESOURCE_GRAPH_GUIDE.md)。

## 6. 第一次运行：先跑小样例，再接数据库

### 6.1 准备环境

以下命令在 **Linux服务器或WSL的Bash** 中执行，工作目录是仓库根目录，即能看到 `backend/`、`ops/`、`config/` 的目录。Windows PowerShell用户先SSH到服务器再执行，不把Bash的 `export` 和换行符直接粘到PowerShell。

若已经有本项目环境，不必重新安装。新机器可这样建立自己的环境（Python 3.10以上）：

```bash
# 仅在还没有项目副本时执行这两行；已有副本直接进入其根目录。
git clone --branch wz/warehouse-design https://github.com/lys200598/warehouse-dashboard.git
cd warehouse-dashboard
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements-platform.txt fastapi 'uvicorn[standard]'
export LADE_ENV_DIR="$PWD/.venv"
bash run_in_lade.sh python -m backend.warehouse status
```

`run_in_lade.sh`负责选择Python环境；`LADE_ENV_DIR`指定刚建的环境。它不安装或启动Neo4j。当前中央仿真及HTTP压测不要求GPU，也不要求启动旧Ray控制台。

### 6.2 无数据库的小样例

```bash
CASE="student-smoke-$(date +%Y%m%d-%H%M%S)-$$"
SCENE=data/layouts/expansion-candidates/medium/layout.json
DATA=data/examples/wheeled-medium-sample19
bash run_in_lade.sh python -m backend.warehouse validate --scene "$SCENE" --dataset "$DATA"
bash run_in_lade.sh python -m backend.warehouse run \
  --scene "$SCENE" --dataset "$DATA" --config-json config/wheeled-medium-100.json \
  --output "data/local-runs/$CASE" --run-id "$CASE" \
  --mode rules --knowledge-backend journal --mobility wheeled --frames
```

这是19任务的小样例。rules表示规则调度，journal表示只保存本地运行记录；本命令不启动100个独立I/O进程，也不验证图谱。完成后查看输出目录的 `results.json`：`status`、完成数、迟到数。`checkpoint.json`是恢复执行的状态，`journal.sqlite`是逐批记录，`frames.sqlite`是保存的画面。`--frames`只影响回放记录，大规模长时测试可省略以节省空间。

输出目录用新名字，避免覆盖已有实验。展示回放：

```bash
bash run_in_lade.sh python -m backend.warehouse serve --output "data/local-runs/$CASE" --port 14582
```

若在自己电脑运行，浏览器打开 `http://127.0.0.1:14582/`。若在Pro6000运行，在本机另一个终端建立 `ssh -N -p 28941 -L 14582:127.0.0.1:14582 wz@58.48.54.157`，保持它运行再打开同一地址。这里的端口仅供个人新测试使用，占用时换一个空闲端口；不要关闭别人的服务。

### 6.3 接Neo4j与100机器人进程

先准备有访问权限的Neo4j测试库。下面交互输入密码，不把密码写进命令或提交到Git；将地址和库名换成分配给自己的实际值：

```bash
export NEO4J_URI=bolt://127.0.0.1:7687
export NEO4J_USER=neo4j
export NEO4J_DATABASE=neo4j
read -rsp 'Neo4j password: ' NEO4J_PASSWORD; echo
export NEO4J_PASSWORD
CASE="student-agents-$(date +%Y%m%d-%H%M%S)-$$"
bash run_in_lade.sh python -m backend.warehouse run \
  --scene data/layouts/expansion-candidates/medium/layout.json \
  --dataset data/examples/wheeled-medium-sample19 \
  --config-json config/wheeled-medium-100.json \
  --output "data/local-runs/$CASE" --run-id "$CASE" \
  --mobility wheeled --robot-agents --resource-graph \
  --mode required --knowledge-backend neo4j --commit-seconds 5 \
  --agent-query-seconds 60 --agent-observation-seconds 30 --frames
```

`--robot-agents`启动独立机器人读写；`--resource-graph`写入货架资源投影，需从新run第一批启用；required要求历史知识授权链可用。每5模拟秒提交权威状态；机器人周期查询60秒、观测30秒，任务/阶段变化等仍会提前触发。

若要测一小时1,728高峰任务，把dataset换为 `data/examples/wheeled-medium-peak`，同时使用新的CASE，并增加 `--max-wall-seconds 7200` 给实际执行预留两小时上限。模拟一小时不等于电脑运行一小时：快慢取决于查询、写入和机器负载。验真命令见[操作手册第5节](LOAD_TEST_COOKBOOK.md#5-机器人读写审计audit_robot_agent_iopy)。

### 6.4 实时网页

沿用6.3的数据库环境，在新目录启动：

```bash
CASE="student-live-$(date +%Y%m%d-%H%M%S)-$$"
bash run_in_lade.sh python -m backend.warehouse live \
  --registry config/live-wheeled-datasets.json --dataset-id medium19 \
  --output-root "data/local-runs/$CASE" \
  --mode required --knowledge-backend neo4j --resource-graph \
  --mobility wheeled --robot-agents --commit-seconds 5 \
  --agent-query-seconds 60 --agent-observation-seconds 30 \
  --query-acceleration summary --port 14583
```

该命令使用原 `dist/index.html` 页面。registry是“网页可选择哪些数据集”的清单；medium19是19任务样例，medium1728是高峰样例。网页开始时为ready，使用开始/暂停/单步控制；切换数据会换run。summary表示启用按提交点维护的任务统计，以降低重复查询成本。

在本机建立 `ssh -N -p 28941 -L 14583:127.0.0.1:14583 wz@58.48.54.157` 后打开 `http://127.0.0.1:14583/`。此前交付的14192等地址属于已有部署，端口本身不标识代码版本；不要为照抄本例重启它们。

## 7. 压测到底依赖什么，改接口后怎么办

压测包含两类事：**测快不快**和**检查答案对不对**。通用HTTP发起器主要依赖接口地址和JSON请求；完整验真另外依赖当前响应结构、原运行记录与图谱模型。

|工具|直接依赖|你改什么会影响它|
|---|---|---|
|`ops/load_harness.py`|HTTP GET/POST、完整JSON对象、负载JSON|URL/参数/认证改变需适配负载；新POST路径还有安全白名单限制|
|`ops/load_probe.py`|ASGI服务、被观察的路径、请求ID头|换路径需改paths；改成非ASGI要重写观测适配|
|`ops/load_validation.py`|本项目catalog响应字段及hash规则|改返回字段、嵌套结构或编码规则需改校验器|
|`ops/run_query_ab.py`|本项目查询服务、封存百万run、journal、oracle、Neo4j、Nginx、冻结源码清单|改图谱模型/查询构造/服务健康路径后需审查编排与真值读取|
|`ops/audit_robot_agent_io.py`|Agent日志、源journal、已结束run；默认还读Neo4j|改Agent协议、日志、图谱标签或恢复记录要改审计|
|`ops/audit_wheeled_capacity.py`|原容量矩阵plan、checkpoint、journal、画面、对应源码和输入|改这些格式或机器人数量/能量边界要改审计|
|`ops/audit_wheeled_day_window.py`|上述记录＋24小时窗口＋当前medium电池配置|改窗口长度、换电模型或固定配置需改专用断言|

**不需要为所有改动重写全部脚本。** 例如只更换数据集、保持输入与响应契约不变，通用发起器可复用；由WHRobot改成Robot，HTTP外形保持时仍可测请求时间，但图谱读取和来源验真必须一起适配。

本页只给选择方法。每个工具的前置条件、真实路径命令、输出解释、退出码、故障处理与API兼容矩阵见[压测操作手册](LOAD_TEST_COOKBOOK.md)；低层字段定义见[原压测参考指南](LOAD_TEST_GUIDE.md)。

**仓库设计还需优化以配合查询优化**：主要是协调状态提交、事件与查询触发频率，避免100机器人重复读取相同全局信息。改变频率会改变负载，要用新实验标识记录，不能把减少请求后的结果算作原突发负载通过。

## 8. 已测到了哪里，哪些还要继续验收

以下是2026-10-29已经保存的回归结果，不是对未来所有输入的保证。

|实验|结果|适用范围|
|---|---|---|
|48架高峰＋100独立机器人I/O进程|1,728任务全完成、0晚点；16,193次查询均低于500ms|该run为244,666节点，纯查询在途峰67；不是同run百万压力验收|
|静态同run百万查询|1,652,135节点；重查询明确热态100峰时最大410.21ms|首次重突发仍47条超500ms，静态读不等于持续写查|
|24小时均匀日窗口|6,912释放、6,902完成、0晚点，10在途且未到期|只跑到一天边界，未来43,200任务未执行；主要检验长期补能|

高峰写请求28,023条，其中67条超过500ms。查询P99表示99%样本不超过的耗时，最大值是本次最慢样本；二者都不能保证未来绝对最坏耗时。

详细证据：[机器人高峰回归](ROBOT_AGENT_PEAK_REGRESSION_20261029.md)、[静态百万A/B](QUERY_AB_REGRESSION_20261029.md)、[24小时窗口](WHEELED_DAY1_WINDOW_20261029.md)。待继续：同run百万持续写查联合验收、冷重查询和慢写长尾、换电服务位与停车位分离。真实设备能耗和长期跨机故障恢复也需单列验证。

## 9. 和原仓库相比，修改集中在哪里

|目录/文件|你要找的内容|
|---|---|
|`backend/warehouse/__main__.py`、`composition.py`|统一命令入口，组装可选的轮式/资源/Agent功能|
|`backend/wheeled/`|轮式运动、充电与有限电池换电|
|`backend/agent_runtime/`|机器人独立进程和共享读写接口|
|`backend/resource_graph/`、`resource_graph_mirror.py`|静态仓库资源与实际货权状态关联|
|`backend/query_catalog/`、`query_views/`、`query_v2/`|查询包、统计加速、历史上下文读取|
|`ops/warehouse_live.py`、`warehouse_live_runtime.py`|实时HTTP网页服务及唯一仿真运行者|
|`ops/load_*.py`、`run_query_ab.py`、`audit_*.py`|压测、测量和离线验真|
|`config/`、`data/layouts/`、`data/examples/`|运行配置、地图和可直接使用的样例输入|

旧入口和默认legacy运动方式保留。新功能通过明确参数启用，避免改变旧实验含义。恢复某次运行时必须使用匹配的源码、配置和数据；源码“冻结清单”就是记录该次实际文件SHA的清单。Git提交版本便于找回源码，原日志与结果继续保留。

## 10. 可以直接转发给李姚舜

> 我更新了 `wz/warehouse-design` 的交付说明。你先看 `docs/WAREHOUSE_WHEELED_HANDOFF_20261029.md` 第2—5节：推荐120×90米、48架2304格，任务七字段用逻辑INBOUND/OUTBOUND，仿真选具体工位，仓容约束保留。新数据用medium地图和位置表更新manifest；旧七类波动样例在 `data/examples/seven-patterns-20261028/`。图谱模型要和机器人接口做标签/主键映射。压测从 `docs/LOAD_TEST_COOKBOOK.md` 开始，里面有各脚本依赖、可复制命令和改API后的兼容清单，不需要先弄懂整套工程才能开始。
