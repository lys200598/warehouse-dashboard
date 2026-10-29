# 给lys：任务数据怎样接到这座仓库

> **先看这里（2026-10-30）**：本文第2节起描述的是旧版90×76米、36架1728货位地图，仅作历史对照。新数据请使用120×90米medium地图 `data/layouts/expansion-candidates/medium/layout.json`（48架2304货位），流程见[轮式扩仓对接总览](WAREHOUSE_WHEELED_HANDOFF_20261029.md)，接口边界见[对接约定草案](INTEGRATION_CONTRACT_DRAFT.md)。

我们完成了一个可重复运行的100机器人仓库场景：固定地图、货位与收发位置、任务分配、避碰、取放、充电、低电接力和时序记录。你生成的任务文件可以直接作为实验输入，让同一批数据分别比较调度、共享知识与查询方案。

本文先讲数据侧要知道的边界，再给出文件格式和验证步骤。仓库与算法背景见[仓库设计指南](WAREHOUSE_DESIGN_GUIDE.md)，完整运行与知识模块见[平台指南](WAREHOUSE_PLATFORM_GUIDE.md)。

## 1. 双方怎样分工

|数据生成侧|仓库/仿真侧|
|---|---|
|安排到货、出货需求何时出现|推进真实接收、取放、搬运与出仓|
|维护货物身份、货位引用与参考库存|检查实际占用、货位预约和总容量|
|声明数量、时间/空间分布和期限公式|选择机器人，规划路线和避碰|
|给出初始库存、种子及数据摘要|处理电量、回充、故障和接力|
|用相同输入复现实验|保存原始状态、事件、知识图谱和结果|

任务CSV描述仓库边界需求，机器人分配、SOC阈值、接力对象和实际完成时刻放在独立配置或运行输出中。这样同一份任务可以公平比较多种算法。

## 2. 先固定地图和编号

权威地图：`data/layouts/warehouse100-wide-v2.json`。

|约束|数值|
|---|---|
|仓库外形|90m×76m，长度单位为米|
|货架|36架，每架48格|
|总货位|1728，每格存一件货|
|收发位置|西侧`IN-1…IN-8`，东侧`OUT-1…OUT-8`|
|机器人|100台，每任务搬一件货|
|物理充电兼待命位|100位，P/C编号是同一物理位的别名|
|仓内总货物上限|1728件，包括已接收入库暂存、机器人携带和在架货物|
|每个IN点暂存上限|6件，8点合计48件|
|超载额度|0|

地图SHA256为：

```text
ce99154d80fc643610dcb6d27384ca3724f95dc9121b8d2934212d56ba9eda24
```

直接使用以下位置表，避免从页面上的像素位置猜坐标：

- `docs/dataset-contract/layout/task-locations.csv`：1728格＋16个收发点，共1744个合法任务端点。
- `docs/dataset-contract/layout/physical-bays.csv`：100个泊位及别名，用于理解充电资源。
- `docs/dataset-contract/contract/layout-contract.json`：机器可读的布局与容量约定。

充电位和其P别名用于运行配置，任务货物的IN/OUT端点按收发规则选择。

## 3. 一份输入包含三份必需文件

```text
my-dataset/
├── tasks.csv
├── initial_inventory.csv
└── manifest.json
```

另外可以附`validation.json`、小时/区域分布图表、场景说明和生成代码。这些帮助判断数据是否具有代表性。

### 3.1 tasks.csv：固定七字段

```csv
task_id,task_type,goods_id,release_time,due_time,source_id,target_id
```

|字段|填写规则|
|---|---|
|`task_id`|整个数据集含全部日期唯一，使用不超过128字符的短而稳定ASCII编号，兼容知识查询接口|
|`task_type`|`INBOUND`或`OUTBOUND`|
|`goods_id`|同一件货从接收、存储、搬运、接力到出仓始终同一编号；不超过128字符|
|`release_time`|该任务何时发布；完整ISO8601时间，含显式时区，整数秒|
|`due_time`|期望完成期限，严格晚于release，可跨午夜|
|`source_id`|IN任务填IN点；OUT任务填货位|
|`target_id`|IN任务填货位；OUT任务填OUT点|

例如，初始在架货物`G-0001`先出库，随后新货`G-0002`入同一格：

```csv
task_id,task_type,goods_id,release_time,due_time,source_id,target_id
T-000001,OUTBOUND,G-0001,2026-10-28T08:00:00+08:00,2026-10-28T08:30:00+08:00,R01-L1-C1-F1,OUT-1
T-000002,INBOUND,G-0002,2026-10-28T08:30:00+08:00,2026-10-28T09:00:00+08:00,IN-1,R01-L1-C1-F1
```

配套初始库存为：

```csv
goods_id,location_id
G-0001,R01-L1-C1-F1
```

此例用于解释字段与依赖。实际数据应同时提供manifest，并经过参考校验及真实运行检验。

### 3.2 initial_inventory.csv：运行开始的一次库存

表头固定为`goods_id,location_id`。每件货只出现在一个货位，每格最多一件；初始库存的货物可以直接安排OUT任务。初始库存可以由你选择，现有默认场景为864件。

当前参考合同允许同一货物最多一次IN和其后一次OUT；已经出仓后再次入库的货物使用新`goods_id`。全数据集保持一个货物身份空间。

### 3.3 manifest.json：输入身份和生成依据

复制`docs/dataset-contract/sample-input/manifest.json`作为结构例子，并按新文件重新填写：

- 版本、场景名称、种子、`start`、`duration_seconds`；
- 地图SHA256和容量1728；
- 任务总数、IN/OUT数量、初始库存数量；
- `tasks.csv`与`initial_inventory.csv`的实际SHA256；
- 到货/出货分布、热点区域与期限计算依据。

`start`、任务时间戳采用同一显式偏移，例如`+08:00`。发布时间落在`[start, start+duration_seconds)`内。固定数据集已解除七天上限；实际时间戳须合法，任务条数和运行资源单独预算。

## 4. 参考库存怎样维护

生成器维护“货位是否空、货物在哪里、哪些任务在参考运输中”这一套连续状态。

```text
初始在架货物 → 安排OUT → 参考出仓完成 → 格子可安排新IN
空格 → 安排新IN并预留 → 参考上架完成 → 该件货可安排OUT
```

建议给出库腾空和下次入库留出明确周转时间，避免在同一格用大量相邻任务制造不必要的容量冲突。仓内数量按`初始库存 + 已接收 - 已出仓`计算；机器人从货位取走OUT货物后，这件货仍在仓内，送达OUT并完成放置才释放总容量。

校验器按`release_second,task_id`重放参考依赖。它能发现不存在的货物、重复编号、占用货位重复IN、越界时间和文件身份错误。真实运行另行检查接收、取放、交通、电量和完成时刻；参考计划通过后，还要通过实际仿真确认需求强度与期限。

## 5. 多天数据怎样连续

每天可选择一种典型变化，整份数据保持同一份库存和全局编号。跨午夜时：

- 在架货物继续留在原位；
- 尚未完成的任务和在途货物继续存在；
- 仿真中的电量、充电、协作状态继续演化；
- 第二天只切换需求强度和热点，保留上一天的状态。

现有七天示例是：均匀、固定峰值、局部热点、批次脉冲、热点迁移、进出潮汐、混合高负载与恢复。它可作为生成思路和格式参考，具体算法见`backend/wide/multiday.py`，逐日参数见[多日数据说明](MULTIDAY_DATASET.md)。

期限建议由声明的参考距离、基准速度、取放时间与额外裕量计算；先固定规则，再运行比较。实际完成时刻留在结果中，避免依据某个策略的结果反向填写输入期限。

## 6. lys拿到分支后先做什么

1. 阅读本文件，固定地图SHA和位置表。
2. 用现有19条样例先运行校验，确认环境。
3. 交付一份小数据，完成真实入库、出库和容量闭环。
4. 用同样文件格式扩大到一天及多天，附分布与生成依据。
5. 双方固定同一输入SHA，分别对算法/知识方案做可重复对照。

从仓库根目录执行：

```bash
# 统一入口；服务器可在前面加 bash run_in_lade.sh
python -m backend.warehouse validate --dataset docs/dataset-contract/sample-input
python -m backend.warehouse validate --dataset /absolute/path/to/my-dataset
```

独立标准库校验器也保留，便于在不启动数据库的机器上先检查：

```bash
python docs/dataset-contract/validate_package.py \
  --dataset /absolute/path/to/my-dataset
```

运行规则对照小样例：

```bash
python -m backend.warehouse run \
  --dataset /absolute/path/to/my-dataset \
  --output data/local-runs/lys-first-input \
  --run-id lys-first-input \
  --mode rules --knowledge-backend journal --max-wall-seconds 1800
```

每次新实验换一个未使用的output和run编号。地图与必需参数默认值、完整选项以子命令`--help`为准。历史授权、图谱查询和检索评估的运行步骤见[平台指南](WAREHOUSE_PLATFORM_GUIDE.md)。

## 7. 交接给仓库侧的清单

- 三份必需输入与完整SHA；
- 数据生成代码版本、种子、时间范围、地图SHA；
- 每天/每小时IN与OUT量，空间热点和收发口分布；
- 期限、周转、参考容量规则及明确假设；
- 校验结果和已观察到的限制；
- 若借用真实到货曲线，注明来源、日期、缩放和业务含义。

仓库侧返回同输入的实际完成数、迟到、积压、库存/交通/能量检查和知识记录。双方据此调整需求强度或实验参数，同时保留原输入和原结果。

货架上架/下架由机器人直接执行，模型使用明确的取放耗时；当前任务合同覆盖仓库进出和存储，数据侧无需加入客户订单、SKU业务或额外有限叉车设备。任务强度决定实际忙碌数量，实验同时观察工作、充电和待命轮换。

### 建议的阅读顺序

`本文件 → docs/dataset-contract/README.md → task-locations.csv → 小样例 → WAREHOUSE_PLATFORM_GUIDE.md → WAREHOUSE_DESIGN_GUIDE.md`。

分支集成、实际验证提交与本轮数据回归范围由[平台指南末尾交付记录](WAREHOUSE_PLATFORM_GUIDE.md#本轮交付记录)统一列出。请使用交付者明确告知的提交，以便双方对齐同一版合同与执行器。

## 8. 给双方第一次对接的简短说明

交付者完成本轮Git提交后，可以把分支链接、明确commit和下面几项一起发给lys：

> 我这边把仓库布局、100机器人调度和仿真、进出库与低电接力、知识图谱记录和查询整理在`wz/warehouse-design`分支。你可以先看`docs/LYS_DATASET_HANDOFF.md`，再看`docs/dataset-contract/README.md`和位置表，运行入口统一为`python -m backend.warehouse`。地图是90×76米、1728货位、100个充电兼待命位，任务还是七字段CSV。你主要安排每天的进出数量、发布时间、空间分布和期限，连续维护库存和货物编号；电量、机器人分配和接力放在我的仿真配置里。先用一份小样例跑过`validate`，再交一天或多天数据，附初始库存、manifest、种子、地图SHA和期限规则，我们就可以固定同一输入一起回归。最新提交和实际通过范围以我附上的commit与平台指南交付记录为准。


## 2026-10-28会议后增量：扩仓与三型号

本文件原90×76m/1728位合同继续对应正式V2基线。新候选独立放在 `data/layouts/expansion-candidates/`，每个目录自带 `layout.json / task-locations.csv / manifest.json / geometry-audit.json`。balanced为100×90m/40架1920位，medium为120×90m/48架2304位，large为150×100m/60架2880位；最终采用哪个版本以[会议增量交付](MEETING_DELIVERY_20261028.md)的实际结果为准。

- 七字段任务CSV与两字段初始库存保持；Robot型号/电量/派单/接力由运行配置负责。
- 请使用所选目录的新位置表与scene SHA，manifest.capacity取该地图容量；原独立 `docs/dataset-contract/validate_package.py`仍验证固定V2交接包，新候选使用 `python -m backend.warehouse validate --scene <候选layout.json> --dataset <数据目录>`。
- 老R01—R36和原货位编号保留，坐标已变化；新增货架ID只在相应新版本存在。
- 原图谱资源设计已通过 `backend/resource_graph/`连接真实时序库存。Warehouse/Rack/Location规范ID按内容身份生成，Dataset包含实际输入SHA；直接沿用“版本+日期”ID容易让不同seed互相覆盖。
- 数据集可用原三型40/40/20或其他显式比例作实验。高层任务多时需要评估X3负载；接力接收者也必须能完成剩余取放。
- 主对照保留同CSV及原时限；额外全货架覆盖实验保持任务ID、货物ID、类型与发布时间/截止时间，仅映射货位，并记录完整映射，独立列为覆盖实验。

建议先看 `docs/MEETING_DELIVERY_20261028.md` 与 `docs/RACK_RESOURCE_GRAPH_GUIDE.md`，再选择地图后生成数据。当前主线继续以共同知识与时序查询为研究目标；新增地图和型号是能够产生可信货物流转的实验环境。

### 2026-10-29入库背压增量

[晚点优化指南](LATENESS_OPTIMIZATION_20261029.md)记录了同一balanced高峰输入的逐任务归因和全部候选。七字段CSV、初始库存、地图SHA与每口6件约束保持；调度新增显式opt-in疏口候选，1728任务晚点23→5，出库0晚点但平均流转有所增加。默认FIFO/off和现有实时服务保持。新日型请继续保留各入口发布时间、数量与期限分布，双方固定同一输入做IN/OUT联合回归；最终选择的完整运行配置见`config/warehouse-balanced-port-relief.json`及指南的收尾记录。

### 2026-10-29逻辑边界与数据对接

[自由边界指南](FREE_BOUNDARIES_GUIDE_20261029.md)增加了更简洁的端口约定：七字段中，入库`source_id=INBOUND`、出库`target_id=OUTBOUND`，货架端点保持；manifest声明`warehouse-logical-boundaries-v1`。用统一`convert-boundaries --dataset ... --scene ... --output ... --role both`创建新数据，再执行`validate`。仿真按实际状态选择工位，入库准入/出库发布后固定，计划与实际位置分别记录到知识图谱。

现100×90m、100台、0.8m/s和8IN/8OUT/每IN六件的高峰1728任务已全部按时完成，旧固定门基线保留。请继续设计货量、货物身份、库存连续性、发布时间及期限；无需预先给每件货指定具体入口。未来扩展站点需同步地图、校验和图谱登记，逻辑CSV可保持。不同日型、多天与设备验收范围见新指南；使用双方约定的commit与完整配置对接。
