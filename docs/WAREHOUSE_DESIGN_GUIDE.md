# 仓库布局、机器人调度与仿真：从业务流程到代码

这份指南介绍本次完成的 **V5 仓库实验环境**：一座90×76米、1728个货位的仓库，100台机器人承担入库上架和下架出库，配套任务分配、避碰、充电、低电互助、日志、恢复和网页回放。

阅读对象是会写程序、但还不熟悉仓储和多机器人仿真的同学。我们希望你读完后能够回答三个问题：仓库怎样工作、代码怎样组织、怎样接入自己生成的数据并运行。

## 阅读路线

- **先理解场景**：读第1—4节，认识布局、货物流向、容量和机器人的一天。
- **负责生成数据**：重点读第5节，按照位置编号、时间和库存约束准备输入。
- **负责算法或后端**：读第6—9节，再按模块对应表阅读源码。
- **想直接跑起来**：跳到第10节，先运行一小时示例，再阅读第11节的验收结果。
- **准备合并与后续协作**：读第12节，区分仓库发布、历史结果与知识查询的工作范围。

本文中的文件链接相对于仓库目录；命令均从仓库根目录执行。V5的启动入口是 [`ops/warehouse_v5_release.py`](../ops/warehouse_v5_release.py)，实验参数入口是 [`backend/scheduling_v5/runner.py`](../backend/scheduling_v5/runner.py)。

## 1. 这套环境完成了什么

可以把系统分成四层：

```text
任务数据：什么时候出现哪件货、从哪里搬到哪里、希望什么时候完成
    │
    ▼
中央仿真进程：维护仓库、库存、任务、电量与统一时钟
    │           │
    │           ├── 调度：为当前任务选择合适机器人
    │           ├── 交通：预约路径、等待位置与取放时间
    │           └── 协作：低电接管、交接与异常恢复
    ▼
100个机器人实体：移动、取货、放货、回充、待命
    │
    ├── checkpoint：能够恢复运行的完整状态
    ├── journal：按提交保存的状态和业务事件
    └── frames：网页回放使用的实际运行帧
```

**仿真**是按规则推进的可重复实验。这里每一步推进1秒模拟时间，代码依据已规划轨迹更新机器人位置、电量和动作；现实中的货架、货物和机器人被表达为程序状态。

**调度**负责“谁做哪项任务”；**路径规划**负责“怎样走、何时走和在哪里等”。这两个问题分开处理，容量和电量规则贯穿其中。

当前V5由中央进程管理100个机器人状态，适合固定输入、确定性重放和算法对照。原项目的FastAPI、WebSocket和local/Ray运行入口继续保留；V5使用独立的实验入口和只读回放页面，便于与原项目逐步合并。

仓库的研究角色是为共享知识与时序记忆提供可信场景。例如一次低电交接会产生任务归属变化、货物持有者变化和协作原因，后续知识系统可以据此查询过程、还原历史或提供协作背景。

## 2. 仓库长什么样

### 2.1 布局总览

权威地图为 [`data/layouts/warehouse100-wide-v2.json`](../data/layouts/warehouse100-wide-v2.json)。长度单位统一为米。

|项目|当前设计|
|---|---|
|外形|90米×76米，面积6840平方米|
|货架|4列×9排，共36架|
|每架货位|双面×8列×3层，共48格|
|总货位|36×48＝1728，每格存一件货物|
|货架尺寸|长9.6米、深1.2米、高1.8米|
|货位列间距|1.2米|
|普通巷道／主通道|4.5米／6米|
|收发边界|西侧8个IN取货点，东侧8个OUT交货点|
|机器人及物理泊位|100台机器人，100个充电兼待命泊位|

下面是示意图，详细坐标以地图为准。图的上边对应较小的y坐标，左边对应较小的x坐标：

```text
                            90 m
        ┌──────────────────────────────────────────┐
        │ A边：50个充电兼待命泊位 C-A01 ... C-A50 │
        │               泊位接近与通行区           │
        │                                          │
        │ IN-1   [R01] [R10] ║ [R19] [R28]  OUT-1 │
        │ IN-2   [R02] [R11] ║ [R20] [R29]  OUT-2 │
        │        [R03] [R12] ║ [R21] [R30]        │
        │ IN-3   [R04] [R13] ║ [R22] [R31]  OUT-3 │
 西侧   │ ═══════════════ 6m主通道 ══════════════ │  东侧
 入库   │ IN-4   [R05] [R14] ║ [R23] [R32]  OUT-4 │  出库
  76 m  │ IN-5   [R06] [R15] ║ [R24] [R33]  OUT-5 │
        │ IN-6   [R07] [R16] ║ [R25] [R34]  OUT-6 │
        │ IN-7   [R08] [R17] ║ [R26] [R35]  OUT-7 │
        │ IN-8   [R09] [R18] ║ [R27] [R36]  OUT-8 │
        │               泊位接近与通行区           │
        │ B边：50个充电兼待命泊位 C-B01 ... C-B50 │
        └──────────────────────────────────────────┘
```

IN、OUT是具体作业点。机器人到指定点位完成取放，页面中的边界色块用于解释功能分区。

巷道宽度为货架两侧作业和中间通行留出了空间；西入东出的安排让入库与出库在仓库两边接入，减少所有任务都挤向同一角落的情况。

地图`spec.version`中的`proposal`字样作为原版本标识保留，当前选定运行已经使用这份地图；数据对接时按文件摘要识别版本。

### 2.2 货位编号怎样读

例如 `R01-L1-C1-F1`：

- `R01`：第1个货架；
- `L1`：第1层；
- `C1`：该面第1列；
- `F1`：货架第1面，另一面是`F2`。

三层货位各有独立库存身份，可以共用同一地面取放接近点。机器人先到合适位置，再执行取放计时。页面展示货架高度，执行层采用二维移动和动作状态，便于将重点放在任务、货物和协作过程。

### 2.3 为什么P与C指向同一个泊位

P是机器人归属待命位的名称，C是充电资源的名称。例如：

```text
机器人1的待命名：P001
对应充电泊位：  C-A01
实际点位：      x=2.25m，y=2.4m
```

两者是同一个物理位置的**别名**。因此物理容量是100，而名称可以有100个P和100个C。

每个泊位占地1.5×1.8米，前方另留接近通道；当前基线每台机器人使用自己的归属泊位。空泊位硬件允许通行，停在上面的机器人持续占据真实空间。停靠、充满电或故障都保持车体占用。

## 3. 一件货怎样进出仓库

### 3.1 入库：从IN点到货位

以任务`T-IN-001`、货物`G-001`为例：

```text
任务在08:00发布
    → 检查仓容、目标货位和IN口暂存容量
    → 接收G-001，放在IN-1，预留目标货位
    → 给一台可行机器人派单
    → 机器人到IN-1，取货10秒
    → 携货走到R01-L1-C1-F1，放货12秒
    → 货位记为已占用，任务完成
```

`release_time`表示需求出现的时刻。真正收到货物时会产生`received`事件；两者在容量紧张时可能有时间差。

### 3.2 出库：从货位到OUT点

同一件货稍后出库：

```text
任务在10:00发布
    → 等待G-001确实存放在指定货位
    → 派单并预留源货位
    → 机器人到货架取货
    → 携货移动到OUT-1，完成放货
    → 货物位置变为SHIPPED，释放仓内总容量
```

本场景的业务边界是仓库收货、存储和交货。机器人自行取放，所有任务共用取放耗时与载荷规则。OUT完成放货时，货物跨出仓库边界；页面保留的出库缓冲面积信息可用于说明布局，当前吞吐由机器人、交通和OUT取放过程决定。

### 3.3 容量准入与库存守恒

**准入**指系统决定是否真正接收一件新货。它同时检查：

1. 仓内货物少于1728件；
2. 目标货位为空，且没有其他入库任务预留；
3. 对应IN口已接收待拿的货物少于6件。

8个IN口合计最多暂存48件。仓满、端口满或目标格被占用时，需求继续等待，保留其原始发布时间。

**库存守恒**是运行中反复检查的恒等关系：

```text
仓内货物数 = 初始库存 + 实际已接收数 - 实际已离仓数
0 <= 仓内货物数 <= 1728

每个货位：实际占用 + 入库预留 <= 1
每个IN口：已接收但尚未被取走 <= 6
每件货物：任一时刻只有一个实际位置或持有者
```

仓内货物包括货架上的、已接收暂存的和机器人携带的货物。把一件货从架上拿下来，会释放该货位，但货仍在仓内；到OUT交货后才减少总仓内计数。

例如仓内已有1728件，出库机器人刚取走一件，此时总数仍是1728。它在OUT放货后总数变为1727，系统才可以接收下一件。

库存检查同时核对任务、货位预留、货物账本和机器人携货状态。例如账本写“机器人1持有G-001”，机器人实体也必须记录携带G-001及对应任务。检查在稳定提交点、交接和恢复等路径执行，见 [`scheduling_v5/invariants.py`](../backend/scheduling_v5/invariants.py)。

## 4. 机器人怎样工作、充电和互助

### 4.1 基础参数

这些参数是当前运输仿真的明确配置，适合重复实验和灵敏度分析：

|参数|当前值|含义|
|---|---:|---|
|`speed`|0.8m/s|移动速度|
|`pick_seconds`|10秒|单件取货动作耗时|
|`place_seconds`|12秒|单件放货动作耗时|
|`standard_box_kg`|2kg|标准货物重量|
|`max_payload_kg`|5kg|机器人载荷上限|
|单次任务携货量|1件|货物与任务身份便于独立追溯|
|初始电量|40%—80%|由固定随机种子生成|
|`seed`|20261027|重复运行使用同一初始随机条件|

配置继承关系见 [`day1/sim.py`](../backend/day1/sim.py)、[`scheduling_v2/sim.py`](../backend/scheduling_v2/sim.py)、[`scheduling_v3/sim.py`](../backend/scheduling_v3/sim.py) 和 [`wide/sim.py`](../backend/wide/sim.py)。最终V5使用`wide.Config`的100泊位配置。

### 4.2 电量模型与充电时间

SOC是电池剩余电量百分比；这里的“每小时下降15”指下降15个百分点。

默认`transport`模型由 [`scheduling_v3/energy.py`](../backend/scheduling_v3/energy.py) 定义：

|状态|电量变化|
|---|---|
|空载移动|每小时下降15个百分点|
|载货移动|空载的1.2倍，即每小时下降18个百分点|
|取放与交接动作|基础消耗的1.2倍|
|待命或等待|每小时下降2个百分点|
|80%以下充电|每小时增加60个百分点|
|80%以上充电|速率减半|

因此，按连续单一工况估算：80%用到35%，空载移动约3小时，载货移动约2.5小时；35%充至80%约45分钟。实际一轮作业混合了移动、取放、等待和充电，运行记录会给出具体消耗。

当前参数是一套显式研究假设。硬件型号更换时，可以单独设置能量profile，并保留地图、任务输入和调度策略，比较参数变化的影响。

### 4.3 派单前怎样保证回得去充电

调度会检查这条完整能量链：

```text
执行当前候选任务的消耗
  + 完成任务后去充电位的消耗
  + 额外等待余量的消耗
  + 任务安全保留电量
  <= 当前电量
```

任务安全保留值`task_reserve_soc=20`，紧急底线`emergency_reserve_soc=10`。安全余量之外，路径和泊位还必须实际可达。

可派单机器人电量到35%及以下时优先安排回充，目标为80%。在正常任务分配之后，空闲机器人低于55%可以机会充电；`desired_working=70`、`standby_headroom=10`使普通机会充电最多同时安排20台。临界低电的回充走优先处理路径。

100个物理泊位提供足够归位空间，轮换策略控制平时何时去充电。资源数量与轮换策略分别配置，便于持续运转场景比较。

`desired_working`用于计算普通机会充电名额，实际业务活动数量可随高峰需求升至100台。

### 4.4 低电与故障时如何交接

正常派单已经预留返充能量，运行过程中还可以注入电量测量修正、有限时间停机和持续故障。携货机器人测得电量到25%及以下，或后续任务与返充能量检查不通过时，系统会尝试寻找帮手。

一次成功接力包括：

1. 确认原机器人是该货物唯一持有者；
2. 选取空载、空闲、电量合适的接收机器人；
3. 为接近路线、交接位置、交接耗时以及双方后续路线统一预约；
4. 双方在合法相邻位置停留10秒；
5. 一次性更新货物持有者、任务责任机器人及双方携货状态，产生`cargo_handoff`事件；
6. 接收方继续送货，原机器人按其状态回充或保持故障占位。

交接时中心距离检查范围为1.1—1.6米；车体与到达路径也参与连续避碰。协作方案失败会保留原有效状态，供后续重试或明确报告未恢复原因。

持续故障机器人即使完成货物交接，其身体也继续占据当前位置。显式清障后才重新启用。普通任务与跨日运行都会延续电量、库存和故障状态。

## 5. 怎样准备自己的任务数据

### 5.1 三个输入文件

每份数据集是一个独立目录：

```text
my-dataset/
├── tasks.csv              任务输入
├── initial_inventory.csv  仿真开始前已在架上的货物
├── manifest.json          数据集说明与文件校验值
└── validation.json        可选：校验工具生成的报告
```

这种组织借鉴了LaDe/KG_Embodied按日期、任务和空间位置组织数据的思路。这里的任务语义是仓内进货与出货，数据生成器可以灵活设计到货节奏、热点和期限。

### 5.2 tasks.csv固定七字段

```csv
task_id,task_type,goods_id,release_time,due_time,source_id,target_id
T-IN-001,INBOUND,G-001,2026-10-27T08:00:00+08:00,2026-10-27T08:20:00+08:00,IN-1,R01-L1-C1-F1
T-OUT-001,OUTBOUND,G-001,2026-10-27T10:00:00+08:00,2026-10-27T10:20:00+08:00,R01-L1-C1-F1,OUT-1
```

上述例子假定该格初始为空，先入库，再将同一件货出库。

|字段|含义与要求|
|---|---|
|`task_id`|任务唯一编号，多日数据中也保持唯一|
|`task_type`|`INBOUND`或`OUTBOUND`|
|`goods_id`|稳定货物编号，接力过程中继续使用原编号|
|`release_time`|需求发布时间，完整ISO8601日期、显式时区、整秒|
|`due_time`|预先设定的完成期限，晚于发布时间，可以跨午夜|
|`source_id`|入库为IN点；出库为货位|
|`target_id`|入库为货位；出库为OUT点|

任务可以使用1728个货位及16个收发点，共1744个业务位置。P/C泊位用于机器人归位与充电，与货物任务端点分开。

CSV描述外生任务；机器人编号、初始SOC、充电策略、故障注入和接力选择放入独立实验配置。运行产生的接单、取货、交接、完成时间写入日志，便于同一输入比较不同算法。

### 5.3 初始库存与manifest

`initial_inventory.csv`只有两列：

```csv
goods_id,location_id
INITIAL-00001,R02-L1-C1-F1
```

每个货物只出现一次，每个初始占用货位只存一件。当前常用回归输入为864件初始库存，占1728货位的一半。

`manifest.json`记录：

- 数据版本、场景名、固定种子；
- `start`与`duration_seconds`；
- 地图`scene_sha256`；
- 初始库存数、总任务数、入库数和出库数；
- `files.tasks.csv`、`files.initial_inventory.csv`的SHA-256；
- 到货分布、期限公式、容量与依赖假设。

SHA-256是根据文件字节计算的摘要，用来确认两次实验使用了相同输入。改坐标、改CSV或改变文件换行，摘要都可能变化，更新数据时应同步manifest并重新校验。

当前地图SHA-256为：

```text
ce99154d80fc643610dcb6d27384ca3724f95dc9121b8d2934212d56ba9eda24
```

### 5.4 数据生成同学需要守住哪些依赖

生成时应先维护一份参考库存，再为每个货位安排进出：初始有货则先OUT，格子空了才安排下一次IN；新入库货物使用新编号，其后对应OUT继续使用同一编号。

校验器按`release_second,task_id`重放参考计划，检查时间、身份、位置和库存逻辑。运行时再依据真实接收、真实上架和实际交通执行。因此可在计划中为出库腾空与后续入库留出周转余量。

建议双方交接时一起固定：地图原文件、七字段表头、初始库存、manifest、随机种子、场景类型、期限计算方法。地图版本相同时可保持位置编号；更换布局时同步坐标和地图摘要，并重新检查热点分区和期限。

仓库内的 [`dataset-contract/`](dataset-contract/) 提供进一步对接材料，位置表、物理泊位表和机器可读契约可以直接交给数据生成同学。自带可运行样例放在 [`data/examples/warehouse-v5/`](../data/examples/warehouse-v5/)。

也可以使用附带的独立标准库校验器校验同学交付的数据：

```bash
bash run_in_lade.sh python docs/dataset-contract/validate_package.py \
  --dataset data/examples/warehouse-v5
```

将`--dataset`替换为待交接目录即可。该工具还会核对本分支派生对接包的文件清单、地图、资源别名和摘要。

### 5.5 现有四种生成场景

[`backend/wide/scenarios.py`](../backend/wide/scenarios.py) 已提供：

|场景|生成方法|主要用途|
|---|---|---|
|`uniform`|各货位随机初相位、周期释放|日常平稳业务|
|`peak`|把释放时刻单调压缩成三波|集中到货与出货压力|
|`hotspot`|最近一片空间货位提高周转频率|局部拥挤与冷热区权衡|
|`continuous`|随机初相位后采用指数到达间隔|持续运转和跨日延续|

`active_slots`控制哪些货位产生任务，仓库总容量仍为1728。热点默认取活跃货位中空间上最近的20%，以4倍频率周转，因此热点场景的任务总量也可能增加。

支持的数据时长最长为7天。可以进一步设计批次脉冲、热点迁移、进出潮汐等代表性日；多日连跑沿用前一天的库存、积压和电量。

## 6. 调度如何选择机器人

### 6.1 用一批任务一起匹配

默认每10秒进行一轮调度，候选任务池最多32个，失败配对有30秒退避，最多进行3轮匹配尝试。

系统先为“任务—机器人”建立成本表，代价包括到取货点的路程、配送与取放耗时、迟到风险，以及较轻的已承担任务量均衡项。明显不可行的组合标记为禁用，暂不分配的任务使用占位列。

V5按以下顺序处理：

1. **最大可行匹配数量**：在当前静态候选中，尽量让更多任务获得机器人；
2. **紧急和久等保护**：保护已经等待较久、或期限余量不足的可行任务；
3. **联合成本优化**：在前两项条件下，联合决定普通任务的选择与机器人配对。

匹配使用经典最小成本分配方法。结果仍需通过实际时空路径、电量和预约检查后才提交。算法参考与实现来源见 [`scheduler_v2_sources.md`](scheduler_v2_sources.md)。

### 6.2 为什么有期限保护和端口压力

默认选定方案记录在 [`config/v5-selected.json`](../config/v5-selected.json)：

```json
{
  "profile": "balanced",
  "policy": "port-guard",
  "planner": "original-sipp",
  "parameters": {
    "wait_limit": 300,
    "deadline_guard": 120.0,
    "port_pressure_weight": 1.0,
    "port_queue_cap": 6
  }
}
```

`wait_limit=300`表示任务进入可调度候选后的等待保护触发点；`deadline_guard=120`表示预计完成后剩余期限小于等于120秒时加强保护。实际能派出的时刻还受候选池、路径和电量约束。

端口压力处理一个直观问题：IN-1已经暂存6件，外面还有可合法接收的新货，搬走IN-1中的一件就能腾出接收位置。此时系统会给相应入库任务适量的优先奖励，默认上限相当于60秒代价；仓容、目标货位和紧急出库保护继续生效。

例如，另一个任务急需先出库以释放目标货位，端口奖励会服从期限保护和容量约束，避免只顾入库而挡住真正释放资源的任务。

### 6.3 完成任务后如何衔接下一单

机器人在完成取放后可以尝试从当前位置接续新任务，节省“先回家再出发”的空驶。系统先准备完整新任务和安全离开路线，检查通过后替换原返程尾段；接续失败时继续原有有效路线。

## 7. 路径预约、避碰和恢复怎样配合

### 7.1 静态空间与动态时间

地图中的货架是静态障碍。导航器根据机器人包络检查路线段，机器人中心保持在合法通行空间中。机器人主体约0.7×0.7米，规划使用半径0.55米的圆形包络，机器人间中心距离阈值为1.1米。

动态交通使用**预约表**：每台已规划机器人登记“哪个时间段经过哪一段空间、何时停留取放”。后来的规划须避开这些已提交预约。

### 7.2 SIPP是什么意思

SIPP全称Safe Interval Path Planning，可理解为“按安全时间区间找路”。同一通道节点可能在第20—30秒被占用，在第30秒后可通行。规划器同时搜索路径和合法等待时间，减少逐秒枚举所有时间点的工作。

当前选定的`original-sipp`与精确几何、静态距离缓存结合使用；`goal-window`保留为显式实验选项。选择默认方案时综合比较了任务均值、迟到和长尾表现。

### 7.3 机器人停下来也占空间

每段运动和停留都参与连续时间检查；两个记录帧之间的相向运动也会检查。计划末端附带停车占用，保持机器人实体存在。

有新任务或恢复方案时，系统先在临时预约表中检查整组轨迹。全部通过后一起替换，任何检查失败都保留原有效预约。这样的**原子替换**使多车恢复过程具备“全部提交或全部保留”的行为。

发生延迟或停机时，可用恢复包括局部时间调整、受影响机器人组重规划、等待与明确的故障清除。进度监视器结合实际位置变化、业务事件与等待依赖判断停滞，避免仅看到短暂排队就反复强制重排。

### 7.4 计算优化放在哪里

已复用的优化主要包括空间/时间索引、精确几何LRU缓存、按规范坐标复用的静态距离缓存，以及安全区间搜索剪枝。LRU是“最近最少使用”缓存，空间达到上限后淘汰较久未用项。

几何缓存上限60000，静态距离缓存上限512。动态预约仍读取当前占用，保证新的机器人活动会进入碰撞判断。

## 8. 运行记录与知识系统怎样衔接

运行记录分工如下：

|产物|作用|
|---|---|
|`results.json`|完成数、迟到、流转时间、能量、安全与协作指标|
|`checkpoint.json`|完整状态及SHA-256，用于恢复|
|`journal.sqlite`|按提交保存机器人状态、任务/货位变化和业务事件|
|`frames.sqlite`|每10模拟秒的页面帧，启用`--frames`时生成|
|`latest.json`|页面当前可读取的最近帧|
|`layout-audit.json`|地图路网与静态安全检查|

默认每300模拟秒保存一次完整状态；业务事件在两次提交之间累积，并在提交时保留。`--commit-seconds`可以调整状态采样密度。更密的完整检查点会增加写盘和JSON编码工作，需要在报告中固定参数。

`journal`模式将记录保存在本地SQLite，适合先验证仓库和调度；`neo4j`模式可将同一运行的状态与事件同步到图谱。图谱按运行、任务、机器人、货物及时间提交组织记录，货物交接能关联双方参与者。

历史知识能够用于新决策。例如低电接管时，查询此前是否已交接、原持有者是谁，再结合当前货权决定是否继续运输。后续的按需知识接口接入可以独立开展。

**当前查询优化通过独立访问层和压测工具完成，已验收布局、七字段任务格式和V5调度可以保持稳定，无需为这轮查询优化重做仓库。**

## 9. 从哪几个代码文件开始读

建议先看`runner → sim → matching/motion → inventory → journal → web`这条链。

### 9.1 V5实验调用链与新增模块

V5逐层复用已有执行能力，主要继承关系为：

```text
day1.Simulation
  → scheduling_v2.Simulation
  → scheduling_v3.Simulation
  → wide.Simulation
  → scheduling_v4.Simulation
  → scheduling_v5.CandidateSimulation
```

|模块|职责|建议查看的内容|
|---|---|---|
|[`ops/warehouse_v5_release.py`](../ops/warehouse_v5_release.py)|选定方案入口|读取默认policy、planner、参数，启动新运行|
|[`config/v5-selected.json`](../config/v5-selected.json)|当前选择|`balanced`对应的实际配置|
|[`config/v5-policy-parameters.json`](../config/v5-policy-parameters.json)|可核对参数|与selected中的参数保持相同|
|[`scheduling_v5/runner.py`](../backend/scheduling_v5/runner.py)|V5命令行适配|策略参数、恢复前身份检查|
|[`scheduling_v5/sim.py`](../backend/scheduling_v5/sim.py)|V5调度决策|`_select_assignment`、`release_and_dispatch`|
|[`scheduling_v5/matching.py`](../backend/scheduling_v5/matching.py)|保护式批量匹配|最大数量、紧急保护、最终成本|
|[`scheduling_v5/invariants.py`](../backend/scheduling_v5/invariants.py)|库存与货权检查|任务、实体携货、账本、货位、端口相互核对|
|[`scheduling_v4/geometry.py`](../backend/scheduling_v4/geometry.py)|计算复用|精确几何与静态距离缓存|
|[`scheduling_v2/motion.py`](../backend/scheduling_v2/motion.py)|SIPP与时空预约|路网、安全区间、连续冲突判断|
|[`scheduling_v3/sim.py`](../backend/scheduling_v3/sim.py)|运行与恢复|能量准入、故障、互助、逐秒执行|
|[`scheduling_v3/energy.py`](../backend/scheduling_v3/energy.py)|能量与交接契约|消耗积分、充电、货权原子转移|
|[`scheduling_v3/transactions.py`](../backend/scheduling_v3/transactions.py)|多车预约事务|整组校验、统一替换与失败保留|
|[`scheduling_v3/progress.py`](../backend/scheduling_v3/progress.py)|进度诊断|实际运动、等待依赖和停滞观察|
|[`wide/data.py`](../backend/wide/data.py)|100泊位地图与数据|别名、米制位置、CSV/manifest与参考库存校验|
|[`wide/scenarios.py`](../backend/wide/scenarios.py)|回归输入生成|uniform、peak、hotspot、continuous|
|[`wide/sim.py`](../backend/wide/sim.py)|宽松地图适配|泊位归属、末端持续占位、场景初始化|
|[`scheduling_v3/runner.py`](../backend/scheduling_v3/runner.py)|统一实验生命周期|参数、扰动、提交、帧、结果|
|[`knowledge.py`](../backend/knowledge.py)、[`checkpoint.py`](../backend/checkpoint.py)|持久化|journal提交、恢复与原子写盘|
|[`wide/graph.py`](../backend/wide/graph.py)|图写入适配|运行、物理泊位/别名、库存和协作关系|
|[`scheduling_v5/web.py`](../backend/scheduling_v5/web.py)|V5回放入口|校验运行身份，再提供页面|
|[`wide/web.py`](../backend/wide/web.py)|只读HTTP服务|`/api/scene`、`/api/frame`、`/api/status`|
|[`dist/v5/wide.js`](../dist/v5/wide.js)|页面呈现|真实帧绘制、投影、相机与回放|

V5在继承已有执行、能量与恢复能力的基础上，集中改进任务选择和库存防线。旧入口继续保留，适合逐步对照合并。宽松地图有专用加载器，因此开发时应从`wide.data`读取100泊位布局，避免沿用早期固定24桩加载逻辑。

### 9.2 对原交互入口具体改了什么

相对fork原始`main`的`e721268`，本次交付也包含一组对原交互入口的兼容增强。读者可以按下面的调用链和文件表核对差异：

```text
原交互入口：backend.server
  → SimulationService / SimulationCoordinator
  → local或Ray统一运行时
  → RobotAgent行动提案、TrafficManager仲裁、实体提交
  → WebSocket快照与原dist页面

V5实验入口：ops/warehouse_v5_release.py
  → scheduling_v5.runner / CandidateSimulation
  → 上表中的调度、SIPP预约、能量和恢复模块
  → journal、实际帧与dist/v5回放
```

|原有文件|本次兼容增强|接入后怎样理解|
|---|---|---|
|[`backend/checkpoint.py`](../backend/checkpoint.py)|为状态增加带SHA-256的envelope（外层封装）；临时文件写入、刷盘后原子替换|写入端生成可校验状态，读取端校验新封装并兼容原始字典格式|
|[`backend/domain.py`](../backend/domain.py)|机器人实体增加固定取货点、等候点、`queued`排队状态与`reroute`改道；一次提交最多跨过一个实际直线运动段|工位接近与等待有明确状态，提交的运动段可直接用于连续扫掠检查|
|[`backend/runtime.py`](../backend/runtime.py)|local与Ray统一增加`reroute_many`和`admit_pickups`|中央协调器使用相同接口安排一组改道或工位准入，运行时负责实际调用|
|[`backend/traffic.py`](../backend/traffic.py)|原交互仲裁使用两段同步运动的连续最小距离；静止及被拒绝移动的机器人持续占据起点|拒绝某台运动后，重新传播它的静止占用，保持所有实体参与避碰|
|[`backend/simulation.py`](../backend/simulation.py)|增加取货工位准入、受阻重规划、批任务注入和知识提交|中央进程统一维护工位、任务与机器人状态，并在实体提交后保存知识|
|[`backend/server.py`](../backend/server.py)|修正WebSocket类型引用；阻塞仿真/持久化操作交给工作线程；提供知识与批任务接口|保留HTTP/WebSocket交互方式，使用协调锁与执行器管理耗时操作和一致性|
|[`dist/app.js`](../dist/app.js)、[`index.html`](../dist/index.html)、[`styles.css`](../dist/styles.css)|显示后端地图与容量指标、真实任务ID，增加批任务操作及连续已提交状态间的显示插值|原页面依据后端状态显示业务，渲染平滑与实际状态提交分工清楚|
|[`requirements.txt`](../requirements.txt)|加入`neo4j>=5.26,<7`|原服务启用Neo4j知识存储时有明确驱动依赖|

这组改动对应原交互界面的运行与兼容能力；V5交通规划仍由`scheduling_v2.motion`、`wide.motion`及继承的恢复模块承担。阅读`backend/traffic.py`时，可以沿原交互调用链理解其作用，V5则沿第9.1节的独立调用链阅读。

## 10. 从零运行一个小示例

下面使用Linux/Bash。Python建议3.10及以上；Pro6000已使用Python3.10运行验收。仓库V5的journal实验与只读回放使用Python标准库；原项目的FastAPI/Ray服务及Neo4j连接按需安装 [`requirements.txt`](../requirements.txt) 中的依赖。

### 10.1 选择Python环境并检查版本

在已配置Python环境的机器上复用现有环境，将路径替换为实际虚拟环境目录：

```bash
export LADE_ENV_DIR=/path/to/your/existing/.venv
bash run_in_lade.sh python --version
bash run_in_lade.sh python ops/warehouse_v5_release.py status
```

在新机器上可以先创建环境：

```bash
python3 -m venv .venv
export LADE_ENV_DIR="$PWD/.venv"
bash run_in_lade.sh python --version
```

运行原FastAPI/Ray服务或连接Neo4j时，再执行`bash run_in_lade.sh python -m pip install -r requirements.txt`。

`status`打印实际policy、planner、参数和当前源码身份，运行前先核对输出。

### 10.2 先校验自带小样例

仓库自带19条任务、1800模拟秒的接口样例，可直接用来熟悉流程。先校验地图摘要、CSV格式、时间和参考库存依赖：

```bash
bash run_in_lade.sh python -c \
  'import json; from backend.wide.data import validate_dataset; print(json.dumps(validate_dataset("data/layouts/warehouse100-wide-v2.json", "data/examples/warehouse-v5"), ensure_ascii=False, indent=2))'
```

你也可以使用现有生成器创建一小时均匀任务，作为下一次独立实验输入：

下面创建新的输入目录。每次重做时使用新目录名称，保留既有结果。

```bash
bash run_in_lade.sh python -m backend.wide.scenarios \
  --scene data/layouts/warehouse100-wide-v2.json \
  --output data/local-runs/demo-v5-uniform-input \
  --scenario uniform \
  --duration-seconds 3600 \
  --active-slots 128 \
  --slot-interval-seconds 3600 \
  --seed 20261027
```

生成器会写入三个输入文件和`validation.json`。生成例子仍注册100台机器人，128个活跃货位用于产生适量业务。同学提供自己的数据后，将校验命令中的数据目录替换为对应路径即可。

### 10.3 使用选定方案启动

```bash
bash run_in_lade.sh python ops/warehouse_v5_release.py run -- \
  --scene data/layouts/warehouse100-wide-v2.json \
  --dataset data/examples/warehouse-v5 \
  --output data/local-runs/demo-v5-run \
  --run-id demo-v5-run \
  --frames \
  --knowledge-backend journal \
  --max-wall-seconds 1800
```

默认使用`port-guard + original-sipp`。独立配置可通过`--config-json`传入，扰动配置可通过`--disturbances-json`传入。普通新运行始终使用新的output和run-id。

运行上一节生成的一小时输入时，将`--dataset`改为`data/local-runs/demo-v5-uniform-input`，同时另用新的`--output data/local-runs/demo-v5-uniform-run`和`--run-id demo-v5-uniform-run`。

仓库还提供了已验收的五份原始合成回归输入，放在 [`data/examples/regression/`](../data/examples/regression/) 下，每份附带`config.json`和`disturbances.json`。例如重跑1728任务的完整高峰：

```bash
bash run_in_lade.sh python ops/warehouse_v5_release.py run -- \
  --scene data/layouts/warehouse100-wide-v2.json \
  --dataset data/examples/regression/peak \
  --config-json data/examples/regression/peak/config.json \
  --disturbances-json data/examples/regression/peak/disturbances.json \
  --output data/local-runs/v5-peak-reproduction \
  --run-id v5-peak-reproduction \
  --frames \
  --knowledge-backend journal \
  --max-wall-seconds 1800
```

其他目录是`day`（6912任务全天）、`hotspot-2h`、`peak-2h`和`continuous-2d`。替换数据、配置和扰动目录，并同时换用独立output/run-id，即可开展对应回归。这样可以直接使用当时的输入与期限，而无需重新生成验收数据。

模拟时间与墙钟时间分别记录：前者是仓库经历的业务时间，后者是计算机真正花费的时间。`--max-wall-seconds`限制本次运行耗时，默认允许业务时间结束后额外清空未完成任务。

检查结果：

```bash
bash run_in_lade.sh python -c \
  'import json; r=json.load(open("data/local-runs/demo-v5-run/results.json")); print({k:r[k] for k in ("status","elapsed","wall_seconds","tasks_total","unfinished","flow_seconds")}); print({k:r["metrics"][k] for k in ("completed","late","handoffs","unsafe_events","min_separation_m")})'
```

评价时一起查看完成数、未完成数、迟到数、安全结果和运行状态，随后再比较平均流转、P99和计算耗时。P99表示99%的已完成任务流转时间不超过该数值。

### 10.4 打开网页回放

在另一个终端启动一个新端口的回放服务：

```bash
bash run_in_lade.sh python -m backend.scheduling_v5.web \
  --output data/local-runs/demo-v5-run \
  --dist dist/v5 \
  --port 14183
```

服务绑定服务器的`127.0.0.1`。本地通过SSH转发访问，将下面的`SSH_PORT`、`SSH_USER`、`SSH_HOST`替换为自己的连接信息：

```bash
ssh -N -p SSH_PORT \
  -L 14183:127.0.0.1:14183 \
  SSH_USER@SSH_HOST
```

然后打开 `http://127.0.0.1:14183/`。已部署的正式V5回放使用14182端口；新示例使用14183，便于两者并存。

页面从后端读取地图和已记录的实体帧，支持顶视、等距视角、机器人选中、时间轴与回放倍速。交互沿用原设计：**左键旋转、右键／中键平移、滚轮缩放**。

### 10.5 恢复与回退

同一源码、地图、输入、配置、策略和run-id下，在原运行命令末尾加`--resume`，从同一个output恢复。恢复时会先检查源码/策略身份及数据一致性。

要对比旧版本，直接使用保留的旧发布目录和对应输出；新的实验继续使用新目录。服务器现有对照页面分别为V5的14182、V4的14180和原算法的14181。

版本与检查点一一对应。这样可以稳定恢复当时的运行，也便于合并后从新源码启动新实验。

**源码身份的计算范围包含`backend/`下所有`.py`文件。** 因此新增后端文件也会产生新的执行身份。新的后端内容随新发布启动新实验，已有结果则使用对应的旧源码恢复；文档、样例和验收材料按各自文件清单追溯。

### 10.6 运行专项测试

```bash
bash run_in_lade.sh python -m unittest \
  tests.test_scheduler_v5_matching \
  tests.test_scheduler_v5_inventory \
  tests.test_scheduler_v5_runtime -v

# 需要完整仓库回归时
bash run_in_lade.sh python -m unittest discover -s tests -v
```

如果已经安装Node.js，还可以执行`node --test tests/*.test.mjs`，验证地图构建、相机交互、显示插值和回放等51项前端检查。

数据库集成测试使用显式测试环境开关和独立运行身份。普通仓库/journal验证可以先完成，再分别安排图谱集成实验。

## 11. 已经怎样验证这套仓库

### 11.1 五类正式输入

以下是2026-10-28交付的冻结V5结果。流转时间指任务发布到完成的模拟秒数，取所有完成任务统计：

|场景|完成／总任务|迟到|平均流转秒|P99秒|接力次数|实际业务并发峰值|
|---|---:|---:|---:|---:|---:|---:|
|原始高峰|1728／1728|0|380.68|888|0|100|
|原始全天|6912／6912|0|128.49|184|2|22|
|热点2小时|924／924|0|131.44|191|0|26|
|独立种子峰值2小时|768／768|0|140.68|215|0|48|
|连续两日|1034／1034|0|126.28|179|0|9|

五类输入均完成，安全审计通过。业务结束后，又在内存中继续推进到100台机器人全部回到各自泊位，允许原地充电，原检查点和日志保持原样。

高峰场景与保留原算法使用相同输入：平均流转从383.09秒到380.68秒，最长待派从843秒到422秒，迟到从1项到0项。V5相较前一版严格优先方案，改善了平均流转和长尾；选择时同时保留计算成本与业务指标，方便读者理解取舍。

每个场景的到货强度不同，低负荷时备用机器人可以待命或充电。100台实体始终存在，实际作业数量跟随任务需求变化。

### 11.2 100台同时作业的具体证据

独立逐秒重放在模拟804秒取得如下实际状态：

```text
to_target：52台，携货前往目标
to_source：35台，前往取货点
placing：   5台，正在放货
picking：   8台，正在取货
合计：    100台，87台移动＋13台取放
```

重放使用同一冻结源码、策略、输入和配置；0—800秒之间81个原有10秒记录点，全部机器人字段对齐。最后1秒独立检查4950对机器人连续运动，最小中心距1.13137085米，达到1.1米要求。

原始10秒采样帧最高记录98台业务活动；804秒的逐秒重放提供了采样间隙中的100台实证。两种证据各自保留来源和文件身份。

### 11.3 测试与恢复

冻结交付的完整Python回归共268项：263通过，5项真实数据库集成按当时验证范围跳过。V5专项25项通过，匹配层另有360个小规模穷举对照，核对最大可行数量、优先保护和最终成本。

覆盖内容包括库存双向核对、低电交接、故障持续占位、显式清障、失败预约原子保留、磁盘恢复与跨策略/源码恢复拒绝。正式全天输入执行了既定扰动，并发生2次真实货物交接。

这些结果归属于当时冻结的源码与输入。新版本启动时会计算自己的源码身份，并保留新结果，便于继续回归和比较。

选定的小型验收材料位于 [`evidence/warehouse-v5/`](evidence/warehouse-v5/)，用于将本文中的结果追溯到运行摘要、阶段门和独立复核。

## 12. 怎样合并，以及后续怎样分工

### 12.1 与原仓库的衔接方式

建议按三组理解本次改动：

1. **业务与地图**：`wide/`、100泊位布局及七字段数据校验，统一收发边界和资源编号；
2. **算法与可靠性**：`scheduling_v2/`到`scheduling_v5/`的可解释分层，最终默认由V5选定入口统一调用；
3. **使用与验证**：`config/`、`ops/`、专项测试、`dist/v5/`回放和本文。

保留原`backend.server`的服务流程，可先合并独立实验入口、对同一数据回归，再按需要将选定执行器接入原在线控制服务。合并时可以通过模块边界阅读，无需将多个实验版本的逻辑揉在一个大文件里。

前端按后端真实地图与帧绘制，原相机交互保留。数据生成同学按地图、位置编号和容量契约准备输入；调度同学按同一输入比较策略；知识系统同学使用实际任务和货物流转事件设计查询与共享上下文。

### 12.2 仓库交付与查询优化分别推进

目前仓库布局、容量规则、能量准入、任务分配、交通与恢复已经形成通过回归的实验载体。继续优化查询时，主要工作是请求触发、访问接口、数据库读取和计时拆分。

本次仓库分支聚焦已经验收的仓库设计、调度、仿真及其文档。新的查询访问层和压测代码可以单独提交、单独对照；按需知识接口真正接入运行决策时，再作为有明确调用链和对照结果的一项工作开展。

### 12.3 交接检查清单

- [ ] 固定本次分支／提交和实际启动入口。
- [ ] 地图SHA、业务位置、100个物理泊位与P/C别名一致。
- [ ] 数据含三份必需文件，时间、货物依赖与摘要校验通过。
- [ ] 电量、故障与调度参数以独立配置保存。
- [ ] 新运行使用独立output和run-id，记录完成、迟到、流转、安全与协作结果。
- [ ] 查看实际帧和日志，并保留原结果供比较和回退。

建议第一次协作先交付一份可校验的一日数据，用固定V5跑通，再逐步增加峰值、热点和持续多日场景。这样每次变化都能对应到明确输入和实际运行结果。
