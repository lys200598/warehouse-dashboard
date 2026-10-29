# 自由入出库工位：数据、调度、图谱与复现指南

日期：2026-10-29。本指南承接[仓库扩展交付](MEETING_DELIVERY_20261028.md)和[固定口晚点优化](LATENESS_OPTIMIZATION_20261029.md)，说明数据集如何交给仿真选择工位，以及本次完整高峰的实际结果。

## 1. 本轮完成的内容

数据集可以描述“货物到达仓库入库边界”及“货物需要离仓”，仿真在现有物理工位中选择实际位置。七字段CSV继续保留，入库源可以写`INBOUND`，出库目标可以写`OUTBOUND`。旧的`IN-8 / OUT-3`等明确位置仍可使用，执行时保留其固定含义。

当前验证地图为100×90米、40架、1920货位，100台机器人按A1/R2/X3=15/15/70配置，速度0.8m/s。仍然是**西侧8个IN、东侧8个OUT**，每个IN最多暂存6件；本次没有增加工位、容量或取放设备。机器人继续直接取放，保留原电量、型号、货权和连续避碰检查。

固定口1728任务有23项晚点；转换为自由口的新场景中，**1728项全部按时完成，最紧的一项仍提前108秒**。源码和数据各自冻结，原输入与原结果继续保留。

## 2. 数据集该怎么写

任务表仍是：

```csv
task_id,task_type,goods_id,release_time,due_time,source_id,target_id
```

|任务类型|source_id|target_id|含义|
|---|---|---|---|
|INBOUND|`INBOUND`|指定货位，如`R36-L2-C1-F2`|货到入库边界，仿真选接收工位，随后送到指定货位|
|OUTBOUND|指定货位|`OUTBOUND`|从指定货位取货，送到仿真选定的出库工位|
|固定入库|`IN-1`等实际口|指定货位|保留数据指定的进货口|
|固定出库|指定货位|`OUT-1`等实际口|保留数据指定的出货口|

同一份数据可混用固定和逻辑边界。任务ID、货物ID、发布时间、截止时间、货架端点及初始库存继续由数据集规定。机器人分配、能量、接力和实际工位选择由运行配置及仿真负责。数据侧无需加入客户订单、SKU或额外叉车资源。

逻辑端点需在manifest中声明：

```json
"boundary_routing": {
  "version": "warehouse-logical-boundaries-v1",
  "logical_inbound": "INBOUND",
  "logical_outbound": "OUTBOUND"
}
```

转换工具会写入声明、新CSV SHA和原件来源。CSV列保持七个，不能把源或目标简单留空。

### 2.1 转换已有数据

先选择与原数据相符的地图，输出目录必须是新目录：

```bash
bash run_in_lade.sh python -m backend.warehouse convert-boundaries \
  --scene data/layouts/expansion-candidates/balanced/layout.json \
  --dataset /absolute/path/to/fixed-dataset \
  --output /absolute/path/to/logical-dataset --role both

bash run_in_lade.sh python -m backend.warehouse validate \
  --scene data/layouts/expansion-candidates/balanced/layout.json \
  --dataset /absolute/path/to/logical-dataset
```

`--role`支持`both / inbound / outbound`。工具保留任务顺序、ID、货物、类型、时间和货架端点，初始库存逐字节复制。新任务CSV只改变选中方向的边界字段，并记录原manifest和原CSV SHA。

因此固定口与自由口是**两份明确区分的场景输入**。比较时列出解除的固定门约束，保留旧输入；不把新场景称为字节相同CSV上的算法收益。

## 3. 仿真何时选择实际工位

### 入库：实际准入时选定

任务发布后，货物仍在入库边界外。目标货位可用、仓内还有容量、某个物理IN口未满时，仿真选择接收口并执行`received`。选择综合：

- 当前各IN口的实际暂存数；
- 工位到目标货位的静态路程；
- 当前空闲或可衔接、能力匹配且高于回充阈值的机器人到工位的ETA。

评分只使用已经公开的任务和当前状态；没有可用机器人时，距离部分仍可用于安排缓冲。真正派单继续通过原能量和路线门。全部入口都满时，货物继续在外等待，不假装入库。

### 出库：任务实际发布时选定

出库根据源货位到各OUT口的路程，以及已经发布但尚未完成的出库负载选择目标。该选择发生在取货之前。后续任务需要等待源货物真正就绪，原库存依赖检查继续保留。

**两个方向都在绑定后固定实际工位。** 准入后的货物不会被改到另一个IN口，正在运输的货物也不会改变实际OUT交付点。入库暂存数在机器人实际`picked`时才减少，出库在实际确认离仓后才释放仓内容量。

## 4. 原计划和实际过程怎样保留

例如CSV写入库`INBOUND → R36-L2-C1-F2`，运行在1200秒选择`IN-7`：

|运行字段|值|用途|
|---|---|---|
|`plan_source_id`|`INBOUND`|保留CSV原始意图|
|`plan_target_id`|原货位|保留固定货架目标|
|`source_id / resolved_source_id`|`IN-7`|供实际路径、取货及货权检查使用|
|`source_bound_at`|1200|记录实际选择时刻|
|`boundary_bindings.source`|逻辑口、物理口、时刻和来源事件|检查唯一绑定与恢复一致性|

`boundary_port_bound`事件记录逻辑/实际端点、方向、时刻和来源事件编号；该事件不创建货权。实际货权仍由原`received / picked / placed / shipped / cargo_handoff`记录。

图谱保留两个层次：

- 静态`WarehouseTask`按原计划连向逻辑入库/出库组或指定货位，跨run的静态计划不会被某次选址覆盖。
- 本run的`WHTaskState`通过执行端点关系关联实际工位；尚未绑定时，该执行关系为空。
- `WHGoodsState`只记录货物真实持有者。逻辑INBOUND/OUTBOUND不会充当货物的物理位置，也没有被赋予虚构坐标。

固定运行模式默认`port_selection_policy="fixed"`。新模式显式使用`"balanced"`，原具体端点仍固定。检查点绑定源码、配置和输入身份；旧run继续用旧冻结源恢复，新模式使用新run。

## 5. 本次高峰结果

三组保持100台、0.8m/s、原时间/货物/货架引用和初始库存。每组1728任务在一小时发布，之后排空。

|场景|完成|晚点|结束秒|平均/P99/最大流转秒|
|---|---:|---:|---:|---|
|固定口控制|1728|23|4419|472.48 / 1125 / 1245|
|自由口基础|1728|0|4169|385.13 / 856 / 1006|
|自由口online|1728|0|4169|385.13 / 856 / 1006|

自由口最紧任务提前108秒完成，实际业务活动峰值100台。每个IN口的实际峰值均为6，未超暂存容量。实际收货分布为114/109/104/108/102/113/108/106；出货分布为113/109/111/104/105/104/107/111。载货距离由112568.00米降至94301.90米，空驶距离基本相同。

默认固定控制与旧23晚点基线的任务、货物、机器人、全部事件和443个保存物理帧完全一致。自由两组的418帧和全部物理状态也一致；绑定记录里的来源事件run前缀单独规范化，其他字段没有删去。

本次自由口online的donor行实际为0。未准入源为逻辑INBOUND，原满物理口捐赠规则没有触发。因此本场景选择更简单的**自由口基础配置**，收益归于实际工位选择，保留online对照原件。

### 5.1 合并查询接入后的完整复跑

S2冻结树`source-combined-v1`的`wh-free-combined`复跑同一份逻辑边界数据，实际退出0，完成1728/1728、0晚点、4169秒；平均/P99/最大流转仍为385.13/856/1006秒，实际业务活动峰值100台。任务、货物、机器人、完整10755条事件及418个保存物理帧，与S1自由口基础运行一致。比较只规范化run/event前缀及绑定记录中的来源事件引用，墙钟性能和源码元信息另外记录。

S2的完整Python回归实际运行803项，796通过、7跳过，退出0。联合物理报告、原始results、退出文件、完整测试日志、配置和计划见证据目录；该报告没有授予百万级或动态查询延迟资格。

### 5.2 冻结来源怎样区分

|版本|冻结目录|清单范围|来源说明|
|---|---|---|---|
|S1|`source-ports-v1`|117个backend Python文件|三组自由/固定口物理对照和19任务实际KG样例的原生产树|
|S2|`source-combined-v1`|120个backend Python文件及1个查询映射JSON，共121项|加入查询视图、查询服务及统一CLI；完成上述1728项联合物理复跑|
|S3|`source-combined-v2`|与S2相同的121项，清单字节相同|只增加实时发布入口优化及对应测试；实际动态服务和延迟资格由主任务另记|

S1与S2的物理核心源码相同，完整backend源码有变化：`backend/query_catalog/service.py`、三个`backend/query_views/`文件和`backend/warehouse/__main__.py`共五个Python路径。S2清单还包含`backend/query_catalog/kg56_mapping.json`，该JSON不计入Python文件数。S2和S3的backend清单完全一致；两棵树的非缓存差异仅为`ops/warehouse_live.py`、`ops/warehouse_live_runtime.py`及`tests/test_live_summary_publisher.py`。每次运行继续保留自己的源码身份。

S2 runtime身份重算值为`0b7eea4049685a93a96e6ba61d21be879b239e87030fa39cb2c0e2bae5cc799b`，与原运行记录一致。S1清单SHA为`953fb3bd388d0086658487947c765d07b6ccdcdbd62e32a9ae195e877e643c99`；S2/S3的121项清单SHA为`ff05ded0ffaafc7032728f577224dac72845d98ce69b27343aa51d33c39fe16a`。

## 6. 知识图谱实际验证

S1独立`required`运行`wh-free-required19`完成19/19任务、0晚点、1970模拟秒，发生一次真实携货低电接力；图谱历史规划授权、交接授权各1次。69批及源/图各6900条实际机器人状态一致。

40架、1920位，在三个历史截止点核对5760个货位值，与原journal一致；19个实际绑定、静态计划边及运行执行边已核对，货权指向逻辑组的数量为0。

该19任务样例中，真实持久化的“未准入、尚未绑定IN”样本为0。此分支当前由离线合同和事务测试覆盖；该小样例不能证明真实满口压力下所有未绑定图路径已覆盖。主任务会分别记录后续联合验证。

首份只读KG报告v1的`source_backend_code_sha256`展示字段被临时变量覆盖，值显示为最后一条绑定对象。工具前面的源码清单与backend哈希断言、`actual_code_identity`检查和业务/图谱核对已经执行；v1原报告和原工具均保留，该对象不能当SHA。独立v2只把backend哈希存入专用变量并检查64位十六进制类型，重复执行相同只读审计，已通过；原run的result/checkpoint/journal三份SHA与v1完全一致。v2报告SHA为`2ba73516d3013a6296b68577fa426bb6c215946260f8d7e160cf4a0147808267`。

## 7. 如何启用与对接

完整配置入口为`config/free-ports-balanced-100.json`，由主交付提供（已测原配置字节）；关键设置为`port_selection_policy="balanced"`和`port_relief_policy="off"`，其余物理参数与已验收场景对齐。

```bash
bash run_in_lade.sh python -m backend.warehouse run \
  --scene data/layouts/expansion-candidates/balanced/layout.json \
  --dataset /absolute/path/to/logical-dataset \
  --config-json config/free-ports-balanced-100.json \
  --output /absolute/path/to/new-run --run-id unique-free-port-run \
  --mode rules --knowledge-backend journal --commit-seconds 300 --frames
```

实际历史授权和资源写入使用`--mode required --knowledge-backend neo4j --resource-graph`及对应数据库环境。查询尾延迟结果由独立查询报告说明，本指南的P99是任务流转秒数。

实时界面接入、端口及部署完成状态以主任务最终记录为准；本指南不把正在探测的服务写成已交付。跨日型、多天持续回充、端侧设备和百万级联合查询继续分别验收。

### 给lys的一句话

> 我这边增加了逻辑入出库边界：七字段CSV中入库source写INBOUND、出库target写OUTBOUND，工位由仿真在准入/发布时选择并固定。请先看docs/FREE_BOUNDARIES_GUIDE_20261029.md，用统一convert-boundaries和validate入口；货物、货位、时间、初始库存继续按你的数据设计，manifest加对应声明。当前100×90m高峰1728任务已经全部按时完成，其他日型仍需共同回归。

当前loader仍校验既有8IN/8OUT。将来扩展物理站点，需同步地图、加载校验和图谱登记；逻辑CSV继续可以保持INBOUND/OUTBOUND，不用替换成新站号。布局尺寸与货位变化仍需更新地图SHA、位置表及相应库存约束。

## 8. 证据与复现边界

紧凑原件见[evidence/free-boundaries-v1](evidence/free-boundaries-v1/)，文件SHA见`SHA256SUMS.txt`。目录保留严格高峰审计、绑定事件引用规范化补充、截止余量、实际KG审计及小工具，不复制大型SQLite或整个源码。

S1物理高峰主审计SHA：`42b6b0174a58fef25eecc5d3b426797fbb455a0e3be583b53dc4e7a227c8424c`。S1实际KG只读审计v2 SHA：`2ba73516d3013a6296b68577fa426bb6c215946260f8d7e160cf4a0147808267`。S2联合物理与来源审计`combined-free-source-audit-v1.json` SHA：`4273da878e4ca181b377e7146460a7df4fe5f11c2117c4ff74a5c7788bdaca55`。

这些结果证明当前一小时高峰及排空场景的按时性，不构成任意负载或长期容量保证。旧固定门基线、所有原始结果和新场景变化范围继续保留。
