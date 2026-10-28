# 仓库标准查询与知识包指南

这部分把参考项目KG_Embodied的决策上下文整理成仓库可执行的只读查询。使用者提供运行编号、已提交的历史位置和业务对象，服务返回来源明确的状态、货物流转、历史反馈或候选事实。规则协调器和原物理规划继续负责真正执行动作。

## 1. 从一个例子理解

机器人66报告低电，当前携带货物G：

1. `self`读取66的状态及任务；
2. `task`或`goods`查看货物归属与此前交接；
3. `assistance`给出附近空闲、空载、电量满足筛选阈值的候选，并返回近期实际协作/任务事件；
4. 调度器继续检查实际路线、电量准入和交通预约，再做接力；
5. `progress`或`memory`回顾之后的取放、充电及协作过程。

当前已经接入实际控制的`knowledge_runtime`继续使用严格的`cooperation_context`进行规划授权和交接前复核。本目录原11个知识包用于稳定的查询入口、审查、实验和后续接线，统一标为`used_by_live_control=false`，避免把可查询信息自动称为已执行决策。

## 2. 统一入口

不连接数据库查看目录：

```bash
./run_in_lade.sh python -m backend.warehouse query --catalog
```

把下面内容保存为`request.json`，替换运行编号和实际已经提交的`seq`：

```json
{
  "query_id": "assistance",
  "run_id": "my-warehouse-run",
  "seq": 100,
  "robot_id": 66,
  "start_seq": 40,
  "radius_m": 100,
  "min_soc": 35,
  "limit": 10
}
```

加载Neo4j环境变量后执行：

```bash
./run_in_lade.sh python -m backend.warehouse query --request-json request.json
```

Python调用为：

```python
from backend.query_catalog import QueryCatalogService
service = QueryCatalogService.from_env(timeout=15, pool_size=120)
try:
    result = service.execute(request)
finally:
    service.close()
```

`QueryCatalogService.describe()`是类方法，不需要数据库。构造服务会连接已有数据库，查询层不创建schema、不写节点或索引。

`latest_metrics.service_ms`计入整个包的完整JSON返回；`statements`只记录本目录直接调用的语句。委托给旧轻量/完整检索接口的内部语句由原服务计量，不能将此字段当成所有包的总Bolt语句数。

## 3. 11个查询包

所有请求都有`query_id`、`run_id`和正整数`seq`。未知参数直接拒绝，参数不会拼接进Cypher。默认`robot_id=1`，建议业务调用明确提供。

|query_id|额外参数|主要返回与用途|
|---|---|---|
|`self`|`robot_id`|该机器人在精确提交点的状态和任务，复用轻量接口；不读取全fleet、全任务或充电位|
|`readiness`|`robot_id`|同一机器人状态、请求点与已提交水位差，提示动作前仍需实时复核|
|`task`|`task_id`；`goods_id`可选|先以索引读取该任务最新历史版本，解析货物，再读取完整货权与协作上下文|
|`goods`|`goods_id`；`task_id`可选|仅给货物编号时查货权链；补任务编号时一并读取该任务协作上下文|
|`energy`|`robot_id`|电量、能量配置、100个物理泊位的占用/充电/预约事实；实际能量和回充路线仍由规划器核验|
|`assistance`|`robot_id`、`radius_m`、`min_soc`、`limit`及窗口参数|空闲、空载、无当前任务的候选，按欧氏距离下界排序；近期事件与观察反馈|
|`progress`|可选`task_id`及窗口参数|窗口中的真实事件、种类计数、已观察的派单到取货及取货/释放到完成时间|
|`memory`|`robot_id`及窗口参数|连续的实际状态样本、机器人事件、坐标/SOC/距离变化、位置范围与状态样本数|
|`local_awareness`|可选`zone`、`robot_id`及窗口参数|按货架分区的任务状态与逾期数量、附近分区机器人状态；默认取该机器人最近货架的分区|
|`global`|窗口参数、可选`max_tasks`|全局Step、100机器人状态负载、物理泊位负载、各货架分区任务数量|
|`similar_history`|`robot_id`，`variant`及检索参数|默认`business-exact`，使用可解释业务特征做有界历史检索；显式`full-ann`保留原完整32维工程ANN接口|

### 相似历史的两个版本

默认业务检索例子：

```json
{"query_id":"similar_history","run_id":"my-warehouse-run","seq":1000,
 "robot_id":66,"variant":"business-exact","limit":5,
 "window_steps":60,"embargo_steps":20,"max_candidates":10000}
```

它使用17维业务状态特征，先限制同run历史窗口、排除临近状态和自身机器人，再精确比较余弦相似度。返回候选来源、候选域、特征版本和实际返回数量。历史不足时允许少于K，明示数量；不会混入未来或复制结果补数。

原完整ANN版本：

```json
{"query_id":"similar_history","run_id":"my-warehouse-run","seq":1000,
 "robot_id":66,"task_id":"T-123","goods_id":"G-123","variant":"full-ann"}
```

该版本保留既有完整接口、旧32维结构特征及ANN100→ANN1000→精确补查，适合与既有工程性能证据对照。两种检索的候选域、特征和语义分别记录。相似结果是建议材料，不授予货权，也不自动成为接力决策。

## 4. 时间、一致性和完整性

- `seq`是知识可见提交批次。固定cutoff时，多次读取允许图head继续向前推进，所有数据仍受同一cutoff约束。
- 复合知识包检查批次SHA及运行创建版本，拒绝一次包内混入另一个批次或重建run的数据。
- `start_seq`到`seq`是包含端点的**提交窗口**，默认最近61批。提交节奏可能含额外扰动边界；它不自动等于61秒。
- 单个窗口最多3601批。默认`event_limit=2000`、`history_limit=3601`，可显式提高到10000；任务实体预算`max_tasks`默认100000，上限1000000。
- 事件或历史超过预算时抛出`IncompleteContext`，调用者缩小窗口或明确提高预算。服务不会拿截断尾页做完整决策背景。
- `memory`要求该窗口的机器人状态提交连续直到cutoff，并核对完整payload SHA。状态样本次数单列，避免把次数当成持续时间。
- `assistance.limit`只限制展示的候选数量，返回`eligible_candidate_count`和`candidate_list_truncated`。它不截断历史证据。
- 某任务在窗口里只观察到完成、未观察到释放/取货时，反馈相应耗时为`null`，并给出缺失端点标志；没有凭空补全之前经历。
- `memory`事件按源事件的`robot_id`执行者过滤，不自动扩展到所有受影响机器人；完整交接双方用任务/货权协作包追溯。`assistance`的近期窗口，以及未指定任务的`progress`窗口，覆盖全run事件；每行保留实际执行者和任务编号，调用者可再按候选组解读，不把全局反馈当成某一候选的个人表现。
- `readiness`的`ready_for_live_action=false`表示普通历史查询不代替当前协调器的能量、唯一货权和预约原子复核。

## 5. 仓库业务约定

### 区域

任务区域由目标货位所属货架的`zone`确定。机器人区域按其当前坐标到最近货架中心确定，返回`robot_region_semantics`说明；该定义不把停在某分区附近的机器人强行绑定给该分区。`local_awareness`中的近期事件计数明确是全局run窗口，事件自身的历史区域尚不从当前任务关系倒推。

### 电量、帮手与泊位

空闲、空载、最低SOC和欧氏半径仅构成候选事实筛选。实际接力还要检查负载能耗、完成后回充余量、真实道路距离、障碍、运动预约和当前货权。当前assigned-bay策略仍由执行器负责；“泊位当前无人”不自动说明该机器人可以立即使用该泊位。

### 全局任务数量

服务从run下的任务实体出发，对每项任务用`(run_id,task_id,seq)`索引取截止位置前最新版本，再按货架分区、状态和逾期情况聚合。它不以实体当前属性替代过去，也不扫描每项任务全部版本后在客户端去重。已完成数量须与对应Step一致；超过任务预算会明确拒绝。

全局与区域包是较重的共享观察，适合中央协调器按需读取一次供多个机器人使用。高频单机器人读取使用`self/readiness`，避免100台每次重复请求相同的全局统计。

## 6. 与KG_Embodied的56模板怎样对应

参考版本为`899c50cf20a1e93ec79861a99d0090d124bf88b5`，机器映射在`backend/query_catalog/kg56_mapping.json`。映射覆盖i0—i55，逐项保留原名称、目的、仓库入口、适配说明和状态：

- `covered`：核心事实或窗口计算可执行；事件名采用仓库版本。
- `warehouse-adapted`：以仓库单任务、货架分区和泊位模型提供对应事实，语义差别明确。
- `planner-owned`：查询提供任务/电量事实，路线、可达性和能量可行性由现有规划器实际计算；不声称只读API计算了这些结果。
- `not-applicable`：当前采用统一历史cutoff，排除把最新可变节点与旧快照混在同一决策点。

下表由同一映射文件生成，便于代码审查。它记录56项的处置，仓库采用合并后的业务知识包和明确的适配语义，实时调用次数由实际需求决定。

|KG ID / 名称|仓库查询包|状态|适配说明|
|---|---|---|---|
|i0 / `current_drone_state`|`self`|covered|robot identity/state at requested committed cutoff; latest mutable node excluded|
|i1 / `latest_agent_state`|`self`|covered|exact committed snapshot, no latest-before ambiguity|
|i2 / `recent_self_trajectory`|`memory`|covered|complete bounded robot state window|
|i3 / `active_tasks_for_drone`|`self`|warehouse-adapted|one transport task per robot; warehouse queue_capacity differs from drone queue|
|i4 / `recent_drone_events`|`memory`|covered|robot-filtered event window|
|i5 / `recent_self_performance`|`memory`|covered|window events and measured feedback; no missing sample inference|
|i6 / `current_region_load`|`local_awareness`|warehouse-adapted|warehouse rack-zone load; no drone region/depot model|
|i7 / `nearby_drones_same_region`|`assistance`|warehouse-adapted|idle empty candidate facts, then planner energy/traffic gates|
|i8 / `known_open_tasks_in_region`|`local_awareness`|warehouse-adapted|exact as-of task versions grouped by target rack zone|
|i9 / `latest_global_step_summary`|`global`|covered|committed Step plus fleet/bay counts|
|i10 / `region_recent_events`|`local_awareness`|warehouse-adapted|global observed event kinds with explicit zone limitation|
|i11 / `global_region_bottlenecks`|`global`|warehouse-adapted|task pressure by rack zone and physical bay/fleet load|
|i12 / `self_capability_limits`|`energy`|warehouse-adapted|warehouse energy configuration and payload policy remain planner authority|
|i13 / `self_snapshot_freshness`|`readiness`|covered|committed cutoff present and head lag; no inferred wall-time freshness|
|i14 / `self_recent_state_changes`|`memory`|covered|endpoint coordinate/SOC/distance changes|
|i15 / `self_recent_status_samples`|`memory`|covered|stage sample counts, explicitly not continuous durations|
|i16 / `self_stationary_evidence`|`memory`|warehouse-adapted|raw complete state window permits stationary review, no automatic fault inference|
|i17 / `self_operational_status`|`self`|covered|actual stage and carrying status|
|i18 / `self_active_task_summary`|`self`|warehouse-adapted|single active transport task, no drone multi-task queue|
|i19 / `self_pending_pickup_locations`|`task`|covered|source/target location in exact task version|
|i20 / `self_onboard_delivery_targets`|`task`|covered|carrying task/target and goods custody|
|i21 / `self_urgent_task_slack`|`local_awareness`|warehouse-adapted|overdue task counts by zone; exact task due available via task|
|i22 / `self_overdue_active_tasks`|`local_awareness`|warehouse-adapted|as-of incomplete overdue task counts|
|i23 / `self_task_action_candidates`|`task`|warehouse-adapted|task source/target, action selection remains warehouse planner|
|i24 / `self_direct_completion_estimates`|`energy`|planner-owned|route completion feasibility remains SIPP/energy planner, no Euclidean ETA presented as route|
|i25 / `self_active_task_event_context`|`progress`|covered|task-filtered committed event window|
|i26 / `self_recent_assignments`|`memory`|covered|assignment events visible in selected robot window|
|i27 / `self_recent_pickups`|`memory`|covered|picked events and actual actor|
|i28 / `self_pending_pickup_ages`|`progress`|covered|assignment-to-pickup observations, window missing starts explicit|
|i29 / `self_onboard_delivery_ages`|`task`|covered|picked_at/due fields in exact task payload; source observation time explicit|
|i30 / `self_recent_completed_tasks`|`progress`|covered|placed/shipped events and task feedback|
|i31 / `self_recent_event_type_summary`|`progress`|covered|actual event-kind counts|
|i32 / `self_nearby_available_peers`|`assistance`|covered|Euclidean radius lower bound plus observed idle/empty/minSOC filters|
|i33 / `self_task_peer_route_estimates`|`assistance`|warehouse-adapted|candidate facts only; shared planner validates actual route/energy|
|i34 / `self_cross_region_peer_candidates`|`assistance`|warehouse-adapted|warehouse peer search spans rack zones within requested radius|
|i35 / `self_nearby_returning_peers`|`energy`|warehouse-adapted|all robot stages and 100 physical bay occupancy facts|
|i36 / `self_region_queue_comparison`|`local_awareness`|warehouse-adapted|warehouse single-task/stage load in place of drone queue comparisons|
|i37 / `self_recent_served_regions`|`progress`|warehouse-adapted|actual task completion events; supplied task IDs retain slot-zone references|
|i38 / `self_known_depot_return_margin`|`energy`|planner-owned|SOC/reserves/bay facts; complete return energy admission stays in planner|
|i39 / `self_reachable_known_depots`|`energy`|planner-owned|canonical bay occupancy and reservations; reachability stays planner controlled|
|i40 / `self_individual_task_energy_estimates`|`energy`|planner-owned|loaded route energy evaluation belongs to current warehouse planner|
|i41 / `self_recent_charge_cycle_events`|`memory`|covered|battery measurement, charge reservation/start/completion events|
|i42 / `self_observed_battery_changes`|`memory`|covered|full sampled SOC changes; no depletion inferred between missing steps|
|i43 / `self_last_known_charge`|`memory`|covered|charge_complete in requested window; absent means unknown|
|i44 / `self_region_known_task_pressure`|`local_awareness`|warehouse-adapted|cutoff task versions/overdue counts by slot rack zone|
|i45 / `self_region_available_queue_capacity`|`local_awareness`|warehouse-adapted|fleet stage load rather than unsupported drone queue capacity|
|i46 / `self_region_recent_completion_quality`|`progress`|warehouse-adapted|observed completion feedback; no future deadline outcome label|
|i47 / `self_region_recent_task_flow`|`local_awareness`|warehouse-adapted|global window event counts; event-zone inference intentionally excluded|
|i48 / `self_nearby_known_open_tasks`|`local_awareness`|warehouse-adapted|target rack-zone task pressure; assignment ownership in exact task query|
|i49 / `self_nearby_low_battery_peers`|`energy`|warehouse-adapted|all fleet SOC/stages available; live emergency decision remains planner|
|i50 / `self_assignment_to_pickup_feedback`|`progress`|covered|observed assignment-to-pickup attempts|
|i51 / `self_release_to_completion_feedback`|`progress`|covered|release-to-completion only when start and finish visible|
|i52 / `self_current_snapshot_comparison`|`readiness`|not-applicable|one coherent historical cutoff; separate mutable-current comparisons excluded|
|i53 / `self_recent_snapshot_coverage`|`memory`|covered|contiguous state commits required through cutoff|
|i54 / `self_recent_delivery_feedback`|`progress`|covered|picked-to-placed/shipped feedback when observed|
|i55 / `self_decision_data_readiness`|`readiness`|covered|snapshot/hash/cutoff checks plus explicit live-action recheck requirement|

## 7. 测试与集成位置

新增实现位于`backend/query_catalog/`，统一CLI调用该服务，原低电协作控制继续保留其已验证的授权门禁。专项测试：

```bash
./run_in_lade.sh python -m unittest tests.test_query_catalog -v
```

固定事实预期覆盖11类接口及筛选/反馈逻辑；负例覆盖错误run、水位、完整性预算、缺失/未来状态、错误payload SHA、run重建、充电别名与源完成数不一致。真实Neo4j编译、同源Oracle及联合运行结果由统一回归单独记录，单元测试不代替这些证据。

## 8. 保存响应后的独立来源验真

正式读取结束后，可运行：

```bash
./run_in_lade.sh python ops/verify_catalog_responses.py \
  --journal /absolute/path/to/journal.sqlite \
  --requests /absolute/path/to/requests.jsonl \
  --responses /absolute/path/to/responses.jsonl.gz \
  --oracle-dir /absolute/path/to/new-oracle-directory \
  --output /absolute/path/to/new-verification.json
```

请求记录保留唯一字符串`request_id`、原始`request`对象、`run_id`、`seq`、正整数`start_ns/end_ns`、`ok=true`和完整返回的`response_sha256`。完整返回文件每行为`{"request_id":"...","result":完整catalog返回}`，支持普通JSONL和gzip。

验证器从原SQLite批次及独立RealOracle重建状态、任务、货权链、事件窗口、物理泊位、区域统计及协作候选；不调用catalog或其聚合函数构造预期。业务相似检索用独立17维标量运算验证整个合法候选域和topK。任意缺失、重复、错误字段或指纹不一致会逐请求保留，最终`passed=false`。全局任务分组的排列顺序允许规范化，行内容和数量全部核对。

旧`full-ann`继续沿用原冻结`query_v2`验真和召回契约，不混进业务17维认证。新验证器遇到此变体会显式失败并提示使用旧工具，避免仅检查来源就宣称验证了业务检索质量。

`tests/test_catalog_verifier.py`的12项正反例另测，包含真实SQLite/Oracle CLI与手工self/goods返回。它们验证工具行为；实际运行响应的联合验真报告仍需从该次实验保存文件生成。

## 9. 完整任务汇总的中途诊断与候选优化

[中途来源验真和聚合对照报告](evidence/platform-v1/catalog-aggregation-report.md)记录了全部11类真实查询、独立源检查，以及两个只读Cypher原型的完整AB/BA和冷首请求。机器可读数据见[紧凑JSON](evidence/platform-v1/catalog-aggregation-summary.json)。

正式七日联合运行继续使用原冻结源码和原查询默认值。原型通过先合并同货位的任务统计，减少重复的货位→货架关联；它们属于我们自行编写的Cypher改写，使用现有Neo4j索引，未复制闭源算法。两组平均聚合耗时分别下降约34.6%和27.5%；冷首请求仍有超过500ms的样本，完整接口还需计算其他读取与响应处理开销。

进一步实现了一个独立的[派生汇总侧车原型](evidence/platform-v1/catalog-summary-sidecar-report.md)：从原journal增量维护每个seq的完整统计，固定历史点的16条完整API返回全部通过独立来源核对；global/local平均约154.6/144.9ms。构建877批耗时6.81秒，成本单列。该原型保留图谱当前实体预算和全部原API读取，生产默认继续不变。

侧车已验证静态历史点；连续追赶、发布水位同步、恢复及100-global并发仍需后续接入测试。缓存键绑定**run创建身份、配置/场景、seq、批次SHA**；若将完整API返回缓存，还需包含全部查询参数。各类原型和正式七日联合结果分别保留自己的覆盖范围。


## 货架资源增量：第12个查询包

新run启用 `--resource-graph` 后可查询 `rack_inventory`。提供 `run_id / seq / rack_id`，返回该地图货架完整容量、占用/空位和各槽位的实际货权。旧run缺少资源bootstrap时明确拒绝，不按初始库存推测当前库存。详细入口与同事务/身份说明见 [货架图谱指南](RACK_RESOURCE_GRAPH_GUIDE.md)。
