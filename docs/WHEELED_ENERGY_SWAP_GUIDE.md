# 轮式运输与有限换电模型

## 任务背景

本轮将仓内移动能力按轮式移动操作机器人建模，并加入有限换电能力。旧版0.8m/s、普通充电和既有冻结实验保持可选，不直接覆盖线上服务。

## 关键结论

- 新增独立`WheeledConfig`与`WheeledSimulation`；速度候选为1.2m/s。空载与载货使用相同上限，路径分段取整后实际均速不超过该值。旧`wide.Config`默认速度仍为0.8m/s。
- 候选保留transport模型：空载15个百分点/小时、载货与取放18个百分点/小时、等待2个百分点/小时，充电80%以下60个百分点/小时，以上30个百分点/小时。参数是轮式运输抽象，未按某一实机标定。
- 类默认12个分散选取的实体泊位兼作换电点，每点4块满电备用电池、2条离线充电通道；独立候选配置 `wheeled-swap-100.json` 与中型推荐配置 `wheeled-medium-100.json` 明确增至每点4条离线通道，共48块备用电池、48条离线通道，保留24通道作敏感性对照。每次换电180秒。与原100个充电/待命泊位复用，不另凭空增加地面空间；离线电池柜由此泊位服务区承载，是待布局核对的空间假设。
- 每块电池有唯一ID：100块装车电池加48块备用电池始终守恒。旧电池在实际换电完成时进入所在站点待充队列；满电备用电池被装车。不同站点之间不调拨电池。
- 备用电池先预留，站点无满电库存或找不到安全完整路径时，回退原普通充电。即使换电模型启用，也保留常规回充、故障与低电接力里的充电路径。
- 进站、180秒静止换电、离站与终点停车都经过原动态连续避碰预留，换电过程中不逐秒给机器人加电；只有完成标记提交时才交换电池。等待与服务期间原电池继续按待机速率耗电，硬应急余量仍约束进站和离站。
- 电池池、装车电池身份、SOC、离线充电进度和预约信息全部进入checkpoint。源码/配置身份不匹配时拒绝跨版本恢复；重规划取消未完成换电则释放预约，已装车电池保持不变。

## 执行过的操作

核对AGENTS.md、V3能耗/调度、Wide实体停车、V5调度和checkpoint链。新增10项测试覆盖有限电池、单充电通道、守恒、取消、普通充电、实际100机器人场景中的完整换电及中途恢复；与旧能耗测试合计27项通过。

另外执行独立CLI 120模拟秒journal烟测，输出data/local-check/wheeled-cli-smoke-20261029。该窗口没有任务发布，证明入口与持久化可运行，不作为任务吞吐、换电能力或零晚点验收。

## 修改内容

- `backend/wheeled/batteries.py`：有限站点电池库存与离线充电状态机。
- `backend/wheeled/sim.py`：可组合`WheeledMixin`及`WheeledSimulation`。
- `backend/wheeled/runner.py`：独立CLI，默认保留original-sipp规划器。
- `backend/scheduling_v3/energy.py`：仅新增`swapping`作为等待耗电阶段；旧默认参数不变。
- `config/wheeled-swap-100.json`：独立1.2m/s和有限换电候选配置。
- `tests/test_wheeled_swap.py`：测试。

## 使用与集成

仓库根目录执行，输出目录必须全新：

```bash
bash run_in_lade.sh python -m backend.wheeled.runner \
  --scene data/layouts/warehouse100-wide-v2.json \
  --dataset data/examples/warehouse-v5 \
  --config-json config/wheeled-swap-100.json \
  --output data/local-check/wheel-swap-new-run \
  --run-id wheel-swap-new-run --planner original-sipp \
  --knowledge-backend journal --commit-seconds 60 \
  --max-wall-seconds 1800 --drain-seconds 14400
```

该独立入口复用WideMirror，负责物理与基础图谱回归。统一平台若需保留完整知识增强逻辑，应按下面组合：

```python
from backend.wheeled.sim import WheeledMixin, WheeledConfig
class WheeledKnowledgeSimulation(WheeledMixin, KnowledgeSimulation):
    pass
```

Mixin透传知识配置kwargs并通过super调用保留step/marker/checkpoint/restore链。统一入口已经接入 `python -m backend.warehouse run --mobility wheeled --robot-agents`；实时 `live` 使用同样开关。基础journal烟测、真实19任务图谱闭环与完整高峰运力分别记录，参见 [本轮对接总览](WAREHOUSE_WHEELED_HANDOFF_20261029.md)。

## 后续建议

1. 在扩大地图上核查电池柜/服务空间，再做完整任务回归，分别报告旧速度普通充电、1.2m/s普通充电、1.2m/s换电；保持数据和期限不变，分离各项收益。
2. 报告换电次数、最大并发换电、各站库存耗尽与回退次数、备用电池充电排队、实际任务晚点和电量最低值。
3. 速度提升但时间能耗保持固定会降低单位距离耗电；这项模型变化须明确记录。对更大地图和高频换电需求，应做速度/耗电/备用电池数敏感性测试。
4. 180秒换电及每站4块电池、2通道是实验资源假设。现实换电机具、规格匹配和维护操作未做机械仿真；任务运行层面的实体占位与能量/库存约束已经加入。

## 组合链审查与修复

修复了预步历史查询拒绝时电池池曾提前走一秒的问题：离线充电现在只在真实步完成后推进；换电完成标记先结算此前充电再接收旧电池。新增required历史授权拒绝后物理状态/电池池不变且checkpoint可恢复、组合MRO中途换电恢复和换电中真实延迟重规划测试。

有限离线充电供给需单独验收：35→100需85分钟，24条通道理想供给约16.94块/小时（此前105分钟估算算术错误已纠正）；若100台一直载货，100→35约3.61小时，需求约27.7块/小时。当前48块初始满电备用会支持短峰值，长时高占用将回退普通充电；不能由短峰值结果宣称长期只换电无需充电。



## 当前扩容对接推荐

独立WheeledConfig的1.2m/s默认保留作基线；本轮完整容量对照后，当前对接推荐为medium 120×90m、48架2304位、1.5m/s、优先换电及48条离线充电通道，采用config/wheeled-medium-100.json。依据和所有负对照见[最终容量只读审计](WHEELED_CAPACITY_AUDIT_20261029.md)。该推荐覆盖已测1728任务峰值，不等于任意负载或24小时完整排空验收。


## 24小时实测发现的服务空间限制

24小时均匀窗口已自然完成：6912释放、6902完成、0已完成晚点，10在途均未过期；这不等于6912排空。运行实际完成25次换电，并178次回退普通充电（178开始、174完成）。在178个实际普通充电预约时点，重放库存均有44—48块满电备用、12/12站均有ready；电池供给不足没有证据支持。共享停车/换电位平均有10.15/12个被稳定idle的homeowner机器人占住，其中12个时点全部12口被占，有直接空间阻塞证据。

其余回退未保存逐候选失败reason，不能把全部178次精确归因于停车。当前普通充电兜底与有限电池守恒继续有效，换电站空间设计仍有改进余地；**不称换电服务布局已经最优**。后续可研究独立换电服务位或停车不占服务口，在明确资源与查询契约后独立实施；本轮不增加112dock或改变100bay合同。

完整原始证据、已证实范围和待归因部分见[24小时窗口审计及回退归因](WHEELED_DAY1_WINDOW_20261029.md)。
