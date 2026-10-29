# 仓库与数据同学对接：扩大布局、轮式运输、换电和独立机器人读写

## 1. 本轮负责的内容

这轮把仓库仿真作为共享知识实验的载体继续完善：扩大可工作的库区，采用轮式移动操作机器人的运输能力，增加有限换电模型，让每台机器人以独立进程写入自身观测并查询知识，同时提供独立压测与来源验真工具。李姚舜负责新数据集及知识图谱设计；双方通过地图、七字段任务、资源编号和运行接口对接。

## 2. 仓库怎么选

推荐继续采用中型布局 `data/layouts/expansion-candidates/medium/layout.json`：

|内容|原100台宽松图|上轮balanced|本轮medium|
|---|---:|---:|---:|
|长×宽|90×76m|100×90m|120×90m|
|面积|6,840m²|9,000m²|10,800m²|
|货架|36|40|48|
|货位|1,728|1,920|2,304|

每架3层、2面、每面8列，共48格。普通巷道5m、主通道8m。西侧8个入库工位、东侧8个出库工位，南北两侧共100个充电兼待命泊位；P和C编号指向同一实体位置。机器人机体、安全半径、货架碰撞和停车占用规则保留。静止机器人有实体，空泊位硬件可通过。

medium相对balanced面积与货位都增加20%，原36架及其货位编号保留，坐标改变。扩大到150×100m、60架的版本在同任务、同期限对照中出现明显晚点，保留为负面结果，不作为推荐。

同一小时1,728任务覆盖所有48架，1.2m/s换电组全部完成、0晚点，平均流转419.1秒、结束4212模拟秒；1.5m/s换电组全部完成、0晚点，平均408.6秒、结束4152秒，最小期限余量112秒。它们使用相同的任务时间，货位映射单独记录，均保持原截止时间。上轮balanced0.8m/s本轮对照平均379.5秒、0晚点：扩仓增加了运输距离，提速和补能帮助控制了损失。该结论覆盖已测高峰，不给任意未来到货强度作零晚点保证。

审计细节与负结果见 [容量审计](WHEELED_CAPACITY_AUDIT_20261029.md)。业务全部完成时个别机器人仍可能在补能/返回，完成任务和全车停妥分别统计。10秒轨迹采样检查与运行器逐秒安全检查分别记录，采样不冒充独立连续轨迹证明。

## 3. 数据集怎样接入自由口

任务仍采用七字段：

```csv
task_id,task_type,goods_id,release_time,due_time,source_id,target_id
T-IN-1,INBOUND,G-1,2026-10-29T08:00:00+08:00,2026-10-29T08:30:00+08:00,INBOUND,R01-L1-C1-F1
T-OUT-1,OUTBOUND,G-1,2026-10-29T10:00:00+08:00,2026-10-29T10:30:00+08:00,R01-L1-C1-F1,OUTBOUND
```

`INBOUND`与`OUTBOUND`是仓库边界的逻辑角色。数据同学可以专注什么时候有多少货进来、出去以及目标货位。仿真在受理/释放时根据可用资源选择现有8个实际工位并固定该次选择，保留可追溯记录。已指定的具体IN/OUT编号仍受支持；使用自由口时配置 `port_selection_policy=balanced`。

真实工位数量、每口6件入库暂存、货位预留、箱货唯一性仍限制执行；逻辑口没有增加无限入口或解除仓容。详见 [自由口指南](FREE_BOUNDARIES_GUIDE_20261029.md)。

给李姚舜的对接清单：

1. 新数据manifest填写medium地图SHA和对应版本，使用它的位置表 `task-locations.csv`。
2. 明确全2,304格中哪些参与初始库存、哪些参与任务；按空间热点生成时使用新坐标。
3. 使用逻辑INBOUND/OUTBOUND端点，沿用任务与货物身份、完整时区时间，按容量和先入后出依赖生成。
4. 需要依据距离产生期限时注明参考速度与裕量，新生成的数据期限应在运行前确定。
5. 旧七天全量参考数据已提交到 `data/examples/seven-patterns-20261028/`，共50,112任务，详见 [七类波动设计](MULTIDAY_DATASET.md)。它配对旧36架地图与具体工位，保留作为对照；新数据由你按新地图生成即可。

货位数量、进出分布与期限属于场景输入；机器人电量、换电、故障和协作策略由运行配置控制。数据集可以持续多天，默认七天是参考样例。

## 4. 轮式速度与换电

- `config/wheeled-medium-100.json`：1.5m/s轮式速度、100台混合能力机器人、自由口、12处换电站。
- `config/wheeled-swap-100.json`：1.2m/s对照候选，旧0.8m/s及普通充电配置继续可用。
- 每站4块备用电池、4条离线充电通道：合计48备电池、48离线通道；100块装车电池加48备电池身份总数守恒。
- 换电180秒，进站、服务和离站占用真实避碰资源。无满电库存或无安全路径时回退普通充电。电池按本站有限队列补能，站间不瞬移电池。
- 能耗沿用轮式transport假设，未按实机Wh标定。1.5m/s参考了[欣奕华SIBOT12官方满载速度](https://www.sineva.com/product/detail/?cid=123&id=4851)，不表示当前仿真对应整台SIBOT12硬件。

24小时均匀日窗口已验证，详见 [补能窗口与停车占口归因](WHEELED_DAY1_WINDOW_20261029.md)：6912释放、6902完成、0晚点，10在途未过期。模型中换电点复用充电/待命泊位，低负载时会被停车机器人占住；这次25次换电完成、178次普通充电开始，回退时满电备用仍充足。当前普通充电兜底有效，换电服务位与停车位分离是下一项布局优化；没有把它写成已经解决。

模型与恢复方法见 [轮式换电指南](WHEELED_ENERGY_SWAP_GUIDE.md)。48离线通道的选择另做了 [12小时及稳态供给敏感性](BATTERY_SUPPLY_SENSITIVITY_20261029.md)，该工具只分析电池供给，不含业务交通。电池柜物理服务空间仍是独立布局假设，后续落地应提供尺寸与障碍几何；本轮保留空间与能量假设标记。

## 5. 每个机器人进程怎样写、怎样查

```text
中央物理/调度进程 → 发布已提交的真实机器人快照
                         ↓ 各自的身份绑定连接
100个独立机器人进程 → 自己的观测/事件 → 共享知识服务 → 批量图谱事务
100个独立机器人进程 ← 自己历史、机器人/任务上下文 ← 共享知识服务
中央调度器          ← 既有历史知识授权、资源和协作查询 ← 图谱
```

中央进程继续掌握任务分配、避碰预留和权威货权事务。每个机器人进程持自己的token，独立发HTTP请求提交观测，收到图谱提交ACK后再查自己的上下文；没有Neo4j密码。新增 `WHAgentObservation`、`WHAgentRuntimeEvent` 追加事实，关联既有run和robot，不各自争写全局head或改写别人的货物。

共享服务做有界批写、幂等序号、持久journal、查询并发预算和失败闭锁。仿真按变化/周期触发查询，低电、状态或任务变化立即触发，周期请求按机器人错相。观测查询返回可用建议；中央原有required模式继续用历史事实授权实际低电接力。这两条调用链都保留证据，各自明确用途。当前100进程是独立OS进程，运动规划仍由中央协调。

详见 [机器人读写接口与完整证据](ROBOT_AGENT_IO_GUIDE.md)。

与李姚舜合并时注意：当前适配器匹配既有 `WHRun/WHRobot/WHAgentState`，新增观察/事件关联这些权威节点。若合并分支采用 `Run/Robot/AgentState` 等另一套标签与key，需要在 `backend/agent_runtime/service.py` 的 `Neo4jBackend.write/query` 以及上下文provider中显式映射，并重跑源验真；不要把两个标签体系当成已自动互通。HTTP协议和进程部分可以保留。

完整高峰的联合验证见 [1728任务与100独立进程回归](ROBOT_AGENT_PEAK_REGRESSION_20261029.md)：100台都完成任务、实际业务活跃峰100；16,193次自身查询均小于500ms，完整读写源验真通过。进程数、业务峰和API在途峰分别观测：混合读写API峰100主要发生于初始化，纯查询峰67；静态百万重查询的实际纯查询峰100由另一批独立压测证明。

## 6. 统一运行入口

新run与新输出目录：

```bash
bash run_in_lade.sh python -m backend.warehouse run \
  --scene data/layouts/expansion-candidates/medium/layout.json \
  --dataset data/examples/wheeled-medium-peak \
  --config-json config/wheeled-medium-100.json \
  --output data/local-runs/medium-agents-new --run-id medium-agents-new \
  --mobility wheeled --robot-agents --resource-graph \
  --mode required --knowledge-backend neo4j --commit-seconds 5 \
  --agent-query-seconds 60 --agent-observation-seconds 30 --frames
```

Neo4j凭证放环境变量，不放Git。原 `run` 默认仍为legacy运动与中央写入；新能力由显式开关选择。实时入口 `python -m backend.warehouse live --help` 使用同样的transport/agent开关，加载原来的 `dist/index.html`，相机交互保留。新版本使用新输出和新run，原冻结检查点配对原源码恢复。

分支同时附上已测medium高峰和19任务原输入，克隆即可选用。实时页可这样启动（端口与输出自行选择未占用的新值）：

```bash
bash run_in_lade.sh python -m backend.warehouse live \
  --registry config/live-wheeled-datasets.json \
  --output-root data/local-runs/medium-live-new \
  --mode required --knowledge-backend neo4j --resource-graph \
  --mobility wheeled --robot-agents --commit-seconds 5 \
  --agent-query-seconds 60 --agent-observation-seconds 30 \
  --query-acceleration summary --port 14192
```

## 7. 压测怎么用

见 [成熟压测入口](LOAD_TEST_GUIDE.md)：

- `ops/load_harness.py`：100个真实OS客户端，闭环同步、周期错峰、固定到达率三种模型，完整请求/响应保留。
- `ops/load_probe.py`：服务端真实API区间，区分客户端数量与实际API峰值。
- `ops/run_query_ab.py`：同一真实百万前缀的可复现A/B，冷、重突发、轻突发、正常混合负载分别统计。
- `ops/audit_robot_agent_io.py`：结束后对机器人完整JSON、原journal和图谱事实验真；验真不在请求热路径运行。
- `ops/audit_wheeled_capacity.py`：箱货、能量、覆盖、最终状态和轨迹采样审计。
- `ops/audit_wheeled_day_window.py`：长时窗口的已释放/未到期/未来任务分层、箱货与电池事件链审计；该独立工具额外2项专项测试通过并完成24h实际审计，未把它冒称冻结929项测试时已有的字节。

完整HTTP到JSON解析时间、服务端排队、后端查询、离线验真分别报告。失败、超时、冷请求和>500ms样本保留。百万规模、100真实并发、持续写入、控制链覆盖及500ms尾延迟分别给资格，任何一个结论都不能替代另一个。

**仓库设计还需优化以配合查询优化**：后续主要协调观测发布周期、任务阶段事件和查询触发频率，让机器人按需取上下文、让中央资源查询复用同一提交点。当前保留5秒权威提交、30秒机器人观测和60秒周期查询的错相参考配置；状态/任务变化仍即时触发。改变这些频率要声明新的负载，不能把减少请求后的结果当作原100重查询同步突发通过。

## 8. 通知李姚舜时可以直接发

> 我把完整七类波动参考数据推到了 `wz/warehouse-design` 的 `data/examples/seven-patterns-20261028/`。仓库侧推荐120×90米、48架2304货位、100机器人，七字段CSV支持逻辑INBOUND/OUTBOUND，具体工位由仿真选，容量约束继续保留。你先看 `docs/WAREHOUSE_WHEELED_HANDOFF_20261029.md`、`docs/FREE_BOUNDARIES_GUIDE_20261029.md` 和 `docs/MULTIDAY_DATASET.md`，新数据用medium地图与位置表、更新manifest哈希。机器人速度/补能与独立进程读写由仿真配置控制，你不需要在任务CSV中安排。图谱的货架/货物设计由你主导，我们这边新增的机器人观测节点与权威货权分开，可按接口关联。

## 9. 原件与Git中的SHA

运行代码、CSV、JSON及压缩原件按字节验证。部分说明Markdown由Git按现有属性做CRLF→LF转换，制作时的文档SHA与Git blob SHA可能不同，发布时单独记录这类换行归一化；它不改变实验输入、生产源码或审计JSON。原始证据、旧失败、原源码树都保留，原Git提交可用于回看冻结版本。
