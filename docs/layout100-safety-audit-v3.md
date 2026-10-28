# 仓库 100 台布局：v3 安全审计与运行约束

## 任务背景

本次为二维仓库仿真补齐“布局先保证可运行”的证据，保留原地图及 v2 调度实验，不把几何/图论检查写成完整工业安全认证。审计对象为 `data/layouts/warehouse100-v1.json`，SHA-256 为 `744e69309c86d9ad5589805a0f5a7e04bd32347ce5f374afaa9efbf731816960`。

## 关键结论

- 60×48m 原地图的全部 100 停车位、24 充电位、1728 货位和 16 个边界取放口，在保留 0.55m 规划半径下均可接入公共路网。1868 个位置 ID 对应 716 个不同的接近位置/私有进出语义；不同货架层共享同一地面接近位置，不允许据此同时重叠作业。
- 公共骨架含 2976 个节点、5582 条无向边，整体连通、割点 0、桥 0。因此删除单个图节点不会把该图切成两块；这不是任意面积障碍的保证。
- 0.7×0.7m 载货外形的外接圆半径约 0.494975m，包含在 0.55m 规划包络内。所有停车/充电中心可容纳该包络；相邻中心最小距离约 1.2m，高于双机器人 1.1m 阈值。全部停车/充电中心的静止机器人包络不侵入公共路网边。
- 3642 个单停障碍探针中，没有发现未被障碍直接覆盖的货架、入库口或出库口被间接隔离；仅有 12 个探针让一个无业务端点的边角图节点脱离主图。不能把这一结果扩张为任意位置、任意数量故障都可绕行。
- 一些停车/充电私有入口可以被局部故障挡住。340 个探针间接遮住至多 4 个端点，其中 126 个探针涉及停车位，214 个涉及充电位。对应业务应等待、换可达充电位或受控降级，不能穿越故障车；“全部端点始终可用”的审计字段明确为 false。
- 576 个货架地面接近点与 16 个边界取放口均存在静态合法的相邻货物交接候选；这只证明几何可行，接收机器人路由、持有者转移、服务时间和电量仍需执行层验证。

## 执行过的操作

新增标准库审计模块和测试，没有改写原场景：

```text
backend/scheduling_v3/layout.py
tests/test_scheduler_v3_layout.py
```

主要接口：

```python
from backend.scheduling_v3.layout import audit_layout, audit_roadmap, handover_candidates

# 初始几何、连通性与私有泊位检查；可用于仿真启动门禁。
report = audit_layout(scene_path, circle_probes=False)
assert report['initial_layout_usable']

# 加上全部路网节点和独立端点中心的有限单停障碍探针。
full_report = audit_layout(scene_path, circle_probes=True, handover_probes=True)

# 返回几何候选，不会预留轨迹或改变场景。
candidates = handover_candidates(roadmap, donor_point)
# [{'point': {'x': ..., 'y': ...}, 'distance_m': 1.2,
#   'connections': [roadmap_node, ...]}, ...]
```

独立命令会拒绝覆盖已有输出：

```bash
./run_in_lade.sh python -m backend.scheduling_v3.layout \
  --scene data/layouts/warehouse100-v1.json --handover-probes --output NEW_AUDIT_PATH.json
```

Windows 本机没有 Bash wrapper，因此测试使用已配置的 bundled Python，未安装依赖：

```powershell
& 'C:/Users/vsx/.cache/codex-runtimes/codex-primary-runtime/dependencies/python/python.exe' `
  -m unittest tests.test_scheduler_v3_layout -v
```

8 项测试通过，涵盖真实布局端点/包络、实际无割点、窄链停障碍反例、一般图割点/不连通反例、交接位置和连接不穿越 donor 包络、无合法空间时返回空候选、跨货架层接近点的几何去重。

## 修改内容

### 圆形停障碍探针的准确口径

假定静止故障机器人同样采用 0.55m 包络，其他机器人中心须避开其中心 1.1m。针对所有去重公共节点和端点接近中心，删除与该圆相交的公共边、受占节点及端点连接，检查剩余主连通分量。

| 项目 | 结果 |
|---|---:|
| 去重探针中心 | 3642 |
| 图分裂的探针 | 12 |
| 主连通分量外最多剩余节点 | 1 |
| 未被直接占用的业务端点间接隔离 | 0 |
| 未被直接占用的停车/充电端点间接隔离探针 | 340 |
| 每探针间接隔离端点最多数量 | 4 |

直接被故障车占住的取放点不可继续服务，是局部工位暂停，不是允许其他车从故障车身上穿过去。该探针覆盖位置有限，粗图也可能保守地拒绝某条连续空间可行路线；结果既不能证明所有连续空间位置可恢复，也不能将粗图隔离直接宣告为物理不可恢复。

### 相邻交接的准确口径

模块默认搜索 32 个方向、中心距 1.2/1.35/1.5m。返回的接收点：

- 位于仓库边界内；载货规划圆不穿货架、暂存区或私有泊位。
- 与 donor 中心距离至少 1.1m。
- 有至少一条公共路网连接，连接线在静态几何上合法，并始终避开 donor 的 1.1m 相对占用圆。
- 只返回候选，不移动任一机器人，也不转移货物。

覆盖审计使用 16 方向、中心距 1.2/1.5m，592/592 个业务接近点存在候选。没有把所有任意中途位置或私有泊位都算作已通过。若某停顿点没有合法候选，应在准入/故障模型中处理，不能假设故障后总能即时交接。

本机完整证据：

```text
C:/E/projects/warehouse/research/scheduler-v3-layout-20261027/audit-complete.json
C:/E/projects/warehouse/research/scheduler-v3-layout-20261027/handover-geometry.json
C:/E/projects/warehouse/research/scheduler-v3-layout-20261027/audit-reproducible-full.json
```

## 后续建议

1. 启动时以 `initial_layout_usable` 作为基础门禁，而不是以圆探针所有私有端点均可用作为不可能的条件。
2. 暂停的充电口与停车口应作为真实障碍/不可用资源处理；禁止在入口附近以“静止不算占用”方式消除故障。
3. 动态执行仍必须原子提交相关机器人轨迹、处理延迟占用和持续进展。图连通检查不能代替防死锁机制。
4. 低电量预防应优先确保能到达可用充电位；相邻接力属于有几何、时间与电量条件的恢复手段，不是无限救援能力。
5. 本轮没有证明原图必须重画，故保留双方已共享的地图与位置 ID。后续若扩充充电位、修改人体/载货外形或实机运动约束，必须重新生成审计与数据地图哈希，不沿用本次通过结论。
