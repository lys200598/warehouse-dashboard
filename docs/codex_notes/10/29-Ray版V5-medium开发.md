# Ray 版 V5 medium 开发

日期：2026-10-29
当前 4173：`http://127.0.0.1:4173/`
当前 runId：`wh-live-20261029T154256Z-40608975`

## 任务背景

用户要求为当前 120×90 medium 布局开发 Ray 版本，并明确 **保留 V5 调度**。原 legacy Ray 控制台是 `backend.server --backend ray` + `backend/runtime.py`，绑定 32×22 旧控制台，不能直接套到 medium V5 上。

## 关键结论

- 当前 4173 已运行 Ray 版：`backend=warehouse-real-scale-v1-ray`、`rayActorCount=100`、场景仍是 120×90/48 架/2304 货位。
- V5 派单、SIPP 交通预留、库存、换电、能量准入和知识写入仍由中央 `WideSimulation` 负责，没有被 Ray 替换。
- 100 个 Ray Actor（默认 `num_cpus=0.1`，共 10 CPU）只执行每台机器人每个 tick 的**运动段计算**；这是保留 V5 语义的最小 Ray 接入层。
- 同一 medium sample19 跑 5 tick，local 与 Ray 的机器人 `x/y/stage/task_id/battery/carrying` 对比 **0 差异**，证明没有改变 V5 物理/调度结果。
- `ops/start_runtime.sh --ray` 和 `RAY=1 bash ops/start_medium.sh` 已可用；原生命令加 `--ray --ray-address 127.0.0.1:26380 --ray-actor-cpus 0.1`。
- 当前 Ray Actor 是运动执行 actor，不是 legacy 那种持有完整 `RobotAgent` 状态并自行 propose 的 actor；如果下一步要做“每个 actor 完整持有机器人状态/电量/任务”，需要继续把 V5 的 energy/charge 逻辑迁到 actor 边界。
- 当前改动未提交 Git。

## 执行过的操作

- 阅读 `backend/scheduling_v3/sim.py` 的 `step()`、`backend/wide/sim.py`、`backend/wheeled/sim.py`、`backend/warehouse/composition.py`、`ops/warehouse_live_runtime.py`、`ops/warehouse_live.py`。
- 阅读 legacy `backend/runtime.py` / `backend/server.py` 的 Ray 入口，确认其绑定旧 `SimulationCoordinator`，不能直接复用。
- 新增 `backend/ray_medium/`：
  - `actors.py`：`MotionActor` + `RayMotionPool`。
  - `sim.py`：`RayWideMixin` + `RayWheeledKnowledgeSimulation`。
- 在 V3 `step()` 加入可选 `motion_executor` 接缝；默认 `None` 时行为完全不变。
- `backend/warehouse/composition.py` 增加 `wheeled_ray`。
- `ops/warehouse_live_runtime.py` 增加 `ray/ray_address/ray_actor_cpus`，identity 记录 Ray 配置。
- `ops/warehouse_live.py` 增加 `--ray`、`--ray-address`、`--ray-actor-cpus`。
- `ops/start_runtime.sh` 增加 `--ray`；`ops/start_medium.sh` 增加 `RAY=1`。
- 在 14584 启动 medium19 Ray 测试，确认 100 actor、`backend=...-ray`、可推进 tick；随后停止。
- 用 `RAY=1` 方式在 4173 启动 medium1728 Ray 版，并 `POST /api/control {"action":"start"}`。
- 运行 27 项相关测试，全部通过。
- 验证 `/kg.html` 仍返回 200，知识图谱入口保留。

## 修改内容

- `backend/ray_medium/actors.py`：100 个 Ray 运动 actor 和批处理池。
- `backend/ray_medium/sim.py`：Ray V5 wheeled 组合，`frame()` 上报 `backend`/`rayActorCount`。
- `backend/scheduling_v3/sim.py`：可选运动执行接缝；local 路径不变。
- `backend/warehouse/composition.py`：`physical_classes('wheeled_ray')`。
- `ops/warehouse_live_runtime.py`：Ray 参数、identity、模拟类选择。
- `ops/warehouse_live.py`：CLI `--ray` 等参数。
- `ops/start_runtime.sh` / `ops/start_medium.sh`：Ray 启动入口。
- `README.md`：120+Ray 使用说明。

## 当前验证

```text
GET /api/health
  runtime status=running, dataset=medium1728
GET /api/snapshot
  backend=warehouse-real-scale-v1-ray
  rayActorCount=100
  tick=4, running=True
GET /kg.html
  200
ray status
  10.0/12.0 CPU（100 × 0.1 CPU）
```

local vs Ray 5 tick：

```text
bad 0 []
ray backend warehouse-real-scale-v1-ray actors 100
```

## 后续建议

1. 先跑一段 1728 任务 Ray 版，观察 tick 速率和 100 actor 调用开销。
2. 下一阶段把电量/充电/换电状态也迁到 actor 边界，向 legacy “actor 持有机器人状态”靠拢。
3. 增加 `tests/test_ray_medium.py`（无 Ray 时 skip），并决定是否把 Ray 设为 medium 默认启动。
4. 当前改动尚未提交；确认后执行 `git add` / `git commit`。
