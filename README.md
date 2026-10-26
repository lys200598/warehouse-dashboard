# 100 台人形机器人仓储调度系统

这是一个面向 100 台轻量人形机器人的仓储仿真与调度系统。当前阶段已经包含 Ray 并行机器人进程、统一仿真时钟、任务调度、连续空间交通仲裁、WebSocket 实时状态流和 checkpoint 恢复；Neo4j 持久化将在下一阶段接入。

## 当前架构

```text
浏览器控制台
    │ HTTP 控制 / WebSocket 快照
    ▼
FastAPI SimulationService
    │
    ▼
SimulationCoordinator ── TrafficManager
    │ fan-out / fan-in barrier
    ▼
HR-001 ... HR-100（每台机器人一个 Ray Actor）
    │
    └── 原子 JSON checkpoint
```

- `SimulationCoordinator` 是仿真时钟、任务、货架和格位状态的唯一权威来源。
- 每台机器人由一个串行 `Ray Actor` 保存独立状态；`local` 后端使用完全相同的接口，便于调试和确定性测试。
- 每个 tick 先并行生成机器人行动提案，再由 `TrafficManager` 仲裁，最后统一提交。
- 默认 100 台机器人在线、最多 20 台同时在途，避免作业区过载；该上限可以配置。
- 前端通过 WebSocket 接收快照。连接后不再在浏览器中自行推进状态，断线时冻结并自动重连。
- checkpoint 包含机器人、任务、货架格位、调度状态、随机数状态和仿真时钟，可确定性续跑。
- Ray barrier 失败时会从最近 checkpoint 重建全部 Actor；连续恢复超过阈值后进入暂停状态，避免半提交运行。

## 安装与启动

建议使用 Python 3.11+：

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

启动 100 个 Ray Robot Actor：

```bash
python -m backend.server \
  --backend ray \
  --robot-count 100 \
  --max-active-robots 20 \
  --ray-actor-cpus 0.1 \
  --port 4173
```

本机 Ray 集群至少需要满足 `robot_count × ray_actor_cpus` 的逻辑 CPU 资源。以上配置需要 10 个 Ray CPU slot，但不代表为每台机器人绑定独占物理核心。

当前机器也可以复用 LaDe 的 Conda 环境：

```bash
/data/zc/datasets/LaDe/viewer/run_in_lade.sh \
  python -m backend.server --backend ray --robot-count 100 --port 4173
```

浏览器访问：

```text
http://127.0.0.1:4173
```

快速调试时可以使用单进程后端：

```bash
python -m backend.server --backend local --robot-count 100
```

## checkpoint 与恢复

默认每 20 个 tick 原子写入：

```text
data/checkpoints/warehouse-latest.json
```

从该 checkpoint 恢复：

```bash
python -m backend.server --backend ray --resume
```

可以通过以下参数调整：

```text
--checkpoint-path PATH
--checkpoint-every-ticks N
--step-seconds 0.25
--wall-interval 0.25
--flow-rate 48
--traffic-safety-radius 0.12
--ray-recovery-attempts 3
--ray-recovery-delay 1
```

`traffic-safety-radius` 当前是仿真坐标中的预留半径，不应直接视为真实机器人的物理安全距离。连接实机前必须结合地图比例、机器人外形和制动距离重新标定。

## 控制和状态接口

- `GET /api/health`：运行状态、后端和 Worker 进程数量
- `GET /api/snapshot`：当前完整快照
- `POST /api/control`：暂停、继续、倍速、流量和重置
- `POST /api/tasks/priority`：插入优先任务
- `POST /api/checkpoint`：立即保存 checkpoint
- `WS /ws/simulation`：实时仿真快照

## 测试

核心测试只依赖 Python 标准库：

```bash
python -m unittest discover -s tests -v
node --check dist/app.js
```

## 目录

```text
backend/
├── domain.py       # 机器人状态机、路线和可序列化状态
├── runtime.py      # local/Ray 双运行时与 Robot Actor
├── traffic.py      # 每个 tick 的交通仲裁
├── simulation.py   # Coordinator、调度、货架和任务
├── checkpoint.py   # 原子 checkpoint 读写
└── server.py       # FastAPI、WebSocket 和启动入口
dist/
├── index.html
├── styles.css
└── app.js          # 后端快照渲染及离线演示回退
tests/
```
