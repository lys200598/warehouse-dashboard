# 本地 tmux 启动的仓库进程清单

日期：2026-10-30
当前项目目录：`/data/workspace/lys/projects/warehouse-dashboard`
当前分支：`integration/lys-wz-261030`

### 任务背景

用户希望不由代理启动，而是在本地用 tmux 自己拉起仓库所需的全部服务进程，需要明确的进程清单、启动顺序和命令。

### 启动失败根因（2026-10-30 实测）

- 当前目录的 `run_in_lade.sh` 默认环境是 `$ROOT/../warehouse-deploy/.venv`，本机不存在；直接执行会报：
  `Python environment unavailable: .../warehouse-deploy/.venv`
- `4173` 和 `14185` 的失败都是这个原因，不是代码或 Neo4j 问题。
- 解决方式：直接用 `./.venv/bin/python`，或先 `export LADE_ENV_DIR="$PWD/.venv"`。
- Ray 成功是因为直接用了 `./.venv/bin/ray`，没有经过 `run_in_lade.sh`。
- `warehouse-neo4j` 里如果再执行 `neo4j console`，会因为 Neo4j 已经在运行而退出；已有实例不需要重复启动。

### 关键结论

- 必须启动的常驻进程只有 4 类：**Neo4j、Ray head、4173 live、14185 replay**。
- 4173 会自己派生 global writer 子进程；Ray head 会自己派生 Ray actor/worker 进程，不需要单独手动启动。
- `kg.html` 由 4173 直接提供，没有独立前端或 kg API 进程。
- 14183 单 run 回放是可选，不属于当前日常服务。
- 若要接管当前已经运行的服务，先停止旧进程；端口占用时不能重复启动。

### 进程清单

| 进程 | 端口 | 是否必需 | 说明 |
|---|---|---|---|
| Neo4j Java | Bolt 47687 / HTTP 47474 | 图谱/robot-graph/global-writer 必需 | 项目本地 Neo4j |
| Ray head/GCS | 26380 | 使用 `--ray` 时必需 | 100 个 motion actor |
| 4173 live 主进程 | 4173 | 必需 | V5 主进程 + 页面 |
| global writer 子进程 | 无 | 自动 | 4173 启动，写 RunStat/Assist/DispatchState |
| Ray actor/worker | 26400-26599 | 自动 | Ray head 管理 |
| 14185 replay | 14185 | 可选 | 多 run 只读回放 |
| 14183 single replay | 14183 | 可选 | 单 run 帧回放，不用启动 |

### tmux 建议布局

```text
tmux session warehouse-neo4j
  window 0: neo4j console

tmux session warehouse
  window 0: ray head（使用 --block 前台运行）
  window 1: 4173 live
  window 2: 14185 replay（可选）
```

也可以把 Ray 放到单独 session `warehouse-ray`，只要 4173 能连接 `127.0.0.1:26380` 即可。

### Ray token 认证（当前 head 是 token 模式）

当前 26380 的 Ray head 进程环境是 `RAY_AUTH_MODE=token`，但客户端 shell 没有带 token，所以 4173/`ray status` 会报：

```text
InvalidAuthToken: Authentication token is missing or incorrect
```

启动 4173 前必须导出：

```bash
export RAY_AUTH_MODE='token'
export RAY_AUTH_TOKEN_PATH="$HOME/.ray/auth_token"
```

验证：

```bash
RAY_AUTH_MODE=token RAY_AUTH_TOKEN_PATH="$HOME/.ray/auth_token" \
  ./.venv/bin/ray status --address=127.0.0.1:26380
```

### 启动顺序

#### 1. Neo4j（warehouse-neo4j 会话）

```bash
cd /data/workspace/lys/projects/warehouse-dashboard

export WAREHOUSE_NEO4J_HOME="$PWD/.runtime/warehouse-neo4j-5.26.32"
export JAVA_HOME="$(./.venv/bin/python -c 'from jdk4py import JAVA_HOME; print(JAVA_HOME)')"

# 仅当 47687 没有被占用时执行；这是前台进程，tmux 窗口会一直显示日志
exec "$WAREHOUSE_NEO4J_HOME/bin/neo4j" console
```

检查：

```bash
ss -ltn | grep ':47687 '
```

#### 2. Ray head（warehouse 会话，window 0）

```bash
cd /data/workspace/lys/projects/warehouse-dashboard

# 仅当 26380 空闲时执行；--block 让 tmux 窗口持有 Ray head
./.venv/bin/ray start --head \
  --node-ip-address=127.0.0.1 --port=26380 --num-cpus=12 \
  --temp-dir=/tmp/wh-ray \
  --min-worker-port=26400 --max-worker-port=26599 \
  --dashboard-port=18265 --ray-client-server-port=20001 \
  --disable-usage-stats --block
```

检查：

```bash
ss -ltn | grep ':26380 '
```

#### 3. 4173 live（warehouse 会话，window 1）

```bash
cd /data/workspace/lys/projects/warehouse-dashboard

export WAREHOUSE_NEO4J_URI='bolt://127.0.0.1:47687'
export WAREHOUSE_NEO4J_USER='neo4j'
export WAREHOUSE_NEO4J_PASSWORD='test123456'
export WAREHOUSE_NEO4J_DATABASE='neo4j'
export RAY_ADDRESS='127.0.0.1:26380'
export RAY_AUTH_MODE='token'
export RAY_AUTH_TOKEN_PATH="$HOME/.ray/auth_token"

./.venv/bin/python -m backend.warehouse live \
  --source-root . --dist dist \
  --registry config/live-wheeled-datasets.json \
  --dataset-id medium1728 --output-root data/local-runs \
  --mode rules --knowledge-backend journal --mobility wheeled \
  --ray --ray-address 127.0.0.1:26380 --ray-actor-cpus 0.1 \
  --robot-graph --graph-state-interval 4 \
  --global-writer --global-writer-queue 256 \
  --commit-seconds 5 --max-active-robots 100 --port 4173
```

说明：

- 这是前台进程；在 tmux 窗口内按 `Ctrl+C` 可停止 4173 及其 global writer 子进程。
- 不需要单独启动 global writer；它由 4173 自动派生。
- 不需要单独启动前端或 kg API；统一页面由 4173 提供。

检查：

```bash
curl -s --noproxy '*' http://127.0.0.1:4173/api/health
curl -s -o /dev/null -w '%{http_code}\n' --noproxy '*' http://127.0.0.1:4173/live.html
curl -s -o /dev/null -w '%{http_code}\n' --noproxy '*' http://127.0.0.1:4173/kg.html
```

#### 4. 14185 多 run 回放（可选，warehouse 会话，window 2）

```bash
cd /data/workspace/lys/projects/warehouse-dashboard

./.venv/bin/python -m backend.replay_service
```

检查：

```bash
curl -s --noproxy '*' http://127.0.0.1:14185/api/runs | head
```

### 如果使用启动脚本

脚本会 nohup 后台启动，不会让 tmux 窗口持有进程；适合快速启动，不适合“窗口关闭即停服务”：

```bash
bash ops/start_neo4j.sh
bash ops/start_ray.sh
bash ops/start_runtime.sh --ray --robot-graph --global-writer
```

`start_runtime.sh` 默认会同时启动 14185；如果只想手动管理 4173，使用上面的直接命令。

### 当前环境注意事项

- 当前 47687、26380、4173、14185 都已有进程在跑；直接执行上面的命令会端口冲突。
- 如果要接管现有服务，先停止旧进程，再在 tmux 中启动；不要同时运行两套 4173/Ray/Neo4j。
- 4173 的 global writer 错误检查：`curl -s http://127.0.0.1:4173/api/snapshot`，看 `runtime.globalWriter.errors`/`dropped`。
- 早期 14183 单 run 回放不是常驻必需服务，除非专门看单 run 帧。

### 执行过的操作

- 读取 `ops/start_neo4j.sh`、`ops/start_ray.sh`、`ops/start_replay.sh`；
- 读取 `ops/start_runtime.sh` 的 medium/Ray/global writer/replay 分支；
- 检查当前 tmux 会话和进程；
- 验证 Ray `--block`、`live --global-writer` 参数存在。

### 修改内容

未修改运行代码；仅新增本启动说明文档。

### 后续建议

- 本地 tmux 启动后先确认 47687→26380→4173→14185 顺序；
- 如果要长期在 tmux 中管理，建议使用前台命令，而不是 `start_runtime.sh` 的 nohup 方式；
- 如果只关心页面而不用图谱，可去掉 `--robot-graph`/`--global-writer`，并可不启 Neo4j；但当前完整功能需要全部启动。
