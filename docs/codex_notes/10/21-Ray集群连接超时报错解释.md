# Ray 集群连接超时报错解释

### 任务背景

用户在当前仓储机器人项目中启动仿真，命令指定 `--backend ray --ray-address 222.20.99.83:38187`，连续出现 GCS 连接超时日志。本次工作仅检查代码、环境和服务状态，解释报错及可选启动方式，不修改业务代码，不启动或停止服务。

### 关键结论

- 这些日志描述同一个问题：Ray 初始化时无法连接指定地址的 GCS，尚未进入机器人 Actor 创建和仿真循环。
- `222.20.99.83` 是当前机器 `eno1` 网卡的 IP。2026-10-21 本次检查时，`38187` 没有 TCP 服务监听，进程检查也没有发现 `gcs_server` 或 `raylet`。这支持“指定地址当前没有运行中的可用 Ray 集群”的判断；不能仅凭这些结果确定此前集群为何退出或当时端口是否不同。
- `--ray-address 222.20.99.83:38187` 表示连接已有集群，不会自动在该端口创建集群；连接失败时也不会自动转为新建本地集群。
- GCS 是 Global Control Service，负责 Ray 的集群元数据。`Failed to get cluster ID` 是连接不可用导致无法取得集群标识，并非要求用户手工配置 cluster ID。
- 日志没有显示 CPU 或内存不足。`100 × 0.1 = 10` 个 Ray 逻辑 CPU 是后续调度资源要求，与当前 GCS 连接失败属于不同阶段；`--max-active-robots` 不会减少初始化时需要创建的机器人 Actor 数。
- 本次检查的 `lade` 环境为 Python `3.13.13`、Ray `2.55.1`，`RAY_ADDRESS` 未设置。当前目录没有 `./run_in_lade.sh`，因此 Python 查询使用用户命令中的 `/data/zc/datasets/LaDe/viewer/run_in_lade.sh`。
- 一个 Ray 集群可以同时运行多个仿真任务，不存在“同时只能被一个仿真使用”的限制。共享时需要足够的集群资源，并为每个仿真指定不同的网页端口和 checkpoint 路径。

#### 日志逐条解释

| 日志 | 含义 |
| --- | --- |
| `INFO worker.py:1814 -- Connecting to existing Ray cluster at address: 222.20.99.83:38187...` | 普通信息：程序开始尝试连接指定的已有集群。这句话不代表连接已经成功。 |
| `W ... rpc_client.h:153: Failed to connect to GCS ... within 5 seconds.` | 警告：一次等待连接 GCS 的过程在 5 秒内未成功。可能原因包括目标服务未启动、地址或端口错误、服务退出、网络阻断或服务无响应。本次检查直接发现目标端口没有服务监听。 |
| `W ... gcs_client.cc:205: Failed to get cluster ID ... TimedOut ...` | 警告：等待 GCS 可用并获取集群标识超时，是前面连接失败的后续表现。 |
| 再次出现 `Failed to connect to GCS ... within 5 seconds.` | Ray 再次尝试连接，但仍未成功，并非出现了一种新的报错。 |

`W` 表示 Warning。`rpc_client.h:153` 和 `gcs_client.cc:205` 是 Ray 内部 C++ 源文件的位置，不是当前项目需要直接修改的文件。日志中的两个 `1960394` 是进程及线程标识。

#### 当前代码中的调用关系

```text
backend.server.main()
  -> create_app(settings)
  -> SimulationService(settings)
  -> build_runtime("ray", ray_address=...)
  -> RayRobotRuntime.__init__()
  -> ray.init(address="222.20.99.83:38187", ignore_reinit_error=True)
     当前卡在这里
```

`backend/runtime.py` 的初始化逻辑会把 `--ray-address` 原样交给 `ray.init()`。`backend/server.py` 在 `uvicorn.run()` 调用前求值 `create_app(settings)`，所以这次启动尝试在 Ray 初始化期间尚未开始监听网页端口。

端口需要区分：当前命令中的 `38187` 被当作 Ray GCS 端口；`backend.server --port 4173` 是当前项目的 HTTP/WebSocket 服务端口。Ray 官方默认 GCS 端口为 `6379`，Ray Client 默认端口为 `10001`，但集群可以使用自定义端口，不能因此直接把 `38187` 改成某个默认值。

#### 补充：多个仿真能否共用集群

可以。Ray 官方 [集群 FAQ](https://docs.ray.io/en/master/cluster/faq.html) 明确支持多个任务同时在同一集群运行。当前项目的机器人 Actor 未设置共享的固定名称，每个后端进程会创建自己的机器人 Actor。

以用户当前参数为例，一个仿真的 100 个机器人 Actor 合计申请 `100 × 0.1 = 10` 个 Ray 逻辑 CPU。两个相同规模仿真的全部 Actor 同时运行，合计需要可用的 20 个逻辑 CPU，另外还要满足实际内存等运行需求。资源不足时，部分 Actor 会等待调度，项目在等待 Actor 返回状态时可能表现为启动卡住；这种情况与 GCS 连接失败不同。参考 [Ray 资源调度规则](https://docs.ray.io/en/latest/ray-core/scheduling/resources.html)。

在同一机器运行两个本项目后端时，需要分别设置：

| 参数 | 仿真 A 示例 | 仿真 B 示例 |
| --- | --- | --- |
| `--ray-address` | 同一个已启动集群的 GCS 地址 | 与 A 相同 |
| `--port` | `4173` | `4174` |
| `--checkpoint-path` | `data/checkpoints/sim-a.json` | `data/checkpoints/sim-b.json` |

不同端口避免 HTTP 服务绑定冲突，不同 checkpoint 路径避免两个仿真互相覆盖快照。若两次都使用 `--ray-address local`，则各自新建本地集群，不是在复用同一个集群。共享集群的任务会竞争底层机器资源，逻辑 CPU 申请不等于严格的物理 CPU 隔离。

### 执行过的操作

- 阅读 `AGENTS.md`、`README.md`、`requirements.txt`、`backend/runtime.py`、`backend/server.py`、`backend/checkpoint.py` 和 LaDe 的环境启动脚本。
- 使用 `rg --files`、`rg -n`、`sed -n`、`cat` 检索启动参数、运行时初始化和服务启动顺序。
- 使用 `git status --short` 了解现有工作区状态，保留已有文件修改。
- 使用以下只读命令检查网络和进程：

  ```bash
  ss -ltnp '( sport = :38187 or sport = :6379 or sport = :10001 or sport = :4173 )'
  ip -brief address
  pgrep -a -x gcs_server
  pgrep -a -x raylet
  ```

  `ss` 与 `ip` 在沙箱内受到权限限制后，已通过工具审批在沙箱外执行。目标端口查询只有表头，没有监听条目。

- 使用以下命令查询环境版本，未调用 `ray.init()`：

  ```bash
  /data/zc/datasets/LaDe/viewer/run_in_lade.sh python -c 'from importlib.metadata import version; import os, sys; print("Python:", sys.version.split()[0]); print("Ray:", version("ray")); print("RAY_ADDRESS:", os.environ.get("RAY_ADDRESS", "<unset>"))'
  ```

- 检查 `/tmp/ray` 和 `session_latest`。历史会话目录存在，但不代表对应集群仍然运行；`/tmp/ray/ray_current_cluster` 在检查时不存在。
- 检索当前安装的 Ray `worker.py` 和 `services.py`，确认指定地址、未指定地址及 `address="local"` 的行为。
- 核对 Ray 官方文档：[GCS 定义](https://docs.ray.io/en/latest/ray-references/glossary.html)、[ray.init 地址规则](https://docs.ray.io/en/latest/ray-core/api/doc/ray.init.html)、[Ray 端口配置](https://docs.ray.io/en/latest/ray-core/configure.html)、[Ray Client 连接方式](https://docs.ray.io/en/latest/cluster/running-applications/job-submission/ray-client.html)。

### 修改内容

仅新增本总结文件。未修改业务代码、配置或依赖，未启动仿真、创建 Ray 集群、停止任何进程或写入 checkpoint。本次没有代码变更，未运行仿真测试。

### 后续建议

1. 如果目标是直接在当前机器运行，可以把原命令的 `--ray-address 222.20.99.83:38187` 改为 `--ray-address local`。这会要求 Ray 创建新的本地实例，仍然使用 Ray 并行后端；它与 `--backend local` 单进程后端含义不同。该行为已核对当前安装的 Ray 源码及官方地址规则。
2. 下面是供用户选择执行的完整本机 Ray 启动命令，本次未执行。使用单独的 checkpoint 路径，避免首次启动写入现有的 `warehouse-latest.json`；如果这个新路径已用于其他实验，请先另选文件名。

   ```bash
   /data/zc/datasets/LaDe/viewer/run_in_lade.sh \
     python -m backend.server \
     --backend ray \
     --ray-address local \
     --robot-count 100 \
     --max-active-robots 100 \
     --ray-actor-cpus 0.1 \
     --checkpoint-path data/checkpoints/warehouse-ray-local-check.json \
     --port 4173
   ```

3. 如果确实要复用固定端口的已有集群，应先核实其实际启动地址和 GCS 端口，并确保集群已运行，再传给 `--ray-address`。目前不能假设 `38187`、`6379` 或 `10001` 任意一个端口已有可用服务。
4. 当前无需用户补充信息即可解释这些日志；若后续要求实际启动，再根据“新建本地 Ray”或“连接已有集群”的目标处理服务和 checkpoint。
