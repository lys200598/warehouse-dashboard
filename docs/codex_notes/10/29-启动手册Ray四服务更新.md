# 启动手册 Ray 四服务更新

日期：2026-10-30

## 任务背景

按当前 Ray 必需路径修订根目录《启动与图谱数据操作手册》，明确 4 个服务、各自启动脚本，以及 Neo4j/回放为可选。

## 关键结论

- Ray 必需路径共 4 个服务：Neo4j、Ray head、4173 主进程、14185 回放。
- Neo4j 可选：仅 kg.html 和 `--robot-graph` 需要。
- 14185 回放可选：只看 120 实时页时不需要；`start_runtime.sh` 默认会启动它。
- 新增 `ops/start_ray.sh`、`ops/start_neo4j.sh`、`ops/start_replay.sh`；主进程继续用 `ops/start_runtime.sh`，只开 14583 用 `ops/start_medium.sh`。
- `start_runtime.sh` 的 medium 分支不会自动启动 Ray/Neo4j；已加 preflight，缺 Ray/Neo4j 时给出明确脚本提示，且不会先停 4173。
- 手册已改掉旧 32×22 legacy 4173、console 自动录制回放、14183 作为主回放等过期描述。
- 手册新增目标架构截图 `docs/assets/ray-target-architecture.png`，逐项列出图中要求，并用 ✅/◐/✗ 标注实现状态。
- 明确：机器人写自己的图谱已实现；主进程只派单、机器人维护位置/电量/任务队列、全局记录进程、`KG_STATE_SOURCE=neo4j` 仍待实现。
- 新增 0.5 节“前端布局协作者：独立 Ray 测试”，给出独立端口 27380/14590、worker 27400–27599，并明确不要占用共享 26380/4173/14185/47687。
- 明确 `ray stop` 会停止本机所有 head，前端协作者只停自己的 live 进程，不要执行全局 `ray stop`。

## 执行过的操作

- 阅读根目录 `启动与图谱数据操作手册.md` 367 行，定位旧 legacy 4173/14183 段落。
- 新增并测试 `ops/start_ray.sh`、`ops/start_neo4j.sh`、`ops/start_replay.sh`。
- 修改 `ops/start_runtime.sh` 的 Ray/Neo4j preflight；修改 `ops/start_medium.sh` 的 Ray/robot-graph 参数与 preflight。
- 更新手册第 0/1/4.3/4.3.1/4.5/6/8/9/10 节。
- 更新 README 的 120/Ray 启动说明。
- `bash -n` 全部脚本通过，`git diff --check` 通过。
- 当前 4173 Ray + robot-graph 仍在运行，未被手册测试中断。

## 修改内容

- `启动与图谱数据操作手册.md`：新增 4 服务总览与启动顺序；更新端口表、4173 Ray 启动、回放来源、FAQ、停止方式、边界。
- `ops/start_ray.sh`：独立 Ray head，幂等，不执行 ray stop。
- `ops/start_neo4j.sh`：项目 Neo4j 启动，幂等，可选。
- `ops/start_replay.sh`：14185 只读回放，幂等，可选；修正 `LADE_ENV_DIR` 继承问题。
- `ops/start_runtime.sh`：Ray/Neo4j preflight，避免先停 4173 再失败。
- `ops/start_medium.sh`：RAY/ROBOT_GRAPH 参数与 preflight。
- `README.md`：120/Ray 启动与当前 4173 描述。

## 后续建议

1. 在干净环境验证 `start_ray.sh` 从零启动 Ray head。
2. 验证 `start_runtime.sh --restart --ray --robot-graph` 在 Neo4j/ray 都就绪时的完整启动。
3. 决定是否把 `--ray --robot-graph` 设为 4173 默认，而不是显式参数。
4. 当前改动尚未提交 Git。
