# 调度 V4：正式入口与可回退部署

## 范围

本版本只正式接入已经验证的调度、规划优化，不改知识图谱 schema、写入器或查询实现，不重新开展百万级图谱和查询尾延迟实验。

- `backend.scheduling_v4.runner` 默认 `optimized`；显式 `--scheduler baseline` 使用原算法。
- `ops/scheduler_release.py run` 读取 `config/scheduler-mode.json`，默认为优化版。
- 原有 `backend.wide.runner` 仍保留原算法。服务器旧目录与旧服务没有被覆盖。
- 这是中央进程管理 100 个机器人实体的仿真，不等于 100 个执行进程，也不等于每个时刻都有 100 台实际取放或移动。

## 接入内容

1. 任务匹配先选最大可行数量、按现有优先序保留任务，再做最小成本机器人配对，避免高成本紧急任务被统一 dummy 代价反向淘汰。
2. 显式 `GoalWindowPlanner` 使用能容纳取放服务的目标安全时间窗下界，减少无用搜索。保留原碰撞、速度、半径、时间窗和搜索预算。
3. 精确几何 footprint 缓存上限 60000；静态距离缓存按坐标与连接点归一，实例级 LRU 上限 512。**不缓存动态避碰是否可行**。
4. 通过默认无行为改变的工厂和匹配钩子接入；正常派单、故障恢复、交接、磁盘恢复都使用同一选定规划器和预约表类型。不做全局 monkeypatch 或运行时源码生成。

优先序调整有权衡：高峰迟到与长尾可以改善，但不保证平均流转时间更短。请以同输入结果而非动画拥挤程度判断。

## 服务器目录与页面

```text
/data/workspace/wz/warehouse-dashboard-scheduler-v4  新正式版
/data/workspace/wz/warehouse-dashboard-wide-v1       原版，保留
/data/workspace/wz/warehouse-validation-v2-20261027   原始输入及对照结果，保留
```

- 优化版全天回放：`http://127.0.0.1:14180/`
- 原算法全天对照：`http://127.0.0.1:14181/`
- 原服务及原知识查询：`http://127.0.0.1:14179/`

新页面绑定各自真实全天运行的 `frames.sqlite`，明确标注是已完成运行的回放，不显示虚构机器人。右键/中键平移、左键旋转、滚轮缩放均复用原脚本。新回归仅用 journal，不连接 Neo4j；原知识查询继续在旧服务中使用，不把新日志冒充已经同步进图谱。

## 启动新运行

服务器：

```bash
cd /data/workspace/wz/warehouse-dashboard-scheduler-v4
./run_in_lade.sh python ops/scheduler_release.py status

# 必须使用新的 output 和 run-id，不覆盖验收结果
./run_in_lade.sh python ops/scheduler_release.py run -- \
  --scene data/layouts/warehouse100-wide-v2.json \
  --dataset data/inputs/day --config-json data/inputs/day-config.json \
  --disturbances-json data/inputs/day-disturbances.json \
  --output data/my-new-run --run-id my-new-run --frames \
  --knowledge-backend journal --max-wall-seconds 1800
```

完整同输入回归：`./run_in_lade.sh python ops/run_release_matrix.py`。它拒绝覆盖已有非空输出，因此复跑应在新的独立发布副本或使用新的输出计划，不要删除旧结果后重跑。

## 回退与恢复

```bash
# 原子地切换“以后新启动运行”的默认算法
./run_in_lade.sh python ops/scheduler_release.py select baseline
./run_in_lade.sh python ops/scheduler_release.py status
# 同一 run 命令会启动原算法，新输出目录仍必须唯一

# 恢复优化版默认值
./run_in_lade.sh python ops/scheduler_release.py select optimized
```

上述操作不修改任何已运行实例或旧检查点。对比现有结果直接访问两个页面；要回到旧服务，使用 14179，无需停止或迁移新版本。

同一版本/模式/源码/输入/配置/run-id 可以在原命令中添加 `--resume`。跨模式、跨源码或旧版检查点明确拒绝，不自动迁移；模式检查在打开 journal 前完成。若要恢复更旧发布，应使用对应冻结目录和对应的检查点。

## 页面服务与本地隧道

```bash
cd /data/workspace/wz/warehouse-dashboard-scheduler-v4
bash ops/start_views.sh
```

Windows：

```powershell
& C:\E\projects\warehouse\deployment\start-scheduler-v4-tunnel.ps1
```

两个端口均仅绑定 loopback。隧道有独立 PID 文件，不替换旧隧道；端口已占用时脚本停止而不杀进程。

## 验收与证据

- `logs/full-python.log`：完整 Python 回归，真实数据库测试按范围跳过。
- `data/recovery-v4/verification.json`：CLI 交接中断续跑、跨模式无写入拒绝、与连续运行最终物理状态及搜索计数一致。
- `logs/rollback-run-*.json` 及两次 smoke 运行：默认模式切换真正影响新实例，最终恢复优化版。
- `logs/final-verification.json`：原始 6912 条全天和 1728 条高峰输入，两种模式同输入对照、原版一致性与帧检查。
- `source-sha256.json`、发布包哈希及交付证据清单：区分版本、代码和输入身份。

不把本轮实验当作真实机器人硬件标定、无死锁数学证明、百万级联合验收或查询延迟达标证明。
