# 全局记录写入接口：数据清单与调用样例（草案）

日期：2026-10-30。作者：wz。状态：**样例草案，供 lys 设计接口时参考**。**传输方式已定：HTTP + JSON。** 端口和路径以 lys 的最终设计为准，字段和语义请尽量保留。

## 1. 谁调用、传什么

- **调用方**：主进程。每一步，100 个机器人进程都回执之后，主进程处理完标记（取放货、交接等）和汇总，把本步的**全局记录**交给全局记录写入进程。
- **不经过这个接口的**：单台机器人的状态、执行过程、本机事件，由各机器人进程直接写图谱（设计说明问题 9）。
- **全局记录写入进程据此负责**：写 `Step(t)` 完成标记和本步摘要、更新 `Run.latest_step`、记录跨机协作（低电互助与交接）、维护执行记录的 `current_robot_id`。

## 2. 调用一览（样例）

| 调用 | 何时 | 作用 |
|---|---|---|
| `POST /v1/runs` | 一次运行开始时 | 登记运行：地图、数据集、机器人数、配置指纹 |
| `POST /v1/runs/{run_id}/steps` | 每步回执之后（可以一次带多步） | 写本步摘要、跨机协作、负责人变更；最后写 `Step(t)` 并更新 `Run.latest_step` |
| `POST /v1/runs/{run_id}/finish` | 运行结束时 | 写最终统计，标记运行结束 |
| `GET /v1/runs/{run_id}/status` | 主进程重启或排查时 | 查询已经写到第几步 |

### 2.1 HTTP 约定

- 请求和响应都是 JSON，`Content-Type: application/json; charset=utf-8`；
- 所有响应都带 `accepted_through_tick`，主进程以它为准决定下一步从哪里发；
- 状态码：

| 状态码 | 含义 | 主进程怎么做 |
|---|---|---|
| 200 | 已写好（包括内容相同的重复提交） | 继续 |
| 409 | 步号不连续（第一步不是 `accepted_through_tick + 1`） | 从 `accepted_through_tick + 1` 重发 |
| 422 | 同一步重复提交但内容不同，或字段不合法 | 停止并报错，不重试 |
| 503 | 写入进程暂时不可用（例如 Neo4j 连接失败） | 按 5.3 节重试 |

出错时响应体为 `{"error": "…", "accepted_through_tick": 281}`。

## 3. 登记运行

请求：

```json
{
  "run_id": "wh-live-20261030T064100Z-1a2b3c4d",
  "started_at": "2026-10-30T14:41:00+08:00",
  "scene": {"path": "data/layouts/expansion-candidates/medium/layout.json", "scene_sha256": "5b8f…"},
  "dataset": {"id": "medium19", "path": "data/examples/wheeled-medium-sample19", "tasks_sha256": "c41e…"},
  "robots": 100,
  "tick_seconds": 1,
  "config_sha256": "9d02…",
  "code_sha256": "e7aa…"
}
```

响应：

```json
{"run_id": "wh-live-20261030T064100Z-1a2b3c4d", "accepted_through_tick": -1}
```

`accepted_through_tick` 表示"写入进程已经完整写好的最后一步"，新运行为 -1。重复登记同一个 `run_id` 且内容相同，视为成功；内容不同则报错。

## 4. 每步记录

请求（一次可以带多步，按步号升序）：

```json
{
  "run_id": "wh-live-20261030T064100Z-1a2b3c4d",
  "steps": [
    {
      "tick": 282,
      "sim_time": 282,
      "robots_reported": 100,
      "summary": {
        "moving": 41, "handling": 6, "charging": 5, "standby": 46, "queued": 2, "working": 49,
        "low_battery": 1, "tasks_completed": 7, "tasks_open": 12, "tasks_late": 0
      },
      "collaborations": [
        {
          "event_id": "wh-live-20261030T064100Z-1a2b3c4d:cargo_handoff:V3-T-0000012:282",
          "type": "cargo_handoff",
          "tick": 282,
          "task_id": "V3-T-0000012",
          "goods_id": "V3-G-0787-0001",
          "donor_id": 5,
          "receiver_id": 6,
          "reason": "measured_low_energy",
          "location": {"location_id": "ROBOT-6", "x": 0.0, "y": 0.0},
          "requested_at": 251
        }
      ],
      "ownership_changes": [
        {"task_id": "V3-T-0000012", "from_robot_id": 5, "to_robot_id": 6, "tick": 282, "cause": "cargo_handoff"}
      ]
    }
  ]
}
```

响应：

```json
{"run_id": "wh-live-20261030T064100Z-1a2b3c4d", "accepted_through_tick": 282, "duplicate_ticks": []}
```

### 4.1 字段说明

| 字段 | 必填 | 说明 |
|---|---|---|
| `tick` | 是 | 步号（整数，从 1 开始，每步 +1）。`(run_id, tick)` 唯一 |
| `sim_time` | 是 | 模拟秒数，目前与 `tick` 相同；单独给出，便于以后调整步长 |
| `robots_reported` | 是 | 本步回执的机器人数。等于机器人总数时，表示本步所有机器人的记录都已写完，可以写 `Step(t)` |
| `summary` | 是 | 本步全局摘要，写进 `Step(t)`。前 6 项来自仿真的 `counts()`：`moving` 行驶、`handling` 取放货或交接、`charging` 充电、`standby` 待命、`queued` 排队等待、`working` 有任务的机器人数；后 4 项是电量低于阈值的机器人数和任务统计 |
| `collaborations` | 否 | 本步发生的跨机协作事件，没有时为空列表。`event_id` 唯一，重复提交只记一次 |
| `ownership_changes` | 否 | 本步任务负责人的变化，用于更新执行记录的 `current_robot_id`；`ASSIGNED_TO` 关系照旧只增不删，表示历史参与 |

### 4.2 跨机协作事件的类型

| `type` | 何时产生 | 字段（除 `event_id`、`type`、`tick`、`task_id` 外） |
|---|---|---|
| `assistance_requested` | 机器人载货途中电量不足，请求接力 | `donor_id`、`reason`（如 `measured_low_energy`）、`battery` |
| `handoff_reserved` | 系统为它预约好接收方 | `donor_id`、`receiver_id` |
| `assistance_deferred` | 暂时找不到能安全接手的机器人 | `donor_id`、`reason`（如 `no_safe_energy_feasible_pair`） |
| `cargo_handoff` | 交接完成 | `goods_id`、`donor_id`、`receiver_id`、`reason`、`location`、`requested_at` |
| `robot_power_isolated` | 持续故障的机器人交出货物后断电隔离 | `robot_id`、`reason` |

这些类型和字段都取自仿真里现有的事件（`backend/scheduling_v3/sim.py`）。上面 JSON 示例中，交接部分来自 2026-10-30 一次真实接力：5 号在第 251 秒报告低电，第 282 秒把货物 `V3-G-0787-0001` 交给 6 号，地点编号 `ROBOT-6`。**`summary` 里的计数和交接地点的 `x`、`y` 是示意值**：仿真现有事件只记录地点编号，坐标需要主进程从接收方当时的位置补上。

## 5. 调用规则

### 5.1 顺序、幂等与"写完"的含义

- **按步号顺序**：同一次运行的步必须连续、升序提交。收到的第一步不是 `accepted_through_tick + 1` 时，返回 HTTP 409，并在响应里带上 `accepted_through_tick`，主进程从那一步开始重发。
- **幂等**：同一个 `(run_id, tick)` 重复提交、且内容相同，视为成功，并在 `duplicate_ticks` 里列出；内容不同则报错，不覆盖已写入的数据。`collaborations` 按 `event_id` 去重。
- **"写完"再确认**：只有当这一步的摘要、协作事件、负责人变更、`Step(t)` 和 `Run.latest_step` 都已提交到 Neo4j 之后，才把 `accepted_through_tick` 推进到这一步。查询以 `Run.latest_step` 为准，所以它必须最后更新。

### 5.2 调用频率

| 仿真速度 | 每秒步数 | 建议调用方式 |
|---|---|---|
| 实时（1 倍） | 1 | 每步调用一次 |
| 加速（例如 8 倍） | 8 | 主进程攒批：每 0.5 秒或每 8 步发一次，以先到者为准 |
| 尽可能快（压测） | 几十 | 同上，单批最多 50 步 |

- 每次调用的处理时间应**在 20ms 以内**，否则会拖慢每步（原型实测：每步总耗时约 120ms，其中写 `Step(t)` 约 13ms）。
- 主进程**异步发送**，不阻塞下一步的推进；查询看到的最新步，最多比仿真落后一个批次。
- 单步请求体约 1KB（没有协作事件时更小）；交接这类协作事件很少，一次运行通常只有个位数到几十次。

### 5.3 失败与重试

- 写入进程暂时不可用时，主进程按 0.5、1、2、4 秒的间隔重试，期间继续缓存未确认的步；
- 缓存超过 300 步（实时速度下约 5 分钟）仍未确认时，**主进程暂停仿真并报错**，不静默丢弃记录；
- 主进程重启后，先调用 `GET /v1/runs/{run_id}/status`，从 `accepted_through_tick + 1` 开始补发。

## 6. 结束运行

```json
{"run_id": "wh-live-20261030T064100Z-1a2b3c4d", "final_tick": 1989, "finished_at": "2026-10-30T15:14:09+08:00",
 "reason": "all_tasks_completed",
 "totals": {"tasks_completed": 19, "assistance_requested": 1, "cargo_handoff": 1, "robot_power_isolated": 0}}
```

`reason` 的取值：`all_tasks_completed`（全部完成）、`stopped`（人为停止）、`failed`（出错终止，附 `error`）。

## 7. 需要 lys 确定的

1. 端口号和路径前缀（传输方式已定为 HTTP + JSON）；
2. `summary` 里还需要哪些全局统计；
3. `current_robot_id` 的最终字段名，以及 `Step`、`Run` 节点上的属性名；
4. 批次大小上限和处理时间目标（本文建议单批最多 50 步、单次 20ms 以内）是否可行。

字段增加可以随时商量；删除或改名请先在飞书说一声，主进程这边要同步修改。
