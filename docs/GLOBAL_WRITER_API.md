# 全局记录写入服务接口

## 职责与边界

服务面向主进程提供 HTTP/JSON 接口。主进程只提交全局快照、完整步骤和跨机器人协作事实；服务写入 Neo4j 的 C 套标签。机器人本机的 `Robot`、`AgentState`、`Execution`、本机 `RuntimeEvent` 仍由各自 Ray Actor 写入。

`POST /v1/records` 是主进程的运行时接口。14284 收到请求后先放入自己拥有的有界队列并返回 `202`，后台 worker 独立重试并写 Neo4j。主进程接入由协作者负责；接口服务不改动 4173 的现有调用链。

## 启动

```bash
cd /data/workspace/lys/projects/warehouse-dashboard

export WAREHOUSE_NEO4J_URI='bolt://127.0.0.1:47687'
export WAREHOUSE_NEO4J_USER='neo4j'
export WAREHOUSE_NEO4J_PASSWORD='test123456'
export WAREHOUSE_NEO4J_DATABASE='neo4j'

# 优先使用项目自身环境；不依赖相邻仓库的 warehouse-deploy/.venv。
./.venv/bin/python -m backend.global_writer_api --host 127.0.0.1 --port 14284 --queue-size 256
```

协作者接入时，主进程只需向 `http://127.0.0.1:14284/v1/records` 提交 JSON；无需管理 Neo4j 连接、重试或服务内部队列。

## 接口

| 方法 | 路径 | 用途 |
|---|---|---|
| GET | `/v1/health` | 服务健康状态 |
| POST | `/v1/records` | 主进程写入一条记录或同一运行的一批有序记录（幂等） |
| GET / POST / DELETE | `/v1/datasets` | 管理逻辑数据集命名空间 |
| POST | `/v1/datasets/{dataset_id}/imports` | 按 `import_id` 一次性导入记录 |
| GET | `/v1/runs/{run_id}/steps/latest` | 读取 `Run.latest_step` 指向的最新完整步骤 |
| GET | `/v1/runs/{run_id}/steps/{tick}` | 单点查询完整步骤 |
| GET | `/v1/runs/{run_id}/steps/{tick}/states` | 读取该完整步骤中每台机器人的最近采样状态 |
| GET | `/v1/runs/{run_id}/handoffs` | 按任务、货物或机器人筛选交接事实 |
| GET | `/v1/runs/{run_id}/handoffs/{event_id}` | 单点查询交接事实 |
| DELETE | `/v1/runs/{run_id}` | 删除一个运行的全局记录，需确认字段 |

这里的“数据集创建/删除”是逻辑命名空间 `GlobalRecordDataset`，不是 Neo4j 物理数据库。这样不会删除其他实验、也兼容 Neo4j Community Edition。

## 主进程记录载荷

最小字段为 `run_id`、`tick`、`elapsed`、`status`、`backend`、`robot_count`、`max_active_robots`。正常运行时由 `build_global_record()` 自动构造。关键扩展字段：

- `step`：由主进程显式提供 `expected_robot_count`、`received_robot_ids`、`complete=true`。服务仅当收齐机器人编号时写 `Step` 并更新 `Run.latest_step`；服务和 `build_global_record()` 都不会默认推断“已收齐”。
- `handoffs`：不可变交接事实，记录 `event_id`、`tick`、`task_id`、`goods_id`、`donor_id`、`receiver_id`、`reason`、`location_id`，并关联两台机器人。
- `ownership_changes`：主进程给出的任务负责人变更。服务按 tick 顺序更新 `Execution.current_robot_id`，并给历史 `ASSIGNED_TO` 关系维护 `since_tick` / `until_tick`。
- `dispatch`：主进程的有界派单快照。每个 tick 可传 `{}`；只在保存点（建议每 5 秒）传完整快照，避免每步重复存储大体积调度数据。

示例：

```bash
curl -sS -X POST http://127.0.0.1:14284/v1/records \
  -H 'Content-Type: application/json' \
  -d '{
    "run_id":"demo-run", "tick":12, "elapsed":12, "status":"running",
    "backend":"warehouse-v5", "robot_count":2, "max_active_robots":2,
    "counts":{}, "tasks":{}, "assists":{}, "metrics":{}, "inside":0,
    "dispatch":{}, "events":[], "ownership_changes": [
      {"change_id":"assign-T-1-12","task_id":"T-1","tick":12,
       "to_robot_id":1,"reason":"assigned"}
    ],
    "step":{"tick":12,"expected_robot_count":2,"received_robot_ids":[1,2],"complete":true},
    "handoffs":[{"event_id":"e-12","tick":12,"task_id":"T-1","goods_id":"G-1","donor_id":1,"receiver_id":2,"reason":"low_energy","location_id":"AISLE-1"}]
  }'
```

## 批量提交与快照频率

`POST /v1/records` 也接受：

```json
{"records": [record_at_tick_120, record_at_tick_121, record_at_tick_122]}
```

限制是单批 1～128 条、总请求不超过 8 MiB、同一批只能属于同一个 `run_id`，并且 `tick` 必须严格递增。14284 将整批连续放入自己的写队列，后台按该顺序落库；响应 `202` 仍只代表入队。

建议主进程每个 tick 产生轻量记录（`Step`、统计、交接、`ownership_changes`），后台每 0.5 秒左右合并成一个小批提交。`dispatch` 仅在保存点附带完整内容；其他记录传 `{}`。这不会影响 `Step` 的完整帧语义。

`ownership_changes` 最小格式：

```json
{
  "change_id": "handoff-T-1-282",
  "task_id": "T-1",
  "tick": 282,
  "from_robot_id": 5,
  "to_robot_id": 6,
  "reason": "cargo_handoff"
}
```

派单时可省略 `from_robot_id`。机器人 Actor 仍可写自己的状态与参与历史，但不能写 `Execution.current_robot_id`；该可变字段只能由 14284 消费 `ownership_changes` 后维护。

## 删除保护

删除运行必须在 JSON body 中重复提供 `confirm_run_id`；删除数据集必须提供 `confirm_dataset_id`。接口绑定到 loopback 地址，当前版本不提供远程访问或鉴权。


### 2026-10-31 大规模运行删除

`DELETE /v1/runs/{run_id}` 的请求确认字段和200响应保持不变。调用方应先停止并排空该run的写入。
删除改为独立有界事务：先清历史/事件等叶子记录，再清Execution、Robot和Run；每批最多512个节点或2048条关系，分别提交，不再对一批高扇出节点直接DETACH DELETE。
每个实际删除操作都重新检查所属run；已完成批次允许中断后重试。删除在工作线程执行，健康接口不会被整个大库清理阻塞。
百万级数据清理在机械盘上可能超过10分钟。全天工具支持`--cleanup-timeout`（默认3600秒），真实基准工具支持`WAREHOUSE_KG_CLEANUP_TIMEOUT`（默认3600秒）；这是独立管理操作预算，不改变仿真Step和读写错误判定。
真实2424547节点、4776327关系已通过该HTTP处理器清理，残留0，重复删除成功，其他运行和19份WHRun保留。
