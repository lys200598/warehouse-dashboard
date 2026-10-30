# C 套机器人决策查询目录

仅新增查询，不修改写者。以 `ops/prototypes/kg_state_source.py` 的六条查询、
`backend/warehouse_runtime_graph.py` 的实际字段为依据，按 LaDe 职责分包。
未实现 LaDe 全部问题集或决策算法。

## 输入与统一时间合同

`read_context(driver, run_id: str, robot_id: int, *, tick=None, state_window=400,
event_window=600, radius=15, helper_min_battery=60, database="neo4j")`。

- `tick=None`：只读取一次 `Run.latest_step`，作为所有查询的固定 t；缺失或没有唯一 Step 时明确抛出 `ContextUnavailable`，不回退 MAX(tick)。
- 显式 `tick=t`：所有状态、事件、附近机器人、候选者与摘要都按 t 限定；Run 必须存在，不要求写者提供完成标记。Step 缺失明确报缺，不声称完整。
- 时间单位是写者的 **tick**，不擅自视为秒。原型当前一 tick 为一模拟秒。
- 自身与其他机器人取最后一个 `AgentState.tick <= t`。返回实际 `observed_tick`、`age_ticks`，是最后已知采样，不伪造时刻 t 的精确状态。可按业务允许的采样陈旧度拒绝决策。
- 最近状态窗口 `(t-state_window, t]`，事件窗口 `(t-event_window, t]`；包含完整属性和窗口内全部行，无隐含 LIMIT 20。
- 不读取 Robot 的可变当前属性，不读取 Execution 当前状态/关系来冒充历史归属。
- 一个只读事务固定 t；Neo4j 的读已提交隔离不是时间旅行快照。依赖写者在推进 latest_step 前完成本步写入、完成后历史不可变。写者若补写/改写旧历史，不能保证原子快照。
- Robot 仅用于运行内身份枚举；晚创建且 t 前无历史的节点列为 `robots_without_history`，不作为附近或求助候选，不表示当时存在。不存在任何历史的身份也在此列出。

## 查询目录

每条除下表参数外均需 `run_id`；内部 t 由上述合同确定。SQL 风格命名仅作为目录键，实际执行 Cypher。

| 目录键 | 额外输入 | 时间口径与输出 |
|---|---|---|
| anchor | tick 可选 | 最新读取 Run.latest_step；显式时刻只检查 Run 存在 |
| step（原型 latest_step） | t | 精确 Step.tick=t 的完整属性；不使用当前 Run 的其他属性 |
| self_state | robot_id, t | 最后一个 <=t 的 AgentState 完整属性，包含 task_id（如已记录） |
| recent_states | robot_id, t, state_window | (t-window,t] 全部状态，tick 降序、id 确定排序 |
| recent_events | robot_id, t, event_window | (t-window,t] 全部事件，兼容原型 robot_id 和正式 robot_node_id，UNION 去重；保留 payload |
| fleet_states（共享内部查询） | t | 每个 Robot 身份取最后一个 <=t 的采样，不读取其当前状态 |
| nearby_robots | robot_id, t, radius | 使用上述采样位置，abs(dx)<radius 且 abs(dy)<radius，正方形范围，与原型一致；返回全部，含欧氏距离 |
| help_candidates | robot_id, t, helper_min_battery | 使用上述采样，其他机器人 idle 且 battery>阈值，按欧氏距离/id 排序，返回全部 |

附近/求助两个目录项共用一次 fleet_states 读取并在客户端计算，避免重复全队采样查询。
五包一起只需 anchor 加 5 次 Cypher。返回所有事实及五包投影；完整结果消费计入性能。

## LaDe 职责分包与缺项

| 分包 | 查询事实 | C 模型无法可靠回答（输出 unavailable） |
|---|---|---|
| task_decision 任务决策 | step、自身、事件、附近 | 待派任务、截止时间、任务要求、历史任务归属 |
| energy_safety 能量安全 | 自身、近期状态、事件 | 路径耗能、可达充电位、充电位占用、安全余量 |
| request_help 请求帮助 | 自身、事件、候选者 | 活跃请求、已预约助手、交接可行性 |
| provide_help 提供帮助 | 自身、事件、附近 | 活跃请求、请求优先级、载荷兼容性、交接可行性 |
| recovery 恢复 | step、自身、近期状态、事件 | 故障诊断、恢复方案、历史归属 |

候选者只是位置/待命/电量粗筛，**不代表能安全帮助**。近期事件是有界原始证据，不能从窗口中“没有取消事件”推断请求仍有效。
不解读尚未落地的全局协作草案、不混入 A 标签，也不从未来 Execution 补全事实。
单机事件可以通过 payload 保留原始交接内容，但不保证全局协作被完整记录。
历史无记录返回 null/空列表和明确缺项，不能推断“从未发生”。历史保留/采样覆盖度未知。

## 索引与调用

`SCHEMA` 提供 Run.id 唯一约束，以及 Step(run_id,tick)、Robot(run_id,robot_id)、
AgentState(run_id,robot_id,tick)、RuntimeEvent 的两种身份+tick 复合索引。
部署方可逐条执行、再 `CALL db.awaitIndexes(60)`；读接口不会自动写 schema。
与已有等价索引可共存/被 IF NOT EXISTS 跳过，不更改已有约束。

```python
from backend.query_c import read_context, SCHEMA
context = read_context(driver, "my-run", 1, tick=100)
assert context["facts"]["self_state"] is None or context["observed_tick"] <= 100
```

验证：`./run_in_lade.sh python -m unittest discover -s tests -p 'test_query_c_*.py' -v`。
集成测试需要显式 `QUERY_C_TEST_URI`（必须指向自己的隔离实例）；
只创建唯一测试 run，测试结束只删除该 run 的数据，不清库。

## 来源差异及接入约束

实际验证版本为 Neo4j Community 5.26.32（子查询使用 5.23+ 的显式作用域语法）。
原型 Robot.id 是 run:robot_id；正式写者为 run:robot:robot_id。查询不用拼接 Robot
身份来读取采样，仅以 run_id/robot_id 匹配；正式事件 robot_node_id 使用正式命名。
原型 RuntimeEvent.payload、正式写者已有平铺字段、B 分支 9d75622 的
payload_json/payload_sha256、交接双方等扩展字段均通过 properties() 原样返回。

已只读核对 B 的 RobotGraphWriter：AgentState 有 run_id/robot_id/tick，四步采样；
其历史额外携带 payload_json，与此目录兼容。但它本身不写 Step 或 Run.latest_step，
仍需全局写者提供完成标记才能使用最新模式。此目录不会代写标记或补造历史。
原有 warehouse_runtime_graph 写者也缺少该完成合同；可用显式 tick 查询并检查 missing。

性能证据单独放在分支外；原型基准不冒充生产写者性能。查询读取全部窗口内原始属性；
不同写者的字段体积、事件密度、采样频率会改变性能。

注意旧 warehouse_runtime_graph._event_rows 对 robot_id=0 使用真假判断，可能未记录
robot_node_id；缺失身份的历史事件无法可靠归属，本目录不会猜测或修补。
B 写者保留 robot_id=0，已另用其真实写入代码验证完整嵌套 payload 和历史隔离。

当前最后采样查询保留同 tick 的 id 稳定排序，PROFILE 显示使用复合索引，但会读取
该机器人 <=t 的历史候选再选最后一条；不能据小规模基准承诺百万历史性能。
