# v3 电量、充电准入与交接契约

## 任务背景

为二维仓库运输仿真建立透明的电量和货物交接边界，替换“固定额外若干秒”式不可解释裕量。这里描述 `backend/scheduling_v3/energy.py` 的纯函数契约；不能将模块单测通过等同于已验证完整车队轮充或真实机器人续航。

## 关键结论

- 全部电量使用 **SOC 百分点**，不是0–1比例，也不是Wh。
- 三个 profile 均为假设模型，`hardware_calibrated=False`。不能从参数表声称复现某个品牌的实际工况。
- 准入包含当前业务的实际规划时间、到已验证可达充电位的路线、额外排队余量以及硬保底SOC。计划中的充电不能预先抵扣任务耗能。
- 已在路线中表示的等待不重复计入 `available_at`；未计入轨迹的等待裕量通过 `queue_seconds` 明确加入。
- 电量足够不代表道路或排队区域安全。路线、等候位置、充电位独占和联合计划提交由仿真器验证。
- 货物接力只改变唯一持有者及执行责任，不提前把业务任务标成完成；只有实际交付目标才完成任务。

## 模型参数

| profile | 空驶消耗 | 载货运动 | 取放/交接 | 等待/待机 | 净充电速率（≤80%） |
|---|---:|---:|---:|---:|---:|
| `legacy-assumed-v2` | 15点/h | ×1.0 | ×1.0 | 0.9点/h | 60点/h |
| `transport-assumed-v1`，默认 | 15点/h | ×1.2 | ×1.2 | 2点/h | 60点/h |
| `conservative-sensitivity-v1` | 30点/h | ×1.2 | ×1.4 | 6点/h | 40点/h |

其中取放/交接倍率作用于空驶基准率，不再与载货倍率相乘。默认/保守模型80%以上使用0.5倍净充电速率，legacy保留恒定速率；仿真若只充到80%，不会进入衰减段。

默认模型从80%降至20%持续载货运动约3小时20分，保守模型约1小时40分；两者只是恒定工况算术结果，不能拿来代替实际任务中包括等待、取放与回充的续航测试。20→80%净充电分别为1小时和1.5小时，不含去充电位的交通、排队、对接退出时间。

### 公开机制参考与边界

[OTTO 官方充电说明，2025-12-15](https://ottomotors.com/blog/how-do-amrs-charge/)介绍了任务间机会充电，并分别列出不同机型10→90%的18、30、60分钟充电时间。它支持“任务间补能”和“型号/SOC区间必须明确”的机制，不支持将其中任意数值当作本项目人形机器人的标定。本模块参数没有假称来自OTTO实测，也不直接使用其软件。

## API

```python
profile = EnergyProfile()               # 也可 .legacy() 或 .conservative()
drain = profile.consume(parts, initial_loaded=False)
seconds = profile.charge_seconds(arrival_soc=20, target_soc=80)
gain = profile.charge_gain(soc=79, seconds=120)

result = assess_admission(
    soc=45,
    task_parts=mission_parts,
    charge_options=[{
        'dock_id': 'C-A01',
        'parts': planned_task_end_to_dock,
        'available_at': planned_station_available_time,
        'queue_seconds': contingency_wait_seconds,
    }],
    profile=profile,
    reserve_soc=20,
    max_queue_seconds=7200,
)
```

`consume` 只按实际 `start/end` 积分；片段时长必须为正、时间必须连续、phase必须已定义，未知阶段不能静默当作零耗能。`picked`/`delivered` 标记在片段结束时切换载货状态，也支持逐片段显式 `loaded`。因此持货途中恢复规划要传 `initial_loaded=True`，不能当空车。

`assess_admission` 只对已经通过外部交通检查的候选桩路线比较电量。拒绝原因包括 `insufficient_energy`、`queue_bound_exceeded`、`no_reachable_charger`。`AdmissionResult.as_dict()` 可写严格JSON；不可达的无穷内部代价导出为null，而不是非法Infinity。

`required_soc(task_parts, to_charge_parts, profile, reserve_soc, queue_seconds)` 是轻量求和入口。`queue_seconds` 是**轨迹之外的额外等待**，调用方不能再重复传已包含于 `traffic_wait` 的时长。

## 交接契约

`validate_handoff(donor, receiver, task, goods)` 要求：

1. 两台不同机器人，中心距默认1.1–1.6米；不得同坐标交接。
2. 交出方持货、任务负责人与货物账本都指向交出方，且是同一个任务。
3. 接收方空手、无其他任务，不在充电/失能状态，账本没有隐藏的第二件货。
4. 调用方已让两台车在合法交接位完成整个交接服务时间；本函数不冒充机械臂运动或接触物理验证。

`transfer_custody(..., now=...)` 返回新对象五元组 `(donor, receiver, task, goods, event)`；原输入成功/失败都不变。返回后还要由仿真器把新任务路线、旧车回充路线和这次责任转移原子提交。对返回状态重复执行同一次转移会被拒绝。事件包含原持有者、新持有者、任务、货物、时间和原因，`task.status`仍为`carrying`，不增加完成数。

## 执行过的操作

- 阅读既有day1能耗与布局参考说明。
- 核查OTTO官方机会充电页面，不引入第三方算法或依赖。
- 新增模块及12项单测，覆盖分段积分、载货变化、充电衰减、准入、排队计费、严格JSON与原子交接。
- 本机Windows没有可直接调用的Bash包装环境，因此使用已配置的bundled Python运行：

```powershell
& 'C:/Users/vsx/.cache/codex-runtimes/codex-primary-runtime/dependencies/python/python.exe' -m unittest tests.test_scheduler_v3_energy -v
```

## 修改内容

- `backend/scheduling_v3/energy.py`
- `tests/test_scheduler_v3_energy.py`
- 本文。旧day1/v2代码、能耗配置及原始实验均未修改。

## 后续建议

主仿真应同时使用此profile进行计划准入与逐秒实际扣电，不能计划用新模型、执行还沿用旧速率。接力必须经过物理双车到位/服务时间/双方安全离开路线的整体验证。真实参数仍需选定目标机型后标定；在此之前用默认/保守模型同数据对照报告任务流转、充电等待、最低SOC和救援次数，不通过换成更快充电来掩盖瓶颈。

## 后续集成审查与回归（2026-10-27）

主进程集成后追加只读审查，发现并由主进程修正了：按旧常速估算90%目标而实际充电包含80%以上衰减、临界回充按机器人ID而非电量紧迫程度排序、业务重规划退化为仅留10%而未验证后续回充、充电位释放时间与实际离桩时刻不一致，以及未解决SOC更正后仍可能执行旧轨迹的边界。

新增 `tests/test_scheduler_v3_energy_runtime.py`，5项当前通过：

1. 高ID但更低SOC的机器人先获得可行回充计划，不被非紧迫车辆占满资源。
2. 90%目标包含80%以上衰减所需完整时长，计划与执行共用充电模型。
3. 保守模型下超过1小时的充电实际逐秒到达80%目标并返回原停车位；仍推进全部100台物理状态并运行安全审计，只隔离其他派单。
4. 未拾取的入库任务因低SOC改派：货物留在入库口，目标货位预留保留，由其他机器人完成，接收/拾取/入架事件各一次。
5. 已携货机器人测得低电且无可用帮手时，明确报告恢复失败，并在下一物理步之前暂停；100台坐标和仿真时间不变，货物仍由原车持有。

**第5项验证的是失败时不会继续危险执行，不是自动恢复成功。**这种故障试验在整体恢复验收中应记为未恢复，不能因回归测试“通过”就计入成功恢复数量。以上不替代正式多场景长时联合实验。
