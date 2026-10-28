# 独立实时仓库生产者与控制状态机

### 任务背景

将固定任务数据、新版仓库与真实运行中的知识提交接入实时界面。采用独立ops运行层，显式选择生产者源码和新输出根目录，保留已有实验、服务及其源码身份。

### 关键结论

- 新增`WarehouseLiveRuntime`，单个owner线程执行真实`KnowledgeSimulation.step()`、journal/图镜像及所有控制。网络层只能读取快照或提交Future，不直接访问sim。
- 每步从实际内存生成frame，`sourceKind=live_simulation_memory`；周期提交默认5模拟秒，交接前关键边界和外部扰动前后按冻结runner语义提交。
- 初态暂停；start/resume、pause、step、墙钟speed、checkpoint和显式new_run分别执行。物理配置变化随new_run进入新身份与输出，固定CSV不接受随机加单、流量变更或原地reset。
- 外部live-manifest绑定源码、adapter、scene、CSV、初始库存、manifest、配置、采样和扰动SHA。恢复要求完全相同身份；存在失败边界时要求人工核对，保留原现场。
- `runtimeInstanceId`在一个服务实例中跨new_run不变，服务重启时更换；`liveRevision`跨new_run单调。UI可据此防止旧WS帧覆盖新run。
- 最终6项单测通过，覆盖真实100机器人step/journal、暂停恢复、新run旧文件保留、输入与控制拒绝、部分step异常冻结及实例身份。
- 实际rules/journal样例完成19/19任务，250秒SOC24触发1次交接；1972模拟秒、61.735墙秒、400批原始SHA及每批100不同机器人状态核对通过。该完整smoke绑定其测量时冻结源码与adapter字节；最终控制加固版仍需部署后的required/Neo4j独立确认。

### 执行过的操作

只读审计原`backend.server`、真实`knowledge_runtime.runner`、`wide.web`、固定数据入口和原Canvas实时协议。原主控台固定32×22，与90×76米宽松图的几何、服务面和列数不同；UI由独立前端任务按真实scene参数对接。

执行：

```bash
python -m unittest tests.test_warehouse_live_runtime -v
```

最终测试耗时28.160秒，6项通过。真实完整smoke及原始journal保存在`C:/E/projects/warehouse/research/live-runtime-smoke-20261028/`，测量时adapter保存在`warehouse_live_runtime.measured.py`。

该smoke的source SHA为`225040a2aa5e210853c475cbebb717041ae595b0c19c9f2f33816a133aa719e3`，与既有冻结source-v2的backend py/json清单digest逐字节匹配；未因本轮CLI转发文件更新放宽恢复门禁。

### 修改内容

- `ops/warehouse_live_runtime.py`：真实生产者、显式注册数据集、控制队列、独立新run、严格恢复、真实内存快照和失败现场。
- `tests/test_warehouse_live_runtime.py`：状态机及真实规则样例的最小操作检查。
- 本说明。未修改原backend、旧生产者或服务器实验。

Runtime构造参数：

```python
WarehouseLiveRuntime(
    source_root='/absolute/frozen/source', output_root='/absolute/new/live/runs',
    datasets={'sample19': {
        'scene': '/absolute/scene.json', 'dataset': '/absolute/fixed-dataset',
        'config': {'charging_policy': 'critical-only'},
        'disturbances_json': '/absolute/low-soc250.json', 'label': '19条任务',
    }},
    dataset_id='sample19', knowledge_mode='required', knowledge_backend='neo4j',
    commit_seconds=5, expected_neo4j_uri='bolt://127.0.0.1:17690',
)
```

`submit(action,value=None,expected_run_id=None)`返回`concurrent.futures.Future`；HTTP层可`await asyncio.wrap_future(...)`。已运行的Future无法被断线取消为物理失败。`command`为阻塞包装；`latest/status/scene/datasets/watch`提供只读结果；`shutdown`在完整step边界关闭资源。

### 后续建议

在独立17690图和14185服务部署最终版本，实测required模式低电交接、图水位推进、实时查询、pause/step/new_run和WS重连。显示物理frame时间与知识已提交时间，分别记录实际推进速度、请求延迟及知识来源；按新run分别保存身份和验证结果。
