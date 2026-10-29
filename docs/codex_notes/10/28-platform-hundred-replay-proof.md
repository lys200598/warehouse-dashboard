# 当前平台版本的100机器人业务活动回放验真

### 任务背景

为当前统一入口版本补充独立证据：在已完成的真实高峰运行中，确认100台机器人同时执行仓库业务，并将回放状态与原始实际采样逐一对齐。所有生产后端源码、已有帧、journal和线上服务保持原样。

### 关键结论

- 原始运行 `wh-platform-peak-v2-20261028` 使用当前冻结 `source-v2`，完成1728条高峰任务。独立回放严格锁定同一run、源码身份、地图、输入和完整配置。
- 从0秒逐步执行到804秒；原实际保存的0、10、……、800秒共81个帧，8100个机器人样本的**全部字段精确一致**，没有浮点宽容差或插值。
- 804秒时100台机器人全部处于业务阶段：35台前往取货、52台运货、8台取货、5台放货。排队、回程、充电和待命均未计入。
- 803→804秒独立计算4950组机器人之间的连续扫掠最近距离，最小1.1313708499m，要求1.1m。整个804步共核对3,979,800组，最小距离1.0999999999999943m，采用明确的1e-7m浮点计算容差。
- 804秒库存守恒通过：仓内924件，货位818件、机器人持货58件、入库暂存48件；初始864件＋已接收入库216件－已出库156件＝924件。
- 804秒帧是有原实际采样锚点的确定性回放产物，原帧数据库按10秒采样，未保存804秒。报告使用 `source_kind=deterministic_replay_anchored_to_actual_saved_frames` 和 `original_saved_target_second=false` 明确区分。
- 原始帧数据库读取前后SHA相同；新工具单元测试6项通过。100个Ray进程、查询并发及硬件标定继续分别验收。

### 执行过的操作

读取 `backend/knowledge_runtime/runner.py`、`backend/real_scale/writer.py`、现有业务活动计数定义、帧格式及库存不变量。使用同一冻结生产源码运行独立ops工具：

```bash
export LADE_ENV_DIR=/data/workspace/wz/warehouse-deploy/.venv
export PYTHONPATH=/data/workspace/wz/warehouse-validation-v2-20261027/driver-rust:/data/workspace/wz/warehouse-platform-joint-20261028/source-v2
bash run_in_lade.sh python ../tools-proof-v1/ops/replay_hundred_frame.py \
  --scene data/layouts/warehouse100-wide-v2.json \
  --dataset ../inputs/peak-v5 --config-json ../inputs/peak-replay-config-v1.json \
  --run-id wh-platform-peak-v2-20261028 \
  --frames-sqlite ../runs/peak-current-v2/frames.sqlite \
  --second 804 --output ../runs/peak-current-v2/hundred-replay-v1.json
```

实际耗时112.029秒。原始报告SHA256：`7350fe36306c49cc68ec3c2e0deceb4fea997a44a83d2883a2602de3e3993451`。

服务器报告：`/data/workspace/wz/warehouse-platform-joint-20261028/runs/peak-current-v2/hundred-replay-v1.json`。

本地原始报告和日志：`C:/E/projects/warehouse/research/platform-joint-20261028/hundred-replay-v1/`。

### 修改内容

- 新增 `ops/replay_hundred_frame.py`：只读实际帧、同身份重放、全部机器人字段比较、独立连续距离计算、库存检查及来源绑定。已存在输出会被拒绝覆盖。
- 新增 `tests/test_replay_hundred_frame.py`：覆盖相交轨迹、静止占位、4950对数量、业务阶段定义、微小字段差异及机器人缺失；6项全部通过。
- 生产 `backend/` 保持原字节，工具通过显式 `PYTHONPATH` 选择冻结源码。

### 后续建议

将此证据与七日完整运行、知识控制审计和同run读写混合查询审计分别关联。后续修改物理算法或地图时，应重新运行此工具并绑定新的原始高峰帧和源码身份。
