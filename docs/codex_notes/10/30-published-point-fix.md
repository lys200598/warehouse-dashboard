### 任务背景

修复图谱接入后的发布回归，并补齐漏测模块。用户确认全天实验已失败后，将隔离副本修复应用到原功能分支。

### 关键结论

- 修改前复现 test_live_published_point 的13项错误；隔离修复后26项相关小测试通过。
- 原工作区完整回归通过，保留精确已知 KeyError('robots') 与环境门控跳过；实际数量见日志。
- 发布字段缺失时 kg_state_source 默认 memory、legacy_a_graph 默认 False；显式值保留。
- 旧查询摘要测试显式启用A路径；回归入口加入 test_live_published_point。
- 全天实验第9264步因Step等待超时失败，清理残留0。写入长尾原因正在单独排查，不能宣称全天通过。
- 接着顺序启动普通派单、撤销重推完整Neo4j基准，运行状态另存，不提前计为通过。

### 执行过的操作

- 只读核对原分支、干净状态、全天失败报告。
- 从隔离副本生成三个文件的patch，git apply --check 后应用。
- bash ops/kg_dev_env.sh python ops/kg_regression.py。
- 日志：/tmp/wz-kg-evidence/published-point-fix/full-regression.log。

### 修改内容

- ops/warehouse_live_runtime.py：三处模式字段添加默认值。
- tests/test_live_published_point.py：启用旧查询摘要，验证默认与显式模式。
- ops/kg_regression.py：补入发布点测试模块。
- 本总结。未改lys负责文件。

### 后续建议

核对两份完整图谱基准；诊断写入卡顿，实现可配置等待与暂停重试后再做全天实验。未推送。
