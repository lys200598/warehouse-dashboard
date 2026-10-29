# Agent 写查结果审计加固

## 任务背景

独立复核100个机器人写查审计工具，避免完整JSON验证、源身份、失败样本覆盖或追踪负资格被误判为通过。生产服务保持冻结，仅修改新的离线审计工具和测试。

## 关键结论

- 原工具对ResponseVerifier.check返回差异已有显式检查，未发现该项被忽略；现提取为受阴性测试保护的check_canonical_context。
- 修复未知endpoint误当query，限定两个准确路径；补HTTP内外request ID/PID/robot/path、时钟阶段与duration一致性、完整response与transport body摘要校验。
- 原始100个client-http日志和完整gzip记录必须逐请求完全覆盖，缺失、重复、失败、额外请求均使审计失败。统计从原始客户端全分母生成，缺失来源标签归unknown，不过滤错误。
- 补trace关闭状态检查：overflow、drop、transport rejection、未drain、先前trace不完整均保留负资格。cold仅定义为canonical seq1；later不宣称缓存充分预热。
- 结果文件、原始journal、进程清单、agent journal和图谱WHRun交叉核对run/config/输入与执行身份。每机器人ledger连续序号、来源时刻、state及自身事件变化逐条核验。
- 图谱查询保持只读MATCH，核验所有Agent观察和本地事件payload、数量与FROM_OBSERVATION关系；关闭proof需closed=true、failed=false、100agents且0存活进程/服务/写线程。
- 12项正/阴性测试通过；实际v3工作负载的新审计版本需主任务独立冻结并重跑，v1结果保留。

## 执行过的操作

阅读ops/audit_robot_agent_io.py、ops/verify_catalog_responses.py、backend/real_scale/oracle.py和既有身份结构；运行tests.test_audit_robot_agent_io共12项，全部通过；工具语法检查通过。

## 修改内容

- ops/audit_robot_agent_io.py：版本warehouse-agent-source-audit-v2，增强完整性和资格门禁，P99使用nearest-rank。
- tests/test_audit_robot_agent_io.py：覆盖tamper、missing、错误状态、外国run/PID、未知路径、observer负资格、ResponseVerifier非空差异、关闭proof和事件来源。
- 本文：保留审查范围与验证状态。

## 后续建议

在新audit-tools-v2目录冻结工具与依赖后，对已完成的真实run只读重审。保留全部v1报告、首次失败和冷启动慢样本；区分源正确性通过与500ms时延通过，百万前缀仍需另验。
