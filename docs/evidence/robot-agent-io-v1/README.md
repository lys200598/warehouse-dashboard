# 100独立机器人进程：真实图谱写查证据

原件根：Pro6000 `/data/workspace/wz/warehouse-wheel-agents-20261029`。

- `agent-v3-source-audit-v2.json`：修复HTTP追踪阻塞后的同步发布回归，19任务、100实际OS进程，13,609请求完整源验证。
- `agent-run-v4-periodic-source-audit-v2.json`：独立错峰生产负载，原同步慢样本保留。
- `agent-run-v5-final-source-audit-v2.json`：最终冻结源码对应19任务与低电接力，13,073请求；每个机器人写查、完整JSON、原journal、实际图谱、追踪与关闭证明分别验真。
- `live-ui-acceptance-v2.json`：原实时页面、单步、查询与切换run实际关闭旧100进程，最后保留高峰输入ready；14192为新增候选，旧服务未变。
- `final-source-integrity-v4-v2.json`：43项生产/运行/压测代码与完整回归冻结树完全一致；容量审计工具后续独立版本和实际10案例审计单列。初次44项不完全匹配检查原件仍留服务器。
- `robot-io-reference-v1.tar.gz`：约40.5 MB，完整每机器人请求/响应gzip、客户端/服务器追踪、Agent持久ledger、运行结果及日志、源码清单和全测试日志。SHA-256：`f572d5963c8be0113042c306904186c9e8a86297d035da33f2eec78ca9e9fa94`。

包内首轮v1驱动Query传入transaction错误和第二轮v2传输门限瞬态503均保留，物理推进前闭锁，未改写为成功。v3同步突发仍有超500ms读写样本；v5错峰生产查询3533条，均值27.08ms、P99 260.15ms、最大316.19ms，0超500，写9540条仍有17条超500。不要将不同负载合并为“所有请求永久小于500ms”。

原canonical `journal.sqlite`、`checkpoint.json`、`frames.sqlite`及其可重建oracle留在服务器各run目录，未将这些较大的文件重复放入Git。每份审计报告记录它们及HTTP原件SHA；重新审计需要对应源journal，或重跑同源输入。运行中原件不打包，不提交数据库凭证。

冻结源码 `source-integrated-v4` 全回归929项：915通过、14显式可选跳过；前端86项通过。容量审计工具最终版额外3项单测与10例实际只读审计通过。本机Windows沙箱部分loopback网络测试曾受环境限制，目标Pro6000关键51项验证50通过/1可选跳过；不以本机失败环境替代真实结果。

此处证明19任务闭环中的100个独立I/O进程，实际机器人同时业务高峰与24小时补能窗口在独立run继续验证；静态百万前缀另见 `../query-ab-v2/` 与 `../query-ab-heavy2-v1/`，两者不能冒充同run持续百万写读联合验收。

后续完整高峰及24h窗口已完成，见 `../robot-agent-peak-v1/` 与 `../wheel-day1-v1/`。本目录39MB包保持原始收集时的样例范围，后续结果单列，不覆盖旧包。
