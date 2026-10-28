# 实时required闭环与浏览器来源检查

### 任务背景

为已部署的实时界面补独立required模式实跑及来源证明，不控制用户正在使用的主run，不干扰原性能实验。

### 关键结论

- 独立run完成19/19任务、1次实际低电交接，2次历史门禁查询；15项控制源检查全通过。
- 最终journal与图镜像均400批；图中40000实际状态与959事件和原source一致。
- 运行中self加完成后11类包，共12条完整服务返回，全部严格源核验通过；单客户端最大482.249ms。
- 主UI保存的seq16 DOM查询另做无计时源检查。严格报告保留0/0.0类型差异；批准的有限精确数值表示profile通过，7项负例与边界测试通过。
- 主UI控制和用户当前运行均未被本次验收修改。

### 执行过的操作

在独立`acceptance-runs`启动Runtime，真实记录内存帧、图水位、完整请求/响应；关闭后执行原控制审计和查询源验证器。主UI的journal使用SQLite在线backup，不复制活跃数据库文件。原DOM及严格负结果保持原样。

### 修改内容

新增`ops/browser_query_source_profile.py`、`tests/test_browser_query_source_profile.py`及`docs/evidence/platform-v1/live-interactive-acceptance-report.md`与配套JSON。Runtime与原验证器保持原字节。

### 后续建议

使用[验收报告](../../evidence/platform-v1/live-interactive-acceptance-report.md)分别理解独立样例、浏览器可见值和性能覆盖；百万级与100并发继续查看专门的联合实验。
