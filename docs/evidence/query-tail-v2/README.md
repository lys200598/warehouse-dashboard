# 查询优化证据索引

- `static/`、RESULTS.json及query-v2-file-sha.json保留早期V2来源。
- `static-s9/`为当前源码的固定百万前缀4类2084响应，全部完整验真；冷global601ms、self峰不足100仍使严格资格false。
- `live-v2`至`live-v9b`保存各动态阶段原报告/真实错误，不回写前版。v9b124131完整JSON源验真，24条超500，最大约713ms；动态样例7000状态，没有同run百万资格。
- source-s9-manifest.json为242项完整来源；source-s9-backend-manifest.json为部署源根source-manifest.json的原字节副本。
- combined-s9-frozen-test-report.json对应独立冻结树850项、845pass/5skip，前端86pass。S8移动开发树848测试没有授予冻结源资格。
- tools/为对应外部测量/后验原脚本；所有同步与归档均在压力后，生产查询未调用Oracle。
- delivery-ui-*为压力归档后的少量只读交付检查，不混入已冻结benchmark分母。原index字节和WebSocket/真实self包通过。
- protected-services-readonly-v1.json为原14186/14188 PID、健康与正式adapter SHA复核。
- 大型源、压缩完整响应、Oracle与观察日志留在各报告的服务器原路径及工作区research/free-boundary-20261029和research/query-tail-20261029。download-sha256.json与报告记录其原件SHA。

所有P99为nearest rank；JSON/图谱正确性、动态规模、每类真实峰、最大500ms分别判定，不能拼接成整体已验收。
