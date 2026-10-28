# S9 当前源码固定百万前缀回归

## 结论

完整执行结束，**严格资格未通过**。2084条完整HTTP响应逐原始journal/独立RealOracle验证全部正确，业务/HTTP错误0。唯一超过500ms的是首次冷global：601.35ms；self自然真实API峰78/84/87/85，保留并发资格缺口。其余三类压力每轮真实API峰均达到100以上。未过滤cold/warm、错误或慢样本，也没有为提高峰值增加服务端延时。

本轮读取同run的**1,652,135节点、507,300个真实32D向量、seq5073**，生产源码为S9的242项manifest，所有请求通过S9实际query-only侧车与Native网关。该结论范围为固定最终前缀读取；动态写入和控制闭环有独立实验记录。

## HTTP完整读取与解析时延

单位ms。P99采用nearest rank ceil(0.99*N)。每类521条，包含cold1、匹配warm8、四轮128压力请求（100不同机器人+额外28条真实请求）。

| 查询 | 均值 | P99 | 最大 | >500 | 四轮真实API峰 |
|---|---:|---:|---:|---:|---|
| global |110.30|240.10|601.35|1|128/128/128/128|
| local_awareness |111.75|275.95|340.17|0|128/123/128/128|
| self |18.38|53.18|58.46|0|78/84/87/85|
| full-ann（完整32D） |178.16|412.85|457.30|0|108/124/119/114|

每类Native前门、侧车实际ASGI区间、catalog执行、客户端body读取/JSON解析分别在final-report.json报告。完整ANN521条都走原ann100，没有改算法或替换为17D/Flat。

## 冷global的保留证据

原请求 `s9static-global-cold-first-call-c001`：catalog583.20ms；fixed-key probe200.49ms、fleet292.13ms、events42.87ms；summary read5.98ms、JSON roundtrip2.61ms。主要成本在实际Bolt冷路径，查询编译或缓存因素尚无PROFILE证据。新summary缓存显式准备34.20s，独立于正式HTTP readiness报告；首次API冷请求仍在完整统计中。

## 来源与运行边界

- 冻结源：`/data/workspace/wz/warehouse-free-ports-20261029/source-combined-v7`。
- 242项源manifest SHA：`d057e847ff37f1ea4d29510e5dc81d66889b3ca0e14acd4aea894500626b42d7`。
- 本轮独立根：`/data/workspace/wz/warehouse-query-S9-static-20261029`。
- 静态DB17689只读，使用新journal副本和新summary-cache，旧V2输入和产物保持不变。
- Native端口14289、8个侧车14220..14227；只停止了自己的静态进程。结束检查这9个端口均无监听，动态14189/14190仍监听。
- Native原模板字节保留，只派生9行端口变化；两份SHA和精确diff在本目录。没有修改缓存、重试、buffering或timeout策略。
- 全部HTTP测量结束且静态进程停止、前后图点和源SHA核验完成后，才启动独立完整JSON验真。验真53.03s，未计入生产查询耗时。

## 原件与SHA

本目录保留紧凑报告。完整请求、原始响应、分段观测和日志位于本地 `research/query-tail-20261029/S9-static-result/` 及服务器上述独立根。evidence-manifest.json与supplement-manifest.json列出大小与SHA，下载后逐文件复核通过。

- final-report.json：`ab9dd5d17edcf0139c687d2a9ba9347e04f0b6559a5dd7bc07de25a932bc07e1`
- requests.jsonl：`f095a50513ea764555c7de5ed00eacc726e50769833b2c2a48174be59e5b1a83`
- responses.jsonl.gz：`aeeb47cc2e1bf0d0ff1de931fafb10bf4eb275312acd19ca0a26eb7172389027`
- 原始journal：`0e00da2577e0f76ab29e9ecf6149d6a061d5b2b3595a63a9ed04b67fe515291c`

按本轮收尾范围，没有追加benchmark、没有覆盖失败结果或旧V2原件。
