# 自由边界紧凑证据

本目录保留固定口与逻辑自由口的S1原件、S2联合物理复跑原件、来源清单及只读工具。大SQLite/检查点/源码树仍保存在Pro6000 `/data/workspace/wz/warehouse-free-ports-20261029`，未复制到仓库。

## S1来源与结果

- 生产树：`source-ports-v1`，后来的查询源码没有替代旧运行身份。
- `free-boundary-plan-v1.json`列出三次实际输入、配置、输出和退出文件。
- `free-boundary-audit-v1.json`核对三次完整CSV、SHA检查点、事件和保存帧、实际源码；固定控制23晚点，自由两组1728任务均0晚点。
- `free-boundary-physical-comparison-v1.json`在验证绑定来源event_seq后，仅规范化binding事件引用的run前缀；两自由口场景418帧及全部物理字段一致。原通用比较因该身份字段得到tasks=false的记录原样保留，补充报告没有删除物理字段。
- `free-boundary-deadline-margins-v1.json`记录自由口最紧仍提前108秒，固定口最坏晚149秒。
- 两自由口的online donor行均0，收益来自自由选口，本场景推荐基础方案。

## 实际KG与v1字段修正

`free-boundary-required19-kg-audit-v2.json`及对应独立v2只读工具是推荐的S1实际KG证明：19/19、0晚点、1接力、69批/6900实际状态、5760个槽位时点值、19绑定与计划/实际边和真实货权核对。

v1报告的`source_backend_code_sha256`展示字段被后续局部变量覆盖成绑定对象。原v1报告及原工具完整保留，不能把该对象当SHA。独立v2只改backend哈希保存变量/展示引用并增加64hex类型检查，其他核对逻辑不变；result/checkpoint/journal三份原件SHA与v1完全相同，仿真未重跑、数据库未写入。v2原日志以`.txt`原字节保存，内容SHA与服务器`.log`一致。

该19任务真实样例的持久化未准入未绑定端点样本为0；离线覆盖与实际压力样本分别记录，不能用这个样例声称该真实路径已有覆盖。

## S2联合复跑与S3来源边界

`combined-plan-v1.json`记录`source-combined-v1`、逻辑数据、配置、输出及退出文件。`combined-free-results.json`、`combined-free.exit`为该run的原件，`combined-free-config.json`保留实际配置。

`combined-free-source-audit-v1.json`只读核对完整CSV、检查点、journal、保存帧、真实退出及源码身份：1728/1728、0晚点、4169秒、业务活动峰值100；平均/P99/最大流转385.126/856/1006秒。相对S1自由口基础运行，最终任务/货物/机器人、全部10755事件及418个物理帧一致，只规范化run/event前缀和绑定来源事件引用，性能/源码元信息另列。报告SHA为`4273da878e4ca181b377e7146460a7df4fe5f11c2117c4ff74a5c7788bdaca55`。

`full-combined-python-v1.txt`为服务器`.log`的原字节，实际803项、796通过、7跳过；对应退出文件为0。日志SHA为`9285c6002095eda5fff3e3f1cb0d9874862966aafa0125b770df3e0d3a020637`。

S1的backend清单含117个Python文件。S2清单`combined-backend-sha-v1.json`含120个Python和`backend/query_catalog/kg56_mapping.json`，合计121项；S1/S2物理核心相同，查询服务、视图及CLI共五个Python路径变化，完整backend并不相同。S2/S3的121项清单字节相同，SHA均为`ff05ded0ffaafc7032728f577224dac72845d98ce69b27343aa51d33c39fe16a`。S3`source-combined-v2`的非缓存树差异仅两个ops实时发布入口与一个对应测试，差异记录见联合来源报告及`combined-livepoint-sha-v1.json`。

S2 runtime身份重算与原run相同：`0b7eea4049685a93a96e6ba61d21be879b239e87030fa39cb2c0e2bae5cc799b`。S1原件与报告继续保留，没有被S2/S3的源码身份替代。

## 范围

CSV转换只改变IN的source及OUT的target为逻辑标识，其余任务/货物/货架/时间/顺序和初始库存保持。两场景任务CSV哈希本来不同，报告没有强迫整个矩阵的同CSV资格通过。

S2完整物理复跑和回归结果已纳入本目录。planning秒数、任务流转秒数及API查询毫秒分别计量，当前一小时0晚点不授予长期、百万查询、动态API尾延迟或端侧资格；动态服务部署状态由主任务的最终记录说明。

文件字节SHA见`SHA256SUMS.txt`。
