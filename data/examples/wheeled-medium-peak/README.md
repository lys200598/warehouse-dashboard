# 已测中型仓库高峰输入

本目录是Pro6000既有 `capacity-v1/input-medium-coverage-logical` 的原字节副本：1小时、1728任务、864件初始库存，覆盖全部48架。配对 `data/layouts/expansion-candidates/medium/layout.json`，工位使用逻辑INBOUND/OUTBOUND。任务时间沿用原高峰，货位统一映射，期限保持不变。

`slot-map.json`与`coverage-remap-report.json`从原coverage输入一并附上，便于追踪manifest中的映射来源。其余四个文件与已测logical输入逐字节相同。

运行说明见 [仓库对接总览](../../../docs/WAREHOUSE_WHEELED_HANDOFF_20261029.md)。本例是布局与算法回归输入，新的多日到货/热点方案由数据同学据新地图扩展。
