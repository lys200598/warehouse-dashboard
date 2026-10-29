# 七类波动：完整七天参考输入

设计、各日数量、生成参数和参考期限见 [MULTIDAY_DATASET.md](../../../docs/MULTIDAY_DATASET.md)。本目录包含原六文件的完整副本，CSV字节未改变：50,112任务、864件初始库存、连续604800模拟秒。

|文件|内容|
|---|---|
|tasks.csv|七字段入出库任务，25,003入库、25,109出库|
|initial_inventory.csv|初始货物与货位|
|manifest.json|地图身份、固定种子、生成参数及CSV摘要|
|profiles-report.json|七类时间与空间波动统计|
|reference-inventory.json|参考库存连续性、容量及跨日统计|
|validation.json|原独立校验结果|

七类：均匀、固定峰值、局部热点、批次脉冲、热点迁移、进出潮汐、混合压力与恢复。初始库存与货物ID跨日延续。所有文件SHA-256记录在 `SHA256SUMS.json`。

本样例使用原地图 `data/layouts/warehouse100-wide-v2.json`，36架1728格及具体IN/OUT工位。新布局、新地图hash和自由口是独立版本，不能直接沿用本目录的地图身份。原样例保留供复现，转换时使用新目录：

```bash
bash run_in_lade.sh python -m backend.warehouse validate \
  --scene data/layouts/warehouse100-wide-v2.json \
  --dataset data/examples/seven-patterns-20261028

bash run_in_lade.sh python -m backend.warehouse convert-boundaries \
  --scene data/layouts/warehouse100-wide-v2.json \
  --dataset data/examples/seven-patterns-20261028 \
  --output data/local-check/seven-patterns-logical
```

参考统计按生成器的参考执行模型计算；实际机器人晚点、充电、换电和交通由仿真运行决定。任务CSV不预先指定机器人及电量行为。本目录不含仿真轨迹、数据库或图谱快照。
