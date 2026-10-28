# 派生任务汇总只读原型

### 任务背景

完整任务聚合的SQL优化仍有长尾，继续检查从原始journal维护独立派生汇总的收益，保护正式七日运行和既有默认值。

### 关键结论

- 固定877批原始源构建5.38MB侧车，耗时6.81秒，未用Oracle生成返回。
- v2共16条完整API，baseline/sidecar各8条；全部独立源验真通过。
- global均值925.77→154.57ms，local953.01→144.93ms；侧车8条样本均低于500ms。
- 9项测试在Windows/Linux通过；原型尚未接live writer，也未新增100-global并发压力。
- 正式backend、源码、图谱和默认入口保持原样。

### 执行过的操作

逐批读取原journal，构建新SQLite；AB/BA记录完整请求与响应；对实际producer在线backup后，通过独立源验真器核对完整字段。保存旧快照观察head覆盖不足的失败报告，再以覆盖实际head的新快照严格重验，未放宽验证器。

### 修改内容

新增`ops/probe_catalog_summary_sidecar.py`、`tests/test_catalog_summary_sidecar.py`、`docs/evidence/platform-v1/catalog-summary-sidecar-report.md`和配套紧凑JSON。

### 后续建议

按[evidence报告](../../evidence/platform-v1/catalog-summary-sidecar-report.md)完成连续水位发布、恢复、身份与参数隔离及混合并发验收，再决定是否切换默认。
