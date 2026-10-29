# 独立压测GUI与图表：待检查

## 任务背景

用户希望保留功能丰富的脚本，再提供独立网页选择参数、查看各种数据与图表。沿用上一条要求：实现和检查完成后暂不提交、暂不推送。

## 关键结论

- 新增独立控制台`http://127.0.0.1:14595/`，原仓库页面、服务和数据库不变。浏览器标签已保留，SSH映射只监听本机。
- 支持4个预设、3种负载模式、8种查询及自身/全局混合，配置客户端/进程数、时间、次数、频率、窗口、预算和超时。参数有后端校验与高负载确认，单控制台仅一个活动实验。
- 显示原分析器的平均、P99、最大、超500ms、错误/丢到达；提供4种SVG图表、请求分页、历史对比、Markdown/JSON/CSV导出。混合P99按分类报告，不平均分位数。
- 测量期间显示阶段、墙钟时间和日志；请求排空后读取已封口原件生成图表。不会提前将部分数据当成完整通过；其他实验测量期间暂缓未缓存旧记录的解压。
- 44项测试全部通过、0跳过，真实浏览器连接、非法参数、两种小负载、分类、明细、历史对比及CSV下载通过。正式查询9条、预检3条，仅作功能检查。
- 检查中修复了请求明细与输入框重复ID、切换实验时旧指标残留、历史列表反复重绘及干净重启时TCP TIME_WAIT问题。Host/Origin/令牌与文件白名单保护保留。
- **没有Git提交和推送。** HEAD保持`83c3263ded70d22c18f4d11dcdb13454a86c4cb7`，等待用户检查后明确授权推送。

## 执行过的操作

- 复用`ops.load_harness`和`load_validation`，扩展受限计划，不改变原时延/错误/结构验真计算。
- 执行`WAREHOUSE_QUICK_LOAD_TEST_100=1 WAREHOUSE_TEST_100=1 python -m unittest tests.test_pressure_gui tests.test_quick_load tests.test_load_harness -v`，44项通过。
- 使用本机Node25.8.1做最终JS语法检查。初次服务器旧Node不支持现代语法导致静态检查退出1，原记录保留；GUI本身不需要Node，实际浏览器正常。
- 仅更新本轮自有GUI进程，确认无活动用户实验后操作；当前PID916081，记录见服务器`warehouse-pressure-gui-20261029-v1/launch-reviewed.json`。原服务14192未重启。
- 通过真实IAB页面执行小检查3条、自定义混合6条，自动预检分别1条与2条；101客户端输入被拒绝，未生成额外压力实验。
- CSV浏览器下载到用户Downloads的requests.csv；完整原始实验保留服务器`warehouse-pressure-gui-20261029-v1/jobs`。界面截图、DOM和紧凑验证记录保存在`docs/evidence/pressure-gui-v1/`。

## 修改内容

- 新增`ops/pressure_plan.py`、`ops/pressure_gui.py`与`tests/test_pressure_gui.py`。
- 扩展待检查的`ops/quick_load.py`，支持受限计划与`--gui`入口，原CLI默认小检查保留。
- 新增`dist/pressure-lab/index.html`、`style.css`、`app.js`，没有修改原`dist/index.html`。
- 新增`docs/PRESSURE_GUI_GUIDE.md`，更新README、快速说明、交付总览和高级手册的入口。
- 新增本总结与截图/DOM/测试/摘要；本地状态记录继续保持推送须明确授权。

## 后续建议

- 用户先访问14595检查交互和图表；仍以小检查为默认，其他压力方案自行选择并确认。
- 这个GUI当前测公开catalog HTTP，完整来源验真、百万前缀、100真实业务与持续写入资格另测，不能由图表或客户端数量自动提升。
- Windows本机GUI后端生命周期、私有Agent和ANN的GUI入口可后续分项加入，当前不声称已经覆盖。
- 待用户明确同意后再整理提交与推送；main不动。
