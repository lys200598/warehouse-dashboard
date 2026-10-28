# 100机器人仓库仿真与调度

本分支交付一套可重复运行的 **V5 仓库实验环境**：90×76米仓库、36架1728货位、100台机器人、100个充电兼待命泊位，以及入库、出库、充电、低电交接、避碰、恢复与网页回放。

## 从这里开始

- **完整设计说明**：[仓库布局、调度与仿真指南](docs/WAREHOUSE_DESIGN_GUIDE.md)。从货物怎样进出仓库讲到算法、源码和数据对接，适合第一次接触项目的计算机专业同学。
- **数据生成同学**：[输入契约、位置表和独立校验器](docs/dataset-contract/README.md)。任务使用七字段CSV，机器人电量和调度策略单独配置。
- **验证结果**：[五类场景与100台同时作业证据](docs/evidence/warehouse-v5/README.md)。五场景任务全部完成，迟到和安全失败记录均为0；高峰取得100台同时搬运/取放的实际状态。
- **原交互控制台**：[FastAPI、WebSocket与Ray入口说明](docs/LEGACY_CONTROL_PANEL.md)。这些入口继续保留，方便逐步合并。

## 先运行19条任务的小例子

从仓库根目录执行，Python建议3.10及以上。V5中央仿真和回放使用Python标准库；需要原交互控制台或Neo4j时，再安装`requirements.txt`。

```bash
python ops/warehouse_v5_release.py status

python ops/warehouse_v5_release.py run -- \
  --scene data/layouts/warehouse100-wide-v2.json \
  --dataset data/examples/warehouse-v5 \
  --output data/local-runs/first-v5-run \
  --run-id first-v5-run \
  --frames --knowledge-backend journal

python -m backend.scheduling_v5.web \
  --output data/local-runs/first-v5-run --dist dist/v5 --port 14183
```

然后访问 <http://127.0.0.1:14183/>。在远程服务器运行时，将此端口通过SSH转发到本机。网页读取刚才生成的实际轨迹，支持时间轴、机器人选择及回放。

每次新实验使用新的`output`与`run-id`，这样结果可以保留并相互比较。已有运行可按指南使用`--resume`恢复。

## 默认方案与目录

默认选择保存在[`config/v5-selected.json`](config/v5-selected.json)：`port-guard + original-sipp`。调度先保证可行分配数量，再保护紧急/久等任务并优化成本；路径采用安全时间区间搜索，泊位和停止机器人持续参与空间预约。

|目录|阅读重点|
|---|---|
|`data/layouts/`|最终仓库几何、货位和物理泊位|
|`data/examples/`|19条上手样例和五种原始合成回归输入|
|`backend/day1/`、`backend/wide/`|库存、能量、任务、事件和空间模型|
|`backend/scheduling_v2/`至`v5/`|匹配、路径、安全事务、缓存及最终调度|
|`ops/warehouse_v5_release.py`|统一选定方案的启动入口|
|`dist/v5/`|最终布局的轨迹回放页面|
|`tests/`|库存、避碰、恢复、调度等自动测试|

V5采用中央进程推进100个机器人实体，便于确定性实验。原Ray交互入口保留独立使用。仓库布局和调度已形成通过回归的知识实验载体；知识查询访问层的后续优化单独对照、单独提交。

## 测试

```bash
python -m unittest discover -s tests -v
python docs/dataset-contract/validate_package.py
```

需要原交互服务相关测试时，先安装项目依赖。真实数据库集成使用显式测试开关和独立实验身份。

算法方法与来源：[独立实现的匹配与SIPP说明](docs/scheduler_v2_sources.md)。
