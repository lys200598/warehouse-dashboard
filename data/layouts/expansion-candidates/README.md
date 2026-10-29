# 独立扩仓候选

原正式地图 `data/layouts/warehouse100-wide-v2.json` 保持不变。这里的地图完成静态几何和路网检查，尚未通过100机器人同负载吞吐验收，不是新的默认布局。

|候选|外框|货架|货位|任务端点|主通道/普通巷道|
|---|---|---:|---:|---:|---|
|balanced|100×90m|40|1920|1936|8m / 5m|
|medium|120×90m|48|2304|2320|8m / 5m|
|large|150×100m|60|2880|2896|8m / 5m|

每架保持3层、2面、8列，共48货位。机器人半径0.55m、机体0.7×0.7m、100个充电待命物理泊位和原P/C/IN/OUT业务ID保留。空桩穿越及停靠机器人避碰规则沿用原定义，未缩小安全包络。

- `layout.json`：独立版本与坐标。
- `task-locations.csv`：所有货位及16个边界任务端点；货位坐标为对应取放作业点，不是箱体中心。
- `geometry-audit.json`：实际WideNavigator/WideRoadmap安全与连通性检查；不包含动态死锁、任意故障或吞吐保证。
- `manifest.json`：源文件、地图和位置表SHA；明确 `throughput_accepted=false`。

既有R01—R36及其货位ID仍存在，但货架排布、分区和坐标有改变；不要把旧地图哈希、旧热点坐标或已测出的任务时延贴到候选上。large在8×8网格中使用60个货架位置，东南部剩余4个网格位置留空，不计库存容量。

## 重新生成

从仓库根目录运行，输出必须是一个不存在的新目录。Linux环境优先使用 `bash run_in_lade.sh python`。

```bash
python -m backend.wide.layout_candidates --candidate medium \
  --output data/local-check/medium-new --audit
python -m backend.wide.layout_candidates --candidate large \
  --output data/local-check/large-new --audit
```

只扩大面积、不增加货位的消融候选：

```bash
python -m backend.wide.layout_candidates --candidate medium --rack-count 36 \
  --output data/local-check/medium-36-new --audit
```

显式 `--rack-count` 最少36，最多为对应网格位置数；它保留默认候选前N架的坐标，使用单独的地图版本后缀，不重排已有ID。

## 与已有数据对照

使用 `backend.wide.data.migrate_dataset(old_scene, new_scene, old_dataset, new_output, deadline_policy="preserve")` 可将同一任务和初始库存CSV逐字节映射到新地图，并更新manifest。它验证格式、库存参考与身份，不证明任务能及时执行。默认不延长截止时间。

新增货位也需要另做覆盖全部库区的负载，不能只测旧36架就宣称扩仓满负载合格。每小时任务率、发布模式、机器人速度和取放耗时应在同负载对比中保持一致；面积、货架数量、混合型号分别消融。

## 容量检查

`wide.data.load_scene`按正整数capacity、每架容量、层/面/列乘积及全部唯一货位求和进行一致性验证，不再固定要求1728位。V5继承路径的V2入库准入和V3物理step边界也按 `len(self.slots)` 检查，保留满额拒绝与溢出硬断言。

旧day1/V3专用场景生成器继续保留其旧固定布局合同；新候选使用wide生成器及V5运行入口。该变更没有自动迁移任何旧实验或运行中的服务。


## 新增货架的独立覆盖实验

原CSV逐字节迁移只使用其引用的旧货位，无法单独证明新增货架参与了任务。可另建覆盖映射实验：

```bash
python ops/remap_capacity_coverage.py \
  --old-scene data/layouts/warehouse100-wide-v2.json \
  --dataset data/examples/warehouse-v5 \
  --new-scene data/layouts/expansion-candidates/medium/layout.json \
  --output data/local-check/medium-coverage-new --seed 20261028
```

工具保持任务ID、货物ID、入出库类型、发布时间、截止时间、任务数、初库存数量，统一单射映射所有出现的storage ID。先分配任务实际使用的货位以均匀覆盖新架，再分配仅初库存的货位；保持层和面，列在覆盖优先的前提下尽量保留。IN/OUT边界ID不变。

输出包含完整 `slot-map.json`、源/目标SHA、严格校验结果和 `coverage-remap-report.json`。这是改变货位引用的独立负载，`csv_byte_identical_control=false`，不得与原CSV不变的对照组混称为同一输入。原热点坐标保留为来源信息，新分布需要重新解释，工具没有使用实际完成时间或延长期限。

报告分别列出 **task_rack_coverage_complete**（实际任务覆盖）和 **all_referenced_rack_coverage_complete**（任务加初库存覆盖）。上例19任务不足以让60个货架都有任务，即使初库存已分布到所有架，也会明确报告任务未全覆盖。正式完整覆盖实验应使用至少拥有目标货架数量的不同任务货位的输入，并核对 `task_target_racks` 和 `uncovered_task_racks`。


## 本轮收敛候选：balanced

此前medium/large在主任务的真实对照中出现明显迟到退化，因此新增保守的100×90m、4列×10行、40架1920位版本，继续独立评估，不自动替换正式地图。

原提议100×84m无法保留现工厂要求的边界循环区：10行货架跨度为 `10×1.2 + 8×5 + 8 = 60m`，84m深度两侧仅12m，低于14m门槛。理论最小深度88m正好达到14m；工厂浮点累计为60.000000000000014，88.0的边界会表示为13.999999999999993。最终取90m，额外留2m总深度、每侧1m，不使用ULP边界调整、不降低安全阈值。

`ops/generate_balanced_layout.py`仅在自身进程中临时注册这一个配置，调用既有工厂并恢复注册表；没有修改已冻结的backend模块。重新生成仍要求全新输出目录：

```bash
python ops/generate_balanced_layout.py --output data/local-check/balanced-new
```

balanced目录额外包含 `boundary-preflight.json`，保留84m、88m的拒绝依据及90m实际通过记录。几何审计通过与运力通过分别记录；主任务将做最后的mix70和全X3两组真实负载对照，未通过时保持候选状态。
