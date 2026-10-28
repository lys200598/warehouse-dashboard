# 货架资源与时序库存对接

## 1. 实现边界

本模块复用李姚舜整合分支 `1e73a75b54fdcf951d3255b43ae9c9ec3a1e8873` 的 `Warehouse → Rack → Location`、`Dataset → WarehouseTask → Goods` 设计，连接当前V5实际提交的货权历史。它提供事务hook和只读服务；默认生产写者不因导入本模块而改变。显式选择资源镜像的接入由统一运行入口负责。

```text
WHRun ─ USES_LAYOUT → Warehouse ─ HAS_RACK → Rack ─ HAS_SLOT → Location
  └── USES_DATASET → Dataset ─ INCLUDES_TASK → WarehouseTask
                        └── INCLUDES_GOODS → Goods ─ INITIAL_AT → Location

WHSlot / WHLocation ─ INSTANCE_OF → Location
WHTask             ─ INSTANCE_OF → WarehouseTask
WHCargo            ─ INSTANCE_OF → Goods
WHGoodsState ─ AT_LOCATION → WHLocation
```

`WarehouseTask`保存计划字段，实际分配与接力继续保存在已有`WHTaskState`、`WHEvent`、`WHGoodsState`中；不会创建第二套相互冲突的货权记录。

## 2. 身份及两个已修正问题

- Warehouse ID包含完整场景内容SHA。同名R01在不同地图坐标下属于不同Warehouse；业务`rack_id=R01`单独保留。
- Rack ID在合并原属性之后设置，原始`id=R01`无法覆盖规范主键。所有货位外键都引用规范ID。
- Dataset ID由场景与任务CSV、初始库存CSV、manifest的完整SHA声明共同派生。不同日期/seed/scenario只要内容不同，就有不同身份；版本和开始日期不再是唯一标识。
- 静态记录独立于run；实际状态仍由run+seq隔离。资源数据与源历史在同一Neo4j事务提交。

输入SHA由现有数据校验、源journal和镜像链负责验证；资源模块不从网络或任意路径补读未提交数据。

## 3. 写入接点

`backend.resource_graph`导出：

```python
SCHEMA_QUERIES
ensure_schema(session)
validate_forward_batches(forward_batches)  # 在父写者写入前调用，返回seq1准备包或None
apply_resources(tx, forward_batches, prepared=None)
```

调用顺序：父写者读取原WHRun头并核对重放checksum、筛出真正前进批次；准备资源记录；调用原`RealMirror._apply(tx,batches)`；最后在**同一个tx**调用`apply_resources`。任何一项失败向外传播，整个事务回滚。不能在父事务提交后另起一条异步写链，也不能吞掉资源错误再声称本批已可查询。

只在seq1建立Warehouse、Rack、Location及初始库存映射。之后复用这些节点，增量投影改变/发布的任务和货物，并更新同事务`resource_graph_seq`。纯机器人状态批次不重写整库货架。

当前V5 checkpoint有意省略尚未发布任务：19任务样例的初始批实际有0个已发布任务、864件初始货物。资源图遵循这一信息边界：`WarehouseTask`随任务实际发布出现，Dataset通过原CSV SHA标识完整输入；不能把未发布的CSV计划提前暴露给机器人。`Dataset.initial_published_tasks`描述初始可见数量，不作为完整任务总数。

资源功能需从新run的seq1启用；对没有资源bootstrap的旧run返回明确错误，避免默默依据增量批次构造不完整静态图。冻结源和旧图谱无需迁移。

## 4. 查询某货架在指定时点装了什么

```python
RackInventoryService(mirror, timeout=5).inventory(
    run_id, seq, rack_id, limit=512, include_empty=True)
```

`mirror`复用既有`query(cypher,params,timeout)`接口，不创建新驱动。一次参数化Cypher同时绑定：

1. 原WHRun和资源投影水位均覆盖指定seq；存在该seq的WHStep/checksum。
2. 本run对应地图中的指定货架及完整静态槽位。
3. 每个槽位在`seq`之前的最新`WHSlotState`。
4. 对被占用槽位，核对该货物在相同截止点的最新`WHGoodsState`确实仍位于此处。

结果包含run、seq、batch checksum、地图和数据身份、容量、占用数、空位数和各槽位事实。库存不完整、重复货物、未来数据、货权不一致或超过槽位预算时明确失败，不返回表面成功的部分结果。

`INITIAL_AT`仅保留最初位置。货物被取走后，当前或后续水位的货架库存会减少；历史水位仍可看到之前的占用。暂停或图谱写入滞后时应使用已镜像seq，不把物理时刻直接当图谱截止序号。

槽位查询预算与知识包默认返回条数是两个概念；48槽的货架不能直接使用ANN查询的默认`limit=5`。

## 5. 本地验证及待验收项

`tests/test_resource_graph.py`覆盖规范ID引用、不同输入身份隔离、完整容量、只建一次静态图、后续任务增量、失败传播、历史库存、未来/串run/货权不一致拒绝、预算和类型检查。另从实际RealSimulation构造0秒首批，核对36架、1728槽、864初始库存及0个已发布任务。

离线fake事务与纯服务测试不等同于真实Neo4j语法、事务回滚及查询性能验收。真实数据库应使用独立库验证：资源关系计数、实际任务发布、取走/放入前后同run多水位库存、重放恢复，以及资源步骤失败时原WHRun头不能提前提交。


## 6. 已接入的运行入口与真实数据库验证

新run显式使用 `python -m backend.warehouse run ... --resource-graph --knowledge-backend neo4j`，或互动入口 `python -m backend.warehouse live ... --resource-graph`。使用 `backend.resource_graph_mirror.ResourceMirror`，它在父状态写入之前验证资源包，在同一个Neo4j事务内建立资源映射并推进资源水位。新run必须从seq1启用；检查点绑定开关版本，避免恢复时悄悄改变投影模式。

统一查询增加第12包 `rack_inventory`，例如：

```json
{"query_id":"rack_inventory","run_id":"替换为当前run","seq":1,"rack_id":"R01"}
```

2026-10-28 Pro6000独立17691实例已完成实际required运行：19/19任务、1次接力、2次历史授权，70批/7000条实际机器人状态，资源和原图水位相同。36架/1728个HAS_SLOT关系计数正确；在水位1/35/70核对5184个货位时点值，全部与原journal一致。幂等重放节点计数不变，注入资源阶段失败后父WHRun写入回滚。108次单客户端库存功能读观测均值8.92ms、最大11.79ms；这些数字仅是该小样例功能审计观测，百万规模和100并发资格仍独立验收。

浏览器也已在150×100m候选中查询新增R60：48位、占用11、空37，来自该run水位1；原始返回保存在本轮交付证据中。

### 跨run可见性

资源包按本run提交逐步物化任务。同Dataset可跨run共享静态Task节点，另一run可能已经发布后续任务。因此Agent接口须绑定当前run与cutoff；未来不应开放任意Dataset遍历后承诺“未发布任务全局不可见”。当前货架和任务知识包均按run/seq限定历史事实。
