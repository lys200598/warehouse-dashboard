# Git交付源码范围只读审计

## 任务背景

为本轮自由工位及查询优化的最终提交准备路径白名单，只读检查Pro6000的交付Git树，并与本地完整`backend/ops/tests`源码比较。Git操作限定为状态、引用、远程地址、HEAD树和blob读取；本轮没有fetch、add、commit或push。

## 关键结论

- 远程交付树`/data/workspace/wz/warehouse-git-delivery-20261028`位于`wz/warehouse-design`，HEAD为`4998682a135848a29cca3862a67001930987545a`。
- `main`及`origin/main`均为`e721268183afbb4b448f38c65ca9cd4124d3d03a`。采集前后的HEAD、引用及status相同，status干净。
- 远程HEAD范围内228份源码，远程实际工作树SHA全部与HEAD相同。
- 本地有效源码245份：217份与HEAD相同、11份修改、17份新增、0份删除。28份差异均已按本轮自由口、图谱合同、查询或主任务集成归属标记。
- 本地缓存和临时测试运行目录共32处单列为不可加入白名单，未把测试生成的SQLite、检查点和结果文件当成生产源码。
- 新sidecar/proxy及关联入口/测试仍按开发中快照记录；本报告提供观察到的SHA，最终冻结和提交前由主任务重新核对。

## 执行过的操作

1. 使用既有`deployment/remote-warehouse.ps1`建立只读远程命令，设置`GIT_OPTIONAL_LOCKS=0`，读取branch/HEAD/main/remotes/status。
2. 通过`git ls-tree`和`git cat-file --batch`读取HEAD的`backend/ops/tests`，计算blob SHA-256，并与远程工作树逐文件比对。
3. 本地遍历同范围有效文件，生成路径、长度、SHA、HEAD blob ID、差异类型、任务归属、读取期稳定性及暂不冻结标记。没有读取或搬运大型运行数据。
4. 向图谱及查询子任务核实明确owned路径。自由口6份、图谱合同7份、查询明确9份、主任务集成5份、查询转主任务共享入口1份，共28份差异。

## 修改内容

只新增审计研究文件及本文，没有修改生产代码、远程仓库或旧服务：

- `research/git-delivery-remote-source-snapshot-20261029.json`：远程引用、status、远程地址及228份HEAD/工作树源码SHA。文件SHA：`db7b2ab417aca65e01605b062fb27abb0a9f5137d5ca393e56ab3f2214620a8d`。
- `research/git-delivery-source-allowlist-audit-20261029-v1.json`：完整本地清单、28份差异、归属及32处生成物排除记录。文件SHA：`323004091344120e1e8defc58f519753f51bc144232d51d2f58a46aaa7555085`。

研究文件位于本地总工作区`C:/E/projects/warehouse/research/`，不包含在`warehouse-dashboard`仓库内。本文记录采集时快照；仓库范围外的README、数据、配置、文档和部署文件不在本次源码清单审计范围。

## 后续建议

1. 主任务按明确白名单准备最终提交；重新计算开发中的`ops/warehouse_live.py`、`ops/warehouse_query_proxy.py`、`ops/warehouse_query_sidecar.py`和两个对应新测试的最终SHA。
2. 配置、文档、部署脚本和紧凑证据另列白名单，继续排除测试临时运行、缓存和本地依赖。
3. 提交前再次检查远程HEAD和status，核对没有并行修改；推送后记录提交身份，保留main原引用。
