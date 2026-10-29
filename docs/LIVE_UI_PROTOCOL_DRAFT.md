# 实时控制台：源码审计、协议与接线范围

状态：2026-10-28，独立前端已实现，服务端真实联调由对应交付记录确认。本文记录代码调用链和约定；离线Node测试与实际网页控制验收分别记录。

## 1. 已核对的三条调用链

### 原实时控制台

`dist/index.html → dist/app.js → WebSocket /ws/simulation → backend.server.SimulationService → backend.simulation.SimulationCoordinator`。

原服务通过`POST /api/control`、`/api/tasks/priority`、`/api/tasks/batch`修改旧引擎。它的快照包含camelCase机器人、旧货架和运行指标。原页面地图固定32×22、每面6列，部分初始展示和离线演示包含生成状态；真实后端帧到达后停止浏览器自主推进。直接让旧页面读取新版raw frame会遇到字段、货架和坐标不匹配。

### V5保存帧回放

`python -m backend.warehouse serve → backend.wide.web → latest.json / frames.sqlite / checkpoint.json → dist/v5/wide.html`。

该服务的`do_POST`明确返回405。回放时间轴选择已保存的真实帧，播放按钮控制浏览器回放。其操作不暂停或继续后台物理仿真。

### 本轮独立实时接线

`dist/live.html → dist/live.js / live-model.mjs → 新实时HTTP/WS适配层 → 单一KnowledgeSimulation协调器 → 原journal及Neo4j提交链`。

新页面复用原`styles.css`三栏调度台风格，保留原鼠标方向与敏感度。`index.html/app.js`、旧`wide.js`及旧服务原样保留。新实时服务根路由选择`live.html`；旧控制台继续由自己的旧服务端口提供。

已运行的冻结联合实验保留其源码、运行目录和写者。新交互服务使用新的唯一run和output。只读观察某个外部长run时，应明确标为监视模式，关闭操作该外部写者的按钮。

## 2. 最小HTTP/WS协议

|接口|请求/返回要点|
|---|---|
|`GET /api/scene`|normalized wide scene；`spec.width/depth`、racks、stations、buffers、docks、capacity|
|`GET /api/snapshot`|当前真实物理帧；可直接frame或`{"snapshot":frame}`|
|`GET /api/health`|服务健康状态；与frame的运行生命周期分别表示|
|`GET /api/datasets`|`datasets`数组，每项id、label、task_count、manifest_sha256；`selected_dataset_id`|
|`GET /api/knowledge/catalog`|原`QueryCatalogService.describe()`，包括queries及参数范围|
|`POST /api/control`|action为start、pause、step、speed、new_run；speed使用value，new_run使用dataset_id|
|`POST /api/knowledge/query`|原catalog请求，绑定run_id和已镜像seq；完整原始返回|
|`WS /ws/simulation`|推送同一真实frame协议；队列可仅保留最新帧，前端不跨跳帧画虚构轨迹|

控制响应可以直接返回frame，也可返回`{"snapshot":frame,"control":...}`。失败使用真实4xx/503和JSON `error`或`detail`。前端展示错误，保留上次真实帧。

### frame字段

- 沿用`sim.frame()`：runId、tick、elapsed、running、robots、counts、metrics、inside、pending、unreleased、occupiedSlots、bufferCounts、totalTasks。
- 机器人沿用raw字段：robot_id、x、y、battery、stage、carrying、task_id、cargo_id、location_id、dock_id等；不补造任务或货物编号。
- `status`：ready、running、paused、complete、failed；`speed`为墙钟节奏倍数。
- `generatedAtMs/capturedAtMs`：真实采集时间；`sourceKind=live_simulation_memory`明确来源。
- `metadata`：dataset_id、dataset_sha256（tasks CSV）、scene_sha256、initial_inventory_sha256、manifest_sha256、source_sha256、adapter_sha256、commit_seconds、knowledge_mode、knowledge_backend、output。
- `knowledge`：backend、durable_seq、mirrored_seq、lag、last_error，以及committed_sim_time。

物理帧可以领先周期提交的图谱。前端并列显示`elapsed`和`committed_sim_time`，查询默认使用`mirrored_seq`。刷新地图不会自动发起ANN或查询全车队上下文。

## 3. 原功能怎样处理

|功能|新页面处理|
|---|---|
|左键旋转|保留，方向/灵敏度沿原app；顶视左拖为平移|
|右键/中键或Shift左键平移|保留；右键不旋转，不触发对象选择|
|滚轮缩放、视图复位|保留，缩放锚定鼠标所指世界点|
|选中机器人、筛选、导出|来自当前真实帧；空task/cargo保持空值|
|货架层与库存展示|按90×76m、36架、8列/面、双面3层和实际occupiedSlots绘制|
|开始/暂停/单步|提交给真实协调器，在完整step边界确认；前端不本地推进机器人|
|倍速|0.5、1、2、4、20、60×；只调整墙钟等待，保持物理速度/能耗/CSV时间|
|新run|选服务端固定数据集，确认后保存旧输出并另开唯一run；不覆盖原始输入与结果|
|随机流量、插入优先任务、新增100单|固定CSV模式关闭，避免改变已声明实验输入|
|热力图、估算ETA、路线|本轮不展示旧页预制热区/示意ETA；真实路线需由写者提供实际预约段后单独接入|
|知识查询|按需调用11种catalog包，显示完整请求与返回；17D精确和32D完整ANN显式区分|
|历史回放|继续使用独立只读回放服务；实时控制按钮不复用回放播放按钮|

## 4. 新文件与边界

- `dist/live.html`：独立实时三栏页面与运行/查询表单。
- `dist/live.css`：复用原样式后的实时布局与窄屏适配。
- `dist/live-model.mjs`：纯协议、镜头、筛选、库存几何和查询绑定函数，可离线单测。
- `dist/live.js`：真实HTTP/WS接线和Canvas绘制，WebSocket断线时1秒HTTP真实源后备。
- `tests/live-view.test.mjs`：21项Node正反例，覆盖鼠标、地图、真值字段、查询水位、生命周期、导出与关闭假仿真。

前端没有修改业务后端或冻结生产源。独立服务需要验证唯一写者、提交顺序、暂停恢复及新run输出保存；这些由服务端联调记录负责。

## 5. 网页验收步骤

1. 打开新版实时服务，核对本run地图、CSV SHA、runId及source身份；网络面板能看到真实WS/HTTP帧。
2. 暂停后记录tick；等候至少两个正常刷新周期，tick不继续；单步只前进1模拟秒；开始后继续增长。
3. 分别操作左键、右键、Shift、滚轮和俯视，确认方向与原界面一致。
4. 对照当前snapshot中两台机器人的位置、电量、阶段、任务和货物，界面逐项相同；跳帧不在货架间直线插值。
5. 选择不同固定数据集另开run，核对新ID、新output和原目录保留；数据集下拉选择不会被周期帧重置。
6. 查询默认绑定已镜像seq，返回run/seq/checksum一致；物理时间领先图谱时保持两种时刻并列。
7. 断开WS后显示重连/后备状态，页面保留实际帧；无任何随机机器人、虚构任务或浏览器自主业务推进。

只有实际网页与真实服务通过这些步骤，才将“实时交互接入”记为完成。离线Node测试只证明被测纯逻辑与静态接线范围。
