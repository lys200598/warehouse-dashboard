# balanced保守扩仓候选

### 任务背景

主任务已报告medium48和large60的真实负载迟到退化，需要收敛为最后一个保守候选；不能通过改变机器人比例掩盖面积和动线带来的运力下降。本子任务仅做配置型地图生成与静态安全审计，不改已冻结backend或正式地图。

### 关键结论

- balanced为100×90m、4列×10行、40架、1920货位、1936任务端点和100个单容量物理泊位。
- 原100×84提议在既有生成器中被拒绝：货架与通道总跨度60m，两侧仅12m，低于14m边界循环区门槛。
- 理论最小深度88m恰达14m边界；实测工厂浮点跨度60.000000000000014，使88m的top为13.999999999999993。最终90m留2m总余量、每侧1m，未调低门槛，未用ULP修饰尺寸。
- 实际load_scene与audit_roadmap审计通过；无不可达端点、不安全连接或边。未进行本子任务内的吞吐验收，manifest仍标throughput_accepted=false。
- base正式地图SHA和工厂源码SHA在操作前后完全一致；仅工具进程内临时注册balanced，结束后恢复注册表。

### 执行过的操作

使用本机标准库Python调用 `ops/generate_balanced_layout.py --output data/layouts/expansion-candidates/balanced`，完成现有工厂生成与实际几何审计。84/88仅在工厂的边界前置检查处拒绝，未写出不满足的地图文件。

运行 `python -m unittest discover -s tests -p test_balanced_layout.py -v`，新增6项全部通过（5.855秒）。测试覆盖最终尺寸/容量、前置拒绝证据、R01—R36等原端点和100泊位、实际几何审计、注册表/源身份不变及拒绝覆盖已有输出。

### 修改内容

- `ops/generate_balanced_layout.py`：纯配置临时注册并调用冻结生成器，保存boundary-preflight；不修改backend文件。
- `data/layouts/expansion-candidates/balanced/`：layout、位置表、manifest、实际几何审计及前置检查。
- `tests/test_balanced_layout.py`：6项回归。
- 扩仓候选README补充最终收敛候选与几何纠正依据。

### 后续建议

主任务仅做最后两组fullcoverage1728、mix70和全X3的同条件运力对照。若任务迟到或积压不能满足要求，保留正式旧地图，不再无限扩大或调参。当前地图未设默认，实时服务和旧实验均未动。

### 证据

- layout SHA-256：`2688227a87fad53a6c50c67ce9f23b54396990de4b574fab2716388689232693`
- task-locations SHA-256：`76c21ede8182cc163d76b7a6a31f3ba23e5ac5691f2d4db77e9a62247156eb36`
- 本地日志：`C:/E/projects/warehouse/research/branch-review-20261028/balanced-layout-tests.log`
- 生成完整输出：同目录 `balanced-layout-generation.json`
