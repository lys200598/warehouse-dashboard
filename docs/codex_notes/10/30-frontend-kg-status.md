### 任务背景

在现有实时页面显示机器人进程模式、派单状态来源、C 图谱完整 Step 水位与机器人写入错误，并验证 Ray 写图谱运行时的页面渲染。旧 A 镜像默认关闭时，避免把旧查询区的零镜像数误读为 C 图谱没有写入。

### 关键结论

- 页脚显示 `robotProcesses`、`kgStateSource`、`robotGraph.latestCompleteStep`、实际滞后步数和写入错误数。未知水位和错误数显示 `—`，不推断为成功。
- `legacy_a_graph=false` 时，旧查询区显示“旧版 A 查询已停用；C 图谱状态见运行状态栏”，隐藏旧 A 查询输入与按钮；真正 C 图谱工作台未隐藏。
- 本地 Node v25.8.1 的 42 项前端检查全部通过；服务器 Node v12.22.9 不支持 `--test`，未升级其系统 Node。远程 `git diff --check` 通过。
- 真实运行 `wz-test-live-neo4j-032e8e4a` 中，浏览器收到 tick 235–263 的 36 帧，全部 `running=true`，机器人 5、29 有真实位置变化与取货/配送阶段。
- 20.006 秒 Chrome headless 渲染观察：1,685 个 rAF，平均间隔 11.87 ms，P95 16.7 ms，最大 33.3 ms，超过 100 ms 的间隔 0 次，JS 异常 0。观察到完整 Step 滞后 1 步、机器人写入错误 0。
- 目视截图中新状态栏无横向溢出；真实帧间隔插值及原有 3 秒播放缓冲保持不变。这是短窗口、少量机器人运动的渲染检查，不代表全天或 100 台同时搬运场景的流畅度验收。
- 旧 A 提示修正后的短检查确认：指定停用文本准确，旧输入区与选择按钮均隐藏，JS 异常 0，页脚无横向溢出；截图 `C:/Users/vsx/AppData/Local/Temp/wz-kg-ui-final/live-status.png`。

### 执行过的操作

- 从服务器当前分支获取前端源码与测试，未覆盖后端代理的工作文件。
- 执行 `node --test tests/dashboard-view.test.mjs tests/frame-buffer.test.mjs tests/live-view.test.mjs`，42/42 通过；执行 JS 语法检查与远程 diff 检查。
- 使用只读 Chrome headless 脚本和专用本地 SSH 隧道采样 14590；没有发送仿真控制命令，结束后关闭自己的浏览器和隧道。
- 有效运动证据位于本机 `C:/Users/vsx/AppData/Local/Temp/wz-kg-ui-moving/`：`live-browser.json`、`live-browser.png`、`live-status.png`。
- 旧 A 区提示修正后仅作短 DOM/截图确认，不重复完整运动采样。

### 修改内容

- `dist/dashboard-view.mjs`：纯展示适配，处理未知值与旧 A 查询开关。
- `dist/app.js`：接入当前真实帧的运行模式、完整 Step、水位差和错误显示。
- `dist/index.html`：添加图谱状态栏与旧 A 控件容器。
- `dist/styles.css`：页脚按内容换行，滞后或错误使用橙色提示。
- `dist/original-knowledge.mjs`：旧 A 镜像关闭时显示停用说明并隐藏旧查询控件。
- `tests/dashboard-view.test.mjs`：新增水位、未知状态及 A 开关展示检查。

### 后续建议

- 保留上述短窗口证据，完整联合验收的数据规模与并发资格由主报告单独判定。
- 若今后需要页面直接执行 C 套五包查询，应单独接入 C 查询接口；本次只移除旧 A 区的误导性展示，不把旧 ANN 查询换名为 C 查询。
