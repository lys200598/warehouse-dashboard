# 仓储项目独立 Neo4j 安装与启动手册

## 任务背景

为仓储项目提供一份新人拿到本仓库后可以独立执行的 Neo4j 安装手册。它不能依赖 `lade-deploy`、`KG_Embodied`、已有 Java 环境或既有 Neo4j 安装，并且不能与 KG 的吉林配送图谱发生数据或端口冲突。

## 关键结论

- 本手册在项目本地 `.venv` 安装固定版本的 `jdk4py 21.0.8.2` 提供 Java 21 运行时，下载官方 Neo4j Community 5.26.32、安装 Python 驱动并启动仓储专用实例；不需要 LaDe 环境、`sudo` 或系统包管理权限。
- Neo4j 5.26 LTS 支持 Java 17 和 Java 21，但不支持 Java 25；`jdk4py` 必须固定到 Java 21 版本，不能直接安装最新版本。
- 仓储服务安装在项目的 `.runtime/warehouse-neo4j-5.26.32` 下，数据、日志、插件与运行文件均在该目录内，与 KG 完全隔离。
- 仓储服务使用 HTTP `47474`、Bolt `47687`；KG/LaDe 已有服务即使使用默认端口或此前的 `28474/28687`，也不会冲突。
- 文档在写入端口配置前检查 `47474/47687` 是否已被监听；若端口已占用，只提示并要求更换端口，不会修改或停止占用端口的服务。
- Python 的 `neo4j` 包只是客户端驱动；真正的图数据库服务由下载的 Java Neo4j 程序启动。

## 执行过的操作

- 参考 Neo4j 官方系统要求与 Linux tarball 安装说明，确认 Neo4j 5.26 LTS 兼容 Java 17/21，且 tarball 安装可以独立运行。
- 在受限自动化环境中验证过 Neo4j 5.26.32 的配置校验和密码初始化命令；自动化环境禁止绑定监听端口，因此最终网络启动需在普通主机终端完成。
- 本文改为不依赖 LaDe 的独立安装方式；未执行下载、安装或修改用户主机上的实际服务。

## 修改内容

- 重写 Neo4j 部署手册，移除对 `lade-deploy`、`sudo` 和系统包管理器的依赖。
- 指定独立的安装目录、端口、初始密码流程、Python 驱动安装、启动验证和停止命令。
- 旧的共享 LaDe 程序目录方案已废弃，不应再执行。

## 后续建议

- 成功启动空库后，实现 `warehouse_tasks.csv` 到 Neo4j 的导入脚本，并先用 10,000 条任务验证图谱模式。
- 将 `.runtime/` 加入本地忽略规则，避免提交数据库文件、日志或凭据。
- 生产环境应使用专用服务账户、密钥管理、备份策略和受控网络暴露；本文仅面向本地开发。

---

## 1. 适用范围与目录

本手册面向 Ubuntu/Debian 开发机。请先在终端中进入本项目根目录（即能看到 `README.md` 的目录）；下文所有路径均相对于该目录。

```bash
test -f README.md && echo "当前目录正确" || echo "请先 cd 到 warehouse-dashboard 项目根目录"
```

本手册不会读取或写入项目外的 KG、LaDe 或其他 Neo4j 安装目录。

## 2. 准备项目本地 Java 环境（无需 sudo）

以下命令只会在当前项目的 `.venv` 中安装 Python 包与 Java 运行时，不会修改系统 Java、LaDe 或其他用户目录。要求系统已有 `python3`、`venv` 模块和 `curl`；它们通常已随开发环境提供。

```bash
if [ ! -x .venv/bin/python ]; then
  python3 -m venv .venv
fi

if ! ./.venv/bin/python -m pip --version >/dev/null 2>&1; then
  echo "项目虚拟环境缺少 pip，尝试使用 ensurepip 修复。"
  ./.venv/bin/python -m ensurepip --upgrade || {
    echo "ensurepip 不可用，改为下载 pip 官方引导脚本。"
    mkdir -p ./.runtime
    curl -fL https://bootstrap.pypa.io/get-pip.py -o ./.runtime/get-pip.py
    ./.venv/bin/python ./.runtime/get-pip.py
  }
fi

if ./.venv/bin/python -m pip --version; then
  ./.venv/bin/python -m pip install --upgrade pip
  ./.venv/bin/python -m pip install --force-reinstall 'jdk4py==21.0.8.2'
else
  echo "项目本地 pip 仍不可用，请保留错误信息并联系环境管理员。"
fi

if ./.venv/bin/python -c 'from jdk4py import JAVA_HOME; print(JAVA_HOME)'; then
  export JAVA_HOME="$(./.venv/bin/python -c 'from jdk4py import JAVA_HOME; print(JAVA_HOME)')"
  "$JAVA_HOME/bin/java" -version
else
  echo "jdk4py 未安装成功；请检查上一条 pip 命令的错误后重试。"
fi
```

最后一条必须显示 Java 21。若显示 Java 25，说明安装了未固定版本的 `jdk4py`，请重新执行 `./.venv/bin/python -m pip install --force-reinstall 'jdk4py==21.0.8.2'`；不要继续第 3 节。若显示 `ModuleNotFoundError: No module named 'jdk4py'`，表示安装未成功完成。若 `python3 -m venv` 报错，需由环境管理员提供 Python venv 支持；不要尝试使用 `sudo` 修改本机系统。

## 3. 下载并解压独立的 Neo4j Community

在项目根目录执行。该命令下载 Neo4j 官方 tarball，不使用任何 KG 或 LaDe 文件：

```bash
export NEO4J_VERSION="5.26.32"
export WAREHOUSE_NEO4J_HOME="./.runtime/warehouse-neo4j-$NEO4J_VERSION"
export NEO4J_ARCHIVE="./.runtime/neo4j-community-$NEO4J_VERSION-unix.tar.gz"

mkdir -p ./.runtime

if [ -e "$WAREHOUSE_NEO4J_HOME" ]; then
  echo "仓储 Neo4j 已存在：$WAREHOUSE_NEO4J_HOME"
  echo "无需再次下载，请直接执行第 5 节的启动命令。"
else
  curl -fL --retry 3 \
    -o "$NEO4J_ARCHIVE" \
    "https://dist.neo4j.org/neo4j-community-$NEO4J_VERSION-unix.tar.gz"
  export NEO4J_EXTRACTED_DIR="$(tar -tzf "$NEO4J_ARCHIVE" | awk -F/ 'NF > 1 && $1 != "." { print $1; exit }')"
  tar -xzf "$NEO4J_ARCHIVE" -C ./.runtime
  if [ -z "$NEO4J_EXTRACTED_DIR" ] || [ ! -x "./.runtime/$NEO4J_EXTRACTED_DIR/bin/neo4j" ]; then
    echo "无法从下载包中识别 Neo4j 程序目录，请检查：$NEO4J_ARCHIVE"
  else
    mv "./.runtime/$NEO4J_EXTRACTED_DIR" "$WAREHOUSE_NEO4J_HOME"
    echo "Neo4j 已安装到：$WAREHOUSE_NEO4J_HOME"
  fi
fi
```

## 4. 预检并配置独立端口

下面命令先检查端口。`47474/47687` 与当前已知 LaDe 端口 `28474/28687` 不同；若本机另有服务占用仓储端口，请修改两个变量后再继续。数据、日志和运行目录默认位于独立的 `$WAREHOUSE_NEO4J_HOME` 内，无需额外指定。

执行本节前必须确认第 3 节已打印“Neo4j 已安装到”。若出现 `conf/neo4j.conf: No such file or directory` 或 `bin/neo4j-admin: No such file or directory`，说明安装目录不存在；不要继续本节，应回到第 3 节完成下载和解压。

```bash
export WAREHOUSE_NEO4J_HTTP_PORT="47474"
export WAREHOUSE_NEO4J_BOLT_PORT="47687"
export WAREHOUSE_NEO4J_CONFIG="$WAREHOUSE_NEO4J_HOME/conf/neo4j.conf"

if [ ! -f "$WAREHOUSE_NEO4J_CONFIG" ]; then
  echo "未找到 Neo4j 安装配置：$WAREHOUSE_NEO4J_CONFIG"
  echo "请先完成第 3 节的下载和解压，再重新执行本节。"
elif ss -ltnH | awk '{print $4}' | grep -Eq ":(${WAREHOUSE_NEO4J_HTTP_PORT}|${WAREHOUSE_NEO4J_BOLT_PORT})$"; then
  echo "端口 ${WAREHOUSE_NEO4J_HTTP_PORT} 或 ${WAREHOUSE_NEO4J_BOLT_PORT} 已被占用。"
  echo "请修改上面两个 WAREHOUSE_NEO4J_*_PORT 变量，然后重新执行本节。"
else
  sed -i \
    -e '/^server\.default_listen_address=/d' \
    -e '/^server\.bolt\.enabled=/d' \
    -e '/^server\.bolt\.listen_address=/d' \
    -e '/^server\.http\.enabled=/d' \
    -e '/^server\.http\.listen_address=/d' \
    -e '/^server\.https\.enabled=/d' \
    "$WAREHOUSE_NEO4J_CONFIG"

  printf '\n# Warehouse dedicated instance\nserver.default_listen_address=127.0.0.1\nserver.bolt.enabled=true\nserver.bolt.listen_address=:%s\nserver.http.enabled=true\nserver.http.listen_address=:%s\nserver.https.enabled=false\n' \
    "$WAREHOUSE_NEO4J_BOLT_PORT" \
    "$WAREHOUSE_NEO4J_HTTP_PORT" \
    >> "$WAREHOUSE_NEO4J_CONFIG"

  "$WAREHOUSE_NEO4J_HOME/bin/neo4j-admin" server validate-config
fi
```

最后一条必须输出 `Configuration file validation successful.`，才能继续。

## 5. 设置初始密码并前台启动

仅在此仓储实例第一次启动前执行密码设置。请自行输入强密码，命令不会回显密码：

```bash
read -r -s -p '设置仓储 Neo4j 的 neo4j 用户密码：' WAREHOUSE_NEO4J_PASSWORD
echo
"$WAREHOUSE_NEO4J_HOME/bin/neo4j-admin" dbms set-initial-password "$WAREHOUSE_NEO4J_PASSWORD"
unset WAREHOUSE_NEO4J_PASSWORD

"$WAREHOUSE_NEO4J_HOME/bin/neo4j" console
```

`console` 会持续占用当前终端；看到启动完成日志后，不要关闭该终端。

访问地址：

```text
浏览器：http://127.0.0.1:${WAREHOUSE_NEO4J_HTTP_PORT}
Bolt： bolt://127.0.0.1:${WAREHOUSE_NEO4J_BOLT_PORT}
用户名：neo4j
```

## 6. 验证数据库连接

保持服务在第一个终端运行，另开一个终端执行：

```bash
export WAREHOUSE_NEO4J_HOME="./.runtime/warehouse-neo4j-5.26.32"
export JAVA_HOME="$(./.venv/bin/python -c 'from jdk4py import JAVA_HOME; print(JAVA_HOME)')"
export WAREHOUSE_NEO4J_BOLT_PORT="47687"

"$WAREHOUSE_NEO4J_HOME/bin/cypher-shell" \
  -a "bolt://127.0.0.1:${WAREHOUSE_NEO4J_BOLT_PORT}" \
  -u neo4j \
  -p '替换为你在第 5 节输入的密码' \
  'RETURN 1 AS connected;'
```

返回 `1` 即表示仓储 Neo4j 已成功运行，且没有连接 KG 的数据库。

注意：`-p` 后面必须替换为第 5 节实际输入的密码；`test123456` 不能作为最后一段查询文本，最后一段必须保持 `RETURN 1 AS connected;`。若报 `authentication failure`，请先核对密码，不要反复执行初始密码命令。

## 7. 安装当前项目的 Python Neo4j 驱动

该步骤用于后续 Python 导入器和查询 API 连接数据库，不会安装或启动数据库服务：

```bash
./.venv/bin/python -m pip install --upgrade pip
./.venv/bin/python -m pip install 'neo4j>=5.26,<6'
```

## 8. 后台运行与停止

前台验证成功后，可以后台启动：

```bash
export WAREHOUSE_NEO4J_HOME="./.runtime/warehouse-neo4j-5.26.32"
export JAVA_HOME="$(./.venv/bin/python -c 'from jdk4py import JAVA_HOME; print(JAVA_HOME)')"

"$WAREHOUSE_NEO4J_HOME/bin/neo4j" start
```

停止仓储实例：

```bash
"$WAREHOUSE_NEO4J_HOME/bin/neo4j" stop
```

查看日志：

```bash
tail -f "$WAREHOUSE_NEO4J_HOME/logs/neo4j.log"
```

## 9. 常见问题

### 命令执行后退出

本文没有使用 `exit` 来中止交互式终端。若目标目录已存在，只会输出提示；不要重复下载或设置初始密码，直接执行第 5 节的启动命令。

### `server.* declared multiple times`

不要手动在配置末尾反复追加端口项。重新执行第 4 节即可：它会先删除现有的有效监听配置，再追加一组唯一配置。

### 端口被占用

检查端口：

```bash
ss -ltn | grep -E ':47474|:47687'
```

若有输出，请修改第 4 节的两个端口值，再重新执行第 4 节的配置校验。

### `Operation not permitted` 且无法绑定端口

这通常表示当前运行环境（例如受限自动化沙箱）不允许创建网络监听 socket。请在普通主机终端执行第 5 节，而不要更改 KG 数据或端口来规避该限制。

### 忘记 `neo4j` 密码（保留仓储图谱数据）

先确保第 2 节的 Java 21 已配置，然后仅针对仓储实例执行。以下过程临时关闭认证；配置已固定为 `127.0.0.1`，不要在恢复期间改为对外监听。

```bash
export WAREHOUSE_NEO4J_HOME="./.runtime/warehouse-neo4j-5.26.32"
export WAREHOUSE_NEO4J_CONFIG="$WAREHOUSE_NEO4J_HOME/conf/neo4j.conf"
export JAVA_HOME="$(./.venv/bin/python -c 'from jdk4py import JAVA_HOME; print(JAVA_HOME)')"

"$WAREHOUSE_NEO4J_HOME/bin/neo4j" stop
cp "$WAREHOUSE_NEO4J_CONFIG" "$WAREHOUSE_NEO4J_CONFIG.auth-recovery.bak"
sed -i '/^dbms\.security\.auth_enabled=/d' "$WAREHOUSE_NEO4J_CONFIG"
printf '\ndbms.security.auth_enabled=false\n' >> "$WAREHOUSE_NEO4J_CONFIG"
"$WAREHOUSE_NEO4J_HOME/bin/neo4j" start
```

随后连接 `system` 数据库（无需密码），在交互式提示符中输入密码修改语句，完成后输入 `:exit`：

```bash
"$WAREHOUSE_NEO4J_HOME/bin/cypher-shell" -a bolt://127.0.0.1:47687 -d system
```

```cypher
ALTER USER neo4j SET PASSWORD '替换为新的强密码';
```

最后恢复认证并重启：

```bash
"$WAREHOUSE_NEO4J_HOME/bin/neo4j" stop
mv "$WAREHOUSE_NEO4J_CONFIG.auth-recovery.bak" "$WAREHOUSE_NEO4J_CONFIG"
"$WAREHOUSE_NEO4J_HOME/bin/neo4j" start
```

不要删除 `data/`、`data/databases/` 或 `data/transactions/`。这些目录保存图谱数据；以上操作只临时改认证配置。

### 开发测试库直接重建并设置新密码

仅当当前仓储图谱可以清空时使用。该操作不会影响 KG/LaDe，但会把当前仓储图谱变为空库；旧数据被改名备份而不是删除。

```bash
export WAREHOUSE_NEO4J_HOME="./.runtime/warehouse-neo4j-5.26.32"
export JAVA_HOME="$(./.venv/bin/python -c 'from jdk4py import JAVA_HOME; print(JAVA_HOME)')"

"$WAREHOUSE_NEO4J_HOME/bin/neo4j" stop || true
BACKUP_SUFFIX="$(date +%Y%m%d-%H%M%S)"
if [ -d "$WAREHOUSE_NEO4J_HOME/data" ]; then
  mv "$WAREHOUSE_NEO4J_HOME/data" "$WAREHOUSE_NEO4J_HOME/data.before-reset-$BACKUP_SUFFIX"
fi
mkdir -p "$WAREHOUSE_NEO4J_HOME/data"

read -r -s -p '设置新的 neo4j 密码：' NEO4J_PASSWORD
echo
"$WAREHOUSE_NEO4J_HOME/bin/neo4j-admin" dbms set-initial-password "$NEO4J_PASSWORD"
unset NEO4J_PASSWORD

"$WAREHOUSE_NEO4J_HOME/bin/neo4j" console
```

在另一个终端使用 Bolt `bolt://127.0.0.1:47687`、用户名 `neo4j` 和新密码验证。确认无误后再决定是否删除 `data.before-reset-*` 备份目录。
