# 已测中型仓库快速闭环输入

本目录是Pro6000既有 `sample19-medium-logical` 的原字节副本：19任务、原初始库存、逻辑进出货口，配对medium地图。用于快速检查统一入口、100独立机器人写查以及低电接力。

需要实际低电接力时显式加 `--disturbances-json config/live-sample-disturbances.json`。这是运行扰动，CSV继续只包含任务。使用 `config/live-wheeled-datasets.json` 的medium19选项会加载该配置。
