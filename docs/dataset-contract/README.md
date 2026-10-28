# 给数据生成同学的对接材料

这份目录把仓库设计转成可直接使用的文件约定。先阅读[仓库指南第5节](../WAREHOUSE_DESIGN_GUIDE.md#5-怎样准备自己的任务数据)，再按本目录的表生成输入。

## 文件导航

- `contract/layout-contract.json`：仓库尺寸、货位、100个物理泊位、容量与收发规则。
- `contract/data-contract.json`：七字段任务、时间、货物身份与库存依赖。
- `layout/task-locations.csv`：1728货位加16个IN/OUT，共1744个合法任务位置。
- `layout/physical-bays.csv`：100个真实充电兼待命位及P/C别名。
- `layout/all-locations.csv`：包含泊位及别名的完整位置索引。
- `schema/`：机器可读的场景与任务行格式。
- `sample-input/`：19条任务、1800模拟秒的接口例子。
- `supplement-inputs/`：热点、峰值和连续两日的固定回归输入。

仓库总容量为1728件，已接收入库暂存和机器人携货也计入。每个IN点最多暂存6件。出库完成放置后记录为`SHIPPED`，释放仓库总容量。

## 使用校验器

从仓库根目录执行：

```bash
python docs/dataset-contract/validate_package.py
python docs/dataset-contract/validate_package.py --dataset /path/to/your-dataset
```

校验器使用Python标准库，检查文件摘要、位置、时间、格式和参考库存依赖。正式运行随后记录实际完成、期限、电量与交通结果。输入目录包含`tasks.csv`、`initial_inventory.csv`和`manifest.json`。

本目录从已交付对接材料选取合同、表和样例，重新生成此目录自己的`package-manifest.json`与`SHA256SUMS.txt`，方便Git克隆后独立校验。源文件字节和数据身份保持一致。仓库运行用地图位于`data/layouts/warehouse100-wide-v2.json`，其SHA与本目录地图相同。

机器人分配、回充阈值和交接决策由仿真配置及调度器管理。数据生成侧负责给出任务出现时间、位置、期限与货物依赖，双方可以在同一输入上重复比较策略。
