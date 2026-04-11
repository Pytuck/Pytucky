# Pytucky PTK7 性能基准

本文档记录当前 `pytucky` 仓库最新一次 PTK7 benchmark 结果，并与 `../pytuck/docs/guide/benchmark.md` 中 100000 条记录的 `Pytuck` 基线做对比。

## 测试环境

- 测试时间：2026-04-11 18:30:40
- 系统环境：Linux 6.18.7-76061807-generic
- Python：3.12.3
- 当前结果文件：
  - `tests/benchmark/benchmark_output/pytucky-v7-1000.json`
  - `tests/benchmark/benchmark_output/pytucky-v7-100000.json`
- 对比基线：`../pytuck/docs/guide/benchmark.md`

> 说明：本页展示的是当前仓库一次完整 benchmark 的快照结果，用于跟踪主路径性能。若要做更严格的结论，建议在安静环境下重复运行 3～5 次，再比较均值和波动范围。

## 当前 benchmark 口径

当前脚本聚焦 PTK7 单文件主路径，只保留这些指标：

- `insert`：插入并 `session.commit()`
- `save`：`db.flush()` 持久化
- `query_pk`：100 次主键查询
- `query_indexed`：100 次索引等值查询（`--extended`）
- `load`：首次重新打开数据库
- `reopen`：再次打开数据库
- `reopen_first_query`：reopen 后首条主键读取
- `file_size`：最终文件体积

这里不再保留 `lazy_load` / `lazy_query_*` 指标，因为 PTK7 默认就是“打开只读目录，记录按需回表”，不再需要额外暴露懒加载配置。

## 当前结果

### 1000 条记录

| 指标 | 结果 |
|------|------|
| `insert` | 6.48ms |
| `save` | 10.10ms |
| `query_pk` | 1.43ms |
| `query_indexed` | 1.37ms |
| `load` | 1.62ms |
| `reopen` | 1.46ms |
| `reopen_first_query` | 39.00μs |
| `file_size` | 56.0KB |

### 100000 条记录

| 指标 | 结果 |
|------|------|
| `insert` | 618.41ms |
| `save` | 487.32ms |
| `query_pk` | 1.45ms |
| `query_indexed` | 1.41ms |
| `load` | 139.37ms |
| `reopen` | 143.72ms |
| `reopen_first_query` | 81.23μs |
| `file_size` | 5.98MB |

## 与 `pytuck` 基线对比（100000 条）

> 说明：`pytuck` 文档表格直接可对齐的列为 `insert / query_indexed / save / load / lazy_load / file_size`。本页用 `reopen` 对齐旧文档中的“懒加载打开”成本，因为 PTK7 已不再区分单独的 `lazy_load` 配置。

| 指标 | 当前 PTK7 | `pytuck` 基线 | 对比 |
|------|-----------|---------------|------|
| 插入 | 618.41ms | 834.38ms | 快约 **25.9%** |
| 索引查询（100 次） | 1.41ms | 1.88ms | 快约 **25.2%** |
| 保存 | 487.32ms | 562.03ms | 快约 **13.3%** |
| 加载 | 139.37ms | 337.28ms | 快约 **58.7%** |
| reopen | 143.72ms | 321.98ms | 快约 **55.4%** |
| 文件大小 | 5.98MB | 6.09MB | 小约 **1.8%** |

### 补充观察

- `query_pk` 现在是 **1.45ms / 100 次**。这项在旧 `pytuck` 表格里没有单独列，但对当前 ORM 实际体验非常关键。
- `reopen_first_query` 现在是 **81.23μs**。旧 `pytuck` 文档正文里提过“首次懒查询约 `121.6μs`”，如果仅作近似参考，当前 PTK7 也更快。
- 相比本仓库前一轮文档中的旧结果，当前 PTK7 的核心提升尤其集中在：
  - `save`：从 881.11ms 降到 487.32ms
  - `load`：从 830.06ms 降到 139.37ms
  - `reopen`：从 797.84ms 降到 143.72ms
  - `query_pk`：在 benchmark 的“100 次主键查询总耗时”口径下，从 7.96s 降到 1.45ms

## 结果解读

### 1. 为什么去掉 `lazy_load` 配置

PTK7 的默认打开路径只读取：

- 文件头
- schema catalog
- 每表 PK 目录
- 索引元数据

不会在打开时把全部记录和全部索引解码进内存，因此“是否开启懒加载”不再需要作为用户配置项暴露。默认行为就是按需读取。

### 2. 为什么 open / reopen 更快

当前 reopen 只恢复目录和索引元数据，不再在打开阶段 eager decode 整个索引块。真正的索引解码发生在：

- `lookup()`
- `range_query()`
- 主键命中后回表读取

因此 100000 条记录下，`load` / `reopen` 已经稳定压到 140ms 左右，而不是秒级初始化。

### 3. 为什么主键查询快

当前高层 ORM 已接上主键 fast path：

- `Session.get()` 直接走 `storage.select()`
- `select(Model).filter_by(id=...)` 直接下推主键读取
- 底层 `StoreV7` 用 `pk -> (offset, length)` 直接定位记录

这也是 `query_pk` 能从此前秒级回落到毫秒级的关键原因。

### 4. 为什么保存更快

本轮 `save/flush` 优化主要做了三件事：

- 高层 `Storage.flush()` 不再通过 row-by-row `StoreV7.insert()` 重建整库
- `StoreV7.flush()` 单表只物化一次 live records，并复用它生成数据区和索引区
- 写盘后直接刷新内存状态，不再立刻重新 `open()` 整个文件回读

因此 `save` 已从此前的 881.11ms 降到 487.32ms，并反超 `pytuck` 文档基线的 562.03ms。

### 5. 为什么文件更小

PTK7 的索引区只保存排序后的 `(value, pk)` 对和轻量元数据，不再保存更膨胀的 `value -> pk_set` 展开结构。配合单文件目录布局，当前 100000 条记录的文件已经降到 5.98MB，略小于 `pytuck` 文档基线的 6.09MB。

## 当前结论

如果只看 100000 条、且只比较 `pytuck` 文档中可以直接对齐的指标，那么当前 PTK7 已经实现：

- 插入更快
- 索引查询更快
- 保存更快
- 加载 / reopen 更快
- 文件更小

也就是说，当前版本终于达到了“在保留基本 ORM 用法前提下，单文件 PTK7 主路径整体领先旧基线”的目标。

## 复现命令

```bash
uv run python tests/benchmark/benchmark.py -n 1000 --extended --output-json tests/benchmark/benchmark_output/pytucky-v7-1000.json
uv run python tests/benchmark/benchmark.py -n 100000 --extended --output-json tests/benchmark/benchmark_output/pytucky-v7-100000.json
```
