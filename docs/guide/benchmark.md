# Pytucky PTK7 性能基准

本文档记录当前 `pytucky` 仓库最新一次 PTK7 benchmark 结果，并与 `../pytuck/docs/guide/benchmark.md` 中 100000 条记录的 `Pytuck` 基线做对比。

## 测试环境

- 测试时间：2026-04-12 00:41:33
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
| `insert` | 5.95ms |
| `save` | 10.34ms |
| `query_pk` | 1.43ms |
| `query_indexed` | 1.40ms |
| `load` | 1.62ms |
| `reopen` | 1.44ms |
| `reopen_first_query` | 36.98μs |
| `file_size` | 56.0KB |

### 100000 条记录

| 指标 | 结果 |
|------|------|
| `insert` | 643.32ms |
| `save` | 500.66ms |
| `query_pk` | 1.59ms |
| `query_indexed` | 1.50ms |
| `load` | 143.25ms |
| `reopen` | 148.83ms |
| `reopen_first_query` | 82.88μs |
| `file_size` | 5.98MB |

## 与 `pytuck` 基线对比（100000 条）

> 说明：`pytuck` 文档表格直接可对齐的列为 `insert / query_indexed / save / load / lazy_load / file_size`。本页用 `reopen` 对齐旧文档中的“懒加载打开”成本，因为 PTK7 已不再区分单独的 `lazy_load` 配置。

| 指标 | 当前 PTK7 | `pytuck` 基线 | 对比 |
|------|-----------|---------------|------|
| 插入 | 643.32ms | 834.38ms | 快约 **22.9%** |
| 索引查询（100 次） | 1.50ms | 1.88ms | 快约 **20.4%** |
| 保存 | 500.66ms | 562.03ms | 快约 **10.9%** |
| 加载 | 143.25ms | 337.28ms | 快约 **57.5%** |
| reopen | 148.83ms | 321.98ms | 快约 **53.8%** |
| 文件大小 | 5.98MB | 6.09MB | 小约 **1.8%** |

### 补充观察

- `query_pk` 现在是 **1.59ms / 100 次**。这项在旧 `pytuck` 表格里没有单独列，但对当前 ORM 实际体验非常关键。
- `reopen_first_query` 现在是 **82.88μs**。旧 `pytuck` 文档正文里提过“首次懒查询约 `121.6μs`”，如果仅作近似参考，当前 PTK7 仍然更快。
- 相比本仓库上一轮文档快照，这次官方 benchmark 的变化不大，`insert / save / reopen / query_*` 都仍处于小幅波动区间；这很符合本轮优化的目标，因为这轮主要瞄准的是“旧库 reopen 后少量更新再 flush”的增量写入路径，而不是 fresh insert 后立刻 flush 的单表 benchmark。
- `insert` / `save` 在单次 benchmark 快照中仍会有小幅波动，因此应更关注多次重复后的均值，而不是把一次结果当作绝对结论。

## 本轮增量写入优化（相对优化前 PTK7）

> 说明：下面这组微基准更贴近本轮优化真正命中的路径——**reopen 已存在的 `.pytucky` 文件后，只修改少量记录再 `flush()`**。这一轮实现的核心是：对 changed lazy table 不再在高层无条件 `_ensure_all_loaded()`，而是由 backend 基于显式 dirty PK 集合构造 overlay。

| 场景 | 优化前 PTK7 | 当前 PTK7 | 对比 |
|------|-------------|-----------|------|
| 单表更新 flush（1 表 × 20000 行，更新 1 条） | 169.49ms | 143.29ms | 快约 **15.5%** |
| 多表更新 flush（8 表 × 5000 行，只改 1 张表） | 344.99ms | 275.24ms | 快约 **20.2%** |

这也是为什么本轮官方 `save` 指标没有出现同量级变化：官方脚本的 `save` 口径主要覆盖“插入后立刻 flush”，而不是“reopen 后增量写入”的真实热路径。
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

另外，读路径现在会复用底层 reader handle，避免重复 `open("rb")` 带来的额外系统调用开销，因此重复主键读取与重复索引读取更稳定。

### 3. 为什么主键查询快

当前高层 ORM 已接上主键 fast path：

- `Session.get()` 直接走 `storage.select()`
- `select(Model).filter_by(id=...)` 直接下推主键读取
- 底层 `Store` 用 `pk -> (offset, length)` 直接定位记录

这也是 `query_pk` 能从此前秒级回落到毫秒级的关键原因。

### 4. 为什么保存更快

当前 `save/flush` 主路径主要做了四件事：

- 高层 `Storage.flush()` 不再通过 row-by-row `Store.insert()` 重建整库
- `Store.flush()` 单表只物化一次 live records，并复用它生成数据区和索引区
- 写盘后直接刷新内存状态，不再立刻重新 `open()` 整个文件回读
- 对 changed lazy table，不再在高层无条件 `_ensure_all_loaded()`；当条件满足时，直接由 backend 基于显式 dirty PK 集合构造 overlay，再参与单文件重写

因此，当前版本一方面仍能把官方 `save` 控制在约 **500.66ms**，继续快于 `pytuck` 文档基线的 **562.03ms**；另一方面，在“reopen 后少量更新再 flush”这条更贴近实际项目增量写入的路径上，本轮又额外带来了约 **15%～20%** 的提升。

### 5. 为什么文件更小

PTK7 的索引区只保存排序后的 `(value, pk)` 对和轻量元数据，不再保存更膨胀的 `value -> pk_set` 展开结构。配合单文件目录布局，当前 100000 条记录的文件已经降到 5.98MB，略小于 `pytuck` 文档基线的 6.09MB。

## 当前结论

如果只看 100000 条、且只比较 `pytuck` 文档中可以直接对齐的指标，那么当前 PTK7 已经实现：

- 插入更快
- 索引查询更快
- 保存更快
- 加载 / reopen 更快
- 文件更小

如果再加上本轮新增的增量写入微基准，那么可以进一步确认：当前 PTK7 不仅在官方单文件主路径 benchmark 上整体领先旧基线，而且在 **reopen 后少量更新再 flush** 这条更贴近嵌入式 ORM 实际用法的热路径上，也比优化前的 PTK7 更快。

## 复现命令

```bash
uv run python tests/benchmark/benchmark.py -n 1000 --extended --output-json tests/benchmark/benchmark_output/pytucky-v7-1000.json
uv run python tests/benchmark/benchmark.py -n 100000 --extended --output-json tests/benchmark/benchmark_output/pytucky-v7-100000.json
```
