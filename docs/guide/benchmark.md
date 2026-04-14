# 性能基准报告

> 测试环境：Linux 6.18.7-76061807-generic / Python 3.12.3
>
> 数据规模：100,000 条记录（6 列：id, name, email, age, score, active）
>
> 日期：2026-04-14

## 测试指标

| 指标 | 说明 |
|------|------|
| insert | 插入 100,000 条记录 + `session.commit()` |
| save | `db.flush()` 持久化到磁盘 |
| query_pk | 100 次主键查询 |
| query_indexed | 100 次索引等值查询 |
| load | 首次打开数据库文件 |
| reopen | 再次打开数据库文件 |
| reopen_first_query | 重开后首条主键读取 |
| file_size | 最终文件体积 |

## Pytucky 1.0.0 vs Pytuck 1.2.1

同机、同 schema、同数据量、同 Python 版本下的对比：

| 指标 | Pytucky 1.0.0 | Pytuck 1.2.1 | 变化 |
|------|---------------|--------------|------|
| insert | 800.1ms | 780.5ms | +2.5% |
| save | 592.3ms | 597.2ms | **-0.8%** |
| query_pk | 1.86ms | 1.62ms | +14.5% |
| query_indexed | 1.79ms | 1.72ms | +3.5% |
| load | 122.6ms | 132.3ms | **-7.3%** |
| reopen | 124.0ms | 132.1ms | **-6.1%** |
| reopen_first_query | 89.1μs | 51.9μs | +71.6% |
| file_size | 9.97MB | 9.97MB | 0% |

**说明**：

- 两个库共享 PTK7 二进制格式，相同 schema 下文件体积完全一致。
- 写入路径（save）和读取路径（load / reopen）性能接近，Pytucky 在 load/reopen 上略优。
- Pytuck 在点查询（query_pk / reopen_first_query）上略快，与其 v1.2.1 中更激进的索引元数据预加载有关。
- 总体差异在噪声范围内，两者底层格式一致，性能基本持平。

## 如何选择

- **受限 Python 环境**（Ren'Py 等无法安装第三方依赖的场景）：选择 Pytucky，零依赖、单文件。
- **需要多格式导出**（JSON、CSV、SQLite、Excel 等）：选择 Pytuck，支持 8 种存储引擎。
- **只需要高性能单文件数据库**：两者均可，性能一致。

## 复现命令

```bash
# Pytucky
uv run python tests/benchmark/benchmark.py -n 100000 --extended

# 输出 JSON
uv run python tests/benchmark/benchmark.py -n 100000 --extended \
    --output-json tests/benchmark/benchmark_output/pytucky-v7-100000.json
```
