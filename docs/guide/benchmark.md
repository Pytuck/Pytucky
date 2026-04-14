# 性能基准报告

> 测试环境：Linux 6.18.7-76061807-generic / Python 3.12.3
>
> 数据规模：100,000 条记录
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

## Pytucky 1.0.0 结果

| 指标 | 结果 |
|------|------|
| insert | 628.03ms |
| save | 463.71ms |
| query_pk | 1.51ms / 100 次 |
| query_indexed | 1.52ms / 100 次 |
| load | 137.92ms |
| reopen | 129.22ms |
| reopen_first_query | 78.1μs |
| file_size | 5.98MB |

## 与 Pytuck 1.2.1 对比

同机、同数据量、同 Python 版本下的对比：

| 指标 | Pytucky 1.0.0 | Pytuck 1.2.1 | 变化 |
|------|---------------|--------------|------|
| insert | 628.03ms | 808.43ms | **-22.3%** |
| save | 463.71ms | 609.01ms | **-23.9%** |
| query_pk | 1.51ms | 1.63ms | **-7.4%** |
| query_indexed | 1.52ms | 1.81ms | **-16.0%** |
| load | 137.92ms | 126.26ms | +9.2% |
| reopen | 129.22ms | 128.92ms | +0.2% |
| reopen_first_query | 78.1μs | 51.3μs | +52.2% |
| file_size | 5.98MB | 9.51MB | **-37.1%** |

**说明**：

- Pytucky 在写入路径（insert / save）和文件体积上显著优于 Pytuck，这是 PTK7 格式精简的主要收益。
- Pytuck 在 load / reopen / reopen_first_query 上略快，因其在 v1.2.1 中引入了更激进的索引元数据预加载策略。
- 查询性能（query_pk / query_indexed）两者接近，Pytucky 略优。

## 复现命令

```bash
# Pytucky
uv run python tests/benchmark/benchmark.py -n 100000 --extended

# 输出 JSON
uv run python tests/benchmark/benchmark.py -n 100000 --extended \
    --output-json tests/benchmark/benchmark_output/pytucky-v7-100000.json
```
