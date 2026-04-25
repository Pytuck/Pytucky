# 性能基准报告

> 测试环境：Darwin 25.4.0 / Python 3.13.11
>
> 数据规模：100,000 条记录（6 列：id, name, email, age, score, active）
>
> 轮次：3 轮均值
>
> 日期：2026-04-25

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

## 当前 Pytucky vs Pytuck 基准结果

当前结果在同一台机器、同一 Python 3.13.11 环境下，对 `pytucky 1.1.2` 与 `pytuck 1.3.0` 使用相同 schema、相同数据量与相同测试流程连续运行 3 轮取均值：

| 指标 | Pytucky 1.1.2 | Pytuck 1.3.0 | 变化 |
|------|---------------|--------------|------|
| insert | 370.5ms | 343.2ms | +8.0% |
| save | 274.8ms | 239.4ms | +14.8% |
| query_pk | 0.86ms | 0.71ms | +20.5% |
| query_indexed | 0.81ms | 0.66ms | +21.4% |
| load | 72.9ms | 53.0ms | +37.5% |
| reopen | 69.7ms | 48.3ms | +44.3% |
| reopen_first_query | 73.3μs | 50.7μs | +44.6% |
| file_size | 9.97MB | 9.97MB | 0% |

**说明**：

- `query_pk` 与 `query_indexed` 均为 100 次查询总耗时。
- `reopen_first_query` 为 reopen 后首次主键点查耗时。
- 本表的唯一变量是库实现；环境、schema、数据量与测试顺序保持一致。

## 如何选择

- **受限 Python 环境**（Ren'Py 等无法安装第三方依赖的场景）：选择 Pytucky，零依赖、单文件。
- **需要多格式导出**（JSON、CSV、SQLite、Excel 等）：选择 Pytuck，支持 8 种存储引擎。
- **只需要高性能单文件数据库**：建议在目标平台上按本文命令复测后再做选择。

## 复现命令

```bash
# Pytucky
uv run python tests/benchmark/benchmark.py -n 100000 --extended

# 输出 JSON
uv run python tests/benchmark/benchmark.py -n 100000 --extended \
    --output-json tests/benchmark/benchmark_output/pytucky-v7-100000.json
```
