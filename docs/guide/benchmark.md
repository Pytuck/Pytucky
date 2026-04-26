# 性能基准报告

> 测试环境：Darwin 25.4.0 / Python 3.13.11
>
> 数据规模：10,000 条记录（6 列：id, name, email, age, score, active）
>
> 轮次：3 轮均值
>
> 日期：2026-04-26

## 测试指标

| 指标 | 说明 |
|------|------|
| insert | 插入 10,000 条记录 + `session.commit()` |
| save | `db.flush()` 持久化到磁盘 |
| query_pk | 100 次主键查询 |
| query_indexed | 100 次索引等值查询 |
| load | 首次打开数据库文件 |
| reopen | 再次打开数据库文件 |
| reopen_first_query | 重开后首条主键读取 |
| file_size | 最终文件体积 |

## 当前 Pytucky vs Pytuck 基准结果

当前结果在同一台机器、同一 Python 3.13.11 环境下，对 `pytucky 1.2.0` 与 `pytuck 1.3.0` 使用相同 schema、相同数据量与相同测试流程连续运行 3 轮取均值：

| 指标 | Pytucky 1.2.0 | Pytuck 1.3.0 | 变化 |
|------|---------------|--------------|------|
| insert | 35.2ms | 30.1ms | +17.2% |
| save | 25.2ms | 22.2ms | +13.1% |
| query_pk | 0.75ms | 0.69ms | +9.4% |
| query_indexed | 0.70ms | 0.64ms | +8.8% |
| load | 4.71ms | 4.74ms | -0.6% |
| reopen | 4.77ms | 4.72ms | +1.0% |
| reopen_first_query | 32.6μs | 35.6μs | -8.2% |
| file_size | 0.92MB | 0.92MB | 0% |

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
uv run python tests/benchmark/benchmark.py -n 10000 --extended

# 输出 JSON
uv run python tests/benchmark/benchmark.py -n 10000 --extended \
    --output-json tests/benchmark/benchmark_output/pytucky-v7-10000.json
```
