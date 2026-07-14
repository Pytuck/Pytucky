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
| small_update_flush | 仅更新一条记录后的单文件 flush |
| transaction_begin | 内存快照事务的启动与提交 |
| transaction_rollback | 修改一条记录后的事务回滚 |
| transaction_peak_memory | 事务启动时的峰值内存 |
| encrypted_reopen | 带 HMAC 的 high 加密文件重新打开 |
| encrypted_reopen_peak_memory | 流式认证期间的峰值内存 |
| temporary_file_leftover | benchmark 完成后是否残留 `.tmp` |

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
- 新增的事务、加密 reopen 和临时文件指标用于观察 Pytucky 自身，不混入上面的历史
  pytuck 对照表；需要发布新基准结果时，应在同一环境重新运行两边后再更新表格。
- benchmark 使用的 `.tmp` 只参与原子替换，数据库仍是可独立复制和打开的单个
  `.pytuck` 文件；未显式使用 `--keep` 时，所有数据库和临时目录均会清理。

## 新增指标观测样例

以下数据是 2026-07-14 在 Darwin / Python 3.10.19 上对 10,000 条记录进行的一次
开发观测，只用于确认指标可用，不替代上方同环境、三轮均值的 pytuck 对照结果：

| 指标 | 单次结果 |
|------|----------|
| small_update_flush | 41.1ms |
| transaction_begin | 40.4ms |
| transaction_rollback | 37.4ms |
| transaction_peak_memory | 7.68MB |
| encrypted_reopen | 204.2ms |
| encrypted_reopen_peak_memory | 3.07MB |
| encrypted_file_size | 0.42MB |
| temporary_file_leftover | false |

这组数据表明内存快照在 10,000 条记录时已有可测成本，但当前阶段只记录事实，不据此
进行事务大重构。加密 reopen 同时包含密码派生和整文件 HMAC 验证成本。

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
