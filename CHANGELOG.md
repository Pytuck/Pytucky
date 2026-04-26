# Changelog

## Unreleased

- 暂无。

## 1.2.0 (2026-04-26)

本次小版本聚焦于查询与持久化热路径的性能收口、CI/发布质量补强，以及 benchmark 与文档一致性的整理。

### 优化

- **查询热路径继续下沉到存储层**
  - `Query._execute()` 在常见路径下推 `limit / offset`
  - `Query.count()` 增加 count fast-path，避免先查全量再 `len()`
  - `Storage.query_table_data()` 收敛为单次分页窗口查询，减少表格浏览场景的双查询开销
- **Session / flush 批量更新成本下降**
  - dirty 对象按模型分组，复用 `bulk_update()` 能力
  - 去掉逐条 `update()` 后固定 readback 的额外成本
- **预取与懒加载读路径继续瘦身**
  - `IN` 条件与 prefetch membership 优先走 `set / frozenset` 路径
  - 缓存 payload layout / codec，降低 reopen 后首查与单条点查成本
  - 继续收口 `load / reopen / save / query_pk / query_indexed` 的固定 Python 层开销，并同步基准结果

### 变更

- **构建与发布流程更可复现**
  - 跟踪 `uv.lock`，固定依赖解析结果
  - CI 纳入 `mypy`、`python -m build` 与 wheel 安装 smoke test
  - 主测试矩阵优先保留快速反馈，compat / benchmark 任务改为分层执行
- **版本与公开接口一致性收口**
  - 版本号统一以 `pytucky.__version__` 为单一来源
  - 补充根包导出与安装态 API 契约验证

### 修复

- **Relationship / 类型与查询路径的已知问题修正**
  - 修正 `Relationship` 相关类型返回问题并补静态回归
  - 修正分页/计数与表格查询回退路径中的重复查询开销与语义分歧
- **benchmark 默认执行不再污染仓库**
  - 默认使用系统临时目录，不在仓库根目录残留 `.tmp_bench*`
  - benchmark 脚本去掉源码路径注入，优先验证安装态行为

### 文档

- **同步当前 benchmark 与迁移说明**
  - README 与 benchmark 指南更新为 10,000 条记录 / 3 轮均值的当前结果
  - README 更换为 SVG logo 展示
  - 最佳实践补充 `import pytuck` → `import pytucky` 的最小迁移说明
  - 补充 `None / low / medium / high` 与 `pytuck` 的加密兼容说明
  - TODO 全部结清并同步当前测试规模

### 测试

- **测试与质量门禁继续扩充**
  - 补充 pytest 内的 mypy 静态回归用例
  - 增加 benchmark runner、API contract、query fast-path、lazy load / flush 等回归覆盖
  - 全量测试扩展到 **205 passed**

## 历史归档

- [1.1.2](docs/changelog/1.1.2.md)
- [1.1.1](docs/changelog/1.1.1.md)
- [1.0.0](docs/changelog/1.0.0.md)
