# Pytucky TODO

基于当前代码库状态重新整理的待办清单。

> 当前结论：项目整体架构方向正确，**不需要大重构**；后续工作应以**查询热路径优化、Session 批处理、CI/发布质量收口**为主。

---

## 当前快照

- [x] 包名已切换为 `pytucky`
- [x] 当前仅保留 **PTK7 单引擎**
- [x] 默认文件后缀为 **`.pytuck`**，显式 **`.pytucky`** 仍兼容
- [x] 已支持 `None / low / medium / high` 四档读写
- [x] 已验证与真实 `pytuck` 双向互读互写
- [x] 已实现懒加载、索引物化缓存、增量 flush、读句柄复用
- [x] 当前测试规模已扩充到 **191 项**
- [x] `mypy` 已清零，并补了 pytest 内的静态回归用例（校验 `mypy pytucky`）

---

## P0：优先处理

### 1. 查询 `limit / offset / count` 真正下推到存储层

- [x] 让 `Query._execute()` 在单列排序和普通查询路径中把 `limit` / `offset` 下推给 `Storage.query()`
  - 相关位置：`pytucky/query/builder.py`、`pytucky/core/storage.py`
- [x] 给 `Query.count()` 增加 count fast-path，避免“先查全量记录再 `len()`”
  - 相关位置：`pytucky/query/builder.py`
- [x] 优化 `Storage.query()` 的常规扫描路径，在无排序或可提前停止时避免全量物化
  - 相关位置：`pytucky/core/storage.py`

**目标**：减少大结果集下的全量解码、dict copy、模型实例化开销。

### 2. 收敛 `query_table_data()` 的双查询问题

- [x] 避免当前“先查总数、再查分页结果”的双查询路径
  - 相关位置：`pytucky/core/storage.py`
- [x] 为 `Storage` 增加单次分页窗口查询能力，避免 fallback 路径重复查询
  - 相关位置：`pytucky/core/storage.py`
- [x] 优先保证 `filters + order_by + has_more + total_count` 的统一语义，并补回归测试
  - 相关位置：`tests/feature/test_storage_query_table_data.py`、`tests/unit/test_storage_query_table_data_backend.py`

**目标**：降低 Web UI / 表格浏览场景下的重复过滤与重复解码成本。

---

## P1：性能与运行时体验

### 3. 优化 `Session.flush()` 的 dirty update 路径

- [ ] 将 dirty 对象按模型/表分组，尽量复用已有 `bulk_update()` 能力
  - 相关位置：`pytucky/core/session.py`、`pytucky/core/storage.py`
- [ ] 去掉逐条 `update()` 后再 `select()` 回读的固定成本
  - 相关位置：`pytucky/core/session.py`
- [ ] 保持 `before_update / after_update` 事件语义不变，并补测试覆盖
  - 相关位置：`tests/feature/test_session_advanced.py`

**目标**：降低批量更新时的额外读放大。

### 4. 优化 prefetch / `IN` 条件的 membership 成本

- [ ] 将 `Condition('IN', value)` 的右值标准化为 `set` / `frozenset`（保留不可 hash 值的回退）
  - 相关位置：`pytucky/query/builder.py`
- [ ] 优化 `_prefetch_one_to_many()` 与 `_prefetch_many_to_one()` 的值收集方式，避免把大批量主键保留为线性查找列表
  - 相关位置：`pytucky/core/prefetch.py`
- [ ] 增加大批量 prefetch 的回归测试或基准
  - 相关位置：`tests/feature/test_relationship.py`

**目标**：降低关系批量预取场景中的 `O(rows × ids)` 级别开销。

### 5. 继续压缩懒加载单行读取热路径

- [ ] 评估在 `Store` / `TableState` 侧缓存 payload layout / codec 解析结果
  - 相关位置：`pytucky/backends/store.py`、`pytucky/backends/format.py`
- [ ] 评估内部 no-copy 快路径，减少 `select()` / `get()` 的重复 copy 成本
  - 相关位置：`pytucky/core/storage.py`
- [ ] 评估是否还有必要在 reopen 时复制整份 `_pk_offsets`
  - 相关位置：`pytucky/backends/backend_pytucky.py`、`pytucky/core/storage.py`

**目标**：进一步优化 reopen 后点查与懒加载首查延迟。

---

## P1：工程质量与发布收口

### 6. 固定依赖解析结果，提升 CI 可复现性

- [ ] 不再忽略 `uv.lock`，提交锁文件
  - 相关位置：`.gitignore`
- [ ] CI 改为使用锁定依赖集，避免未来因上游版本漂移导致“代码没变但 CI 失败”
  - 相关位置：`.github/workflows/ci.yml`

### 7. 把类型检查与打包验证纳入 CI

- [x] 修复当前 `mypy` 的 3 个错误
  - 相关位置：`pytucky/core/orm.py`
- [x] 新增 pytest 内的 `mypy` 静态回归用例，确保 `uv run python -m pytest` 可直接暴露类型回归
  - 相关位置：`tests/feature/test_mypy_typecheck.py`
- [ ] CI 增加 `mypy` 检查
  - 相关位置：`.github/workflows/ci.yml`、`pyproject.toml`
- [ ] CI 增加 `python -m build` 与 wheel 安装 smoke test
  - 相关位置：`.github/workflows/ci.yml`
- [ ] 验证安装态下 `py.typed`、公开 API 导出、基础导入路径可用
  - 相关位置：`pyproject.toml`、`pytucky/__init__.py`

### 8. 调整测试矩阵与测试分层

- [ ] 将 compat / benchmark 从主测试矩阵中拆分或降频执行
  - 相关位置：`.github/workflows/ci.yml`
- [ ] 主矩阵优先保证 unit / feature 的快速反馈
- [ ] 保留 system / benchmark，但避免拖慢所有平台和 Python 版本组合

---

## P2：文档、API 契约与发布一致性

### 9. 统一版本信息来源

- [ ] 消除 `README.md`、`pyproject.toml`、`pytucky/__init__.py` 之间的版本漂移
- [ ] 约束版本号为单一来源，避免发布时多处手工同步

### 10. 扩充公开 API 契约测试

- [ ] 为根包 `pytucky.__all__` 增加更完整的 import / smoke 测试
  - 相关位置：`pytucky/__init__.py`、`tests/feature/test_api_contract.py`
- [ ] 覆盖兼容别名和常用入口，降低导出回归风险

### 11. 复核 benchmark 与文档描述

- [ ] 重新在当前代码上跑一次 `pytucky vs pytuck` benchmark
  - 相关位置：`tests/benchmark/benchmark.py`
- [ ] 如结果更新，同步刷新 `README.md` 与 `docs/guide/benchmark.md`
- [ ] 去掉 benchmark 中对源码路径的硬编码依赖，优先验证安装态行为

### 12. 校正文档中的过时信息

- [ ] 校正 README 中过时版本号与测试规模描述
- [ ] 检查 `docs/` 下是否还保留旧后缀、旧测试数、旧阶段性表述
- [ ] 按需要补一段简明迁移说明：`import pytuck` → `import pytucky`
- [ ] 按需要补一段加密兼容说明：`None / low / medium / high` 与 `pytuck` 的互通范围

---

## 已完成且不再回退的方向

以下方向已经完成，或明确不再作为当前路线继续推进：

- [x] `pytuck` → `pytucky` 包名替换
- [x] 多引擎架构移除
- [x] connector / native SQL 路径移除
- [x] PTK5 / 旧格式兼容路线移除
- [x] WAL / sidecar 方向废弃

---

## 设计约束（持续保留）

- **格式兼容**：`pytucky` 生成的 PTK7 文件必须能被 `pytuck` 读取，反之亦然
- **API 兼容**：基础 ORM 用法仅需更改 import 即可从 `pytuck` 切换
- **零运行时依赖**：核心库不依赖第三方运行时包
- **Python 3.10+**：维持当前支持范围
- **性能优先**：在共享 PTK7 格式前提下，优先保证读写性能
