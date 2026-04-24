# Pytucky 当前待办清单

本文件已按当前代码库状态重写，用于替代早期那份已经滞后的 TODO。

> 当前项目是 **PTK7 单格式库**，默认后缀为 **`.pytuck`**，显式 **`.pytucky`** 仍兼容。
> 
> 已完成：`pytuck -> pytucky` 包名替换、单引擎化、移除 connector/native SQL、多数核心性能优化、`None / low / medium / high` 四档与真实 `pytuck` 的双向互通。

---

## 当前快照

- [x] 包名已切换为 `pytucky`
- [x] 当前仅保留 **PTK7 单引擎**
- [x] 默认文件后缀已统一为 **`.pytuck`**
- [x] 显式 **`.pytucky`** 后缀仍兼容
- [x] 已支持 `None / low / medium / high` 四档读写
- [x] 已验证与真实 `pytuck` 双向互读互写
- [x] 当前全量测试：`164 passed`

---

## 旧 TODO 核对结果

### 阶段一：包名替换与基础可用（已完成）

- [x] 全局替换 `pytuck` → `pytucky`
- [x] 重命名 `PytuckException` → `PytuckyException`，并保留兼容别名
- [x] 验证 `import pytucky` 与公开 API 导出可用
- [x] 基础冒烟 / 核心 CRUD 测试已具备

> 结论：**旧 TODO 的阶段一应视为完成，不再继续做这一阶段的旧条目。**

### 阶段二：移除多引擎架构（已完成）

- [x] Storage 已切换为单引擎路径
- [x] `backends/registry.py` 已移除
- [x] `StorageBackend` 已精简为最小抽象接口
- [x] 自动注册机制已移除
- [x] `backends/__init__.py` 已收敛为单引擎导出
- [x] `common/options.py` 已简化为 PTK7 所需配置

### 阶段三：移除连接器 / SQL 相关代码（已完成）

- [x] Session 中 connector / native SQL 路径已清理
- [x] Storage 中 connector 初始化与相关方法已清理
- [x] `query/compiler.py` 已不再保留为当前实现的一部分

### 阶段四：性能优化（部分完成，剩余项已转入当前待办）

- [x] 按需读写主路径已实现
- [x] 懒加载已强化
- [x] 索引物化缓存已实现
- [x] 增量 flush 已实现
- [x] 读句柄复用已实现
- [x] WAL / sidecar 方向已废弃，不再作为当前路线继续推进
- [ ] 重新复核当前版本 `pytucky vs pytuck` benchmark，并同步文档数据
- [ ] 视需要补性能回归基线或自动化说明

### 阶段五：完善（部分完成，剩余项已转入当前待办）

- [x] `__init__.py` 的 `__all__` 已更新
- [x] 核心测试已迁移并扩充
- [x] README / 文档 / 使用示例已存在
- [ ] 校正文档中的过时表述（默认后缀、格式说明、测试数、加密互通说明）
- [ ] 视需要补更明确的迁移 / 加密示例

---

## 当前仍可继续推进的事项

### 1. 文档校准

- [ ] 更新 `README.md` 中的过时描述：
  - `.pytucky` → 默认 `.pytuck`、显式 `.pytucky` 兼容
  - 测试统计 `65 passed` → 当前真实测试数
  - 补充 PTK7 与 `pytuck` 互通、三档加密现状
- [ ] 检查 `docs/` 中是否仍有旧表述（PTK5、旧后缀、旧测试数等）并统一

### 2. benchmark 数据复核

- [ ] 在当前 `develop` 上重新跑一次 `pytucky vs pytuck` benchmark
- [ ] 如果结果有变化，同步更新 `docs/guide/benchmark.md` 与 `README.md`
- [ ] 如有必要，补一份简单的 benchmark 运行说明或回归基线说明

### 3. 对外说明收尾

- [ ] 视需要补一段明确迁移说明：`import pytuck` → `import pytucky`
- [ ] 视需要补一段加密兼容说明：`None / low / medium / high` 与真实 `pytuck` 互通

---

## 暂不继续的旧方向

以下内容不再作为当前 TODO 推进：

- PTK5 兼容路线
- 多引擎架构回归
- connector / native SQL 回归
- WAL / sidecar 方案

---

## 设计约束（保留）

- **格式兼容**：`pytucky` 生成的 PTK7 文件必须能被 `pytuck` 读取，反之亦然
- **API 兼容**：基础 ORM 用法仅需更改 import 即可从 `pytuck` 切换
- **零外部依赖**：核心库不依赖任何第三方运行时包
- **Python 3.10+**：不再兼容已停止维护的旧版本
- **性能优先**：在共享 PTK7 格式前提下，优先保证读写性能
