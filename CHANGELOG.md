# Changelog

## 1.1.0 (2026-04-24)

这是 `v1.0.0` 之后的首个小版本汇总发布，覆盖从 `v1.0.0` 的主要能力演进与兼容性收敛。

### 新增

- **PTK7 三档加密与 pytuck 互通**
  - 增加 `low / medium / high` 三档加密支持
  - 保持与 pytuck 的 PTK7 文件互读互写路径
  - 加密文件的 reopen、lazy index 与范围查询路径已补齐验证

- **Relationship 支持显式指定目标 `Storage`**
  - `Relationship(..., storage=base_db)` 现在可以把关系读取定向到另一份数据库文件
  - 支持典型的“基础库只读 + 用户库动态写入”模型组织方式
  - 不要求两个数据库文件使用相同引擎，只依赖统一的 `Storage` 抽象接口

- **跨 `Storage` 预取支持**
  - `prefetch()` 现在会跟随 relationship 的目标 `Storage` 做批量查询
  - `select(...).options(prefetch(...))` 同步支持跨库 relationship

- **Storage / Session 线程安全保护**
  - 为 `Storage` 与 `Session` 的关键操作增加线程锁保护
  - 补齐共享 `Storage` 插入、`auto_flush` 保存、共享 `Session.add()` 的并发保护测试

### 变更

- **单引擎 PTK7 路径继续收敛**
  - 统一后端选项命名并清理单引擎残留接口
  - 继续压缩与多引擎时代相关的遗留复杂度，公开层保持当前 PTK7 单格式叙事

- **清理无效的 relationship 参数**
  - 删除 `Relationship.lazy` 参数
  - relationship 继续默认保持惰性读取，批量加载统一通过 `prefetch()` 完成

- **类型系统与开发环境收敛**
  - 清理静态类型隐患并打通 mypy 路径
  - 类型注解语法与开发文档对齐到当前 Python 版本要求
  - 若干初始化参数改为更明确的关键字语义，减少误用空间

- **公开文档与版本号同步到 1.1.0**
  - README、API 参考与 benchmark 版本号已统一升级
  - 常规文档现在直接陈述当前能力，不再把“本次更新内容”混入正文结构
  - 新增跨 `Storage` relationship / `prefetch()` 的当前用法示例

### 约束

- 仍然**不支持 join**
- relationship 的跨库能力仅用于**读取与预取**
- 不提供跨多个 `Storage` 的原子事务语义

### 修复

- **read_lazy_record offset 映射读取修正**
  - 修复按 offset 映射读取记录时的主键定位问题
  - 避免 reopen 后 lazy 读取路径出现记录映射错误

- **多个已确认缺陷修复**
  - `v1.0.0` 后已合并一轮已确认问题修复
  - 同步补上了对应的回归测试覆盖

### 测试

- 测试规模从 `65` 项扩展到 `179` 项
- 新增 PTK7 加密、跨库 relationship、跨库 `prefetch()`、线程安全、查询与类型系统等回归覆盖
- 全量测试通过：`179 passed`

### 历史归档

- [1.0.0](docs/changelog/1.0.0.md)
