# Changelog

## 1.1.2 (2026-04-25)

本次补丁版本聚焦于 `Relationship` 解析与双向关系缓存的一致性优化，并同步修正文档中的版本表述。

### 变更

- **统一 `Relationship` 解析路径**
  - 统一解析 relationship 的目标模型、`Storage` 与表名
  - 字符串形式的目标现在明确按表名处理，不再回退为类名字符串
  - 字符串目标在未显式传入 `storage` 时默认同库，跨库场景仍需显式指定

- **增强 `back_populates` 的关系缓存回填**
  - 配置 `back_populates` 后会校验双向关系定义的对称性
  - 延迟加载与 `prefetch()` 现在都会自动执行反向缓存回填
  - 关系缓存写入收敛为统一路径，减少重复分支并提升行为一致性

### 修复

- **改进 relationship 配置错误提示与稳定性**
  - 优化异常提示信息，帮助更快定位关系声明配置问题
  - 补充字符串目标、反向缓存回填与双向校验相关测试覆盖

### 文档

- **修正 changelog 版本表述**
  - 更正文档中的版本号描述，避免发布说明与当前版本不一致

## 1.1.1 (2026-04-24)

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

- **公开文档与版本号同步到 1.1.1**
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
