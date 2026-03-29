# Pytucky 开发待办清单

本文件记录 Pytucky 项目的开发计划。

> Pytucky 是 [Pytuck](../pytuck) 的单格式精简版，仅支持 PTK5 / `.pytuck` 引擎。

---

## 阶段一：包名替换与基础可用

- [ ] 全局替换 `pytuck` → `pytucky`（import 路径、字符串引用、docstring）
- [ ] 重命名 `PytuckException` → `PytuckyException` 及相关标识符
- [ ] 验证 `import pytucky` 和 `from pytucky import Storage, Column, ...` 正常工作
- [ ] 编写基础冒烟测试，确保核心 CRUD 流程可用

## 阶段二：移除多引擎架构

- [ ] 简化 Storage：移除 `engine` 参数，直接实例化 BinaryBackend
- [ ] 删除 `backends/registry.py`
- [ ] 简化或移除 `backends/base.py`（StorageBackend 抽象基类）
- [ ] 移除 `__init_subclass__` 自动注册机制
- [ ] 清理 `backends/__init__.py`，仅导出 BinaryBackend
- [ ] 精简 `common/options.py`：移除其他后端的 Options dataclass

## 阶段三：移除连接器/SQL 相关代码

- [ ] 清理 Session 中的 `_native_sql_mode` 和 connector 代码路径
- [ ] 清理 Storage 中的 connector 初始化和方法
- [ ] 评估 `query/compiler.py` 是否仍需保留

## 阶段四：性能优化（核心目标）

- [ ] 优化读写模式：按需读写，不再全量加载/保存
- [ ] 强化懒加载：大文件场景只加载需要的数据
- [ ] 评估和优化 WAL 机制
- [ ] 对比 pytuck 基准测试，确保性能不退步

## 阶段五：完善

- [ ] 更新 `__init__.py` 的 `__all__` 导出
- [ ] 从 pytuck 迁移并适配核心测试
- [ ] 编写 README.md
- [ ] 编写使用示例

---

## 设计约束

- **格式兼容**：pytucky 生成的 `.pytuck` 文件必须能被 pytuck 读取，反之亦然
- **API 兼容**：基础 ORM 用法仅需更改 import 即可从 pytuck 切换
- **零外部依赖**：核心库不依赖任何第三方包
- **Python 3.7+**：保持与 pytuck 相同的兼容性
