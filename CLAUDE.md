## 基本要求

- 始终使用中文回答

### 运行环境

- **Windows/Linux/macOS**：都使用默认 shell（bash/zsh），命令无法执行时再尝试 PowerShell/cmd 等
- 项目使用**uv**管理依赖和运行，虚拟环境位于 `.venv` 目录下，请使用uv或虚拟环境的python运行项目

---

# Pytucky 项目说明

## 项目简介

Pytucky 是从 [Pytuck](../pytuck) 精简而来的**单格式轻量级文档数据库**。

**与 Pytuck 的关系**：
- 使用完全相同的 **PTK5 / `.pytuck`** 文件格式
- 保留兼容的核心 ORM API（Column、declarative_base、Session、select/insert/update/delete）
- **去掉**了多引擎支持（JSON、CSV、SQLite、DuckDB、Excel、XML、JSONL）
- **去掉**了 connectors/、tools/ 等辅助模块
- 目标：用户仅需更改 `import pytuck` → `import pytucky` 即可切换基础用法

**定位**：专为受限 Python 环境设计（如 Ren'Py），像 SQLite 一样高性能读写，不再每次全量加载到内存、全量保存。

## 当前状态：初始迁移（未重构）

> **重要**：当前代码是从 pytuck v1.0.0 直接复制并粗裁剪的，包名内部仍然引用 `pytuck` 而非 `pytucky`。代码可以运行但需要系统性重构。

### 已完成
- [x] 从 pytuck 复制核心源码（core/、common/、query/、backends/backend_binary.py）
- [x] 删除不需要的后端文件（json、csv、sqlite、duckdb、excel、xml、jsonl）
- [x] 删除不需要的 common 模块（encrypted_zip.py、zipcrypto.py —— 仅 CSV/JSONL 后端使用）
- [x] 裁剪 backends/__init__.py 中对已删除后端的导入
- [x] 创建项目配置（pyproject.toml）

### 待完成的重构工作（按优先级排序）

#### P0 — 包名替换（必须先做）
1. **全局替换包名**：所有源文件中 `pytuck` → `pytucky`（import 路径、字符串引用、docstring 等）
2. **异常类重命名**：`PytuckException` → `PytuckyException`，以及其他包含 "pytuck" 的标识符
3. **验证基础导入**：确保 `import pytucky` 和 `from pytucky import Storage, Column, ...` 能正常工作

#### P1 — 移除多引擎架构
4. **简化 Storage**：移除 `engine` 参数和多引擎选择逻辑，Storage 直接实例化 BinaryBackend
5. **移除 BackendRegistry**：删除 registry.py，移除 `__init_subclass__` 自动注册机制
6. **精简 StorageBackend 基类**：base.py 要么简化为最小接口，要么完全移除让 BinaryBackend 独立
7. **清理 backends/__init__.py**：移除 registry 导入，仅导出 BinaryBackend
8. **精简 options.py**：移除其他后端的 Options dataclass（JsonBackendOptions、CsvBackendOptions 等），仅保留 BinaryBackendOptions 和 SyncOptions/SyncResult

#### P2 — 移除连接器/SQL 相关代码
9. **清理 Session**：移除 `_native_sql_mode`、connector 相关代码路径
10. **清理 Storage**：移除 connector 相关的初始化和方法
11. **清理 query/compiler.py**：如果仅为 SQL 后端服务则可移除，检查是否有内存查询也依赖它

#### P3 — 性能优化（核心目标）
12. **优化读写模式**：像 SQLite 一样按需读写，不再全量加载到内存后全量保存
13. **强化 BinaryBackend 的懒加载**：确保大文件场景下只加载需要的数据
14. **评估 WAL 机制**：确保 WAL 在精简场景下仍然高效

#### P4 — 清理和完善
15. **更新 `__init__.py`**：确保 `__all__` 只导出精简后的 API
16. **编写测试**：从 pytuck 迁移核心测试，确保单引擎场景全部通过
17. **编写 README.md**：项目说明、安装方法、基础使用示例
18. **编写示例**：展示 pytucky 的基本用法，强调与 pytuck 的 API 兼容性

## 目录结构

```
pytucky/
├── pytucky/                   # 核心库
│   ├── __init__.py           # 公开 API 导出
│   ├── py.typed              # 类型注解标记文件
│   ├── common/               # 公共模块（无内部依赖）
│   │   ├── __init__.py
│   │   ├── options.py        # 配置选项（待裁剪：移除其他后端选项）
│   │   ├── typing.py         # 类型别名定义
│   │   ├── utils.py          # 工具函数
│   │   ├── crypto.py         # 加密支持（PTK5 加密功能）
│   │   └── exceptions.py     # 异常定义
│   ├── core/                 # 核心模块
│   │   ├── __init__.py
│   │   ├── orm.py            # ORM 核心：Column, PureBaseModel, CRUDBaseModel, declarative_base
│   │   ├── storage.py        # 存储引擎封装（待简化：移除多引擎逻辑）
│   │   ├── session.py        # 会话管理（待简化：移除 native SQL 模式）
│   │   ├── index.py          # 索引管理
│   │   ├── types.py          # 类型编解码
│   │   ├── event.py          # 事件钩子系统
│   │   └── prefetch.py       # 关系预取
│   ├── query/                # 查询子系统
│   │   ├── __init__.py
│   │   ├── builder.py        # 查询构建器
│   │   ├── compiler.py       # 查询编译器（待评估是否需要）
│   │   ├── statements.py     # SQL 风格语句构建
│   │   └── result.py         # 查询结果封装
│   └── backends/             # 存储引擎（仅 pytuck 格式）
│       ├── __init__.py       # 后端导出（已裁剪，仅 binary）
│       ├── base.py           # StorageBackend 基类（待简化或移除）
│       ├── registry.py       # 后端注册器（待移除）
│       ├── versions.py       # 引擎版本管理
│       └── backend_binary.py # PTK5 二进制引擎（核心）
├── tests/                    # 测试文件
├── examples/                 # 示例代码
├── pyproject.toml            # 项目配置
├── CLAUDE.md                 # 本文件
├── TODO.md                   # 开发路线图
└── .gitignore
```

## 源项目参考

pytucky 的代码来自 pytuck v1.0.0（路径：`../pytuck`）。当需要理解设计意图或查看原始实现时，可以参考源项目：

| pytucky 文件 | 源自 pytuck | 说明 |
|--------------|-------------|------|
| backends/backend_binary.py | 完全相同 | PTK5 引擎核心，两库共享同一格式 |
| core/orm.py | 完全相同 | ORM 核心，API 兼容基础 |
| core/session.py | 需裁剪 | 含有 native SQL 和 connector 相关代码需移除 |
| core/storage.py | 需裁剪 | 含有多引擎选择逻辑需移除 |
| common/options.py | 需裁剪 | 含有其他后端的 Options 类需移除 |
| backends/registry.py | 待删除 | 多引擎注册机制，pytucky 不需要 |
| backends/base.py | 待简化 | 抽象基类，可简化或移除 |

## 核心 API（目标兼容）

pytucky 的最终 API 应与 pytuck 基础用法完全兼容，仅包名不同：

```python
# pytuck 原始用法
from pytuck import Storage, declarative_base, Session, Column
from pytuck import PureBaseModel, select, insert, update, delete

# pytucky 对应用法（仅改 import）
from pytucky import Storage, declarative_base, Session, Column
from pytucky import PureBaseModel, select, insert, update, delete
```

### 两种模型模式

1. **PureBaseModel**（纯模型） — 通过 Session + Statement API 操作
2. **CRUDBaseModel**（Active Record） — 模型自带 CRUD 方法

### Storage 初始化（简化目标）

```python
# pytuck: 需要指定 engine 或依赖文件后缀推断
db = Storage(file_path='mydb.pytuck')

# pytucky: 无需 engine 参数，永远使用 PTK5 格式
db = Storage(file_path='mydb.pytuck')
```

## 开发约定

### 代码风格
- 使用 Python 3.7+ 类型注解
- 遵循 PEP 8 规范
- 中文注释，英文代码

### 路径操作规范（强制）

**所有文件路径操作必须使用 `pathlib.Path`**，避免混合使用 `os.path` 和字符串操作。

### 类型提示规范（强制）
- **所有函数和方法必须有完整的类型提示**
- 使用 `typing` 模块中的类型
- 使用 `TYPE_CHECKING` 避免循环引用

### 测试（强制）

```bash
# 一键运行所有测试
pytest tests/ -v
```

**强制要求**：每次代码改动后必须运行全部测试并确保通过。

### 模块职责规范（强制）

| 模块 | 职责 |
|------|------|
| `common/exceptions.py` | 所有自定义异常类 |
| `common/utils.py` | 工具函数 |
| `common/options.py` | 配置选项 dataclass |
| `core/orm.py` | ORM 核心 |
| `core/storage.py` | 存储封装 |
| `query/builder.py` | 查询构建 |
| `backends/backend_binary.py` | PTK5 引擎实现 |

异常只在 `common/exceptions.py` 定义，工具函数只在 `common/utils.py` 定义。

## 常用命令

```bash
# 安装开发依赖
pip install -e ".[dev]"

# 一键运行所有测试
pytest tests/ -v

# 运行单个测试文件
pytest tests/test_orm.py -v
```
