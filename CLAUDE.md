## 基本要求

- 始终使用中文回答、编写注释、文档等，但代码本身除外

### 运行环境

- **Windows/Linux/macOS**：都使用默认 shell（bash/zsh），命令无法执行时再尝试 PowerShell/cmd 等
- 项目使用**uv**管理依赖和运行，虚拟环境位于 `.venv` 目录下，请使用uv或虚拟环境的python运行项目

---

# Pytucky 项目说明

## 项目简介

Pytucky 是**单文件、高性能、纯 Python 文档数据库**，基于 **PTK7** 二进制格式。

**定位**：专为受限 Python 环境设计（如 Ren'Py），像 SQLite 一样高性能按需读写，无外部依赖。提供类似 SQLAlchemy 的声明式 ORM API。

**与 Pytuck 的关系**：
- 两个库共享 **PTK7** 二进制格式
- 保留兼容的核心 ORM API（Column、declarative_base、Session、select/insert/update/delete）
- pytucky 是精简版：只有 PTK7 单引擎，无多引擎支持、无 connectors、无 native-SQL
- 用户仅需更改 `import pytuck` → `import pytucky` 即可切换基础用法

## 当前状态：PTK7 单格式库（已完成重构）

### 架构特点
- **单引擎**：Storage 直接实例化 PytuckyBackend，无引擎注册/选择机制
- **索引物化缓存**：HashIndexProxy / SortedIndexProxy 首次 lookup 时物化索引到内存，后续查询零解码开销
- **增量 flush**：只写入有变更的表，未改动的表跳过物化
- **复用读句柄**：同一 Store 实例内复用文件句柄，避免重复 open/close

### 已完成的重构
- [x] 全局包名替换 pytuck → pytucky
- [x] P0 索引等值查询物化缓存（性能关键）
- [x] 移除多引擎注册机制（BackendRegistry、registry.py）
- [x] 移除 native-SQL / connector 路径（compiler.py、所有 _native_sql 方法）
- [x] 移除 WAL 残留和过时选项（lazy_load、sidecar_wal、encryption 等）
- [x] 移除 PTK5 / backend_binary.py 及迁移工具
- [x] 精简 BinaryBackendOptions 为空 dataclass
- [x] 精简 StorageBackend 基类为最小抽象接口
- [x] 完整测试覆盖（65 个测试全部通过）

## 目录结构

```
pytucky/
├── pytucky/                   # 核心库
│   ├── __init__.py           # 公开 API 导出
│   ├── py.typed              # 类型注解标记文件
│   ├── common/               # 公共模块（无内部依赖）
│   │   ├── __init__.py
│   │   ├── options.py        # 配置选项：BinaryBackendOptions、SyncOptions、SyncResult
│   │   ├── typing.py         # 类型别名定义
│   │   ├── utils.py          # 工具函数
│   │   ├── crypto.py         # 加密支持
│   │   └── exceptions.py     # 异常定义
│   ├── core/                 # 核心模块
│   │   ├── __init__.py
│   │   ├── orm.py            # ORM 核心：Column, PureBaseModel, CRUDBaseModel, declarative_base
│   │   ├── storage.py        # 存储引擎封装（直接实例化 PytuckyBackend）
│   │   ├── session.py        # 会话管理
│   │   ├── index.py          # 索引管理（HashIndex, SortedIndex 基类）
│   │   ├── types.py          # 类型编解码
│   │   ├── event.py          # 事件钩子系统
│   │   └── prefetch.py       # 关系预取
│   ├── query/                # 查询子系统
│   │   ├── __init__.py
│   │   ├── builder.py        # 查询构建器
│   │   ├── statements.py     # SQL 风格语句构建（select/insert/update/delete）
│   │   └── result.py         # 查询结果封装
│   └── backends/             # PTK7 引擎
│       ├── __init__.py       # 导出 StorageBackend、PytuckyBackend
│       ├── base.py           # StorageBackend 最小抽象基类
│       ├── backend_pytucky.py # PTK7 引擎实现（含 HashIndexProxy/SortedIndexProxy 物化缓存）
│       ├── store.py          # PTK7 底层存储（Store 类，页式读写）
│       ├── format.py         # PTK7 二进制格式编解码
│       ├── index.py          # PTK7 索引编解码（encode/decode sorted pairs）
│       └── versions.py       # 格式版本号（pytucky: 7）
├── tests/                    # 测试文件
├── pyproject.toml            # 项目配置
├── CLAUDE.md                 # 本文件
└── .gitignore
```

## 核心 API

```python
from pytucky import Storage, declarative_base, Session, Column
from pytucky import PureBaseModel, select, insert, update, delete

# 创建数据库
db = Storage(file_path='mydb.pytucky')
```

### 两种模型模式

1. **PureBaseModel**（纯模型） — 通过 Session + Statement API 操作
2. **CRUDBaseModel**（Active Record） — 模型自带 CRUD 方法

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
uv run pytest tests/ -v
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
| `backends/backend_pytucky.py` | PTK7 引擎实现 |
| `backends/store.py` | PTK7 底层存储 |
| `backends/format.py` | PTK7 二进制格式 |

异常只在 `common/exceptions.py` 定义，工具函数只在 `common/utils.py` 定义。

## 常用命令

```bash
# 安装开发依赖
uv pip install -e ".[dev]"

# 一键运行所有测试
uv run pytest tests/ -v

# 运行单个测试文件
uv run pytest tests/unit/test_store.py -v
```
