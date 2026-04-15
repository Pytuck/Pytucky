# 开发指南

## 环境搭建

### 前置要求

- Python >= 3.7
- [uv](https://docs.astral.sh/uv/)（推荐）或 pip

### 安装

```bash
# 克隆仓库
git clone <repo-url>
cd pytucky

# 使用 uv（推荐）
uv sync

# 或使用 pip
pip install -e ".[dev]"
```

### 开发依赖

| 包 | 用途 |
|---|---|
| pytest | 测试框架 |
| pytest-cov | 覆盖率 |
| mypy | 类型检查 |
| build | 构建 |
| twine | 发布 |

---

## 测试

### 运行全部测试

```bash
uv run pytest tests/ -v
```

### 运行单个测试文件

```bash
uv run pytest tests/unit/test_store.py -v
```

### 运行特定标记的测试

```bash
uv run pytest tests/ -v -m unit
uv run pytest tests/ -v -m feature
uv run pytest tests/ -v -m system
```

### 测试标记

| 标记 | 说明 |
|------|------|
| `unit` | 快速单元测试 |
| `feature` | 功能测试 |
| `system` | 系统集成测试 |
| `recovery` | 恢复路径测试 |
| `benchmark` | 性能基准测试 |
| `slow` | 耗时测试 |

---

## 运行 Benchmark

```bash
# 1000 条记录
uv run python tests/benchmark/benchmark.py -n 1000 --extended

# 100000 条记录（标准基线）
uv run python tests/benchmark/benchmark.py -n 100000 --extended

# 输出 JSON 结果
uv run python tests/benchmark/benchmark.py -n 100000 --extended \
    --output-json tests/benchmark/benchmark_output/pytucky-v7-100000.json
```

---

## 代码规范

### 类型注解

所有函数和方法必须有完整的类型提示。使用 `typing` 模块，`TYPE_CHECKING` 避免循环引用。

```python
from typing import Any, Dict, List, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from .storage import Storage
```

### 路径操作

所有文件路径操作必须使用 `pathlib.Path`：

```python
from pathlib import Path

path = Path(file_path).expanduser()
```

### 异常定义

异常只在 `common/exceptions.py` 中定义。

### 工具函数

通用工具函数只在 `common/utils.py` 中定义。

### 注释语言

中文注释，英文代码。

---

## 目录结构

```
pytucky/
├── pytucky/                   # 核心库
│   ├── __init__.py           # 公开 API 导出
│   ├── common/               # 公共模块
│   │   ├── exceptions.py     # 异常定义
│   │   ├── options.py        # 配置选项
│   │   ├── typing.py         # 类型别名
│   │   └── utils.py          # 工具函数
│   ├── core/                 # 核心模块
│   │   ├── orm.py            # ORM（Column, Model, declarative_base）
│   │   ├── storage.py        # Storage 引擎
│   │   ├── session.py        # Session 管理
│   │   ├── index.py          # 索引基类
│   │   ├── types.py          # 类型编解码
│   │   ├── event.py          # 事件系统
│   │   └── prefetch.py       # 关系预取
│   ├── query/                # 查询子系统
│   │   ├── builder.py        # 查询构建器
│   │   ├── statements.py     # Statement API
│   │   └── result.py         # 查询结果
│   └── backends/             # PTK7 引擎
│       ├── base.py           # StorageBackend 基类
│       ├── backend_pytucky.py # PTK7 引擎实现
│       ├── store.py          # PTK7 底层存储
│       ├── format.py         # PTK7 格式
│       └── index.py          # PTK7 索引编解码
├── tests/                    # 测试
├── docs/                     # 文档
└── pyproject.toml            # 项目配置
```

---

## 提交规范

提交信息使用中文，格式参考：

```
<type>(<scope>): <描述>

类型: feat, fix, refactor, perf, test, docs, chore
范围: ptk7, session, orm, query, ...
```

示例：

```
feat(session): 添加 bulk_update 批量更新支持
fix(ptk7): 修复懒加载表 flush 时的偏移计算
perf(ptk7): 优化 _materialize_records 批量读取
```
