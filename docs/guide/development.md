# Pytucky 开发与发布指南

本页汇总 README 首页之外、更偏向安装细节、贡献开发和发布维护的说明。

## 安装方式

### 从 PyPI 安装

```bash
pip install pytucky
```

### 在 uv 项目中添加依赖（推荐）

[uv](https://github.com/astral-sh/uv) 是一个极快的 Python 项目与包管理器。如果你的应用本身使用 uv 管理，推荐直接把 `pytucky` 添加到当前项目依赖中：

```bash
uv add pytucky
```

## 贡献者：同步源码开发环境

如果你是克隆仓库后准备参与开发，不要使用 editable install 方式手动把项目装进当前环境，而是直接同步项目开发环境：

```bash
git clone <repo-url>
cd pytucky

# 同步开发环境（包含测试、类型检查与打包工具）
uv sync --extra dev

# 运行测试
uv run pytest tests/ -v
```

> `uv sync --extra dev` 只会同步当前项目的开发环境，不会像 `pip install -e ".[dev]"` 那样把仓库本身作为可编辑库再次安装进环境。

## 测试与校验

```bash
# 全量测试
uv run pytest tests/ -v

# 单个测试文件
uv run pytest tests/unit/test_store.py -v

# 指定标记
uv run pytest tests/ -v -m unit
uv run pytest tests/ -v -m feature
uv run pytest tests/ -v -m system
```

### 测试标记

| 标记 | 说明 |
|------|------|
| `unit` | 快速单元测试 |
| `feature` | 功能测试 |
| `system` | 系统级兼容测试 |
| `recovery` | 恢复路径测试 |
| `benchmark` | 性能基准测试 |
| `slow` | 耗时测试 |

### 临时文件约定

- 测试优先使用 `tmp_path` / `TemporaryDirectory()` 托管临时目录。
- 需要手动创建的临时文件必须在测试结束前删除。
- 基准脚本如果写出中间文件，必须落在临时目录或显式清理，不要把垃圾文件留在仓库根目录。

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

## 代码约定

- 最低支持 Python 版本为 `3.10+`，不再支持已停止维护的 `3.9` 及以下版本。
- 所有函数和方法必须有完整类型提示，统一使用现代写法：`dict[...]`、`list[...]`、`str | None`、`type[...]`。
- 路径操作统一使用 `pathlib.Path`。
- 异常只在 `pytucky/common/exceptions.py` 中定义。
- 通用工具函数只在 `pytucky/common/utils.py` 中定义。
- 文档、注释、说明使用中文；代码保持英文。

## 打包与发布

```bash
# 构建 wheel 和源码分发包
uv build

# 上传到 PyPI（使用已配置凭证，或显式传入 token）
uv publish
# uv publish --token $PYPI_TOKEN
```

## 发布检查清单

在正式发布前，建议按下面顺序执行：

1. 更新 `pyproject.toml` 中的版本号。
2. 同步 `README.md`、`docs/guide/development.md` 与其他用户可见文档。
3. 运行发布前验证：

   ```bash
   uv run pytest tests/ -v
   uv build
   ```

4. 验证通过后再执行：

   ```bash
   uv publish
   ```

## 相关文档

- [README 首页](../../README.md)
- [API 文档索引](../api/index.md)
- [性能基准报告](benchmark.md)
- [开发待办 TODO](../../TODO.md)
