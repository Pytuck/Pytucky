## Pytucky 项目开发指引

### 语言与输出
- 所有说明、注释、文档使用中文；代码本身保持英文。

### 运行方式
- 项目统一使用 `uv` 管理依赖与命令，默认通过 `uv run ...` 执行。
- 开发环境使用项目 `.venv` / `uv sync --extra dev`，不要使用 editable install。
- 常用全量验证命令：`uv run python -m pytest`。

### 当前产品边界
- 只维护 **PTK7 单引擎**，不要回退多引擎、connector、native-SQL、WAL / sidecar 或 PTK5 兼容路线。
- 默认文件后缀是 `.pytuck`；显式 `.pytucky` 仅作兼容。
- 与 `pytuck` 共享 PTK7 格式；任何格式、lazy load、flush、reopen 相关改动都不能破坏双向互读互写。
- 保持零第三方运行时依赖与 Python 3.10+ 支持范围。

### 代码约束
- 路径操作统一使用 `pathlib.Path`。
- 所有函数和方法都保持完整类型提示；优先用 `TYPE_CHECKING` 处理循环引用。
- 不要随意打破模块职责：
  - `common/exceptions.py`：自定义异常
  - `common/utils.py`：通用工具函数
  - `common/options.py`：配置 dataclass
  - `core/orm.py`：模型与声明式 API
  - `core/storage.py`：Storage 封装
  - `core/session.py`：Session、flush、对象状态
  - `query/*`：查询构建与结果封装
  - `backends/backend_pytucky.py`：后端适配层
  - `backends/store.py`：PTK7 底层读写
  - `backends/format.py`：PTK7 编解码

### 测试与发布
- 每次代码改动后必须运行 `uv run python -m pytest` 并确保全绿。
- benchmark / test 默认不能在仓库里留下产物；只有显式 `--keep` 或输出文件参数时才允许保留。
- 发布版本只改 `pytucky/__init__.py` 中的 `__version__`；`pyproject.toml` 通过动态版本读取它。
- 更新版本时同步检查 `README.md`、`docs/guide/benchmark.md`、`CHANGELOG.md`、`TODO.md` 的版本号与测试数是否一致。
