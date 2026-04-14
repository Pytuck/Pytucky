# Changelog

## 1.0.0 (2026-04-14)

首个正式版本。从 pytuck v1.0.0 fork 并重构为 PTK7 单引擎库。

### 架构

- 从 pytuck 多引擎架构（8 种引擎）收敛到 **PTK7 单引擎**
- 移除多引擎注册机制（BackendRegistry）、native-SQL、connectors
- 移除 PTK5 / PTK6 后端及迁移工具
- 移除 WAL 残留和过时选项（lazy_load、sidecar_wal、encryption 等）
- 精简 StorageBackend 基类为最小抽象接口

### 性能

与 Pytuck 1.2.1 同机对比（100,000 条记录）：

| 指标 | Pytucky 1.0.0 | Pytuck 1.2.1 | 变化 |
|------|---------------|--------------|------|
| insert | 628ms | 808ms | **-22%** |
| save | 464ms | 609ms | **-24%** |
| query_indexed | 1.52ms | 1.81ms | **-16%** |
| file_size | 5.98MB | 9.51MB | **-37%** |

具体优化项：

- **索引物化缓存**：HashIndexProxy / SortedIndexProxy 首次 lookup 时物化索引到内存，后续查询零解码开销
- **增量 flush**：只写入有变更的表，未改动的表跳过物化
- **复用读句柄**：同一 Store 实例内复用文件句柄
- **Session 批量插入**：`flush()` 按模型类分组走 `bulk_insert`，替代逐条 `insert` + readback
- **add O(1) 去重**：`session.add()` 使用 id-based set 替代列表扫描，100k add_all 从 74s 降至 1s
- **materialize 批量读取**：单次 `_read_bytes_at()` + 内存切片替代逐行 seek+read
- **decode_row codecs 缓存**：避免每行每列重复查找编解码器
- **P1 优化**：移除 fsync、未改表字节直通、消除 `_offset_map` 冗余字典

### PTK7 格式

- 默认按需读取：打开文件时只恢复文件头、schema、PK 目录和索引元数据
- 主键直达读取：`pk → (offset, length)` 直接定位记录
- 索引按需读取：索引元数据在打开时恢复，索引块在查询时再读取
- lazy flush overlay：reopen 后少量更新只构造 overlay，不需全表物化

### ORM

- 保留 pytuck 核心 API：Column、declarative_base、Session、select/insert/update/delete
- 保留 PureBaseModel + CRUDBaseModel 两种模式
- 保留 Relationship 延迟加载
- 保留事件系统（before/after_insert/update/delete/flush）
- 保留 Schema 同步（sync_schema、add_column、drop_column、alter_column 等）

### 测试

- 65 个测试全部通过
- 覆盖 unit / feature / system / recovery 层级

### 迁移

从 pytuck 迁移只需更改 import：

```python
# 之前
from pytuck import Storage, declarative_base, Session, Column

# 之后
from pytucky import Storage, declarative_base, Session, Column
```
