"""
Pytuckyy 后端配置选项（精简版）

本模块保留当前仓库实际使用的选项类型：BinaryBackendOptions、SyncOptions、SyncResult。
为兼容外部引用，保留 BackendOptions 名称作为 BinaryBackendOptions 的类型别名。
get_default_backend_options() 已简化为始终返回 BinaryBackendOptions()，以降低因移除多后端选项引入的断裂风险。
"""
from dataclasses import dataclass, field
from typing import Optional, Literal, List


@dataclass
class BinaryBackendOptions:
    """Pytucky 二进制后端（PTK5/PTK7）配置选项

    注意：此 dataclass 的字段保持不变以兼容现有代码和测试。
    """
    lazy_load: bool = True  # 是否懒加载（只加载 schema 和索引，按需读取数据）
    sidecar_wal: bool = False  # 是否将 WAL 写入独立 sidecar 文件（.<文件名>.wal）

    # 加密选项
    encryption: Optional[Literal['low', 'medium', 'high']] = None  # 加密等级: 'low' | 'medium' | 'high' | None
    password: Optional[str] = None    # 加密密码（仅 encryption 非 None 时生效）


# 保留 BackendOptions 名称作为 BinaryBackendOptions 的别名，减少外部 breakage
BackendOptions = BinaryBackendOptions


# 默认选项获取函数（简化）
def get_default_backend_options(engine: str) -> BinaryBackendOptions:
    """返回默认后端选项。

    当前 pytucky 项目仅支持二进制 PTK 引擎，函数签名保留以兼容历史调用。
    无论传入何种 engine 名称，均返回 BinaryBackendOptions()。
    """
    return BinaryBackendOptions()


# ========== Schema 同步选项 ==========


@dataclass
class SyncOptions:
    """Schema 同步选项

    控制 sync_table_schema 和 declarative_base(sync_schema=True) 的行为。
    """
    sync_table_comment: bool = True       # 是否同步表备注
    sync_column_comments: bool = True     # 是否同步列备注
    add_new_columns: bool = True          # 是否添加新列
    # 以下为安全选项，默认不启用
    drop_missing_columns: bool = False    # 是否删除模型中不存在的列（危险）
    update_column_types: bool = False     # 是否更新列类型（危险，暂未实现）


@dataclass
class SyncResult:
    """Schema 同步结果

    记录 sync_table_schema 执行后的变更详情。
    """
    table_name: str
    table_comment_updated: bool = False
    columns_added: List[str] = field(default_factory=list)
    columns_dropped: List[str] = field(default_factory=list)
    column_comments_updated: List[str] = field(default_factory=list)

    @property
    def has_changes(self) -> bool:
        """是否有任何变更"""
        return (
            self.table_comment_updated or
            bool(self.columns_added) or
            bool(self.columns_dropped) or
            bool(self.column_comments_updated)
        )
