"""
Pytucky 后端配置选项
"""
from dataclasses import dataclass, field
from typing import List, Optional
from typing_extensions import Literal


@dataclass
class PytuckBackendOptions:
    """PTK7 后端配置选项

    承载 PTK7 读写所需的加密配置。
    - encryption: 可选的级别标识，允许 'low'|'medium'|'high' 或 None
    - password: 可选的原始密码字符串，用于派生密钥（空表示未配置）
    """
    encryption: Optional["Literal['low','medium','high']"] = None
    password: Optional[str] = None


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
