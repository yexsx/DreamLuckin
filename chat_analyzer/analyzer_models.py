import dataclasses
from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional, TypeAlias, Dict
from enum import Enum


# 新增联系人类型枚举（贴合local_type映射）
class ContactType(Enum):
    FRIEND = (1, "friend")          # 好友
    GROUP = (2, "group")            # 群聊
    GROUP_FRIEND = (3, "group_friend")  # 群友（没加过的）
    UNKNOWN = (-1, "unknown")       # 未知类型（兜底）

    @classmethod
    def get_type_by_local_type_id(cls, local_type: int) -> "ContactType":
        """根据local_type获取对应的类型字符串（适配原有逻辑）"""
        for member in cls:
            if member.value[0] == local_type:
                return member
        return cls.UNKNOWN  # 兜底返回unknown

    def __repr__(self):
        return self.value[1]


@dataclass
class ContactRecord:
    """映射缓存的单个联系人/群聊缓存对象（替代原嵌套字典）"""
    username: str          # 联系人username
    nickname: str          # 联系人昵称（remark/nick_name）
    type: ContactType      # 类型：friend/group/group_friend/unknown（原type字段，避免关键字冲突）
    # type_code: int         # 原始local_type值（1/2/3）



# ============ 聊天记录Record类 ============
@dataclass
class ChatRecordCore:
    """聊天记录核心数据对象,用于回溯记录"""
    local_id: int               # 自增主键
    message_content: str        # 纯文字聊天内容
    real_sender_id: int         # 发送者ID（1=自己，其他=好友/群友）
    create_time: int            # 时间戳
    create_time_format: Optional[datetime] = None  # 格式化时间（可选，兼容空值）

@dataclass
class ChatRecordCommon(ChatRecordCore):
    """聊天记录普通对象,对应Msg表（继承核心字段 + 拓展口头禅/上下文字段）"""
    matched_phrases: List[str] = dataclasses.field(default_factory=list)  # 命中的口头禅列表（默认空列表）

@dataclass
class ChatRecordExtend(ChatRecordCommon):
    """聊天记录完全对象（继承核心字段 + 拓展口头禅/上下文字段）"""
    context_front_records: List[ChatRecordCore] = dataclasses.field(default_factory=list)  # 上下文记录（默认空列表）
    context_last_records: List[ChatRecordCore] = dataclasses.field(default_factory=list)  # 上下文记录（默认空列表）
# =========================================


@dataclass
class AnalyzerResult:
    """聚合结果"""
    contact: ContactRecord  # 该表关联的联系人/群聊信息
    chat_records: List[ChatRecordExtend]



# ========== 2. 定义TypeAlias（语义化命名，便于复用） ==========
# 缓存：{键名: 联系人记录}
MappingCacheType: TypeAlias = Dict[str, ContactRecord]
# 缓存：表处理结果 {表名: {local_id: 通用聊天记录}}
ProcessResultType: TypeAlias = Dict[str, Dict[int, ChatRecordCommon]]
# 缓存：回溯表记录结果 {表名: {local_id: 核心聊天记录}}
BacktrackedRecordType: TypeAlias = Dict[str, Dict[int, List[ChatRecordCore]]]



