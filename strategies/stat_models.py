from dataclasses import dataclass
from datetime import datetime
from typing import List


@dataclass
class ContactRecord:
    """映射缓存的单个联系人/群聊缓存对象（替代原嵌套字典）"""
    username: str          # 联系人username
    nickname: str          # 联系人昵称（remark/nick_name）
    type: str      # 类型：friend/group/group_friend/unknown（原type字段，避免关键字冲突）
    type_code: int         # 原始local_type值（1/2/3）


@dataclass
class ChatRecord:
    """聊天记录核心数据对象（对应Msg表关键字段）"""
    local_id: int               # 自增主键
    message_content: str        # 纯文字聊天内容
    real_sender_id: int         # 发送者ID（1=自己，其他=好友/群友）
    create_time: datetime            # 发送时间戳（秒级）
    matched_phrases: List[str]  # 命中的口头禅列表


@dataclass
class BacktrackedRecord:
    """单条带上下文的核心记录"""
    core_record: ChatRecord  # 核心记录（复用已封装的ChatRecord）
    context_records: List[ChatRecord]  # 前2条上下文（不足则为空列表）
    table_name: str  # 所属表名（关联mapping_cache）
    context_count: int = 0  # 实际追溯到的上下文数量（便捷统计）


@dataclass
class TableResult:
    """单表/单类数据的聚合结果（如某张表的处理结果）"""
    table_name: str  # 对应的数据表名（如"msg_202405"）
    contact: ContactRecord  # 该表关联的联系人/群聊信息
    raw_records: List[ChatRecord]  # 该表的原始聊天记录
    backtracked_records: List[BacktrackedRecord]  # 该表中带上下文的记录



@dataclass
class StrategyResult:
    """整体策略处理的最终结果（聚合多个TableResult）"""
    strategy_name: str  # 策略名称（如"日常聊天分析"）
    table_results: List[TableResult]  # 各表的处理结果集合
