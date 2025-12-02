from dataclasses import dataclass
from typing import Optional, List, Literal


# ------------------------------
# 统计模式配置
# ------------------------------
@dataclass
class StatModeConfig:
    mode_type: str  # self_all / self_to_target / target_to_self
    target_contact: Optional[str] = None


# ------------------------------
# 时间配置
# ------------------------------
@dataclass
class TimeConfig:
    stat_dimension: str  # day / week / month
    time_range_type: str  # recent / custom
    recent_num: Optional[int] = None
    custom_start_date: Optional[str] = None
    custom_end_date: Optional[str] = None
    # 解析后的值（供SQL查询用）
    sql_date_format: str = ""  # 日期格式化字符串（如 '%Y-%m-%d' 对应day维度）
    sql_time_condition: str = ""  # 时间查询条件（如 "create_time BETWEEN xxx AND xxx"）


# ------------------------------
# 口头禅配置
# ------------------------------
@dataclass
class PetPhraseConfig:
    pet_phrases: List[str]
    case_sensitive: bool = False  # 是否大小写敏感
    whole_word_match: bool = False  # 是否全词匹配
    ignore_emoji_space: bool = True  # 是否忽略表情和空格


# ------------------------------
# 过滤配置
# ------------------------------
@dataclass
class FilterConfig:
    filter_group_chat: bool = True  # 是否过滤群聊
    filter_msg_types: List[Literal["voice", "image", "video", "file", "location", "link"]] = None  # 过滤的消息类型
    min_phrase_length: int = 1  # 口头禅最小长度


# ------------------------------
# 输出配置
# ------------------------------
@dataclass
class OutputConfig:
    output_path: str  # 输出路径
    output_format: Literal["json", "csv", "txt"] = "json"  # 输出格式
    show_detail_distribution: bool = True  # 是否显示维度明细
    sort_by: Literal["count_desc", "phrase_asc"] = "count_desc"  # 排序方式


# ------------------------------
# 应用总配置（整合所有子配置）
# ------------------------------
@dataclass
class AppConfig:
    stat_mode: StatModeConfig
    time_config: TimeConfig
    pet_phrase_config: PetPhraseConfig
    filter_config: FilterConfig
    output_config: OutputConfig
    self_identifier: str  # 从db_connection提取