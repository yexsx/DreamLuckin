import datetime
from typing import Dict
import os
# 导入结构化配置类
from configs.config_models import (
    AppConfig, StatModeConfig, TimeConfig,
    PetPhraseConfig, FilterConfig, OutputConfig
)


# ------------------------------
# 配置解析器（核心：校验+转换配置）
# ------------------------------
class ConfigParser:
    """配置解析器：校验合法性 + 转换为SQL可用条件"""

    @staticmethod
    def parse(config_dict: Dict) -> AppConfig:
        """主解析方法：将原始JSON字典转换为结构化AppConfig"""
        stat_mode = ConfigParser._parse_stat_mode(config_dict.get("stat_mode", {}))
        time_config = ConfigParser._parse_time_config(config_dict.get("time_config", {}))
        pet_phrase = ConfigParser._parse_pet_phrase(config_dict.get("pet_phrase_config", {}))
        filter_cfg = ConfigParser._parse_filter(config_dict.get("filter_config", {}))
        output_cfg = ConfigParser._parse_output(config_dict.get("output_config", {}))

        self_id = config_dict.get("db_connection", {}).get("self_identifier")
        if not self_id:
            raise ValueError("配置缺少 self_identifier")

        return AppConfig(
            stat_mode=stat_mode,
            time_config=time_config,
            pet_phrase_config=pet_phrase,
            filter_config=filter_cfg,
            output_config=output_cfg,
            self_identifier=self_id
        )

    @staticmethod
    def _parse_stat_mode(stat_mode_dict: Dict) -> StatModeConfig:
        """解析并校验统计模式"""
        mode_type = stat_mode_dict.get("mode_type")
        valid_modes = ["self_all", "self_to_target", "target_to_self"]
        if not mode_type or mode_type not in valid_modes:
            raise ValueError(f"stat_mode.mode_type 必须是 {valid_modes} 中的一种")

        target_contact = stat_mode_dict.get("target_contact")
        # 后两种模式必须指定target_contact
        if mode_type in ["self_to_target", "target_to_self"] and not target_contact:
            raise ValueError(f"mode_type={mode_type} 时，必须填写 target_contact")

        return StatModeConfig(
            mode_type=mode_type,
            target_contact=target_contact.strip() if target_contact else None
        )

    @staticmethod
    def _parse_time_config(time_config_dict: Dict) -> TimeConfig:
        """解析并校验时间配置，生成SQL可用条件"""
        # 校验维度
        stat_dimension = time_config_dict.get("stat_dimension")
        valid_dimensions = ["day", "week", "month"]
        if not stat_dimension or stat_dimension not in valid_dimensions:
            raise ValueError(f"time_config.stat_dimension 必须是 {valid_dimensions} 中的一种")

        # 校验时间范围类型
        time_range_type = time_config_dict.get("time_range_type")
        valid_range_types = ["recent", "custom"]
        if not time_range_type or time_range_type not in valid_range_types:
            raise ValueError(f"time_config.time_range_type 必须是 {valid_range_types} 中的一种")

        # 初始化时间配置
        time_config = TimeConfig(
            stat_dimension=stat_dimension,
            time_range_type=time_range_type,
            recent_num=time_config_dict.get("recent_num"),
            custom_start_date=time_config_dict.get("custom_start_date"),
            custom_end_date=time_config_dict.get("custom_end_date")
        )

        # 1. 设置SQL日期格式化字符串（用于GROUP BY分组）
        dimension_format = {
            "day": "%Y-%m-%d",
            "week": "%Y-W%W", # 格式：2025 - W45（2025年第45周）
            "month": "%Y-%m"
        }
        time_config.sql_date_format = dimension_format[stat_dimension]

        # 2. 生成SQL时间查询条件（BETWEEN 起始时间戳 AND 结束时间戳）
        time_config.sql_time_condition = ConfigParser._generate_sql_time_condition(time_config)

        return time_config

    @staticmethod
    def _generate_sql_time_condition(time_config: TimeConfig) -> str:
        """生成SQL中的时间查询条件（假设create_time是unix时间戳）"""
        now = datetime.datetime.now()

        if time_config.time_range_type == "recent":
            # 最近N个时间单位
            recent_num = time_config.recent_num or 7
            if not isinstance(recent_num, int) or recent_num < 1:
                raise ValueError("recent_num 必须是大于等于1的整数")

            # 计算起始时间
            if time_config.stat_dimension == "day":
                start_date = now - datetime.timedelta(days=recent_num)
            elif time_config.stat_dimension == "week":
                start_date = now - datetime.timedelta(weeks=recent_num)
            else:  # month（简化：按30天算，精确可改用dateutil.relativedelta）
                start_date = now - datetime.timedelta(days=recent_num * 30)

            end_date = now
        else:  # custom
            # 自定义日期范围
            start_date_str = time_config.custom_start_date
            end_date_str = time_config.custom_end_date
            if not start_date_str or not end_date_str:
                raise ValueError("time_range_type=custom 时，必须填写 custom_start_date 和 custom_end_date")
            try:
                start_date = datetime.datetime.strptime(start_date_str, "%Y-%m-%d")
                end_date = datetime.datetime.strptime(end_date_str, "%Y-%m-%d")
                if start_date > end_date:
                    raise ValueError("custom_start_date 不能晚于 custom_end_date")
            except ValueError as e:
                raise ValueError(f"日期格式错误（需YYYY-MM-DD）：{e}")

        # 转换为unix时间戳（SQLite中可直接对比）
        start_timestamp = int(start_date.timestamp())
        end_timestamp = int(end_date.timestamp())
        return f"create_time BETWEEN {start_timestamp} AND {end_timestamp}"

    @staticmethod
    def _parse_pet_phrase(pet_phrase_dict: Dict) -> PetPhraseConfig:
        """解析口头禅配置（含匹配规则校验）"""
        # 核心列表校验
        pet_phrases = pet_phrase_dict.get("pet_phrases", [])
        if not isinstance(pet_phrases, list) or len(pet_phrases) == 0:
            raise ValueError("pet_phrase_config.pet_phrases 必须是非空列表")

        # 过滤空字符串
        pet_phrases = [phrase.strip() for phrase in pet_phrases if phrase.strip()]
        if len(pet_phrases) == 0:
            raise ValueError("pet_phrase_config.pet_phrases 列表中不能全是空字符串")

        # 布尔型参数校验（默认False/True）
        case_sensitive = pet_phrase_dict.get("case_sensitive", False)
        if not isinstance(case_sensitive, bool):
            raise ValueError("pet_phrase_config.case_sensitive 必须是布尔值（true/false）")

        whole_word_match = pet_phrase_dict.get("whole_word_match", False)
        if not isinstance(whole_word_match, bool):
            raise ValueError("pet_phrase_config.whole_word_match 必须是布尔值（true/false）")

        ignore_emoji_space = pet_phrase_dict.get("ignore_emoji_space", True)
        if not isinstance(ignore_emoji_space, bool):
            raise ValueError("pet_phrase_config.ignore_emoji_space 必须是布尔值（true/false）")

        return PetPhraseConfig(
            pet_phrases=pet_phrases,
            case_sensitive=case_sensitive,
            whole_word_match=whole_word_match,
            ignore_emoji_space=ignore_emoji_space
        )

    @staticmethod
    def _parse_filter(filter_dict: Dict) -> FilterConfig:
        """解析过滤配置"""
        # 过滤群聊（默认True）
        filter_group_chat = filter_dict.get("filter_group_chat", True)
        if not isinstance(filter_group_chat, bool):
            raise ValueError("filter_config.filter_group_chat 必须是布尔值（true/false）")

        # 过滤消息类型（默认过滤语音/图片/视频/文件）
        filter_msg_types = filter_dict.get("filter_msg_types", ["voice", "image", "video", "file"])
        valid_msg_types = ["voice", "image", "video", "file", "location", "link"]
        if not isinstance(filter_msg_types, list):
            raise ValueError("filter_config.filter_msg_types 必须是列表")
        for msg_type in filter_msg_types:
            if msg_type not in valid_msg_types:
                raise ValueError(f"filter_msg_types 包含不支持的类型：{msg_type}，可选值：{valid_msg_types}")

        # 口头禅最小长度（默认1，≥1）
        min_phrase_length = filter_dict.get("min_phrase_length", 1)
        if not isinstance(min_phrase_length, int) or min_phrase_length < 1:
            raise ValueError("filter_config.min_phrase_length 必须是 ≥1 的整数")

        return FilterConfig(
            filter_group_chat=filter_group_chat,
            filter_msg_types=filter_msg_types,
            min_phrase_length=min_phrase_length
        )

    @staticmethod
    def _parse_output(output_dict: Dict) -> OutputConfig:
        """解析输出配置（含路径校验）"""
        # 输出路径（必填）
        output_path = output_dict.get("output_path")
        if not output_path:
            raise ValueError("output_config.output_path 必须填写（如：/Users/xxx/Desktop/统计结果）")

        # 确保路径存在（不存在则创建）
        output_path = os.path.abspath(output_path)
        if not os.path.exists(output_path):
            try:
                os.makedirs(output_path)
                print(f"⚠️  输出路径不存在，已自动创建：{output_path}")
            except Exception as e:
                raise ValueError(f"创建输出路径失败：{e}")

        # 输出格式（默认json）
        output_format = output_dict.get("output_format", "json")
        valid_formats = ["json", "csv"]
        if output_format not in valid_formats:
            raise ValueError(f"output_config.output_format 必须是 {valid_formats} 中的一种")

        # 显示明细（默认True）
        show_detail = output_dict.get("show_detail_distribution", True)
        if not isinstance(show_detail, bool):
            raise ValueError("output_config.show_detail_distribution 必须是布尔值（true/false）")

        # 排序方式（默认count_desc）
        sort_by = output_dict.get("sort_by", "count_desc")
        valid_sort = ["count_desc", "phrase_asc"]
        if sort_by not in valid_sort:
            raise ValueError(f"output_config.sort_by 必须是 {valid_sort} 中的一种")

        return OutputConfig(
            output_path=output_path,
            output_format=output_format,
            show_detail_distribution=show_detail,
            sort_by=sort_by
        )