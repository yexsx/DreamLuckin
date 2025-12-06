import datetime
import logging
from typing import Dict
import os
# 导入结构化配置类
from .config_models import *
from exceptions import (
    MissingRequiredFieldError,
    InvalidTypeError,
    ParseFileNotFoundError,
    InvalidValueError,
    DateFormatError
)

logger = logging.getLogger(__name__)


# ------------------------------
# 配置解析器（核心：校验+转换配置）
# ------------------------------
class ConfigParser:
    """配置解析器：校验合法性 + 转换为SQL可用条件"""


    @staticmethod
    def parse(config_dict: Dict) -> AppConfig:
        """主解析方法：将原始JSON字典转换为结构化AppConfig"""
        db_config = ConfigParser._parse_db_config(config_dict.get("db_config", {}))
        stat_mode = ConfigParser._parse_stat_mode(config_dict.get("stat_mode", {}))
        time_config = ConfigParser._parse_time_config(config_dict.get("time_config", {}))
        pet_phrase = ConfigParser._parse_pet_phrase(config_dict.get("pet_phrase_config", {}))
        filter_cfg = ConfigParser._parse_filter(config_dict.get("filter_config", {}))
        output_cfg = ConfigParser._parse_output_config(config_dict.get("output_config", {}))

        return AppConfig(
            db_config=db_config,
            stat_mode=stat_mode,
            time_config=time_config,
            pet_phrase_config=pet_phrase,
            filter_config=filter_cfg,
            output_config=output_cfg
        )


    @staticmethod
    def _parse_db_config(db_config_dict: Dict) -> DBConfig:
        """校验数据库配置合法性（含路径、文件存在性、并发数完整校验）"""
        # ========== 1. 校验聊天记录DB路径（chat_db_path） ==========
        chat_db_path = db_config_dict.get("chat_db_path")
        # 1.1 非空校验
        if not chat_db_path:
            raise MissingRequiredFieldError("db_config.chat_db_path 为必填项，不能为空（聊天记录数据库路径）")
        # 1.2 类型校验
        if not isinstance(chat_db_path, str):
            raise InvalidTypeError("db_config.chat_db_path 必须是字符串类型（聊天记录数据库文件路径）")
        # 1.3 文件存在性校验
        if not os.path.exists(chat_db_path):
            raise ParseFileNotFoundError(f"聊天记录数据库文件不存在：{chat_db_path}（请检查路径是否正确）")

        # ========== 2. 校验联系人DB路径（contact_db_path） ==========
        contact_db_path = db_config_dict.get("contact_db_path")
        # 2.1 非空校验
        if not contact_db_path:
            raise MissingRequiredFieldError("db_config.contact_db_path 为必填项，不能为空（联系人数据库路径）")
        # 2.2 类型校验
        if not isinstance(contact_db_path, str):
            raise InvalidTypeError("db_config.contact_db_path 必须是字符串类型（联系人数据库文件路径）")
        # 2.3 文件存在性校验
        if not os.path.exists(contact_db_path):
            raise ParseFileNotFoundError(f"联系人数据库文件不存在：{contact_db_path}（请检查路径是否正确）")

        # ========== 3. max_concurrency 校验（原有逻辑不变） ==========
        max_concurrency = db_config_dict.get("max_concurrency", 10)  # 默认值10
        # 3.1 校验类型（必须是整数）
        if not isinstance(max_concurrency, int):
            raise InvalidTypeError("db_config.max_concurrency 必须是整数类型")
        # 3.2 校验取值范围（必须大于0，且不超过20）
        if max_concurrency <= 0:
            raise InvalidValueError("db_config.max_concurrency 必须大于0")
        if max_concurrency > 20:
            raise InvalidValueError("db_config.max_concurrency 最大不能超过20（避免数据库压力过大）")

        return DBConfig(
            chat_db_path=chat_db_path,
            contact_db_path=contact_db_path,
            max_concurrency=max_concurrency
        )



    @staticmethod
    def _parse_stat_mode(stat_mode_dict: Dict) -> StatModeConfig:
        """解析并校验统计模式"""
        # 1. 方法启动日志（INFO级：标记解析开始，进度可视）
        # logger.info("[stat_mode] 开始解析统计模式配置")
        # logger.debug(f"[stat_mode] 原始配置字典：{stat_mode_dict}")

        # 解析mode_type并校验
        mode_type = stat_mode_dict.get("mode_type")
        valid_modes = ["self_all", "self_to_target", "target_to_self"]
        # logger.debug(f"[stat_mode] 解析到mode_type值：{mode_type}（合法值范围：{valid_modes}）")

        if not mode_type or mode_type not in valid_modes:
            # 2. 校验失败日志（ERROR级：记录错误原因，便于排查）
            # logger.error(f"[stat_mode] mode_type校验失败：值为{mode_type}，必须是{valid_modes}中的一种")
            raise InvalidValueError(f"stat_mode.mode_type 必须是 {valid_modes} 中的一种")

        # 校验通过日志（DEBUG级：细节追踪）
        # logger.debug(f"[stat_mode] mode_type={mode_type} 校验通过")

        # 解析target_contact并校验
        target_contact = stat_mode_dict.get("target_contact")
        # logger.debug(f"[stat_mode] 解析到target_contact值：{target_contact}")

        # 后两种模式必须指定target_contact
        if mode_type in ["self_to_target", "target_to_self"] and not target_contact:
            # logger.error(f"[stat_mode] target_contact校验失败：mode_type={mode_type} 时，target_contact不能为空")
            raise MissingRequiredFieldError(f"mode_type={mode_type} 时，必须填写 target_contact")

        # 非必填场景的日志（DEBUG级）
        # if mode_type == "self_all" and not target_contact:
        #     logger.debug(f"[stat_mode] mode_type={mode_type}，target_contact非必填，当前值为None")
        # elif target_contact:
        #     logger.debug(f"[stat_mode] target_contact={target_contact} 校验通过（已自动去除首尾空格）")

        # 构造返回对象
        stat_mode_config = StatModeConfig(
            mode_type=mode_type,
            target_contact=target_contact.strip() if target_contact else None
        )

        # 3. 方法结束日志（INFO级：标记解析完成，进度闭环）
        # logger.info(
        #     f"[stat_mode] 统计模式配置解析完成，最终配置：mode_type={mode_type}，target_contact={stat_mode_config.target_contact}")

        return stat_mode_config




    @staticmethod
    def _parse_time_config(time_config_dict: Dict) -> TimeConfig:
        """解析并校验时间配置，生成SQL可用条件"""
        # 1. 校验维度
        stat_dimension = time_config_dict.get("stat_dimension")
        valid_dimensions = ["day", "week", "month"]
        if not stat_dimension or stat_dimension not in valid_dimensions:
            raise InvalidValueError(f"time_config.stat_dimension 必须是 {valid_dimensions} 中的一种")

        # 2. 校验时间范围类型
        time_range_type = time_config_dict.get("time_range_type")
        valid_range_types = ["recent", "custom"]
        if not time_range_type or time_range_type not in valid_range_types:
            raise InvalidValueError(f"time_config.time_range_type 必须是 {valid_range_types} 中的一种")

        # 3. 校验recent场景参数
        recent_num = time_config_dict.get("recent_num")
        if time_range_type == "recent":
            if recent_num is None:
                recent_num = 7  # 默认最近7个单位
            if not isinstance(recent_num, int) or recent_num < 1:
                raise InvalidValueError("recent_num 必须是≥1的整数")

        # 4. 校验custom场景参数
        custom_start_date = time_config_dict.get("custom_start_date")
        custom_end_date = time_config_dict.get("custom_end_date")
        if time_range_type == "custom":
            if not custom_start_date or not custom_end_date:
                raise MissingRequiredFieldError("time_range_type=custom 时，必须填写 custom_start_date 和 custom_end_date")
            # 校验日期格式
            try:
                datetime.datetime.strptime(custom_start_date, "%Y-%m-%d")
                datetime.datetime.strptime(custom_end_date, "%Y-%m-%d")
                if custom_start_date > custom_end_date:
                    raise InvalidValueError("custom_start_date 不能晚于 custom_end_date")
            except ValueError as e:
                raise DateFormatError(f"日期格式错误（需YYYY-MM-DD）：{e}")

        # 初始化时间配置
        return TimeConfig(
            stat_dimension=stat_dimension,
            time_range_type=time_range_type,
            recent_num=recent_num,
            custom_start_date=custom_start_date,
            custom_end_date=custom_end_date
        )

    @staticmethod
    def _parse_pet_phrase(pet_phrase_dict: Dict) -> PetPhraseConfig:
        """解析口头禅配置（含匹配规则校验）"""
        # 核心列表校验
        pet_phrases = pet_phrase_dict.get("pet_phrases", [])
        if not isinstance(pet_phrases, list) or len(pet_phrases) == 0:
            raise InvalidValueError("pet_phrase_config.pet_phrases 必须是非空列表")

        # 过滤空字符串
        pet_phrases = [phrase.strip() for phrase in pet_phrases if phrase.strip()]
        if len(pet_phrases) == 0:
            raise InvalidValueError("pet_phrase_config.pet_phrases 列表中不能全是空字符串")

        # 布尔型参数校验（默认False/True）
        # case_sensitive = pet_phrase_dict.get("case_sensitive", False)
        # if not isinstance(case_sensitive, bool):
        #     raise ValueError("pet_phrase_config.case_sensitive 必须是布尔值（true/false）")

        whole_word_match = pet_phrase_dict.get("whole_word_match", False)
        if not isinstance(whole_word_match, bool):
            raise InvalidTypeError("pet_phrase_config.whole_word_match 必须是布尔值（true/false）")

        # ignore_emoji_space = pet_phrase_dict.get("ignore_emoji_space", True)
        # if not isinstance(ignore_emoji_space, bool):
        #     raise ValueError("pet_phrase_config.ignore_emoji_space 必须是布尔值（true/false）")

        return PetPhraseConfig(
            pet_phrases=pet_phrases,
            # case_sensitive=case_sensitive,
            whole_word_match=whole_word_match,
            # ignore_emoji_space=ignore_emoji_space
        )

    @staticmethod
    def _parse_filter(filter_dict: Dict) -> FilterConfig:
        """解析过滤配置"""
        # 过滤群聊（默认True）
        filter_group_chat = filter_dict.get("filter_group_chat", True)
        if not isinstance(filter_group_chat, bool):
            raise InvalidTypeError("filter_config.filter_group_chat 必须是布尔值（true/false）")

        # 过滤消息类型（默认过滤语音/图片/视频/文件）
        # filter_msg_types = filter_dict.get("filter_msg_types", ["voice", "image", "video", "file"])
        # valid_msg_types = ["voice", "image", "video", "file", "location", "link"]
        # if not isinstance(filter_msg_types, list):
        #     raise ValueError("filter_config.filter_msg_types 必须是列表")
        # for msg_type in filter_msg_types:
        #     if msg_type not in valid_msg_types:
        #         raise ValueError(f"filter_msg_types 包含不支持的类型：{msg_type}，可选值：{valid_msg_types}")

        # 口头禅最小长度（默认1，≥1）
        # min_phrase_length = filter_dict.get("min_phrase_length", 1)
        # if not isinstance(min_phrase_length, int) or min_phrase_length < 1:
        #     raise InvalidValueError("filter_config.min_phrase_length 必须是 ≥1 的整数")

        return FilterConfig(
            filter_group_chat=filter_group_chat,
            # filter_msg_types=filter_msg_types,
            # min_phrase_length=min_phrase_length
        )

    @staticmethod
    def _parse_output_config(output_config_dict: Dict) -> OutputConfig:
        """校验并解析输出配置（极简版，仅处理display_dimension+export_path）"""
        # 1. 校验 display_dimension
        valid_dimensions = ["year", "month", "day"]
        display_dimension = output_config_dict.get("display_dimension", "month")
        if display_dimension not in valid_dimensions:
            raise InvalidValueError(f"output_config.display_dimension 仅支持 {valid_dimensions}，当前值：{display_dimension}")

        # 2. 校验 export_path（默认值+路径合法性+自动创建）
        export_path = output_config_dict.get("export_path", "configs/output/")
        if not isinstance(export_path, str):
            raise InvalidTypeError("output_config.export_path 必须是字符串类型（文件输出路径）")

        # 自动创建输出目录（不存在则创建）
        if not os.path.exists(export_path):
            os.makedirs(export_path, exist_ok=True)
            logging.info("📁 输出目录不存在，已自动创建：%s",export_path)

        # 3. 返回解析后的 OutputConfig
        return OutputConfig(
            display_dimension=display_dimension,
            export_path=export_path
        )