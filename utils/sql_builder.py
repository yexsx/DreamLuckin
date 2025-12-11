import re
from typing import List, Tuple

from parser import TimeConfig, PetPhraseConfig
import datetime


class SQLBuilder:
    """SQL生成工具类：仅接收合法配置，生成SQL片段，不做任何校验"""

    @staticmethod
    def build_time_condition(time_config: TimeConfig) -> str:
        """
        根据已校验的TimeConfig生成时间过滤SQL片段
        前提：TimeConfig已通过ConfigParser校验，参数均合法
        """
        now = datetime.datetime.now()
        end_date = now
        start_date = None

        # 1. 处理recent场景（无需校验，ConfigParser已确保recent_num≥1）
        if time_config.time_range_type == "recent":
            recent_num = time_config.recent_num or 7
            match time_config.stat_dimension:
                case "day":
                    start_date = now - datetime.timedelta(days=recent_num)
                case "week":
                    start_date = now - datetime.timedelta(weeks=recent_num)
                case "month":
                    start_date = now - datetime.timedelta(days=recent_num * 30)

        # 2. 处理custom场景（无需校验，ConfigParser已确保日期合法）
        elif time_config.time_range_type == "custom":
            start_date = datetime.datetime.strptime(time_config.custom_start_date, "%Y-%m-%d")
            end_date = datetime.datetime.strptime(time_config.custom_end_date, "%Y-%m-%d")

        # 3. 转换为unix时间戳，拼接SQL
        start_timestamp = int(start_date.timestamp())
        end_timestamp = int(end_date.timestamp())
        return f"create_time BETWEEN {start_timestamp} AND {end_timestamp}"


    @staticmethod
    def build_phrase_condition(pet_phrase_config: PetPhraseConfig) -> Tuple[str, tuple]:
        """
        静态方法：根据已解析的PetPhraseConfig生成关键词匹配SQL条件+参数
        :param pet_phrase_config: 已校验的口头禅配置（含phrases+match_type）
        :return: (关键词匹配条件字符串, 参数元组)；无关键词时返回("", ())
        """
        phrases = pet_phrase_config.pet_phrases  # 已过滤空字符串的关键词列表
        match_type = pet_phrase_config.match_type  # contains/exact

        phrase_conditions = []
        phrase_params = []

        # 遍历关键词生成匹配条件
        for phrase in phrases:
            if match_type == "contains":
                phrase_conditions.append("message_content LIKE ?")
                phrase_params.append(f"%{phrase}%")
            elif match_type == "exact":
                phrase_conditions.append("message_content = ?")
                phrase_params.append(phrase)

        # 多关键词用OR连接，包裹括号避免优先级问题
        condition_str = f"({' OR '.join(phrase_conditions)})"
        return condition_str, tuple(phrase_params)