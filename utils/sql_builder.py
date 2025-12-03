import re
from typing import List

from configs import TimeConfig, PetPhraseConfig
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
    def build_date_format(time_config: TimeConfig) -> str:
        """生成SQL分组用的日期格式字符串（如 '%Y-%m-%d'）"""
        dimension_format = {
            "day": "%Y-%m-%d",
            "week": "%Y-W%W",
            "month": "%Y-%m"
        }
        return dimension_format[time_config.stat_dimension]


    @staticmethod
    def build_pet_phrase_condition(pet_config: PetPhraseConfig) -> str:
        """
        根据PetPhraseConfig拼接SQL条件（仅处理pet_phrases和whole_word_match）
        前提：pet_config已通过ConfigParser校验（pet_phrases非空、whole_word_match为布尔值）
        :return: 拼接后的SQL条件字符串（如 "(content LIKE '%哈哈%' OR content LIKE '%绝了%')"）
        """
        # 1. 提取并处理口头禅列表（兜底过滤空字符串，ConfigParser已校验，此处防极端情况）
        phrases: List[str] = [p.strip() for p in pet_config.pet_phrases if p.strip()]
        if not phrases:
            raise ValueError("pet_phrases 不能为空列表（已通过ConfigParser校验，此为兜底异常）")

        # 2. SQL特殊字符转义（处理 '%' '_' 等LIKE/REGEXP关键字，避免SQL语法错误）
        escaped_phrases = [SQLBuilder._escape_sql_special_chars(phrase) for phrase in phrases]

        # 3. 按whole_word_match拼接不同SQL条件
        if not pet_config.whole_word_match:
            # 场景1：包含匹配 → LIKE + OR
            like_clauses = [f"message_content LIKE '%{phrase}%'" for phrase in escaped_phrases]
            return f"({') OR ('.join(like_clauses)})"
        else:
            # 场景2：全词匹配 → REGEXP（依赖SQLite REGEXP扩展，大部分Python驱动已支持）
            # 正则表达式：\b 匹配单词边界，| 分隔多个口头禅，整体用括号包裹
            regex_pattern = r"\b(" + "|".join(escaped_phrases) + r")\b"
            # 转义正则中的特殊字符（如 '.' '*' 等），确保正则语法正确
            escaped_regex = re.escape(regex_pattern)
            # 修复re.escape转义\b的问题（re.escape会把\b转义为\\b，需还原为\b）
            escaped_regex = escaped_regex.replace(r"\\b", r"\b")
            return f"(content REGEXP '{escaped_regex}')"

    @staticmethod
    def _escape_sql_special_chars(phrase: str) -> str:
        """
        转义SQL LIKE/REGEXP中的特殊字符（避免破坏SQL语法）
        需转义的字符：%（匹配任意字符）、_（匹配单个字符）、\（转义符）
        """
        # 定义需要转义的SQL特殊字符映射
        sql_special_chars = {
            '%': r'\%',
            '_': r'\_',
            '\\': r'\\'
        }
        # 替换特殊字符
        for char, escaped_char in sql_special_chars.items():
            phrase = phrase.replace(char, escaped_char)
        return phrase