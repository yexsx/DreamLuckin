import datetime
from typing import Tuple

from parser import TimeConfig, PetPhraseConfig


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


    # 生成 match_keywords 的 CASE WHEN SQL 片段 + 参数
    @staticmethod
    def build_match_keywords_sql(pet_phrase_config: PetPhraseConfig) -> Tuple[str, tuple]:
        """
        生成 SELECT 子句中「命中关键词拼接」的 SQL 片段 + 对应参数
        :param pet_phrase_config: 已校验的口头禅配置
        :return: (match_keywords的SQL片段, 参数元组)；无关键词时返回("", ())
        """
        phrases = pet_phrase_config.pet_phrases  # 已过滤空字符串的关键词列表
        match_type = pet_phrase_config.match_type  # contains/exact

        # 1. 生成每个关键词的 CASE WHEN 片段 + 对应参数
        case_fragments = []
        case_params = []
        for phrase in phrases:
            if match_type == "exact":
                # 精确匹配：直接判断等于关键词（参数化）
                fragment = "COALESCE(CASE WHEN message_content = ? THEN ? || ',' ELSE '' END, '')"
                case_params.extend([phrase, phrase])
            else:
                # 模糊匹配：INSTR 判断是否包含关键词（参数化，避免注入）
                fragment = "COALESCE(CASE WHEN INSTR(message_content, ?) > 0 THEN ? || ',' ELSE '' END, '')"
                # 参数1：用于 INSTR 判断的关键词；参数2：用于拼接的关键词（保持一致）
                case_params.extend([phrase, phrase])

            case_fragments.append(fragment)

        # 2. 拼接所有 CASE WHEN 片段 + TRIM 去除最后一个逗号
        case_join_str = " || ".join(case_fragments)
        match_keywords_sql = f"TRIM({case_join_str}, ',') AS matched_phrases"

        return match_keywords_sql, tuple(case_params)