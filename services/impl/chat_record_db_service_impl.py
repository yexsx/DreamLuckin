import logging
from typing import List, Dict, Any, Iterable, Set

import aiosqlite

from exceptions import DBPreloadFailedError
from ..base.lucky_base_db_service_async import LuckyDBPoolServiceAsync

logger = logging.getLogger(__name__)

class ChatRecordDBService(LuckyDBPoolServiceAsync):
    """聊天记录数据库服务"""

    @classmethod
    async def _test_db_connection(cls, conn: aiosqlite.Connection) -> bool:
        """优化：仅检查sqlite_sequence表是否存在记录（非空则返回True）"""
        try:
            # 优化SQL：仅查询是否存在记录，找到1条即返回（避免全表扫描）
            test_sql = "SELECT 1 FROM sqlite_sequence LIMIT 1;"

            # 使用原始连接执行查询（注意：父类定义参数为aiosqlite.Connection，而非PooledConnection）
            async with conn.execute(test_sql) as cursor:
                # 直接获取第一条结果，存在则表非空
                result = await cursor.fetchone()

                if result:
                    logger.debug("✅ 聊天记录数据库连接测试通过,sqlite_sequence表非空")
                    return True
                else:
                    logger.debug("ℹ️ 聊天记录数据库连接测试通过,但sqlite_sequence表为空")
                    return False

        except Exception as e:
            raise DBPreloadFailedError(
                f"❌ 聊天记录数据库连接测试失败：{e}"
            ) from e


    @classmethod
    async def check_tables_exist(cls, table_names: List[str]) -> Dict[str, int]:
        """
        批量校验表是否存在于sqlite_sequence，并返回存在表的seq值（总记录数）
        :param table_names: 待校验表名列表
        :return: 存在的表名→对应seq值的字典（不存在的表名不包含在内）
        """

        # 生成IN的占位符（如3个表名则为 ?,?,?）
        placeholders = ", ".join(["?"] * len(table_names))
        check_sql = f"SELECT name, seq FROM sqlite_sequence WHERE name IN ({placeholders}) ORDER BY seq DESC "

        # check_sql += f" AND name != 'Msg_5a7e0f7f14697c580c0702e21853c051'"

        # 执行查询并转换为{表名: seq}的字典
        # 核心：用上下文管理器自动管理连接（替代try/finally）
        async with cls.acquire_connection() as conn:
            # 直接调用execute_query，异常由execute_query统一抛出
            exist_result = await conn.execute_query(check_sql, tuple(table_names))
        return {item["name"]: item["seq"] for item in exist_result}

    @classmethod
    async def get_chat_records_by_phrase_and_time(
            cls,
            table_name: str,
            phrase_condition: str,
            phrase_params: tuple,
            match_keywords_sql: str,
            match_params: tuple,
            time_condition: str,
            only_self_msg: bool
    ) -> List[Dict[str, Any]]:
        """
        根据关键词配置和时间配置查询指定Msg表的纯文字聊天记录
        :param time_condition: 预构建的时间条件
        :param phrase_params: 预构建的口头禅参数
        :param phrase_condition: 预构建的口头禅条件
        :param match_keywords_sql: 可选，命中关键词拼接的SQL片段（来自build_match_keywords_sql）
        :param match_params: 可选，命中关键词的参数元组（来自build_match_keywords_sql）
        :param table_name: 目标Msg表名（如Msg_123456abc）
        :param only_self_msg: 必填，True=仅查询自己发送的消息（real_sender_id=1），False=仅查询非自己发送的消息（real_sender_id≠1）
        :return: 符合条件的聊天记录列表，每条记录包含：
            local_id、message_content、real_sender_id、create_time 等关键字段
            传入match_keywords_sql则额外包含match_keywords字段
        """

        # 1. 构建SELECT字段（动态追加match_keywords）
        select_fields = ["local_id", "message_content", "real_sender_id", "create_time", match_keywords_sql]
        select_sql = ", ".join(select_fields)

        # 2. 构建WHERE条件（过滤空字符串，避免AND连接空条件导致语法错误）
        where_conditions = [
            "local_type = 1",
            "real_sender_id = 1" if only_self_msg else "real_sender_id != 1",
            time_condition,
            phrase_condition
        ]
        # 过滤空条件（比如phrase_condition为空时，移除该元素）
        where_conditions = [cond for cond in where_conditions if cond.strip()]
        where_sql = " AND ".join(where_conditions)

        # 3. 拼接完整SQL（格式化，去除多余空格）
        base_sql = f"""
                    SELECT {select_sql}
                    FROM {table_name}
                    WHERE {where_sql}
                """
        base_sql = " ".join(base_sql.split())  # 格式化SQL，去除换行/多余空格

        # 4. 合并参数（口头禅参数 + 命中关键词参数）
        all_params = match_params + phrase_params

        # 5. 异步执行查询
        async with cls.acquire_connection() as conn:
            raw_records = await conn.execute_query(base_sql, all_params)

        return raw_records


    @classmethod
    async def get_batch_records_by_local_ids(
            cls,
            table_name: str,
            local_id_set: List[int]
    ) -> List[Dict[str, Any]]:
        """
        批量查询聊天记录根据local_id_set
        """

        # 修复1：处理空ID列表，避免SQL语法错误（IN () 非法）
        if not local_id_set:
            return []

        # 修复2：去重ID，避免重复占位符和冗余查询
        unique_local_ids = list(set(local_id_set))
        # 构建批量查询SQL（IN+主键，精准无冗余）
        placeholders = ", ".join(["?"] * len(unique_local_ids))
        sql = f"""
                SELECT local_id, real_sender_id, create_time,
                    CASE 
                        WHEN local_type = 1 THEN message_content 
                        ELSE '[非文本消息类型暂且无法展示]' 
                    END AS message_content
                FROM {table_name}
                WHERE local_id IN ({placeholders})
            """

        async with cls.acquire_connection() as conn:
            # 修复3：列表转元组（execute_query要求params是tuple类型）
            raw_records = await conn.execute_query(sql, tuple(unique_local_ids))

        return raw_records

