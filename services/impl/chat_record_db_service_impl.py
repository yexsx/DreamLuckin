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
    async def get_batch_context_records_by_local_ids(
            cls,
            table_name: str,
            core_local_id_set: Iterable[int],  # 同表的核心local_id集合/列表
            limit: int = 3  # 每条核心记录追溯前3条
    ) -> Dict[int, List[Dict[str, Any]]]:
        """
        批量查询同表多个核心ID的上下文：
        1. 批量计算所有核心ID的前N条上下文ID
        2. 一次IN查询所有上下文ID，减少DB调用
        3. 按核心ID分组返回上下文（核心ID→对应上下文列表）
        """
        # 1. 去重+转集合（避免重复计算）
        core_ids = set(core_local_id_set)

        # 2. 批量计算所有核心ID的前N个上下文ID（核心逻辑）
        # 例：核心ID={100,200} → 计算100-1=99、100-2=98；200-1=199、200-2=198 → 合并为{98,99,198,199}
        context_id_candidates = []
        for core_id in core_ids:
            # 计算当前核心ID的前limit个ID
            core_context_ids = [core_id - i for i in range(1, limit + 1)]
            context_id_candidates.extend(core_context_ids)

        # 3. 过滤无效ID（>0）+ 去重（避免重复查询同一ID）
        valid_context_ids: Set[int] = set(filter(lambda x: x > 0, context_id_candidates))
        if not valid_context_ids:
            logger.debug(f"📌 无有效上下文ID：表名={table_name} | 核心ID={core_ids}")
            return {core_id: [] for core_id in core_ids}

        # 4. 构建批量查询SQL（IN+主键，精准无冗余）
        placeholders = ", ".join(["?"] * len(valid_context_ids))
        sql = f"""
            SELECT local_id, message_content, real_sender_id, create_time
            FROM {table_name}
            WHERE local_type = 1
              AND local_id IN ({placeholders})
        """

        # 5. 执行查询（复用你的execute_query）
        try:
            # 批量查询所有上下文记录
            async with cls.acquire_connection() as conn:
                all_context_records = await conn.execute_query(sql, tuple(valid_context_ids))
            # 构建「上下文ID→上下文记录」的映射（方便后续分组）
            context_id_map = {rec["local_id"]: rec for rec in all_context_records}

            # 6. 按核心ID分组上下文（核心步骤：匹配每个核心ID对应的上下文）
            core_context_map = {}
            for core_id in core_ids:
                # 重新计算当前核心ID的前limit个ID（保证顺序）
                core_target_ids = [core_id - i for i in range(1, limit + 1)]
                # 过滤有效ID + 从context_id_map中取值 + 按local_id升序
                core_context = []
                for target_id in core_target_ids:
                    if target_id > 0 and target_id in context_id_map:
                        core_context.append(context_id_map[target_id])
                # 按local_id升序（保证上下文顺序正确）
                core_context.sort(key=lambda x: x["local_id"])
                core_context_map[core_id] = core_context

            logger.debug(
                f"📥 批量上下文查询完成：表名={table_name} | 核心ID={core_ids} | "
                f"查询上下文ID={valid_context_ids} | 实际命中={len(all_context_records)}条"
            )
            return core_context_map

        except Exception as e:
            logger.error(
                f"❌ 批量上下文查询失败：表名={table_name} | 核心ID={core_ids} | 错误={str(e)}",
                exc_info=True
            )
            # 异常时返回空上下文，不中断业务
            return {core_id: [] for core_id in core_ids}
