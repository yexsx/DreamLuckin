import logging
from typing import List, Dict, Any, Iterable, Set

from exceptions import DBPreloadFailedError
from services.base.lucky_base_db_service_async import LuckyDBBaseServiceAsync

logger = logging.getLogger(__name__)

class ChatRecordDBService(LuckyDBBaseServiceAsync):
    """聊天记录数据库服务"""

    async def check_tables_exist(self, table_names: List[str]) -> Dict[str, int]:
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
        exist_result = await self.execute_query(check_sql, tuple(table_names))
        return {item["name"]: item["seq"] for item in exist_result}


    async def get_chat_records_by_phrase_and_time(
            self,
            table_name: str,
            phrase_condition: str,
            phrase_params: tuple,
            time_condition: str,
            only_self_msg: bool
    ) -> List[Dict[str, Any]]:
        """
        根据关键词配置和时间配置查询指定Msg表的纯文字聊天记录
        :param time_condition: 预构建的时间条件
        :param phrase_params: 预构建的口头禅参数
        :param phrase_condition: 预构建的口头禅条件
        :param table_name: 目标Msg表名（如Msg_123456abc）
        :param only_self_msg: 必填，True=仅查询自己发送的消息（real_sender_id=1），False=仅查询非自己发送的消息（real_sender_id≠1）
        :return: 符合条件的聊天记录列表，每条记录包含：
            local_id、message_content、real_sender_id、create_time 等关键字段
        """

        where_conditions = [
            "local_type = 1",
            "real_sender_id = 1" if only_self_msg else "real_sender_id != 1",
            time_condition,
            phrase_condition
        ]

        base_sql = f"""
                    SELECT local_id, message_content, real_sender_id, create_time
                    FROM {table_name}
                    WHERE {' AND '.join(where_conditions)}
                """
        base_sql = " ".join(base_sql.split())  # 格式化SQL

        # 6. 异步执行查询
        raw_records = await self.execute_query(base_sql, phrase_params)

        return raw_records

    async def get_batch_context_records_by_local_ids(
            self,
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
            all_context_records = await self.execute_query(sql, tuple(valid_context_ids))
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


    async def _test_db_connection(self) -> None:
        """实现异步抽象方法：测试数据库连接，查询sqlite_sequence总条数并输出聊天对象总数"""
        try:
            # 1. 构造查询SQL：统计sqlite_sequence系统表的总条数（代表聊天对象总数）
            test_sql = "SELECT COUNT(*) AS total_chat_objects FROM sqlite_sequence;"

            # 2. 执行异步查询（调用父类异步execute_query方法）
            result = await self.execute_query(test_sql)

            # 3. 解析查询结果（sqlite_sequence无数据时默认总数为0）
            total_chat_objects = result[0]["total_chat_objects"] if result else 0

            # 4. 日志输出统计结果
            logger.info(
                "✅ 聊天记录数据库连接测试通过：总聊天对象数=%d",
                total_chat_objects
            )

        except Exception as e:
            # 5. 连接/查询失败时抛出异常，终止初始化
            raise DBPreloadFailedError(
                f"❌ 聊天记录数据库连接测试失败：{e}"
            ) from e