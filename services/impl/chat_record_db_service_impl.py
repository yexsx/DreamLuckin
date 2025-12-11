import logging
from typing import List, Dict, Any

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