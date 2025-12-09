import logging
from typing import List, Dict

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