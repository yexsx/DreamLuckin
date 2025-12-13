import logging
from typing import Optional, List, Dict, Any

from exceptions import DBPreloadFailedError
from ..base.lucky_base_db_service_sync import LuckyDBBaseServiceSync

logger = logging.getLogger(__name__)

class ContactDBService(LuckyDBBaseServiceSync):
    """联系人数据库服务"""

    @classmethod
    def get_contacts(cls,
                     target_values: Optional[List[str]] = None,
                     filter_group_chat: bool = False) -> List[Dict[str, Any]]:
        """
        根据remark/nick_name批量查询联系人（适配列表参数）
        :param target_values: 匹配的remark/nick_name值列表（为空/None则查所有联系人）
        :param filter_group_chat: True=仅查好友(local_type=1)，False=查好友+群聊+群友(local_type IN (1,2,3))
        :return: 联系人列表
        """

        # 1. 动态拼接local_type条件（核心简化点）
        if filter_group_chat:
            local_type_cond = "local_type = 1"  # 过滤群聊：仅好友
        else:
            local_type_cond = "local_type IN (1, 2, 3)"  # 不过滤：好友+群聊+群友

        # 2. 基础SQL
        base_sql = f"SELECT username, local_type, remark, nick_name FROM contact WHERE {local_type_cond}"
        params = tuple()

        # 3. 有效列表则拼接IN条件
        if target_values:
            # 生成IN的占位符（如3个值则为 ?,?,?）
            placeholders = ", ".join(["?"] * len(target_values))
            # 拼接WHERE条件（remark在列表 或 nick_name在列表）
            base_sql += f" AND (remark IN ({placeholders}) OR nick_name IN ({placeholders}))"
            # 参数：target_values 传两次（对应remark和nick_name的IN）
            params = tuple(target_values) * 2

        # 3. 执行查询（同步版，异步版加await）
        return cls.execute_query(base_sql, params)


    @classmethod
    def test_db_connection(cls) -> bool:
        """实现抽象方法：测试数据库连接，统计contact表中好友/群聊数量"""
        try:
            # 1. 构造统计SQL：按local_type分组计数
            test_sql = """
                       SELECT local_type, COUNT(*) AS count
                       FROM contact
                       GROUP BY local_type \
                       """
            # 2. 执行同步查询
            result = cls.execute_query(test_sql)

            # 3. 初始化统计结果
            friend_count = 0  # local_type=1 好友
            group_count = 0  # local_type=2 群聊

            # 4. 解析查询结果
            for row in result:
                local_type = row.get("local_type")
                count = row.get("count", 0)
                if local_type == 1:
                    friend_count = count
                elif local_type == 2:
                    group_count = count

            # 5. 日志输出统计结果
            logger.debug(
                "✅ 联系人数据库连接测试通过：好友数=%d，群聊数=%d",
                friend_count, group_count
            )

            return len(result) > 0

        except Exception as e:
            # 6. 连接/查询失败时抛出异常，终止初始化
            raise DBPreloadFailedError(
                f"❌ 联系人数据库连接测试失败：{e}"
            ) from e

