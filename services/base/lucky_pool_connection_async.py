# from typing import Optional, List, Dict, Any, Type
#
# import aiosqlite
#
# from exceptions import (
#     DBConnectionNotInitializedError,
#     SQLQueryFailedError
# )
# from services.base.lucky_base_db_service_async import LuckyDBPoolServiceAsync
#
#
# class PooledConnection:
#     """池化连接包装类，用于管理单个连接的操作"""
#
#     def __init__(self, db_connection: aiosqlite.Connection, service_cls: Type[LuckyDBPoolServiceAsync]):
#         self.db_connection = db_connection
#         self._service_cls = service_cls  # 保存服务类引用（子类实例）
#
#     async def execute_query(self, sql: str, params: Optional[tuple] = None) -> List[Dict[str, Any]]:
#         """
#         优化后：增强日志+边界处理+统一异常，减少业务层冗余
#         """
#         # 1. 前置校验：连接是否有效
#         if not self.db_connection:
#             raise DBConnectionNotInitializedError("池化连接已关闭/未初始化")
#
#         # 2. 格式化SQL（日志友好）
#         clean_sql = " ".join(sql.strip().split())
#         params = params or ()
#
#         try:
#
#             # 3. 执行SQL（处理cursor.description为空的情况，如SELECT 1无结果）
#             async with self.db_connection.execute(clean_sql, params) as cursor:
#                 # 无返回列的情况（如PRAGMA、空结果）
#                 if not cursor.description:
#                     return []
#
#                 # 解析列名+结果
#                 columns = [desc[0] for desc in cursor.description]
#                 rows = await cursor.fetchall()
#                 result = [dict(zip(columns, row)) for row in rows]
#
#             return result
#
#         except Exception as e:
#             # 4. 统一封装异常，增强错误信息
#             error_msg = f"SQL执行失败 | SQL: {clean_sql} | 参数: {params} | 错误: {str(e)}"
#             raise SQLQueryFailedError(sql, params, error_msg) from e
#
#     # async def execute_query(self, sql: str, params: Optional[tuple] = None) -> List[Dict[str, Any]]:
#     #     """执行SQL查询并返回结果"""
#     #     if not self.db_connection:
#     #         raise DBConnectionNotInitializedError()
#     #
#     #     try:
#     #         async with self.db_connection.execute(sql, params or ()) as cursor:
#     #             columns = [desc[0] for desc in cursor.description]
#     #             return [dict(zip(columns, row)) for row in await cursor.fetchall()]
#     #     except Exception as e:
#     #         raise SQLQueryFailedError(sql, params, f"❌ SQL 查询失败：{e}") from e
#
#     async def is_valid(self) -> bool:
#         """检查连接是否有效（使用 bool 返回值判断）"""
#         return await self.test_db_connection()
#
#     async def test_db_connection(self) -> bool:
#         """调用具体服务类的测试方法，返回有效性结果"""
#         return await self._service_cls._test_db_connection(self.db_connection)