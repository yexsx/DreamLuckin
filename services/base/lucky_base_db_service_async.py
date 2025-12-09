import logging
from abc import abstractmethod, ABCMeta
from typing import Optional, List, Dict, Any

import aiosqlite

from exceptions import (
    DBServiceNotPreloadedError,
    DBPreloadFailedError,
    DBConnectionNotInitializedError,
    SQLQueryFailedError
)

logger = logging.getLogger(__name__)

class LuckyDBBaseServiceAsync(metaclass=ABCMeta):
    """异步数据库服务类（预加载单例模式+协程），基于 aiosqlite"""
    _instance: Optional["LuckyDBBaseServiceAsync"] = None
    _db_connection: Optional[aiosqlite.Connection] = None

    def __new__(cls):
        raise NotImplementedError("不能直接实例化 LuckyDBBaseServiceAsync，请使用 async 方法 init_instance() 预加载")

    @classmethod
    async def init_instance(cls, db_path: str) -> "LuckyDBBaseServiceAsync":
        """预加载初始化单例（程序启动时调用一次）"""
        if cls._instance is not None:
            # 已初始化，直接返回实例（避免重复初始化）
            return cls._instance

        # 直接创建实例+初始化连接（无并发风险，因程序启动时仅调用一次）
        cls._instance = super().__new__(cls)
        await cls._instance._init_db(db_path)
        await cls._instance._test_db_connection()
        return cls._instance

    @classmethod
    def get_instance(cls) -> "LuckyDBBaseServiceAsync":
        """获取已预加载的单例（无需await，直接返回）"""
        if cls._instance is None:
            raise DBServiceNotPreloadedError()
        return cls._instance

    async def _init_db(self, db_path: str):
        """初始化数据库连接（仅调用一次）"""
        try:
            self._db_connection = await aiosqlite.connect(
                db_path,
                check_same_thread=False
            )
            await self._db_connection.execute("PRAGMA foreign_keys = ON")
            logger.debug("✅ 异步数据库预加载连接成功：%s", db_path)
        except Exception as e:
            raise DBPreloadFailedError(f"❌ 异步数据库预加载失败：{e}") from e

    # 以下方法（execute_query/get_msg_tables/close）完全不变
    async def execute_query(self, sql: str, params: Optional[tuple] = None) -> List[Dict[str, Any]]:
        if not self._db_connection:
            raise DBConnectionNotInitializedError()
        try:
            async with self._db_connection.execute(sql, params or ()) as cursor:
                columns = [desc[0] for desc in cursor.description]
                return [dict(zip(columns, row)) for row in await cursor.fetchall()]
        except Exception as e:
            raise SQLQueryFailedError(sql, params, f"❌ SQL 查询失败：{e}") from e

    async def close(self):
        if self._db_connection:
            await self._db_connection.close()
            self._db_connection = None
            self._instance = None
            logger.info("✅ 异步数据库连接已关闭")

    @classmethod
    async def destroy_instance(cls):
        if cls._instance:
            await cls._instance.close()

    # 定义异步抽象方法（测试数据库连接，子类必须实现）
    @abstractmethod
    async def _test_db_connection(self) -> None:
        """
        异步抽象方法：测试数据库是否正常连接
        子类需实现具体的异步测试逻辑（如执行简单查询、检查连接状态等）
        """
        pass  # 暂时不实现，仅定义接口