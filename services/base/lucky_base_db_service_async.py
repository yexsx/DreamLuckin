import asyncio
import logging
from abc import ABCMeta, abstractmethod
from contextlib import asynccontextmanager
from typing import Optional, List, Dict, Any, Type

import aiosqlite

from exceptions import (
    DBServiceNotPreloadedError,
    DBPreloadFailedError,
    DBPoolExhaustedError, SQLQueryFailedError, DBConnectionNotInitializedError
)


logger = logging.getLogger(__name__)


class LuckyDBPoolServiceAsync(metaclass=ABCMeta):
    """异步数据库连接池服务类，基于 aiosqlite"""
    _pool: Optional[asyncio.Queue] = None
    _db_path: str = ""
    _max_connections: int = 10
    _min_connections: int = 5
    _is_initialized: bool = False

    def __new__(cls):
        raise NotImplementedError("不能直接实例化 LuckyDBPoolServiceAsync，请使用 async 方法 init_pool() 初始化连接池")

    @classmethod
    async def init_pool(
            cls,
            db_path: str,
            max_connections: int = 10,
            min_connections: int = 5
    ) -> None:
        """初始化数据库连接池（程序启动时调用一次）"""
        if cls._is_initialized:
            logger.warning("数据库连接池已初始化，无需重复操作")
            return

        cls._db_path = db_path
        cls._max_connections = max_connections
        cls._min_connections = min_connections
        cls._pool = asyncio.Queue(maxsize=max_connections)

        # 预创建最小连接数
        try:
            for _ in range(min_connections):
                connection = await cls._create_connection()
                await cls._pool.put(connection)
            cls._is_initialized = True
            logger.debug(
                "✅ 异步数据库连接池初始化成功 "
                f"(路径: {db_path}, 最小连接: {min_connections}, 最大连接: {max_connections})"
            )
        except Exception as e:
            raise DBPreloadFailedError(f"❌ 数据库连接池初始化失败: {e}") from e

    @classmethod
    async def _create_connection(cls) -> aiosqlite.Connection:
        """创建新的数据库连接（只读模式优化）"""
        try:
            # 使用 URI 格式指定只读模式（mode=ro）
            db_uri = f"file:{cls._db_path}?mode=ro"
            conn = await aiosqlite.connect(
                db_uri,
                check_same_thread=False,
                uri=True  # 启用 URI 解析
            )
            # 启用外键约束（只读不影响，保留数据完整性检查）
            await conn.execute("PRAGMA foreign_keys = ON")
            # 强制连接只读（防止通过 SQL 语句修改数据）
            await conn.execute("PRAGMA query_only = 1")
            # 优化只读缓存（增大缓存大小，单位为页，默认4096，可根据内存调整）
            await conn.execute("PRAGMA cache_size = -20000")  # -表示KB，此处为20MB
            # 禁用写日志（只读场景无需）
            await conn.execute("PRAGMA synchronous = OFF")
            # 关闭自动整理（只读无需）
            await conn.execute("PRAGMA auto_vacuum = NONE")
            # 禁用事务自动开启（SQLite 默认会为每个操作开启事务）
            conn.isolation_level = None

            # 测试连接有效性
            test_conn = PooledConnection(conn, service_cls=cls)
            await test_conn.test_db_connection()
            return conn
        except Exception as e:
            raise DBPreloadFailedError(f"❌ 创建只读数据库连接失败: {e}") from e

    @classmethod
    async def get_connection(cls) -> "PooledConnection":
        """从连接池获取一个连接（使用后需调用 release_connection 释放）"""
        if not cls._is_initialized or not cls._pool:
            raise DBServiceNotPreloadedError("数据库连接池未初始化")

        try:
            # 尝试从池中获取连接，超时时间3秒
            conn = await asyncio.wait_for(cls._pool.get(), timeout=3)
            # 验证连接有效性
            pooled_conn = PooledConnection(conn, service_cls=cls)
            if await pooled_conn.is_valid():
                return pooled_conn
            # 连接无效则创建新连接替换
            logger.warning("检测到无效连接，将创建新连接替换")
            new_conn = await cls._create_connection()
            return PooledConnection(new_conn, service_cls=cls)
        except asyncio.TimeoutError:
            raise DBPoolExhaustedError(
                cls._max_connections,
                f"连接池已耗尽 (最大连接数: {cls._max_connections})"
            )

    @classmethod
    async def release_connection(cls, conn: "PooledConnection") -> None:
        """将连接释放回连接池"""
        if not cls._is_initialized or not cls._pool:
            raise DBServiceNotPreloadedError("数据库连接池未初始化")

        if not conn or not conn.db_connection:
            logger.warning("尝试释放无效连接，已忽略")
            return

        # 检查连接是否还存活
        if await conn.is_valid() and not cls._pool.full():
            await cls._pool.put(conn.db_connection)
        else:
            # 连接无效或池已满，直接关闭
            await conn.db_connection.close()
            # 若当前连接数低于最小值，补充新连接
            if cls._pool.qsize() < cls._min_connections:
                logger.debug("连接池连接数低于最小值，补充新连接")
                new_conn = await cls._create_connection()
                await cls._pool.put(new_conn)

    @classmethod
    def is_pool_initialized(cls) -> bool:
        """检查连接池是否已初始化"""
        return cls._is_initialized

    @classmethod
    async def close_pool(cls) -> None:
        """关闭连接池所有连接"""
        if not cls._is_initialized or not cls._pool:
            return

        # 标记连接池正在关闭，防止新的连接获取
        cls._is_initialized = False

        # 关闭池中的所有连接
        closed_count = 0
        while True:
            try:
                # 使用 get_nowait 避免阻塞，如果队列为空会抛出 QueueEmpty 异常
                conn = cls._pool.get_nowait()
                try:
                    # 关闭连接，aiosqlite 会自动等待所有操作完成
                    await conn.close()
                    closed_count += 1
                except Exception as e:
                    logger.warning(f"关闭连接时出错: {e}")
            except asyncio.QueueEmpty:
                # 队列已空，退出循环
                break

        # 清空连接池引用
        cls._pool = None
        logger.info(f"✅ 数据库连接池已关闭（共关闭 {closed_count} 个连接）")

    @classmethod
    @asynccontextmanager
    async def acquire_connection(cls):
        """
        异步上下文管理器：自动获取+释放连接（替代try/finally）
        使用方式：async with cls.acquire_connection() as conn:
        """
        # 1. 获取连接（复用原有get_connection逻辑）
        conn = await cls.get_connection()
        try:
            # 2. 上下文管理器返回连接，供业务方法使用
            yield conn
        finally:
            # 3. 无论是否异常，自动释放连接
            await cls.release_connection(conn)

    @classmethod
    @abstractmethod
    async def _test_db_connection(cls, conn: aiosqlite.Connection) -> bool:
        """
        异步抽象方法：测试数据库连接有效性
        返回 True 表示连接有效，False 表示无效
        """
        pass


class PooledConnection:
    """池化连接包装类，用于管理单个连接的操作"""

    def __init__(self, db_connection: aiosqlite.Connection, service_cls: Type[LuckyDBPoolServiceAsync]):
        self.db_connection = db_connection
        self._service_cls = service_cls  # 保存服务类引用（子类实例）

    async def execute_query(self, sql: str, params: Optional[tuple] = None) -> List[Dict[str, Any]]:
        """
        优化后：增强日志+边界处理+统一异常，减少业务层冗余
        """
        # 1. 前置校验：连接是否有效
        if not self.db_connection:
            raise DBConnectionNotInitializedError("池化连接已关闭/未初始化")

        # 2. 格式化SQL（日志友好）
        clean_sql = " ".join(sql.strip().split())
        params = params or ()

        try:

            # 3. 执行SQL（处理cursor.description为空的情况，如SELECT 1无结果）
            async with self.db_connection.execute(clean_sql, params) as cursor:
                # 无返回列的情况（如PRAGMA、空结果）
                if not cursor.description:
                    return []

                # 解析列名+结果
                columns = [desc[0] for desc in cursor.description]
                rows = await cursor.fetchall()
                result = [dict(zip(columns, row)) for row in rows]

            return result

        except Exception as e:
            # 4. 统一封装异常，增强错误信息
            error_msg = f"SQL执行失败 | SQL: {clean_sql} | 参数: {params} | 错误: {str(e)}"
            raise SQLQueryFailedError(sql, params, error_msg) from e

    async def is_valid(self) -> bool:
        """检查连接是否有效（使用 bool 返回值判断）"""
        return await self.test_db_connection()

    async def test_db_connection(self) -> bool:
        """调用具体服务类的测试方法，返回有效性结果"""
        return await self._service_cls._test_db_connection(self.db_connection)