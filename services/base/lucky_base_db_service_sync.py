import logging
from abc import abstractmethod, ABCMeta
from typing import Optional, List, Dict, Any
import sqlite3  # 同步库

from exceptions import (
    DBServiceNotPreloadedError,
    DBPreloadFailedError,
    DBConnectionNotInitializedError,
    SQLQueryFailedError
)

logger = logging.getLogger(__name__)

class LuckyDBBaseServiceSync(metaclass=ABCMeta):
    """同步数据库服务类（预加载单例模式），基于 sqlite3"""
    _instance: Optional["LuckyDBBaseServiceSync"] = None
    _db_connection: Optional[sqlite3.Connection] = None

    def __new__(cls):
        raise NotImplementedError("不能直接实例化 LuckyDBBaseServiceSync，请使用 init_instance() 预加载")

    @classmethod
    def init_instance(cls, db_path: str) -> "LuckyDBBaseServiceSync":
        """预加载初始化单例（程序启动时调用一次）"""
        if cls._instance is not None:
            # 已初始化，直接返回实例（避免重复初始化）
            return cls._instance

        # 直接创建实例+初始化连接（无并发风险，因程序启动时仅调用一次）
        cls._instance = super().__new__(cls)
        cls._instance._init_db(db_path)
        cls._instance._test_db_connection()
        return cls._instance

    @classmethod
    def get_instance(cls) -> "LuckyDBBaseServiceSync":
        """获取已预加载的单例（直接返回）"""
        if cls._instance is None:
            raise DBServiceNotPreloadedError()
        return cls._instance

    def _init_db(self, db_path: str):
        """初始化数据库连接（仅调用一次）"""
        try:
            self._db_connection = sqlite3.connect(
                db_path,
                check_same_thread=False
            )
            self._db_connection.execute("PRAGMA foreign_keys = ON")
            logger.debug("✅ 同步数据库预加载连接成功：%s", db_path)
        except Exception as e:
            raise DBPreloadFailedError(f"❌ 同步数据库预加载失败：{e}") from e

    def execute_query(self, sql: str, params: Optional[tuple] = None) -> List[Dict[str, Any]]:
        if not self._db_connection:
            raise DBConnectionNotInitializedError()
        try:
            # 直接获取cursor，不使用with语句管理
            cursor = self._db_connection.execute(sql, params or ())
            columns = [desc[0] for desc in cursor.description]
            result = [dict(zip(columns, row)) for row in cursor.fetchall()]
            cursor.close()  # 手动关闭cursor
            return result
        except Exception as e:
            raise SQLQueryFailedError(sql, params, f"❌ 同步SQL查询失败：{e}") from e

    def close(self):
        if self._db_connection:
            self._db_connection.close()
            self._db_connection = None
            self._instance = None
            logger.info("✅ 同步数据库连接已关闭")

    @classmethod
    def destroy_instance(cls):
        if cls._instance:
            cls._instance.close()

    #测试数据库连接，子类必须实现
    @abstractmethod
    def _test_db_connection(self) -> None:
        """
        抽象方法：测试数据库是否正常连接
        子类需实现具体的测试逻辑（如执行简单查询、检查连接状态等）
        """
        pass  # 暂时不实现，仅定义接口
