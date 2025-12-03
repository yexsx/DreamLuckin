import aiosqlite
import asyncio
from typing import Optional, List, Dict, Any


class LuckyChatDBService:
    """异步数据库服务类（预加载单例模式+协程），基于 aiosqlite"""
    _instance: Optional["LuckyChatDBService"] = None
    _db_connection: Optional[aiosqlite.Connection] = None

    # _lock = asyncio.Lock()  # 异步锁，确保单例初始化线程安全

    def __new__(cls):
        raise NotImplementedError("不能直接实例化 LuckyChatDBService，请使用 async 方法 init_instance() 预加载")

    @classmethod
    async def init_instance(cls, db_path: str) -> "LuckyChatDBService":
        """预加载初始化单例（程序启动时调用一次）"""
        if cls._instance is not None:
            # 已初始化，直接返回实例（避免重复初始化）
            return cls._instance

        # 直接创建实例+初始化连接（无并发风险，因程序启动时仅调用一次）
        cls._instance = super().__new__(cls)
        await cls._instance._init_db(db_path)
        return cls._instance

    # 懒汉加载
    # @classmethod
    # async def get_instance(cls, db_path: str) -> "LuckyChatDBService":
    #     """异步获取单例实例，初始化数据库连接"""
    #     if cls._instance is None:
    #         async with cls._lock:
    #             # 双重检查锁定，避免并发初始化
    #             if cls._instance is None:
    #                 cls._instance = super().__new__(cls)
    #                 await cls._instance._init_db(db_path)
    #     return cls._instance

    @classmethod
    def get_instance(cls) -> "LuckyChatDBService":
        """获取已预加载的单例（无需await，直接返回）"""
        if cls._instance is None:
            raise RuntimeError("❌ 数据库服务未预加载，请先调用 init_instance(db_path)")
        return cls._instance

    async def _init_db(self, db_path: str):
        """初始化数据库连接（仅调用一次）"""
        try:
            self._db_connection = await aiosqlite.connect(
                db_path,
                check_same_thread=False
            )
            await self._db_connection.execute("PRAGMA foreign_keys = ON")
            print(f"✅ 数据库预加载连接成功：{db_path}")
        except Exception as e:
            raise RuntimeError(f"❌ 数据库预加载失败：{e}") from e

    # 以下方法（execute_query/get_msg_tables/close）完全不变
    async def execute_query(self, sql: str, params: Optional[tuple] = None) -> List[Dict[str, Any]]:
        if not self._db_connection:
            raise RuntimeError("❌ 数据库连接未初始化")
        try:
            async with self._db_connection.execute(sql, params or ()) as cursor:
                columns = [desc[0] for desc in cursor.description]
                return [dict(zip(columns, row)) for row in await cursor.fetchall()]
        except Exception as e:
            raise RuntimeError(f"❌ SQL 查询失败（SQL: {sql}, params: {params}）：{e}") from e

    async def get_msg_tables(self) -> List[str]:
        sql = """
              SELECT name \
              FROM sqlite_master
              WHERE type = 'table' \
                AND name LIKE 'Msg_%'
              ORDER BY name \
              """
        results = await self.execute_query(sql)
        return list({row["name"] for row in results})

    async def close(self):
        if self._db_connection:
            await self._db_connection.close()
            self._db_connection = None
            self._instance = None
            print("✅ 数据库连接已关闭")

    @classmethod
    async def destroy_instance(cls):
        if cls._instance:
            await cls._instance.close()


# 使用示例（配合 main.py 异步流程）
if __name__ == "__main__":
    async def test_db_service():
        # 1. 获取单例实例
        try:
            await LuckyChatDBService.init_instance("D:\programmer\soul\DreamLuckin\Reference\message_0_decrypted.db")
        except RuntimeError as e:
            print(f"❌ 数据库预加载失败：{e}")
            return

        # 3. 后续协程中，直接同步获取实例（无需await）
        db_service = LuckyChatDBService.get_instance()

        # 2. 查询所有 Msg_ 开头的表名
        msg_tables = await db_service.get_msg_tables()
        print(f"查询到 {len(msg_tables)} 个聊天表：{msg_tables[:5]}...")  # 打印前5个表名

        # 3. 执行测试查询（以某个表为例）
        if msg_tables:
            test_table = msg_tables[0]
            sql = f"SELECT local_type, real_sender_id, message_content FROM {test_table} LIMIT 10"
            test_results = await db_service.execute_query(sql)
            print(f"\n{test_table} 表前10条数据：")
            for idx, row in enumerate(test_results, 1):
                print(
                    f"  {idx}. 发送人：{row['real_sender_id']}, 消息类型：{row['local_type']}, 内容：{row['message_content'][:20]}...")

        # 4. 关闭连接
        await db_service.close()


    asyncio.run(test_db_service())