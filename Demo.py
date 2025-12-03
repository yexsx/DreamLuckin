import asyncio

from services.lucky_chat_db_service import LuckyChatDBService

if __name__ == '__main__':

    async def main():
        # 1. 加载配置→校验配置（已有逻辑）
        # ...（省略配置加载代码）

        # 2. 预加载DB单例（程序启动时仅调用一次）
        try:
            await LuckyChatDBService.init_instance("D:\programmer\soul\DreamLuckin\Reference\message_0_decrypted.db")
        except RuntimeError as e:
            print(f"❌ 数据库预加载失败：{e}")
            return

        # 3. 后续协程中，直接同步获取实例（无需await）
        db_service = LuckyChatDBService.get_instance()

        # 4. 查询所有聊天表名（正常异步调用）
        try:
            msg_tables = await db_service.get_msg_tables()
            print(f"✅ 查询到 {len(msg_tables)} 个聊天表")
        except Exception as e:
            print(f"❌ 查询聊天表失败：{e}")
            await db_service.close()
            return

        # 5. 协程并行处理表（每个协程中直接 get_instance() 获取）
        # ...（后续协程逻辑）

        # 6. 程序结束关闭连接
        await db_service.close()


    asyncio.run(main())