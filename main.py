import asyncio
import logging
import sys

# ====================== 1. 全局日志配置 ======================
from log.log_config import setup_global_logging

setup_global_logging()
logger = logging.getLogger(__name__)

# ====================== 2. 导入自定义模块 ======================
# 配置解析相关
from parser import AppConfig, ConfigParser  # 统一的应用配置模型（包含所有子配置）
# 数据库服务相关
from services import ContactDBService,ChatRecordDBService
# 异常相关
from exceptions import ParseBaseError, LuckyChatDBError, AnalyzerBaseException
# # 策略工厂 + 接口
from chat_analyzer import ChatRecordAnalyzer
# 导入配置加载门面类
from utils import ConfigLoader

# ====================== 3. 核心异步主函数 ======================
async def main():
    """程序主入口：读取配置 → 统一解析 → 初始化数据库 → 工厂创建策略 → 执行策略"""
    logger.info("===== 聊天记录统计程序启动 =====")

    try:
        # -------------------------- 步骤1：读取+统一解析配置 --------------------------
        logger.info("【步骤1/4】开始读取并解析配置文件")
        # 调用门面类加载配置（默认路径：./configs/config.json；如需自定义可传参：ConfigLoader.load_config("D:/xxx/config.json")）
        config_dict = ConfigLoader.load_config()
        # 统一调用ConfigParser的parse方法（核心修正：替代逐个调用）
        app_config: AppConfig = ConfigParser.parse(config_dict)
        logger.info("✅ 所有配置统一解析完成")

        # -------------------------- 步骤2：初始化数据库 --------------------------
        logger.info("【步骤2/4】开始初始化数据库服务")
        # 初始化聊天记录DB对象池
        await ChatRecordDBService.init_pool(
            db_path=app_config.db_config.chat_db_path,  # 替换为实际数据库路径
            max_connections=10,
            min_connections=3
        )
        logger.info(f"✅ 聊天记录异步数据对象池初始化成功（路径：{app_config.db_config.chat_db_path}）")

        # 初始化联系人DB单例
        ContactDBService.init_instance(app_config.db_config.contact_db_path)
        logger.info(f"✅ 联系人同步单例数据库初始化成功（路径：{app_config.db_config.contact_db_path}）")
        logger.info("【步骤2/4】数据库服务初始化完成")

        # -------------------------- 步骤3：工厂创建策略实例 --------------------------
        logger.info("【步骤3/4】开始创建聊天记录分析实例")

        analyzer = ChatRecordAnalyzer(app_config=app_config)

        logger.info(f"✅ 成功创建聊天记录分析实例")
        # -------------------------- 步骤4：执行策略 --------------------------
        logger.info("【步骤4/4】开始执行统计策略")
        analyzer_result = await analyzer.run()  # 异步执行策略
        logger.info("✅ 统计策略执行完成")
        logger.info(f"【最终统计结果】\n{analyzer_result}")

        # -------------------------- 可选：导出结果 --------------------------
        # 按output_config导出结果（示例）
        # export_path = app_config.output_config.export_path
        # with open(export_path, "w", encoding="utf-8") as f:
        #     json.dump(stat_result, f, ensure_ascii=False, indent=2)
        # logger.info(f"✅ 统计结果已导出至：{export_path}")

    except KeyboardInterrupt:
        logger.info("⚠️ 程序被手动终止")
        ContactDBService.close()  # 释放资源
        sys.exit(1)
    except ParseBaseError as e:
        logger.error(f"【配置解析/读取失败】{e}", exc_info=True)
        sys.exit(1)
    except LuckyChatDBError as e:
        logger.error(f"【数据库初始化失败】{e}", exc_info=True)
        sys.exit(1)
    except AnalyzerBaseException as e:
        logger.error(f"【统计策略执行失败】{e}", exc_info=True)
        sys.exit(1)
    except Exception as e:
        logger.error(f"【程序执行异常】未知错误：{e}", exc_info=True)
        sys.exit(1)
    finally:
        # 关闭数据库连接
        if ContactDBService.get_instance():
            ContactDBService.get_instance().close()
        if await ChatRecordDBService.get_connection():
            await ChatRecordDBService.close_pool()
        logger.info("===== 聊天记录统计程序结束 =====")


# ====================== 4. 程序入口 ======================
if __name__ == "__main__":
    asyncio.run(main())



# TODO
# √ 1.数据库启动检验是否正确，有concat或message
# √ 2.策略实现类封装sql方法到服务类
# √ 3.main方法读取文件封装工具类
# √ 4.实现策略类自定义业务异常
# √ 5.main的日志方法优化
# √ 6.sql_builder逻辑实现
# × 7.selfToTarget子类实现
# √ 8.策略接口变成具体实现类
# √ 9.待处理列表从高到低排
# o 10.业务类必要方法实现协程
# × 11.未过滤群聊的精确搜索
# √ 12.联系人类型枚举
# √ 13.实现_backtrack_context
# √ 14.create_time时间戳转换
# √ 15.完成群聊wxid翻译
# √ 16.重构table_chat_records结构,local_id为key的字典
# √ 17.聊天记录数据库已改成对象池模式
# o 18.结果分析转换成json
# o 19.对象池导致的程序终止需要两次
# o 20.对象输出方法简化封装好
# o 21.整理utils文件夹

