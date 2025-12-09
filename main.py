import asyncio
import logging
import sys

# ====================== 1. 全局日志配置 ======================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("./log/chat_stat.log", encoding="utf-8")
    ]
)
logger = logging.getLogger(__name__)

# ====================== 2. 导入自定义模块 ======================
# 配置解析相关
from parser import AppConfig, ConfigParser  # 统一的应用配置模型（包含所有子配置）
# 数据库服务相关
from services import ContactDBService,ChatRecordDBService
# 异常相关
from exceptions import ParseBaseError, LuckyChatDBError, StatBaseException
# # 策略工厂 + 接口
from strategies import StatStrategyFactory
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
        # 初始化聊天记录DB
        await ChatRecordDBService.init_instance(app_config.db_config.chat_db_path)
        chat_db_service = ChatRecordDBService.get_instance()
        logger.info(f"✅ 聊天记录异步数据库初始化成功（路径：{app_config.db_config.chat_db_path}）")

        # 初始化联系人DB
        ContactDBService.init_instance(app_config.db_config.contact_db_path)
        contact_db_service = ContactDBService.get_instance()
        logger.info(f"✅ 联系人同步数据库初始化成功（路径：{app_config.db_config.contact_db_path}）")
        logger.info("【步骤2/4】数据库服务初始化完成")

        # -------------------------- 步骤3：工厂创建策略实例 --------------------------
        logger.info("【步骤3/4】开始创建统计策略实例")
        # 工厂方法根据mode_type创建对应策略
        strategy = StatStrategyFactory.create_strategy(
            mode_type=app_config.stat_mode.mode_type,
            chat_db_service=chat_db_service,
            contact_db_service=contact_db_service,
            app_config=app_config
        )
        logger.info(f"✅ 成功创建[{app_config.stat_mode.mode_type}]策略实例")

        # -------------------------- 步骤4：执行策略 --------------------------
        logger.info("【步骤4/4】开始执行统计策略")
        stat_result = await strategy.run()  # 异步执行策略
        logger.info("✅ 统计策略执行完成")
        logger.info(f"【最终统计结果】\n{stat_result}")

        # -------------------------- 可选：导出结果 --------------------------
        # 按output_config导出结果（示例）
        # export_path = app_config.output_config.export_path
        # with open(export_path, "w", encoding="utf-8") as f:
        #     json.dump(stat_result, f, ensure_ascii=False, indent=2)
        # logger.info(f"✅ 统计结果已导出至：{export_path}")

    except ParseBaseError as e:
        logger.error(f"【配置解析/读取失败】{e}", exc_info=True)
        sys.exit(1)
    except LuckyChatDBError as e:
        logger.error(f"【数据库初始化失败】{e}", exc_info=True)
        sys.exit(1)
    except StatBaseException as e:
        logger.error(f"【统计策略执行失败】{e}", exc_info=True)
        sys.exit(1)
    except Exception as e:
        logger.error(f"【程序执行异常】未知错误：{e}", exc_info=True)
        sys.exit(1)
    finally:
        # 关闭数据库连接
        if ContactDBService.get_instance():
            ContactDBService.get_instance().close()
        if ChatRecordDBService.get_instance():
            await ChatRecordDBService.get_instance().close()
        logger.info("===== 聊天记录统计程序结束 =====")


# ====================== 4. 程序入口 ======================
if __name__ == "__main__":
    asyncio.run(main())



# TODO
# 1.数据库启动检验是否正确，有concat或message √
# 2.策略实现类封装sql方法到服务类 √
# 3.main方法读取文件封装工具类 √
# 4.实现策略类自定义业务异常 √
# 5.main的日志方法优化
# 6.sql_builder逻辑实现
# 7.selfToTarget子类实现