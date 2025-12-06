import asyncio
import json
import logging
import os
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
from exceptions import ParseBaseError, LuckyChatDBError
# # 策略工厂 + 接口
from strategies import StatStrategyFactory

# ====================== 3. 配置文件路径（Windows路径处理） ======================
CONFIG_FILE_PATH = r"./configs/config.json"  # 原始字符串避免转义


# ====================== 4. 读取配置文件 ======================
def load_config_file(file_path: str) -> dict:
    """读取JSON配置文件"""
    try:
        # 检查文件是否存在
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"配置文件不存在：{file_path}")

        # 读取并解析JSON
        with open(file_path, "r", encoding="utf-8") as f:
            config_dict = json.load(f)
        logger.info(f"✅ 成功读取配置文件：{file_path}")
        return config_dict
    except json.JSONDecodeError as e:
        raise ParseBaseError(f"配置文件JSON格式错误：{e}")
    except Exception as e:
        raise ParseBaseError(f"读取配置文件失败：{e}")


# ====================== 5. 核心异步主函数 ======================
async def main():
    """程序主入口：读取配置 → 统一解析 → 初始化数据库 → 工厂创建策略 → 执行策略"""
    logger.info("===== 聊天记录统计程序启动 =====")

    try:
        # -------------------------- 步骤1：读取+统一解析配置 --------------------------
        logger.info("【步骤1/4】开始读取并解析配置文件")
        # 读取配置文件
        config_dict = load_config_file(CONFIG_FILE_PATH)
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


# ====================== 6. 程序入口 ======================
if __name__ == "__main__":
    asyncio.run(main())