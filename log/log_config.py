import logging
import sys
import os
from datetime import datetime

def setup_global_logging():
    """
    全局日志配置：
    1. 自动创建log文件夹（不存在则创建）
    2. 每次运行生成独立日志文件（按启动时间命名）
    3. 同时输出到控制台+日志文件
    """
    # ========== 1. 检测并创建log文件夹 ==========
    log_dir = "./log_back"
    folder_created = False
    if not os.path.exists(log_dir):
        os.makedirs(log_dir, exist_ok=True)
        folder_created = True

    # ========== 2. 生成带启动时间的日志文件名 ==========
    # 时间格式：年-月-日_时-分-秒（无特殊字符，避免路径错误）
    run_time = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_filename = f"chat_stat_{run_time}.log"
    log_filepath = os.path.join(log_dir, log_filename)  # 完整日志路径

    # ========== 3. 配置日志格式+输出 ==========
    log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    date_format = "%Y-%m-%d %H:%M:%S"

    logging.basicConfig(
        level=logging.INFO,
        format=log_format,
        datefmt=date_format,
        handlers=[
            logging.StreamHandler(sys.stdout),  # 控制台输出
            logging.FileHandler(
                log_filepath,
                encoding="utf-8",  # 避免中文乱码
                mode="w"  # 明确指定写入模式，覆盖已存在的文件
            )  # 按时间命名的文件输出
        ],
        force=True  # 强制重新配置，避免重复配置导致的问题
    )

    # 日志配置完成提示（在配置完成后记录）
    logger = logging.getLogger(__name__)
    if folder_created:
        logger.info(f"✅ 日志文件夹不存在，已自动创建：{log_dir}")
    logger.info(f"📁 全局日志配置完成，日志文件路径：{log_filepath}")