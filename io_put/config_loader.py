import json
import logging
from pathlib import Path
from typing import Dict

# 导入自定义异常（根据实际路径调整）
from exceptions import ParseBaseError

# 模块级日志
logger = logging.getLogger(__name__)


class ConfigLoader:
    """配置加载门面类：封装路径处理、文件读取、JSON解析的复杂逻辑，对外提供统一简单接口"""

    # 默认配置文件路径（支持跨平台路径解析）
    DEFAULT_CONFIG_PATH = Path("./configs/config.json")

    @classmethod
    def load_config(cls, config_path: str = None) -> Dict:
        """
        门面核心方法：加载配置文件（封装所有底层逻辑）
        :param config_path: 自定义配置文件路径（可选，默认使用DEFAULT_CONFIG_PATH）
        :return: 解析后的配置字典
        :raise ParseBaseError: 路径/文件/格式异常时抛出统一异常
        """
        # 1. 处理配置路径（优先自定义路径，无则用默认）
        target_path = Path(config_path) if config_path else cls.DEFAULT_CONFIG_PATH
        # 标准化路径（自动处理Windows反斜杠/相对路径）
        target_path = target_path.resolve()
        logger.info(f"✅ 开始加载配置文件，目标路径：{target_path}")

        try:
            # 2. 检查文件是否存在
            if not target_path.exists():
                raise FileNotFoundError(f"⚠️ 配置文件不存在：{target_path}")

            # 3. 读取并解析JSON
            with open(target_path, "r", encoding="utf-8") as f:
                config_dict = json.load(f)

            logger.info(f"✅ 配置文件加载成功（路径：{target_path}）")
            return config_dict

        except FileNotFoundError as e:
            raise ParseBaseError(f"⚠️ 配置文件加载失败：文件不存在 → {e}") from e
        except json.JSONDecodeError as e:
            raise ParseBaseError(f"⚠️ 配置文件加载失败：JSON格式错误 → {e}") from e
        except PermissionError as e:
            raise ParseBaseError(f"⚠️ 配置文件加载失败：无读取权限 → {e}") from e
        except Exception as e:
            raise ParseBaseError(f"⚠️ 配置文件加载失败：未知异常 → {e}") from e

    @classmethod
    def get_default_config_path(cls) -> str:
        """获取默认配置文件路径（标准化后）"""
        return str(cls.DEFAULT_CONFIG_PATH.resolve())