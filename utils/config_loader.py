from datetime import datetime, timezone
import json
import logging
from dataclasses import asdict
from enum import Enum
from pathlib import Path
from typing import Dict, Any, Union, List

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
        logger.info(f"开始加载配置文件，目标路径：{target_path}")

        try:
            # 2. 检查文件是否存在
            if not target_path.exists():
                raise FileNotFoundError(f"配置文件不存在：{target_path}")

            # 3. 读取并解析JSON
            with open(target_path, "r", encoding="utf-8") as f:
                config_dict = json.load(f)

            logger.info(f"✅ 配置文件加载成功（路径：{target_path}）")
            return config_dict

        except FileNotFoundError as e:
            raise ParseBaseError(f"配置文件加载失败：文件不存在 → {e}") from e
        except json.JSONDecodeError as e:
            raise ParseBaseError(f"配置文件加载失败：JSON格式错误 → {e}") from e
        except PermissionError as e:
            raise ParseBaseError(f"配置文件加载失败：无读取权限 → {e}") from e
        except Exception as e:
            raise ParseBaseError(f"配置文件加载失败：未知异常 → {e}") from e

    @classmethod
    def get_default_config_path(cls) -> str:
        """获取默认配置文件路径（标准化后）"""
        return str(cls.DEFAULT_CONFIG_PATH.resolve())

    @classmethod
    def dataclass_to_dict(cls, obj: Any) -> Union[Dict, List, str, int, float, bool, None]:
        # ========== 1. 基础特殊类型（最高优先级） ==========
        # 处理datetime（对象/时间戳）
        if type(obj).__name__ == 'datetime' or isinstance(obj, datetime):
            return obj.astimezone(timezone.utc).isoformat()
        elif isinstance(obj, (int, float)) and 1000000000 <= obj <= 9999999999:
            try:
                return datetime.fromtimestamp(obj, timezone.utc).isoformat()
            except:
                return obj

        # 处理Enum（实例/元组/数字）
        elif isinstance(obj, Enum):
            return obj.value[1] if isinstance(obj.value, tuple) else obj.value
        elif isinstance(obj, tuple) and len(obj) == 2 and obj[0] in [1, 2]:
            return obj[1]
        elif isinstance(obj, int) and obj in [1, 2]:
            return 'friend' if obj == 1 else 'group'

        # ========== 2. 容器类型（递归处理，核心新增Dict分支） ==========
        # 处理列表
        elif isinstance(obj, list):
            return [cls.dataclass_to_dict(item) for item in obj]

        # 处理原生字典（新增！关键修复）
        elif isinstance(obj, dict):
            return {k: cls.dataclass_to_dict(v) for k, v in obj.items()}

        # 处理dataclass对象
        elif hasattr(obj, '__dataclass_fields__'):
            data = asdict(obj)
            return {k: cls.dataclass_to_dict(v) for k, v in data.items()}

        # ========== 3. 其他基础类型 ==========
        else:
            return obj

    @classmethod
    def save_to_json(cls, raw_data, file_path):
        """
        将数据保存为 JSON 文件
        """
        data = cls.dataclass_to_dict(raw_data)
        with open(file_path, 'w', encoding='utf-8') as f:
            # ensure_ascii=False 保证中文正常显示
            json.dump(data, f, indent=4, ensure_ascii=False)