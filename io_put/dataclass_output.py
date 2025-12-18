import dataclasses
import json
import logging
from enum import Enum
from typing import Any, List

logger = logging.getLogger(__name__)

class DataConverterFacade:
    """数据转换门面类（解除对AnalyzerResult的直接依赖）"""

    @staticmethod
    def _to_dict(obj: Any) -> Any:
        """内部递归转换方法（通过类型特征判断，不依赖具体类）"""
        # 处理dataclass对象（通过dataclasses模块判断，不依赖具体类）
        if dataclasses.is_dataclass(obj) and not isinstance(obj, type):
            return {
                field.name: DataConverterFacade._to_dict(getattr(obj, field.name))
                for field in dataclasses.fields(obj)
            }
        # 处理枚举类型
        elif isinstance(obj, Enum):
            return obj.value  # 保持枚举值转换逻辑
        # 处理列表/元组等可迭代对象
        elif isinstance(obj, (list, tuple, set)):
            return [DataConverterFacade._to_dict(item) for item in obj]
        # 基本类型直接返回
        else:
            return obj

    @classmethod
    def to_dict_list(cls, data_list: List[Any]) -> List[dict]:
        """将任意dataclass列表转换为字典列表（支持AnalyzerResult等任意dataclass）"""
        return [cls._to_dict(item) for item in data_list]

    @classmethod
    def to_json(cls, data_list: List[Any], indent: int = 2) -> str:
        """将任意dataclass列表转换为JSON字符串"""
        dict_list = cls.to_dict_list(data_list)
        return json.dumps(dict_list, ensure_ascii=False, indent=indent)

    @classmethod
    def print_json(cls, data_list: List[Any], indent: int = 2) -> None:
        """直接打印转换后的JSON"""
        print(cls.to_json(data_list, indent))

    @classmethod
    def log_json(cls, data_list: List[Any], indent: int = 2) -> None:
        """日志打印转换后的JSON"""
        logger.info(cls.to_json(data_list, indent))

    @classmethod
    def save_json(cls, data_list: List[Any], file_path: str, indent: int = 2) -> None:
        """将JSON保存到文件"""
        json_str = cls.to_json(data_list, indent)
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(json_str)
