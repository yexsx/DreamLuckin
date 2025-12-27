import logging
import os
from datetime import datetime
from typing import List

from chat_analyzer.analyzer_models import AnalyzerResult
from io_put.dataclass_output import DataConverterFacade
from parser import AppConfig

logger = logging.getLogger(__name__)


def save_analyzer_result_to_json(analyzer_result: List[AnalyzerResult], app_config: AppConfig) -> str:
    """
    将分析结果保存为JSON文件
    
    参数:
        analyzer_result: 分析结果列表
        app_config: 应用配置对象
    
    返回:
        str: 保存的文件完整路径
    """
    pet_phrases = app_config.pet_phrase_config.pet_phrases
    export_path = app_config.output_config.export_path

    # 处理pet_phrases：取前3个关键词（避免过长），用下划线拼接
    # 若为空则用"no_phrases"标识
    phrase_suffix = "_".join(pet_phrases[:3]) if pet_phrases else "no_phrases"
    # 替换可能影响文件名的特殊字符
    phrase_suffix = phrase_suffix.replace(" ", "_").replace("/", "_").replace("\\", "_")

    # 获取当前时间并格式化（年-月-日_时-分-秒）
    current_time = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

    # 拼接文件名（格式：导出路径/时间_关键词组合.json）
    filename = f"{current_time}_{phrase_suffix}.json"
    # 组合完整文件路径
    full_path = os.path.join(export_path, filename)

    # 确保输出目录存在
    os.makedirs(export_path, exist_ok=True)

    # 保存JSON文件
    DataConverterFacade.save_json(analyzer_result, full_path)
    
    logger.info(f"✅ 分析结果已保存到：{full_path}")
    
    return full_path

