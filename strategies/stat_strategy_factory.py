import logging
from typing import Type

# 导入策略接口（核心：定义所有策略必须实现的方法）
from .stat_strategies import StatStrategy  # 策略接口文件，需确保存在
# 导入具体策略实现类（根据需要扩展）
from strategies.impl.self_to_target_strategy_impl import (
    SelfToTargetStrategy,
    # 后续扩展：TargetToSelfStrategy, SelfAllStrategy
)
# 导入自定义异常（配置值无效时抛出）
from exceptions import InvalidValueError

# 模块级日志
logger = logging.getLogger(__name__)


class StatStrategyFactory:
    """统计策略工厂类：统一创建不同类型的策略实例"""

    # 策略类型映射表：key=mode_type，value=对应的策略类
    _STRATEGY_MAP = {
        "self_to_target": SelfToTargetStrategy,
        # 预留扩展：
        # "target_to_self": TargetToSelfStrategy,
        # "self_all": SelfAllStrategy
    }

    @classmethod
    def create_strategy(cls, mode_type: str, **kwargs) -> StatStrategy:
        """
        工厂核心方法：创建指定类型的策略实例
        :param mode_type: 策略类型（如 "self_to_target"）
        :param kwargs: 传递给策略类的初始化参数（如db服务、全局配置）
        :return: 策略接口实例
        :raise InvalidValueError: 不支持的策略类型时抛出
        """
        logger.info(f"开始创建统计策略，类型：{mode_type}")

        # 1. 校验策略类型是否支持
        if mode_type not in cls._STRATEGY_MAP:
            raise InvalidValueError(
                f"不支持的统计策略类型：{mode_type}，支持类型：{list(cls._STRATEGY_MAP.keys())}"
            )

        # 2. 获取对应的策略类并实例化
        strategy_class: Type[StatStrategy] = cls._STRATEGY_MAP[mode_type]
        try:
            strategy_instance = strategy_class(**kwargs)
            logger.info(f"✅ 成功创建[{mode_type}]策略实例（类名：{strategy_class.__name__}）")
            return strategy_instance
        except Exception as e:
            raise RuntimeError(f"创建[{mode_type}]策略实例失败：{e}") from e