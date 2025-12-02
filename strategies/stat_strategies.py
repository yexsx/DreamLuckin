from abc import ABC, abstractmethod
from typing import Dict
from configs.parser import AppConfig


# ------------------------------
# 策略接口（统一所有统计模式的方法）
# ------------------------------
class StatStrategy(ABC):
    """统计策略接口：所有模式必须实现该接口"""

    @abstractmethod
    def get_sql_filter(self, app_config: AppConfig) -> str:
        """获取SQL查询的过滤条件（不同模式过滤条件不同）"""
        pass

    @abstractmethod
    def execute(self, app_config: AppConfig, db_service) -> Dict:
        """执行统计逻辑（返回统计结果）"""
        pass


# ------------------------------
# 三种具体策略实现（每种模式一个类）
# ------------------------------
class SelfAllStrategy(StatStrategy):
    """策略1：自己所有聊天的口头禅统计"""

    def get_sql_filter(self, app_config: AppConfig) -> str:
        # 过滤条件：自己发出的消息 + 时间范围
        self_id = app_config.self_identifier
        time_condition = app_config.time_config.sql_time_condition
        return f"talker = '{self_id}' AND {time_condition}"

    def execute(self, app_config: AppConfig, db_service) -> Dict:
        """执行统计（仅搭骨架，后续填具体逻辑）"""
        print(f"📊 执行【自己所有聊天】统计模式")
        # 后续步骤：
        # 1. 获取SQL过滤条件
        sql_filter = self.get_sql_filter(app_config)
        # 2. 调用db_service执行查询
        # 3. 统计口头禅出现次数
        # 4. 返回结果
        return {"mode": "self_all", "filter": sql_filter, "result": {}}


class SelfToTargetStrategy(StatStrategy):
    """策略2：自己对某个人的口头禅统计"""

    def get_sql_filter(self, app_config: AppConfig) -> str:
        # 过滤条件：自己发出的消息 + 接收方是target + 时间范围
        self_id = app_config.self_identifier
        target = app_config.stat_mode.target_contact
        time_condition = app_config.time_config.sql_time_condition
        return f"talker = '{self_id}' AND receiver = '{target}' AND {time_condition}"  # 假设receiver是接收方字段，需按实际表结构调整

    def execute(self, app_config: AppConfig, db_service) -> Dict:
        print(f"📊 执行【自己对{app_config.stat_mode.target_contact}】统计模式")
        sql_filter = self.get_sql_filter(app_config)
        # 后续填统计逻辑
        return {"mode": "self_to_target", "target": app_config.stat_mode.target_contact, "filter": sql_filter,
                "result": {}}


class TargetToSelfStrategy(StatStrategy):
    """策略3：某个人对自己的口头禅统计"""

    def get_sql_filter(self, app_config: AppConfig) -> str:
        # 过滤条件：发送方是target + 接收方是自己 + 时间范围
        self_id = app_config.self_identifier
        target = app_config.stat_mode.target_contact
        time_condition = app_config.time_config.sql_time_condition
        return f"talker = '{target}' AND receiver = '{self_id}' AND {time_condition}"  # 需按实际表结构调整字段名

    def execute(self, app_config: AppConfig, db_service) -> Dict:
        print(f"📊 执行【{app_config.stat_mode.target_contact}对自己】统计模式")
        sql_filter = self.get_sql_filter(app_config)
        # 后续填统计逻辑
        return {"mode": "target_to_self", "target": app_config.stat_mode.target_contact, "filter": sql_filter,
                "result": {}}


# ------------------------------
# 策略工厂（根据mode_type创建对应策略实例）
# ------------------------------
class StatStrategyFactory:
    """策略工厂：隐藏策略创建细节，统一入口"""

    @staticmethod
    def create_strategy(mode_type: str) -> StatStrategy:
        strategy_map = {
            "self_all": SelfAllStrategy(),
            "self_to_target": SelfToTargetStrategy(),
            "target_to_self": TargetToSelfStrategy()
        }
        if mode_type not in strategy_map:
            raise ValueError(f"不支持的统计模式：{mode_type}")
        return strategy_map[mode_type]