"""统计策略相关业务异常"""
class StatBaseException(Exception):
    """统计策略基础异常（所有统计业务异常的父类）"""
    pass

class ContactNotFoundError(StatBaseException):
    """未找到指定联系人/群聊异常"""
    def __init__(self, target_value: list, message: str = "❌ 未找到指定联系人/群聊"):
        self.target_value = target_value
        # 拼接完整异常信息（保留默认message+业务参数）
        full_message = f"{message}：remark或nick_name等于[{target_value}]"
        super().__init__(full_message)

class TargetTableNotFoundError(StatBaseException):
    """目标表不存在于聊天记录数据库异常"""
    def __init__(self, target_table_name: str, message: str = "❌ 目标表不存在于聊天记录数据库"):
        self.target_table_name = target_table_name
        # 拼接完整异常信息（保留默认message+业务参数）
        full_message = f"{message}：{target_table_name}（sqlite_sequence无该表记录）"
        super().__init__(full_message)