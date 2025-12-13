from enum import Enum

# 新增联系人类型枚举（贴合local_type映射）
class ContactType(Enum):
    FRIEND = (1, "friend")          # 好友
    GROUP = (2, "group")            # 群聊
    GROUP_FRIEND = (3, "group_friend")  # 群友（没加过的）
    UNKNOWN = (-1, "unknown")       # 未知类型（兜底）

    @classmethod
    def get_type_by_local_type_id(cls, local_type: int) -> "ContactType":
        """根据local_type获取对应的类型字符串（适配原有逻辑）"""
        for member in cls:
            if member.value[0] == local_type:
                return member
        return cls.UNKNOWN  # 兜底返回unknown