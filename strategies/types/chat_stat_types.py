from typing import TypeAlias, Dict, Any

# 1. 原子级核心记录（对应Msg表单条原始记录）
CoreRecord: TypeAlias = Dict[str, int | str | float | bool | None]

# 2. 单联系人的加工结果（半成品）
ProcessResult: TypeAlias = Dict[str, Any]  # 可细化为具体字段，比如：
# ProcessResult: TypeAlias = {
#     "contact_id": str | int,
#     "total_msg_count": int,
#     "pet_phrase_top3": List[str],
#     "latest_msg_time": int
# }

# 3. 所有目标联系人的聚合结果（成品）
AggregateResult: TypeAlias = Dict[str, Any]  # 可细化为具体字段，比如：
# AggregateResult: TypeAlias = {
#     "target_contacts": List[str | int],
#     "process_results": List[ProcessResult],
#     "total_msg_count_all": int,
#     "pet_phrase_global_top5": List[str]
# }

# 4. 缓存映射字典（示例：mapping_cache）
MappingCache: TypeAlias = Dict[str, Dict[str, str]]  # 按实际业务细化