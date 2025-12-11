import datetime
import logging
from typing import List, Dict

from ..stat_models import ChatRecord
from ..stat_strategies import StatStrategy
# from ..types import (AggregateResult)
from utils import SQLBuilder

logger = logging.getLogger(__name__)

class SelfToTargetStrategy(StatStrategy):
    """self_to_target 策略具体实现类（本人发给目标）"""





        # target_table_name = pending_tables[0]
        # core_records: List[CoreRecord] = []
        #
        # # 1. 读取时间范围配置
        # start_time = self.app_config.time_config.start_time  # 时间戳
        # end_time = self.app_config.time_config.end_time  # 时间戳
        #
        # # 2. 构建self_to_target过滤条件：本人发送+纯文本+时间范围+含口头禅
        # base_filter = (
        #     "real_sender_id = 1 "  # 本人发送
        #     "AND local_type = 1 "  # 纯文本
        #     "AND create_time BETWEEN ? AND ?"
        # )
        # # 拼接口头禅匹配条件（多个口头禅用OR）
        # pet_phrases = self.app_config.pet_phrase_config.phrases
        # phrase_filter = " OR ".join([f"message_content LIKE ?" for _ in pet_phrases])
        # full_sql = f"""
        #     SELECT local_id, create_time, message_content, real_sender_id
        #     FROM {target_table_name}
        #     WHERE {base_filter} AND ({phrase_filter})
        #     ORDER BY local_id ASC
        # """
        # # 组装参数：时间范围 + 口头禅模糊匹配参数
        # sql_params = (start_time, end_time) + tuple([f"%{phrase}%" for phrase in pet_phrases])
        #
        # # 3. 执行查询并封装核心记录
        # query_result = self.chat_db_service.execute_query(full_sql, sql_params)
        # if not query_result:
        #     raise RuntimeError(f"未找到{target_table_name}中本人发给目标的含口头禅记录")
        #
        # for row in query_result:
        #     core_record: CoreRecord = {
        #         "local_id": row["local_id"],
        #         "create_time": row["create_time"],
        #         "message_content": row["message_content"],
        #         "real_sender_id": row["real_sender_id"],
        #         "sender_type": "self",  # 本人发送
        #         "chat_type": self.mapping_cache[target_table_name]["type"]
        #     }
        #     core_records.append(core_record)
        #
        # # 4. 返回统一格式的处理结果
        # return {target_table_name: core_records}

    # def _backtrack_context(self) -> None:
    #     """步骤4：回溯核心记录的上两条纯文字上下文"""
        # pass
        # target_table_name = list(self.process_result.keys())[0]
        # core_records = self.process_result[target_table_name]
        # context_records_list: List[Dict[str, Any]] = []
        #
        # # 1. 构建上下文查询模板
        # context_sql = """
        #     SELECT local_id, create_time, message_content, real_sender_id
        #     FROM {table}
        #     WHERE local_id < ? AND local_type = 1 AND real_sender_id = 1
        #     ORDER BY local_id DESC LIMIT 2
        # """.format(table=target_table_name)
        #
        # # 2. 遍历每条核心记录，回溯上下文
        # for core_record in core_records:
        #     local_id = core_record["local_id"]
        #     # 执行上下文查询
        #     context_result = self.chat_db_service.execute_query(context_sql, (local_id,))
        #     # 上下文按local_id升序排列（更早的在前）
        #     context_records = sorted(context_result, key=lambda x: x["local_id"])
        #     # 封装核心记录+上下文
        #     context_records_list.append({
        #         "core_record": core_record,
        #         "context_records": context_records  # 最多2条，不足则返回实际条数
        #     })
        #
        # # 3. 存入上下文缓存
        # self.context_result[target_table_name] = context_records_list

    def _aggregate_stat(self):
    #     """步骤5：按display_dimension聚合统计（年/月/日）"""
        pass
        # target_table_name = list(self.context_result.keys())[0]
        # context_records = self.context_result[target_table_name]
        # display_dimension = self.app_config.output_config.display_dimension
        # contact_info = self.mapping_cache[target_table_name]
        #
        # # 1. 初始化聚合结构
        # aggregate_data = {
        #     "strategy_type": "self_to_target",
        #     "target_info": contact_info,
        #     "display_dimension": display_dimension,
        #     "total_count": len(context_records),
        #     "dimension_stats": {},
        #     "detail_records": context_records
        # }
        #
        # # 2. 按维度聚合
        # for item in context_records:
        #     core_record = item["core_record"]
        #     # 时间戳转datetime
        #     create_dt = datetime.datetime.fromtimestamp(core_record["create_time"])
        #     # 按维度生成key
        #     if display_dimension == "year":
        #         dim_key = str(create_dt.year)
        #     elif display_dimension == "month":
        #         dim_key = f"{create_dt.year}-{create_dt.month:02d}"
        #     else:  # day
        #         dim_key = f"{create_dt.year}-{create_dt.month:02d}-{create_dt.day:02d}"
        #
        #     # 更新维度统计
        #     if dim_key not in aggregate_data["dimension_stats"]:
        #         aggregate_data["dimension_stats"][dim_key] = {
        #             "count": 0,
        #             "records": []
        #         }
        #     aggregate_data["dimension_stats"][dim_key]["count"] += 1
        #     aggregate_data["dimension_stats"][dim_key]["records"].append(item)
        #
        # return aggregate_data
