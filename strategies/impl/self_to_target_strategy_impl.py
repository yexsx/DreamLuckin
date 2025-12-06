import hashlib
import datetime
from typing import Dict, List, Any
import logging
from strategies import *

class SelfToTargetStrategy(StatStrategy):
    """self_to_target 策略具体实现类（本人发给目标）"""

    def _associate_mapping(self) -> None:
        """步骤1：预获取目标的单表映射（remark/nick_name→username→MD5→表名）"""
        # 1. 从配置读取目标值（无需区分match_type，仅读目标值）
        target_value = self.app_config.stat_mode.target_contact  # 仅读取目标匹配值

        # 2. 精准查询contact表（同时匹配remark和nick_name，OR条件）
        contact_sql = """
                      SELECT username, local_type, remark, nick_name FROM contact WHERE remark = ? OR nick_name = ?
                      """
        contact_result = self.contact_db_service.execute_query(contact_sql, (target_value, target_value))

        # 校验结果数量：0条/多条均报错，仅1条合法
        if len(contact_result) == 0:
            raise RuntimeError(f"未找到remark或nick_name等于[{target_value}]的联系人/群聊")
        elif len(contact_result) > 1:
            raise RuntimeError(
                f"找到多条remark或nick_name等于[{target_value}]的记录（共{len(contact_result)}条），请缩小匹配范围")
        contact_info = contact_result[0]

        # 3. username转MD5生成目标表名
        username = contact_info["username"]
        md5_username = hashlib.md5(username.encode()).hexdigest().lower()
        target_table_name = f"Msg_{md5_username}"

        # 4. 存入映射缓存（表名→联系人信息）
        self.mapping_cache[target_table_name] = {
            "name": contact_info["remark"] or contact_info["nick_name"],
            "type": "friend" if contact_info["local_type"] == 1 else "group",
            "username": username
        }

    async def _get_pending_tables(self) -> List[str]:
        """步骤2：获取唯一待处理表（从映射缓存中提取）"""
        if not self.mapping_cache:
            raise RuntimeError("映射缓存为空，请先执行_associate_mapping")
        target_table_name = list(self.mapping_cache.keys())[0]

        # 替换：从sqlite_sequence校验表存在性，并获取记录总数
        table_exist_sql = "SELECT seq FROM sqlite_sequence WHERE name = ?"
        exist_result = await self.chat_db_service.execute_query(table_exist_sql, (target_table_name,))

        if not exist_result:
            raise RuntimeError(f"目标表{target_table_name}不存在于聊天记录数据库（sqlite_sequence无该表记录）")

        # 提取记录总数并打印日志
        total_records = exist_result[0]["seq"]
        logging.info(f"✅ 目标表[{target_table_name}]存在，该表总聊天记录数：{total_records}条")

        return [target_table_name]  # 单表返回长度为1的列表


    def _process_tables(self, pending_tables: List[str]) -> ProcessResult:
        """步骤3：单表同步处理（本人发给目标的核心记录）"""
        pass
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

    def _backtrack_context(self) -> None:
        """步骤4：回溯核心记录的上两条纯文字上下文"""
        pass
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

    def _aggregate_stat(self) -> AggregateResult:
        """步骤5：按display_dimension聚合统计（年/月/日）"""
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
