import datetime
import hashlib
import logging
from typing import Dict, List

from exceptions import ContactNotFoundError, TargetTableNotFoundError
from parser import AppConfig
from services import ContactDBService, ChatRecordDBService
from utils import SQLBuilder
from .analyzer_models import ContactRecord, ChatRecord, StrategyResult, BacktrackedRecord
from .analyzer_enums import ContactType

logger = logging.getLogger(__name__)

class ChatRecordAnalyzer:
    """聊天记录分析器（核心业务类）"""

    def __init__(
            self,
            app_config: AppConfig  # 全局配置实例（AppConfig）
    ):
        self.app_config = app_config
        # 缓存：映射关系（表名→联系人信息）
        self.mapping_cache: Dict[str, ContactRecord] = {}
        # 缓存：表处理结果（后续步骤复用）
        self.process_result: Dict[str, Dict[int, ChatRecord]] = {}
        # 缓存：回溯表记录结果
        self.backtracked_record: Dict[str, Dict[int, List[ChatRecord]]] = {}
        # 缓存：带上下文的核心记录
        # self.context_result: Dict[str, List[Dict[str, any]]] = {}

    # async def run(self) -> StrategyResult:
    async def run(self) -> None:
        """策略执行入口（统一串联所有步骤，无需重写）"""
        # 步骤1：获取映射关系
        self.mapping_cache = self._associate_mapping()
        # 步骤2：获取待处理表
        pending_tables = await self._get_pending_tables()
        # 步骤3：处理表数据
        self.process_result = await self._process_tables(pending_tables)
        # 步骤4：回溯上下文
        self.backtracked_record = await self._backtrack_context()
        # 步骤5：聚合统计
        # return self._aggregate_stat()
        pass


    # @abstractmethod
    def _aggregate_stat(self) -> StrategyResult:
        """步骤5：按维度聚合统计
        返回：
            StrategyResult：聚合后的统计结果（含维度概览、明细等）
        """
        pass

    def _associate_mapping(self) -> Dict[str, ContactRecord]:
        """
            步骤1：预获取目标的全量映射（remark/nick_name→username→MD5→表名）
        """

        associate_mapping: Dict[str, ContactRecord] = {}

        # 1. 从配置读取目标值（无需区分match_type，仅读目标值）
        target_value = self.app_config.stat_mode.target_contact_list  # 仅读取目标匹配值
        filter_group_chat = self.app_config.filter_config.filter_group_chat  # 过滤群聊配置

        # ========== 执行查询前日志（仅必要信息） ==========
        logger.info(f"🔍 开始查询联系人：目标值列表={target_value} | 过滤群聊={filter_group_chat}")

        # 2. 精准查询contact表（同时匹配remark和nick_name，OR条件）
        contact_result = ContactDBService.get_contacts(target_value, filter_group_chat)

        # 校验结果数量：0条报错
        if len(contact_result) == 0:
            raise ContactNotFoundError(target_value)

        # 提取查询结果中实际匹配到的名称集合
        matched_names = set()
        for info in contact_result:
            if info["remark"]:
                matched_names.add(info["remark"].strip())
            if info["nick_name"]:
                matched_names.add(info["nick_name"].strip())
        # 筛选配置值中未匹配到的项
        unmatched_config_values = [val for val in target_value if val.strip() not in matched_names]
        # ========== 新增逻辑结束 ==========

        # 3. 遍历所有联系人结果，逐个处理并存入缓存（核心修改：从单元素改为循环）
        for idx, contact_info in enumerate(contact_result, 1):
            # 3.1 提取username并生成MD5表名
            username = contact_info["username"]
            md5_username = hashlib.md5(username.encode()).hexdigest().lower()
            target_table_name = f"Msg_{md5_username}"

            # 3.2 构造联系人信息（兼容remark/nick_name为空的情况）
            contact_name = contact_info["remark"] or contact_info["nick_name"] or "未知联系人"

            local_type = contact_info["local_type"]
            contact_type = ContactType.get_type_by_local_type_id(local_type)

            # 3.3 存入映射缓存（表名→联系人信息，自动覆盖重复key）
            associate_mapping[target_table_name] = ContactRecord(
                username=username,
                nickname=contact_name,
                type=contact_type
                # type_code=contact_info["local_type"]  # 对应原字典的type_code
            )

            logger.info(
                f"✅ 【映射缓存-{idx}/{len(contact_result)}】"
                f"联系人名称：{contact_name} | "
                f"类型：{contact_type}（原始local_type：{local_type}） | "
                f"username：{username} | "
                f"生成目标表名：{target_table_name}"
            )

        # ========== 未匹配日志（对齐_get_pending_tables风格） ==========
        if unmatched_config_values:
            for val in unmatched_config_values:
                logger.warning(f"⚠️ 配置值[{val}]未在联系人表中匹配到对应的联系人/群聊")

        logger.info(
            f"✅ 【映射缓存汇总】配置目标值总数：{len(target_value)} | "
            f"匹配到联系人数量：{len(contact_result)} | "
            f"未匹配的配置值数量：{len(unmatched_config_values)} | "
            f"缓存表名数量：{len(associate_mapping)}"
        )

        return associate_mapping


    async def _get_pending_tables(self) -> List[str]:
        """
            步骤2：获取所有待处理表（适配多表），校验存在性并输出日志
            返回：List[str]：待处理的Msg表名列表
        """

        # 1,获取映射缓存中所有表名
        pending_table_names = list(self.mapping_cache.keys())
        total_pending = len(pending_table_names)

        # 2,调用封装方法批量校验表存在性（name IN 逻辑）
        table_seq_dict = await ChatRecordDBService.check_tables_exist(pending_table_names)

        # 3,先单独收集缺失的表（不影响排序，改动1）
        missing_contacts = []
        for table_name in pending_table_names:
            if table_name not in table_seq_dict:
                contact_info = self.mapping_cache[table_name]
                missing_contacts.append(
                    f"联系人[{contact_info.nickname}](类型：{contact_info.type})的聊天记录表[{table_name}]缺失"
                )

        # 4,遍历table_seq_dict.keys()（已排序）收集有效表（核心改动2）
        valid_tables = []
        for table_name in table_seq_dict.keys():  # 替换原遍历pending_table_names
            total_records = table_seq_dict[table_name]
            contact_info = self.mapping_cache[table_name]
            logger.info(
                f"✅ 联系人[{contact_info.nickname}]的目标表[{table_name}]存在，该表总聊天记录数：{total_records}条"
            )
            valid_tables.append(table_name)

        # 日志记录缺失的联系人
        if missing_contacts:
            for missing_info in missing_contacts:
                logger.warning(f"⚠️ {missing_info}")

        # 所有表都缺失→抛异常；部分缺失仅日志，返回有效表名
        if not valid_tables:
            raise TargetTableNotFoundError(
                target_table_name=",".join(pending_table_names),
                message="❌ 所有待处理的聊天记录表均不存在"
            )

        total_valid = len(valid_tables)
        total_missing = len(missing_contacts)
        logger.info(
            f"✅ 【待处理表校验汇总】"
            f"总待处理表数：{total_pending} | "
            f"有效存在表数：{total_valid} | "
            f"缺失表数：{total_missing} | "
            f"最终待处理表列表：{valid_tables}"
        )

        return valid_tables


    async def _process_tables(self, pending_tables: List[str]) -> Dict[str, Dict[int, ChatRecord]]:
        """
            步骤3：处理表数据（协程）
            参数：
                pending_tables：_get_pending_tables返回的待处理表列表
            返回：
                Dict[str, list[ChatRecord]]：{表名: 聊天记录列表}
        """

        table_chat_records: Dict[str, Dict[int, ChatRecord]] = {}
        pet_phrase_config = self.app_config.pet_phrase_config
        max_concurrency = self.app_config.db_config.max_concurrency

        # 1. 构建时间条件（所有表共用）
        time_condition = SQLBuilder.build_time_condition(self.app_config.time_config)
        # 2. 构建口头禅条件+参数（所有表共用）
        phrase_condition, phrase_params = SQLBuilder.build_phrase_condition(pet_phrase_config)
        # 3. 构建命中关键词列表别名
        match_keywords_sql, match_params = SQLBuilder.build_match_keywords_sql(pet_phrase_config)

        logger.info(
            f"🔧 构建公共查询条件：待处理表数={len(pending_tables)} | "
            f"📝 口头禅列表={pet_phrase_config.pet_phrases}（匹配类型={pet_phrase_config.match_type}） | "
            f"🕒 时间范围={time_condition} | "
            f"🤖 仅查自己消息={True}"
        )

        for table_name in pending_tables:
            # 1. 调用DB服务获取原始记录（字典列表）
            raw_records = await ChatRecordDBService.get_chat_records_by_phrase_and_time(
                table_name=table_name,
                phrase_condition=phrase_condition,
                phrase_params=phrase_params,
                match_keywords_sql=match_keywords_sql,
                match_params=match_params,
                time_condition=time_condition,
                only_self_msg=self.app_config.stat_mode.mode_type != "target_to_self"
            )

            # 2. 转换为ChatRecord对象（核心：字典→结构化类，改为local_id为key的dict）
            chat_records = {}  # 初始化改为字典，替代列表
            for raw in raw_records:
                # 匹配ChatRecord字段，补充matched_phrases（空列表兜底）

                raw_create_time = raw["create_time"]
                raw_matched_phrases = raw["matched_phrases"]

                chat_record = ChatRecord(
                    local_id=raw["local_id"],
                    message_content=raw["message_content"],
                    real_sender_id=raw["real_sender_id"],
                    create_time=raw_create_time,
                    create_time_format=datetime.datetime.fromtimestamp(raw_create_time) if raw_create_time else None,
                    matched_phrases=raw_matched_phrases.split(',') if raw_matched_phrases and raw_matched_phrases.strip() else []
                )
                chat_records[chat_record.local_id] = chat_record  # 以local_id为key存入字典

            # 3. 存入结果字典
            table_chat_records[table_name] = chat_records

            logger.info(f"📊 处理表完成：表名={table_name} | 有效记录数={len(chat_records.keys())}")

        return table_chat_records


    async def _backtrack_context(self) -> Dict[
        str, List[BacktrackedRecord]]:
        """
            步骤4：回溯核心记录的上两条上下文
            按表批量追溯上下文：同表的核心记录一次查询，减少DB调用
            :return: 表名→带上下文的BacktrackedRecord列表
        """
        pass
        # backtrack_result: Dict[str, List[BacktrackedRecord]] = {}
        # total_core_records = sum(len(records) for records in self.process_result.values())
        #
        # # 日志埋点（贴合你的风格）
        # logger.info(
        #     f"🔍 开始批量追溯上下文：待处理表数={len(self.process_result)} | 核心记录总数={total_core_records} | 每条追溯前2条")
        #
        # # 遍历每个表，批量处理
        # for table_name, core_records in self.process_result.items():
        #     # 1. 提取当前表的所有核心local_id（用于批量查询）
        #     core_local_ids = [rec.local_id for rec in core_records]
        #     # 2. 批量查询当前表所有核心ID的上下文（仅1次DB调用）
        #     core_context_map = await self.chat_db_service.get_batch_context_records_by_local_ids(
        #         table_name=table_name,
        #         core_local_id_set=core_local_ids
        #     )
        #
        #     # 3. 构建BacktrackedRecord
        #     backtrack_records = []
        #     for core_record in core_records:
        #         # 获取当前核心记录的上下文（已按ID升序）
        #         context_raw = core_context_map[core_record.local_id]
        #         # 转换为ChatRecord（和核心记录结构一致）
        #         context_records = [
        #             ChatRecord(
        #                 local_id=raw["local_id"],
        #                 message_content=raw["message_content"],
        #                 real_sender_id=raw["real_sender_id"],
        #                 create_time=datetime.datetime.fromtimestamp(raw["create_time"]),
        #                 matched_phrases=[]  # 上下文无需匹配口头禅
        #             ) for raw in context_raw
        #         ]
        #
        #         # 封装为BacktrackedRecord
        #         backtrack_record = BacktrackedRecord(
        #             core_record=core_record,
        #             context_records=context_records,
        #             context_count=len(context_records),
        #             table_name=table_name
        #         )
        #         backtrack_records.append(backtrack_record)
        #
        #     # 4. 存入结果
        #     backtrack_result[table_name] = backtrack_records
        # #     logger.debug(
        # #         f"📊 表上下文追溯完成：表名={table_name} | 处理核心记录数={len(core_records)} | "
        # #         f"平均每条追溯{sum(len(v) for v in core_context_map.values()) / len(core_records):.1f}条"
        # #     )
        # #
        # # # 完成日志
        # # logger.info(
        # #     f"✅ 上下文追溯完成：处理表数={len(backtrack_result)} | "
        # #     f"总带上下文记录数={sum(len(v) for v in backtrack_result.values())}"
        # # )
        # return backtrack_result