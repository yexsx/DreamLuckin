import asyncio
import dataclasses
import hashlib
import logging
import re
from typing import Dict, List

from exceptions import ContactNotFoundError, TargetTableNotFoundError
from parser import AppConfig
from services import ContactDBService, ChatRecordDBService, SQLBuilder
from .analyzer_models import (
    ContactType,
    ContactRecord,
    ChatRecordCommon,
    ChatRecordCore,
    MappingCacheType,
    ProcessResultType,
    BacktrackedRecordType, AnalyzerResult, ChatRecordExtend
)

logger = logging.getLogger(__name__)

class ChatRecordAnalyzer:
    """聊天记录分析器（核心业务类）"""

    def __init__(
            self,
            app_config: AppConfig  # 全局配置实例（AppConfig）
    ):
        self.app_config = app_config
        # 缓存：映射关系（表名→联系人信息）
        self.mapping_cache: MappingCacheType = {}
        # 缓存：表处理结果（后续步骤复用）
        self.process_result: ProcessResultType = {}
        # 缓存：回溯表记录结果,带上下文的核心记录
        self.backtracked_front_record: BacktrackedRecordType = {}
        self.backtracked_last_record: BacktrackedRecordType = {}
        self.analyzer_result: List[AnalyzerResult] = []


    async def run(self) -> List[AnalyzerResult]:
        """策略执行入口（统一串联所有步骤，无需重写）"""
        # 步骤1：获取映射关系
        self.mapping_cache = self._associate_mapping()
        # 步骤2：获取待处理表
        pending_tables = await self._get_pending_tables()
        # 步骤3：处理表数据
        self.process_result = await self._process_tables(pending_tables)
        # 步骤4：回溯上下文
        self.backtracked_front_record, self.backtracked_last_record = await self._backtrack_context()
        # 步骤5：聚合分析结果
        self.analyzer_result = self._aggregate_analyzer_results()
        # 步骤6：翻译wxid群聊名称
        self._replace_wxid_with_nickname()

        return self.analyzer_result



    def _associate_mapping(self) -> MappingCacheType:
        """
            步骤1：预获取目标的全量映射（remark/nick_name→username→MD5→表名）
        """

        associate_mapping: MappingCacheType = {}

        # 1. 从配置读取目标值（无需区分match_type，仅读目标值）
        target_value = self.app_config.stat_mode.target_contact_list  # 仅读取目标匹配值
        filter_group_chat = self.app_config.filter_config.filter_group_chat  # 过滤群聊配置

        # ========== 执行查询前日志（仅必要信息） ==========
        logger.info(f"🎷 开始查询联系人：目标值列表={target_value} | 过滤群聊={filter_group_chat}")

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
                type=contact_type,
                type_code=local_type
            )

            logger.info(
                f"🎷 【映射缓存-{idx}/{len(contact_result)}】"
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
            f"🎷 【映射缓存汇总】配置目标值总数：{len(target_value)} | "
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
                f"🎸 联系人[{contact_info.nickname}]的目标表[{table_name}]存在，该表总聊天记录数：{total_records}条"
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
            f"🎸 【待处理表校验汇总】"
            f"总待处理表数：{total_pending} | "
            f"有效存在表数：{total_valid} | "
            f"缺失表数：{total_missing} | "
            f"最终待处理表列表：{valid_tables}"
        )

        return valid_tables


    async def _process_tables(self, pending_tables: List[str]) -> ProcessResultType:
        """
            步骤3：处理表数据（协程，支持并发）
            参数：
                pending_tables：_get_pending_tables返回的待处理表列表
            返回：
                Dict[str, list[ChatRecord]]：{表名: 聊天记录列表}
        """

        table_chat_records: ProcessResultType = {}
        pet_phrase_config = self.app_config.pet_phrase_config
        max_concurrency = self.app_config.db_config.max_concurrency

        # 1. 构建时间条件（所有表共用）
        time_condition = SQLBuilder.build_time_condition(self.app_config.time_config)
        # 2. 构建口头禅条件+参数（所有表共用）
        phrase_condition, phrase_params = SQLBuilder.build_phrase_condition(pet_phrase_config)
        # 3. 构建命中关键词列表别名
        match_keywords_sql, match_params = SQLBuilder.build_match_keywords_sql(pet_phrase_config)

        logger.info(
            f"🎹 构建公共查询条件：待处理表数={len(pending_tables)} | "
            f"口头禅列表={pet_phrase_config.pet_phrases}（匹配类型={pet_phrase_config.match_type}） | "
            f"时间范围={time_condition} | "
            f"仅查自己消息={True} | "
            f"最大并发数={max_concurrency}"
        )

        # 创建信号量限制并发数
        semaphore = asyncio.Semaphore(max_concurrency)

        async def process_single_table(tbl_name: str) -> tuple[str, Dict[int, ChatRecordCommon]]:
            """处理单个表的协程函数"""
            async with semaphore:
                # 1. 调用DB服务获取原始记录（字典列表）
                raw_records = await ChatRecordDBService.get_chat_records_by_phrase_and_time(
                    table_name=tbl_name,
                    phrase_condition=phrase_condition,
                    phrase_params=phrase_params,
                    match_keywords_sql=match_keywords_sql,
                    match_params=match_params,
                    time_condition=time_condition,
                    only_self_msg=self.app_config.stat_mode.mode_type != "target_to_self"
                )

                # 2. 转换为ChatRecord对象（核心：字典→结构化类，改为local_id为key的dict）
                records_dict = {}  # 初始化改为字典，替代列表
                for raw in raw_records:
                    # 匹配ChatRecord字段，补充matched_phrases（空列表兜底）
                    raw_create_time = raw["create_time"]
                    raw_matched_phrases = raw["matched_phrases"]

                    chat_record = ChatRecordCommon(
                        local_id=raw["local_id"],
                        message_content=raw["message_content"],
                        real_sender_id=raw["real_sender_id"],
                        create_time=raw_create_time,
                        matched_phrases=raw_matched_phrases.split(',') if raw_matched_phrases and raw_matched_phrases.strip() else []
                    )
                    records_dict[chat_record.local_id] = chat_record  # 以local_id为key存入字典

                logger.info(f"🎹 处理表完成：表名={tbl_name} | 有效记录数={len(records_dict.keys())}")
                return tbl_name, records_dict

        # 并发处理所有表
        tasks = [process_single_table(table_name) for table_name in pending_tables]
        results = await asyncio.gather(*tasks)

        # 将结果存入字典
        for table_name, chat_records in results:
            table_chat_records[table_name] = chat_records

        return table_chat_records


    #region 步骤4:回溯核心记录的上下文
    async def _backtrack_context(self) -> tuple[BacktrackedRecordType,BacktrackedRecordType]:
        """
            步骤4：回溯核心记录的上下文
            按表批量追溯上下文：同表的核心记录一次查询，减少DB调用
            :return: 表名→上下文列表
        """
        total_core_records = sum(len(records) for records in self.process_result.values())
        context_front_limit = self.app_config.pet_phrase_config.context_front_limit
        context_end_limit = self.app_config.pet_phrase_config.context_end_limit

        # 日志埋点（贴合你的风格）
        logger.info(
            f"🎻 开始批量追溯上下文：待处理表数={len(self.process_result)} | "
            f"核心记录总数={total_core_records} | "
            f"每条追溯(前{context_front_limit},后{context_end_limit})条")

        # 调用封装的私有方法获取前/后回溯ID映射
        backtrack_front_id_map, backtrack_last_id_map = await self._calculate_backtrack_ids()

        # 初始化前/后回溯结果（严格匹配拆分后的类型别名）
        backtrack_front_result: BacktrackedRecordType = {}
        backtrack_last_result: BacktrackedRecordType = {}

        # ========== 核心步骤：按表→核心ID维度，分别处理前/后上下文（并发优化） ==========
        max_concurrency = self.app_config.db_config.max_concurrency
        semaphore = asyncio.Semaphore(max_concurrency)

        async def process_context_for_core_id(
            tbl_name: str,
            core_id: int,
            front_id_list: List[int],
            last_id_list: List[int]
        ) -> tuple[str, int, List[ChatRecordCore], List[ChatRecordCore]]:
            """处理单个核心ID的前后上下文"""
            async with semaphore:
                front_ctx = await self._get_and_convert_context_records(tbl_name, front_id_list)
                last_ctx = await self._get_and_convert_context_records(tbl_name, last_id_list)
                return tbl_name, core_id, front_ctx, last_ctx

        # 收集所有需要处理的上下文查询任务
        context_tasks = []
        for table_name in self.process_result.keys():
            # 获取当前表的前/后上下文ID映射
            table_front_id_map = backtrack_front_id_map.get(table_name, {})
            table_last_id_map = backtrack_last_id_map.get(table_name, {})

            # 为当前表的每个核心ID创建任务
            for core_local_id in self.process_result[table_name].keys():
                front_ids = table_front_id_map.get(core_local_id, [])
                last_ids = table_last_id_map.get(core_local_id, [])
                context_tasks.append(
                    process_context_for_core_id(table_name, core_local_id, front_ids, last_ids)
                )

        # 并发执行所有上下文查询
        if context_tasks:
            context_results = await asyncio.gather(*context_tasks)

            # 初始化所有表的结果字典
            for table_name in self.process_result.keys():
                backtrack_front_result[table_name] = {}
                backtrack_last_result[table_name] = {}

            # 将结果存入对应的表结构
            for table_name, core_local_id, front_context, last_context in context_results:
                backtrack_front_result[table_name][core_local_id] = front_context
                backtrack_last_result[table_name][core_local_id] = last_context
        else:
            # 如果没有任务，初始化空结果
            for table_name in self.process_result.keys():
                backtrack_front_result[table_name] = {}
                backtrack_last_result[table_name] = {}

        # ========== 新增日志：统计回溯结果并输出 ==========
        # 4. 输出各表明细日志（可选，按需开启）
        for table_log_name in self.process_result.keys():
            front_core_count = len(backtrack_front_result.get(table_log_name, {}))
            front_ctx_count = sum(len(ctx) for ctx in backtrack_front_result.get(table_log_name, {}).values())
            last_core_count = len(backtrack_last_result.get(table_log_name, {}))
            last_ctx_count = sum(len(ctx) for ctx in backtrack_last_result.get(table_log_name, {}).values())

            logger.info(
                f"🎻 回溯表明细：表名={table_log_name} | "
                f"前向：核心ID数={front_core_count} 上下文记录数={front_ctx_count} | "
                f"后向：核心ID数={last_core_count} 上下文记录数={last_ctx_count}"
            )

        # 1. 统计前向回溯数据
        total_front_tables = len([t for t in backtrack_front_result.values() if t])  # 非空前向表数
        total_front_core_ids = sum(len(core_ids) for core_ids in backtrack_front_result.values())  # 前向核心ID总数
        total_front_context = sum(
            len(ctx) for core_ctx in backtrack_front_result.values() for ctx in core_ctx.values())  # 前向上下文记录总数

        # 2. 统计后向回溯数据
        total_last_tables = len([t for t in backtrack_last_result.values() if t])  # 非空后向表数
        total_last_core_ids = sum(len(core_ids) for core_ids in backtrack_last_result.values())  # 后向核心ID总数
        total_last_context = sum(
            len(ctx) for core_ctx in backtrack_last_result.values() for ctx in core_ctx.values())  # 后向上下文记录总数

        # 3. 输出汇总日志
        logger.info(
            f"🎻 上下文回溯完成 | "
            f"前向：非空表数={total_front_tables} 核心ID数={total_front_core_ids} 上下文记录数={total_front_context} | "
            f"后向：非空表数={total_last_tables} 核心ID数={total_last_core_ids} 上下文记录数={total_last_context}"
        )

        return backtrack_front_result, backtrack_last_result

    @staticmethod
    async def _get_and_convert_context_records(table_name: str, context_ids: List[int]) -> List[ChatRecordCore]:
        """
        【私有方法】通用上下文记录查询+转换逻辑
        :param table_name: 表名
        :param context_ids: 需查询的local_id列表
        :return: 转换后的ChatRecordCore列表（空列表若context_ids为空）
        """
        # 空ID列表直接返回空
        if not context_ids:
            return []

        # 调用批量查询获取原始记录
        raw_records = await ChatRecordDBService.get_batch_records_by_local_ids(
            table_name=table_name,
            local_id_set=context_ids
        )

        # 转换为ChatRecordCore列表
        context_records = [
            ChatRecordCore(
                local_id=raw["local_id"],
                message_content=raw["message_content"],
                real_sender_id=raw["real_sender_id"],
                create_time=raw["create_time"],
            )
            for raw in raw_records
        ]

        return context_records

    async def _calculate_backtrack_ids(self) -> tuple[Dict[str, Dict[int, List[int]]], Dict[str, Dict[int, List[int]]]]:
        """
        【私有方法】计算需回溯的local_id（拆分前/后限制，无入参）
        规则：
        - context_front_limit：每个核心local_id减去1到该值的数值（如2→id-1、id-2）
        - context_end_limit：每个核心local_id加上1到该值的数值（如2→id+1、id+2）
        :return: (backtrack_front_id_map, backtrack_last_id_map)
                 backtrack_front_id_map：表名→{核心local_id: 前N条的local_id列表}
                 backtrack_last_id_map：表名→{核心local_id: 后N条的local_id列表}
        """
        # 初始化前/后回溯ID映射（结构：{表名: {核心local_id: [回溯ID列表]}}）
        backtrack_front_id_map: Dict[str, Dict[int, List[int]]] = {}
        backtrack_last_id_map: Dict[str, Dict[int, List[int]]] = {}

        # 从配置读取前/后限制参数（无入参，内部读取self属性）
        context_front_limit = self.app_config.pet_phrase_config.context_front_limit
        context_end_limit = self.app_config.pet_phrase_config.context_end_limit

        # 遍历每个表计算前/后回溯ID
        for table_name, core_records_dict in self.process_result.items():
            # 初始化当前表的前/后回溯ID字典（内层key=核心local_id，value=回溯ID列表）
            table_front_ids: Dict[int, List[int]] = {}
            table_last_ids: Dict[int, List[int]] = {}

            # 跳过空表
            if not core_records_dict:
                backtrack_front_id_map[table_name] = table_front_ids
                backtrack_last_id_map[table_name] = table_last_ids
                continue

            # 遍历当前表的每个核心local_id
            for core_local_id in core_records_dict.keys():
                # ========== 1. 计算当前核心ID的前N条ID（context_front_limit） ==========
                front_id_list = []
                if context_front_limit > 0:
                    for i in range(1, context_front_limit + 1):
                        front_id = core_local_id - i
                        if front_id > 0:  # 过滤负数ID（数据库自增主键无负数）
                            front_id_list.append(front_id)
                    front_id_list.sort()  # 升序排列
                table_front_ids[core_local_id] = front_id_list

                # ========== 2. 计算当前核心ID的后N条ID（context_end_limit） ==========
                last_id_list = []
                if context_end_limit > 0:
                    for i in range(1, context_end_limit + 1):
                        last_id = core_local_id + i
                        last_id_list.append(last_id)
                    last_id_list.sort()  # 升序排列
                table_last_ids[core_local_id] = last_id_list

            # 存入当前表的前/后回溯ID结果
            backtrack_front_id_map[table_name] = table_front_ids
            backtrack_last_id_map[table_name] = table_last_ids

        # 日志输出结果
        total_front_ids = sum(len(ids) for table in backtrack_front_id_map.values() for ids in table.values())
        total_last_ids = sum(len(ids) for table in backtrack_last_id_map.values() for ids in table.values())
        logger.info(
            f"🎻 上下文回溯local_id计算完成：\n"
            f"  - 前{context_front_limit}条共需查询{total_front_ids}条ID | 示例={list(backtrack_front_id_map.values())[:1]}\n"
            f"  - 后{context_end_limit}条共需查询{total_last_ids}条ID | 示例={list(backtrack_last_id_map.values())[:1]}")

        return backtrack_front_id_map, backtrack_last_id_map
    #endregion


    def _aggregate_analyzer_results(self) -> List[AnalyzerResult]:
        """
            步骤5:将各环节处理结果聚合为AnalyzerResult列表
            按联系人聚合的完整分析结果列表
        """
        logger.info(f"🪉 开始聚合分析结果")

        analyzer_results: List[AnalyzerResult] = []

        # 按表名（对应联系人username）分组处理
        for username, contact in self.mapping_cache.items():
            # 获取当前联系人的基础聊天记录
            contact_records: Dict[int, ChatRecordExtend] = {}
            if username in self.process_result:
                for local_id, common_record in self.process_result[username].items():
                    # 转换为扩展记录并初始化上下文
                    extend_record = ChatRecordExtend(
                        **dataclasses.asdict(common_record),
                        context_front_records=[],
                        context_last_records=[]
                    )
                    contact_records[local_id] = extend_record

            # 填充前置上下文
            if username in self.backtracked_front_record:
                for local_id, front_context in self.backtracked_front_record[username].items():
                    if local_id in contact_records:
                        contact_records[local_id].context_front_records = front_context

            # 填充后置上下文
            if username in self.backtracked_last_record:
                for local_id, last_context in self.backtracked_last_record[username].items():
                    if local_id in contact_records:
                        contact_records[local_id].context_last_records = last_context

            # 构建当前联系人的分析结果
            analyzer_results.append(AnalyzerResult(
                contact=contact,
                chat_records=list(contact_records.values())
            ))

        logger.info(f"🪉 聚合完成，共生成{len(analyzer_results)}个分析结果对象")

        return analyzer_results


    #region 步骤6:将群聊记录中message_content里的"wxid_:"前缀替换为对应的nickname
    def _replace_wxid_with_nickname(self):
        """
        步骤6:将群聊记录中message_content里的"wxid_:"前缀替换为对应的nickname
        """
        # 步骤1: 检查是否需要执行替换操作
        if self.app_config.filter_config.filter_group_chat:
            logger.info("🥁 已过滤群聊，不执行wxid替换操作")
            return

        # 检查是否存在群组类型的联系人
        has_group = any(
            contact.type == ContactType.GROUP.value[1]
            for contact in self.mapping_cache.values()
        )
        if not has_group:
            logger.info("🥁 无群组类型联系人，不执行wxid替换操作")
            return

        # 步骤2: 获取联系人信息并建立映射字典
        # 假设target_value为None时获取所有联系人，实际使用时请根据需求调整
        contact_result = ContactDBService.get_contacts(None, False)
        logger.info(f"🥁 获取到[{len(contact_result)}]条联系人信息用于wxid映射")

        # 构建username到nickname的映射: 优先使用remark，否则使用nick_name
        username_to_nickname: Dict[str, str] = {}
        for contact in contact_result:
            username = contact.get('username')
            if not username:
                continue
            # 优先使用remark，为空则使用nick_name
            nickname = contact.get('remark') or contact.get('nick_name', username)
            username_to_nickname[username] = nickname

        # 步骤3: 遍历分析结果并执行替换操作
        for analyzer_result in self.analyzer_result:
            # 处理群聊记录中的扩展聊天记录
            for chat_record in analyzer_result.chat_records:
                # 替换当前聊天记录内容
                if self.app_config.stat_mode.mode_type == "target_to_self":
                    self._replace_wxid_content(chat_record, username_to_nickname)

                # 替换前置上下文记录
                for front_record in chat_record.context_front_records:
                    self._replace_wxid_content(front_record, username_to_nickname)

                # 替换后置上下文记录
                for last_record in chat_record.context_last_records:
                    self._replace_wxid_content(last_record, username_to_nickname)

        logger.info("🥁 翻译群聊成员昵称任务完成")


    @staticmethod
    def _replace_wxid_content(record: ChatRecordExtend | ChatRecordCore, mapping: Dict[str, str]) -> None:
        """
        替换单个记录中的message_content内容，返回是否发生替换
        """
        if record.real_sender_id == 1:
            return

        content = record.message_content
        # 匹配以wxid_开头的用户名前缀（包含冒号和可选的换行符）
        match = re.match(r'^(wxid_\w+):\n?', content)

        if not match:
            return

        username = match.group(1)
        nickname = mapping.get(username)
        if not nickname:
            logger.debug(f"⚠️ 未找到wxid[{username}]对应的昵称映射")

        # 执行替换
        original_prefix = match.group(0)
        record.message_content = content.replace(original_prefix, f'{nickname}:', 1)
        logger.debug(f"🥁 wxid替换完成: {username} -> {nickname}")
    #endregion

