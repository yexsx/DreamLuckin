import datetime
import hashlib
import logging
from abc import ABC, abstractmethod
from typing import Dict, List

from exceptions import ContactNotFoundError, TargetTableNotFoundError
from parser import AppConfig
from services import ContactDBService, ChatRecordDBService
from utils import SQLBuilder
from .stat_models import ContactRecord, ChatRecord, StrategyResult

logger = logging.getLogger(__name__)

class StatStrategy(ABC):
    """统计策略抽象接口类（所有策略的基类）"""

    def __init__(
            self,
            chat_db_service: ChatRecordDBService,  # 聊天记录DB服务实例（LuckyChatDBService）
            contact_db_service: ContactDBService,  # 联系人DB服务实例（ContactDBService）
            app_config: AppConfig  # 全局配置实例（AppConfig）
    ):
        self.chat_db_service = chat_db_service
        self.contact_db_service = contact_db_service
        self.app_config = app_config
        # 缓存：映射关系（表名→联系人信息）
        self.mapping_cache: Dict[str, ContactRecord] = {}
        # 缓存：表处理结果（后续步骤复用）
        self.process_result: Dict[str, ChatRecord] = {}
        # 缓存：带上下文的核心记录
        self.context_result: Dict[str, List[Dict[str, any]]] = {}

    async def run(self) -> StrategyResult:
        """策略执行入口（统一串联所有步骤，无需重写）"""
        # 步骤1：获取映射关系
        self._associate_mapping()
        # 步骤2：获取待处理表
        pending_tables = await self._get_pending_tables()
        # 步骤3：处理表数据
        self.process_result = await self._process_tables(pending_tables)
        # 步骤4：回溯上下文
        self._backtrack_context()
        # 步骤5：聚合统计
        return self._aggregate_stat()


    @abstractmethod
    def _backtrack_context(self) -> None:
        """步骤4：回溯核心记录的上两条上下文
        处理self.process_result，补充上下文后存入self.context_result
        """
        pass

    @abstractmethod
    def _aggregate_stat(self) -> StrategyResult:
        """步骤5：按维度聚合统计
        返回：
            StrategyResult：聚合后的统计结果（含维度概览、明细等）
        """
        pass

    def _associate_mapping(self) -> None:
        """
            步骤1：预获取目标的全量映射（remark/nick_name→username→MD5→表名）
        """

        # 1. 从配置读取目标值（无需区分match_type，仅读目标值）
        target_value = self.app_config.stat_mode.target_contact_list  # 仅读取目标匹配值
        filter_group_chat = self.app_config.filter_config.filter_group_chat  # 过滤群聊配置

        # ========== 执行查询前日志（仅必要信息） ==========
        logger.info(f"🔍 开始查询联系人：目标值列表={target_value} | 过滤群聊={filter_group_chat}")

        # 2. 精准查询contact表（同时匹配remark和nick_name，OR条件）
        contact_result = self.contact_db_service.get_contacts(target_value, filter_group_chat)

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
            if local_type == 1:
                contact_type = "friend"  # 好友
            elif local_type == 2:
                contact_type = "group"  # 群聊
            elif local_type == 3:
                contact_type = "group_friend"  # 群友（没加过的）
            else:
                contact_type = "unknown"  # 未知类型（兜底）

            # 3.3 存入映射缓存（表名→联系人信息，自动覆盖重复key）
            self.mapping_cache[target_table_name] = ContactRecord(
                username=username,
                nickname=contact_name,
                type=contact_type,
                type_code=contact_info["local_type"]  # 对应原字典的type_code
            )

            logger.info(
                f"✅ 【映射缓存-{idx}/{len(contact_result)}】"
                f"联系人名称：{contact_name} | "
                f"类型：{contact_type}（原始local_type：{local_type}） | "
                f"username：{username} | "
                f"生成目标表名：{target_table_name}"
            )

        # ========== 仅新增未匹配日志（对齐_get_pending_tables风格） ==========
        if unmatched_config_values:
            for val in unmatched_config_values:
                logger.warning(f"⚠️ 配置值[{val}]未在联系人表中匹配到对应的联系人/群聊")

        logger.info(
            f"✅ 【映射缓存汇总】配置目标值总数：{len(target_value)} | "
            f"匹配到联系人数量：{len(contact_result)} | "
            f"未匹配的配置值数量：{len(unmatched_config_values)} | "
            f"缓存表名数量：{len(self.mapping_cache)}"
        )


    async def _get_pending_tables(self) -> List[str]:
        """
            步骤2：获取所有待处理表（适配多表），校验存在性并输出日志
            返回：List[str]：待处理的Msg表名列表
        """

        # 1,获取映射缓存中所有表名
        pending_table_names = list(self.mapping_cache.keys())
        total_pending = len(pending_table_names)

        # 2,调用封装方法批量校验表存在性（name IN 逻辑）
        table_seq_dict = await self.chat_db_service.check_tables_exist(pending_table_names)

        # 3,分情况处理表存在性逻辑
        valid_tables = []
        missing_contacts = []
        for table_name in pending_table_names:
            # 表不存在→记录缺失的联系人信息
            if table_name not in table_seq_dict:
                contact_info = self.mapping_cache[table_name]
                missing_contacts.append(
                    f"联系人[{contact_info.nickname}](类型：{contact_info.type})的聊天记录表[{table_name}]缺失"
                )
                continue

            # 表存在→输出日志（含总记录数）
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


    async def _process_tables(self, pending_tables: List[str]) -> dict[str, list[ChatRecord]]:
        """
            步骤3：处理表数据（协程）
            参数：
                pending_tables：_get_pending_tables返回的待处理表列表
            返回：
                Dict[str, list[ChatRecord]]：{表名: 聊天记录列表}
        """

        table_chat_records: Dict[str, List[ChatRecord]] = {}

        pet_phrase_config = self.app_config.pet_phrase_config

        # 1. 构建时间条件（所有表共用）
        time_condition = SQLBuilder.build_time_condition(self.app_config.time_config)
        # 2. 构建口头禅条件+参数（所有表共用）
        phrase_condition, phrase_params = SQLBuilder.build_phrase_condition(pet_phrase_config)

        logger.info(
            f"🔧 构建公共查询条件：待处理表数={len(pending_tables)} | "
            f"📝 口头禅列表={pet_phrase_config.pet_phrases}（匹配类型={pet_phrase_config.match_type}） | "
            f"🕒 时间范围={time_condition} | "
            f"🤖 仅查自己消息={True}"
        )

        for table_name in pending_tables:
            # 1. 调用DB服务获取原始记录（字典列表）
            raw_records = await self.chat_db_service.get_chat_records_by_phrase_and_time(
                table_name=table_name,
                phrase_condition=phrase_condition,
                phrase_params=phrase_params,
                time_condition=time_condition,
                only_self_msg=self.app_config.stat_mode.mode_type is not "target_to_self"
            )

            # 2. 转换为ChatRecord对象（核心：字典→结构化类）
            chat_records = []
            for raw in raw_records:
                # 匹配ChatRecord字段，补充matched_phrases（空列表兜底）
                chat_record = ChatRecord(
                    local_id=raw["local_id"],
                    message_content=raw["message_content"],
                    real_sender_id=raw["real_sender_id"],
                    create_time=datetime.datetime.fromtimestamp(raw["create_time"]),
                    matched_phrases=raw.get("matched_phrases", [])  # 若DB返回已匹配则直接用，否则空列表
                )
                chat_records.append(chat_record)

            # 3. 存入结果字典
            table_chat_records[table_name] = chat_records

            logger.info(f"📊 处理表完成：表名={table_name} | 有效记录数={len(chat_records)}")

        return table_chat_records