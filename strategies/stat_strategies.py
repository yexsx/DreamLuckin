from abc import ABC, abstractmethod
from typing import Dict, List, TypeAlias

from parser import AppConfig
from services import ContactDBService,ChatRecordDBService

# 类型别名：统一核心记录结构（所有策略复用）
CoreRecord: TypeAlias = Dict[str, int | str]
# 类型别名：统一表处理结果结构（单表/多表返回类型一致）
ProcessResult: TypeAlias = Dict[str, List[CoreRecord]]
# 类型别名：统一聚合统计结果结构
AggregateResult: TypeAlias = Dict[str, any]


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
        self.mapping_cache: Dict[str, Dict[str, str]] = {}
        # 缓存：表处理结果（后续步骤复用）
        self.process_result: ProcessResult = {}
        # 缓存：带上下文的核心记录
        self.context_result: Dict[str, List[Dict[str, any]]] = {}

    @abstractmethod
    def _associate_mapping(self) -> None:
        """步骤1：提前获取映射关系（单表/全量），结果存入mapping_cache"""
        pass

    @abstractmethod
    async def _get_pending_tables(self) -> List[str]:
        """步骤2：获取待处理的表列表
        返回：
            List[str]：待处理的Msg表名列表（单表策略返回长度为1的列表）
        """
        pass

    @abstractmethod
    def _process_tables(self, pending_tables: List[str]) -> ProcessResult:
        """步骤3：处理表数据（同步/协程）
        参数：
            pending_tables：_get_pending_tables返回的待处理表列表
        返回：
            ProcessResult：{表名: 核心记录列表}
        """
        pass

    @abstractmethod
    def _backtrack_context(self) -> None:
        """步骤4：回溯核心记录的上两条上下文
        处理self.process_result，补充上下文后存入self.context_result
        """
        pass

    @abstractmethod
    def _aggregate_stat(self) -> AggregateResult:
        """步骤5：按维度聚合统计
        返回：
            AggregateResult：聚合后的统计结果（含维度概览、明细等）
        """
        pass

    async def run(self) -> AggregateResult:
        """策略执行入口（统一串联所有步骤，无需重写）"""
        # 步骤1：获取映射关系
        self._associate_mapping()
        # 步骤2：获取待处理表
        pending_tables = await self._get_pending_tables()
        # 步骤3：处理表数据
        self.process_result = self._process_tables(pending_tables)
        # 步骤4：回溯上下文
        self._backtrack_context()
        # 步骤5：聚合统计
        return self._aggregate_stat()
