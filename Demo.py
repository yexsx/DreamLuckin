from abc import ABC, abstractmethod
from typing import Dict, List, Tuple
from configs import AppConfig
from utils.pet_phrase_matcher import PetPhraseMatcher
from services.lucky_chat_db_service import LuckyChatDBService
import concurrent.futures
import math


class StatStrategy(ABC):
    """统计策略接口：统一SQL过滤+多线程统计流程"""

    @abstractmethod
    def build_sql(self, app_config: AppConfig) -> str:
        """构建包含所有SQL过滤条件的查询语句"""
        pass

    @abstractmethod
    def execute(self, app_config: AppConfig) -> Dict:
        """执行统计（SQL查询→多线程统计→结果汇总）"""
        pass


class SelfAllStrategy(StatStrategy):
    """策略1：自己所有聊天的口头禅统计"""

    def build_sql(self, app_config: AppConfig) -> str:
        """构建SQL（需用户补充实际表名/字段名）"""
        # 1. 基础过滤条件（SQL优先过滤）
        self_id = app_config.self_identifier
        time_condition = app_config.time_config.sql_time_condition
        filter_config = app_config.filter_config

        # 2. 消息类型过滤（假设msg_type字段存储字符串：'text'/'voice'/'image'等）
        # 需用户替换：msg_type字段名、文本消息类型标识（如'text'或数字1）
        filter_msg_types = filter_config.filter_msg_types
        msg_type_condition = f"msg_type NOT IN ({','.join([f"'{t}'" for t in filter_msg_types])})"

        # 3. 群聊过滤（假设is_group是布尔字段，0=单聊，1=群聊；或群聊ID含'@chatroom'）
        # 需用户替换：群聊判断字段/规则（如 talker LIKE '%@chatroom%'）
        group_condition = "is_group = 0" if filter_config.filter_group_chat else "1=1"

        # 4. 只查询需要的字段（content=消息内容，create_time=时间戳）
        # 需用户替换：表名（message）、content字段名、create_time字段名
        sql = f"""
            SELECT content, create_time 
            FROM message  -- 替换为实际消息表名
            WHERE talker = '{self_id}'  -- talker=发送方字段（替换为实际字段名）
              AND {time_condition}
              AND {msg_type_condition}
              AND {group_condition}
              AND LENGTH(content) >= {filter_config.min_phrase_length}  -- 提前过滤短消息
        """
        return sql.strip()

    def execute(self, app_config: AppConfig) -> Dict:
        print(f"📊 执行【自己所有聊天】统计模式")
        phrase_config = app_config.pet_phrase_config
        filter_config = app_config.filter_config
        result = {
            "mode": "self_all",
            "total_count": 0,
            "phrase_counts": {},
            "message_count": 0
        }

        try:
            # 步骤1：执行SQL查询（单线程批量获取，避免多连接竞争）
            sql = self.build_sql(app_config)
            print(f"🔍 执行SQL：{sql}")
            conn, cursor = LuckyChatDBService.create_connection()
            messages: List[Dict[str, any]] = []
            try:
                # 批量查询所有符合条件的消息（几万条数据无压力）
                LuckyChatDBService.execute_query(cursor, sql)
                # 转换为字典列表（便于后续处理）
                for row in cursor.fetchall():
                    messages.append({
                        "content": row[0],  # 对应SELECT的content字段
                        "create_time": row[1]  # 对应SELECT的create_time字段
                    })
                result["message_count"] = len(messages)
                print(f"📥 查询到 {len(messages)} 条符合条件的消息")
            finally:
                LuckyChatDBService.close_connection(conn)

            # 步骤2：无符合条件的消息，直接返回
            if len(messages) == 0:
                return result

            # 步骤3：多线程统计（拆分消息列表为子任务）
            max_workers = min(4, len(messages))  # 线程数=CPU核心数（4核→4线程）
            task_chunks = self._split_messages(messages, max_workers)

            # 步骤4：多线程并行执行统计
            phrase_total = {}
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                # 提交子任务
                futures = [
                    executor.submit(PetPhraseMatcher.batch_count_phrases, chunk, phrase_config)
                    for chunk in task_chunks
                ]
                # 汇总子任务结果
                for future in concurrent.futures.as_completed(futures):
                    chunk_result = future.result()
                    for phrase, count in chunk_result.items():
                        phrase_total[phrase] = phrase_total.get(phrase, 0) + count

            # 步骤5：排序（按配置的sort_by规则）
            sorted_phrase = self._sort_result(phrase_total, app_config.output_config.sort_by)

            # 步骤6：组装最终结果
            result["phrase_counts"] = sorted_phrase
            result["total_count"] = sum(sorted_phrase.values())
            return result

        except Exception as e:
            print(f"❌ 统计失败：{e}")
            result["error"] = str(e)
            return result

    @staticmethod
    def _split_messages(messages: List[Dict[str, any]], chunk_num: int) -> List[List[Dict[str, any]]]:
        """拆分消息列表为N个子任务（平均分配）"""
        chunk_size = math.ceil(len(messages) / chunk_num)
        return [
            messages[i * chunk_size: (i + 1) * chunk_size]
            for i in range(chunk_num)
        ]

    @staticmethod
    def _sort_result(phrase_counts: Dict[str, int], sort_by: str) -> Dict[str, int]:
        """按配置排序结果"""
        if sort_by == "count_desc":
            # 按出现次数降序
            return dict(sorted(phrase_counts.items(), key=lambda x: x[1], reverse=True))
        else:
            # 按口头禅字母升序
            return dict(sorted(phrase_counts.items(), key=lambda x: x[0]))


# 其他策略类（SelfToTargetStrategy、TargetToSelfStrategy）仅需修改build_sql方法的过滤条件
class SelfToTargetStrategy(StatStrategy):
    def build_sql(self, app_config: AppConfig) -> str:
        """构建SQL：自己→目标联系人的统计（需用户补充字段）"""
        self_id = app_config.self_identifier
        target_contact = app_config.stat_mode.target_contact
        time_condition = app_config.time_config.sql_time_condition
        filter_config = app_config.filter_config

        # 消息类型过滤（同SelfAllStrategy）
        filter_msg_types = filter_config.filter_msg_types
        msg_type_condition = f"msg_type NOT IN ({','.join([f"'{t}'" for t in filter_msg_types])})"

        # 群聊过滤（同SelfAllStrategy）
        group_condition = "is_group = 0" if filter_config.filter_group_chat else "1=1"

        # 需用户替换：表名、字段名（talker=发送方，receiver=接收方）
        sql = f"""
            SELECT content, create_time 
            FROM message 
            WHERE talker = '{self_id}' 
              AND receiver = '{target_contact}'  -- 接收方=目标联系人（替换为实际字段名）
              AND {time_condition}
              AND {msg_type_condition}
              AND {group_condition}
              AND LENGTH(content) >= {filter_config.min_phrase_length}
        """
        return sql.strip()

    def execute(self, app_config: AppConfig) -> Dict:
        # 复用SelfAllStrategy的执行流程，仅SQL不同
        print(f"📊 执行【自己对{app_config.stat_mode.target_contact}】统计模式")
        result = {
            "mode": "self_to_target",
            "target_contact": app_config.stat_mode.target_contact,
            "total_count": 0,
            "phrase_counts": {},
            "message_count": 0
        }

        try:
            # 步骤1：SQL查询（同SelfAllStrategy）
            sql = self.build_sql(app_config)
            conn, cursor = LuckyChatDBService.create_connection()
            messages: List[Dict[str, any]] = []
            try:
                LuckyChatDBService.execute_query(cursor, sql)
                for row in cursor.fetchall():
                    messages.append({"content": row[0], "create_time": row[1]})
                result["message_count"] = len(messages)
                print(f"📥 查询到 {len(messages)} 条符合条件的消息")
            finally:
                LuckyChatDBService.close_connection(conn)

            if len(messages) == 0:
                return result

            # 步骤2：多线程统计（完全复用工具类和拆分逻辑）
            max_workers = min(4, len(messages))
            task_chunks = SelfAllStrategy._split_messages(messages, max_workers)
            phrase_total = {}

            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = [
                    executor.submit(PetPhraseMatcher.batch_count_phrases, chunk, app_config.pet_phrase_config)
                    for chunk in task_chunks
                ]
                for future in concurrent.futures.as_completed(futures):
                    chunk_result = future.result()
                    for phrase, count in chunk_result.items():
                        phrase_total[phrase] = phrase_total.get(phrase, 0) + count

            # 步骤3：排序+汇总
            sorted_phrase = SelfAllStrategy._sort_result(phrase_total, app_config.output_config.sort_by)
            result["phrase_counts"] = sorted_phrase
            result["total_count"] = sum(sorted_phrase.values())
            return result

        except Exception as e:
            print(f"❌ 统计失败：{e}")
            result["error"] = str(e)
            return result


class TargetToSelfStrategy(StatStrategy):
    """策略3：目标联系人→自己的统计（仅build_sql不同，execute复用流程）"""

    def build_sql(self, app_config: AppConfig) -> str:
        self_id = app_config.self_identifier
        target_contact = app_config.stat_mode.target_contact
        time_condition = app_config.time_config.sql_time_condition
        filter_config = app_config.filter_config

        filter_msg_types = filter_config.filter_msg_types
        msg_type_condition = f"msg_type NOT IN ({','.join([f"'{t}'" for t in filter_msg_types])})"
        group_condition = "is_group = 0" if filter_config.filter_group_chat else "1=1"

        # 需用户替换：表名、字段名（talker=发送方→目标联系人，receiver=接收方→自己）
        sql = f"""
            SELECT content, create_time 
            FROM message 
            WHERE talker = '{target_contact}' 
              AND receiver = '{self_id}' 
              AND {time_condition}
              AND {msg_type_condition}
              AND {group_condition}
              AND LENGTH(content) >= {filter_config.min_phrase_length}
        """
        return sql.strip()

    def execute(self, app_config: AppConfig) -> Dict:
        # 完全复用SelfToTargetStrategy的execute逻辑，仅模式名称不同
        print(f"📊 执行【{app_config.stat_mode.target_contact}对自己】统计模式")
        result = {
            "mode": "target_to_self",
            "target_contact": app_config.stat_mode.target_contact,
            "total_count": 0,
            "phrase_counts": {},
            "message_count": 0
        }

        # 以下代码完全复制SelfToTargetStrategy的execute方法，无需修改
        try:
            sql = self.build_sql(app_config)
            conn, cursor = LuckyChatDBService.create_connection()
            messages: List[Dict[str, any]] = []
            try:
                LuckyChatDBService.execute_query(cursor, sql)
                for row in cursor.fetchall():
                    messages.append({"content": row[0], "create_time": row[1]})
                result["message_count"] = len(messages)
                print(f"📥 查询到 {len(messages)} 条符合条件的消息")
            finally:
                LuckyChatDBService.close_connection(conn)

            if len(messages) == 0:
                return result

            max_workers = min(4, len(messages))
            task_chunks = SelfAllStrategy._split_messages(messages, max_workers)
            phrase_total = {}

            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = [
                    executor.submit(PetPhraseMatcher.batch_count_phrases, chunk, app_config.pet_phrase_config)
                    for chunk in task_chunks
                ]
                for future in concurrent.futures.as_completed(futures):
                    chunk_result = future.result()
                    for phrase, count in chunk_result.items():
                        phrase_total[phrase] = phrase_total.get(phrase, 0) + count

            sorted_phrase = SelfAllStrategy._sort_result(phrase_total, app_config.output_config.sort_by)
            result["phrase_counts"] = sorted_phrase
            result["total_count"] = sum(sorted_phrase.values())
            return result

        except Exception as e:
            print(f"❌ 统计失败：{e}")
            result["error"] = str(e)
            return result


# 策略工厂（不变）
class StatStrategyFactory:
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