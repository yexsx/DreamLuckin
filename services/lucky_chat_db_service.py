import json
import os
import sqlite3
from dataclasses import dataclass
from typing import Optional, Tuple


@dataclass
class DBConfig:
    db_path: str
    self_identifier: str


class LuckyChatDBService:
    """微信聊天数据库工具类（仅实现核心连接功能，多线程安全）"""

    _global_config: Optional[DBConfig] = None

    @classmethod
    def init_config(cls, config_path: str) -> None:
        """初始化全局配置（程序启动时调用一次）"""
        if not os.path.exists(config_path):
            raise FileNotFoundError(f"配置文件不存在：{config_path}")

        with open(config_path, 'r', encoding='utf-8') as f:
            config_dict = json.load(f)

        # 验证必填字段
        required_fields = ["db_path", "self_identifier"]
        db_conn_config = config_dict.get("db_connection", {})
        for field in required_fields:
            if field not in db_conn_config:
                raise KeyError(f"配置文件缺少必填字段：db_connection.{field}")

        cls._global_config = DBConfig(
            db_path=db_conn_config["db_path"],
            self_identifier=db_conn_config["self_identifier"]
        )
        print(f"✅ 配置初始化成功！")

    @classmethod
    def create_connection(cls) -> Tuple[sqlite3.Connection, sqlite3.Cursor]:
        """创建独立数据库连接（每个线程调用一次）"""
        if not cls._global_config:
            raise RuntimeError("请先调用 init_config() 初始化配置")

        db_path = cls._global_config.db_path
        if not os.path.exists(db_path):
            raise FileNotFoundError(f"数据库文件不存在：{db_path}")

        try:
            conn = sqlite3.connect(db_path, check_same_thread=False)
            cursor = conn.cursor()
            return conn, cursor
        except sqlite3.Error as e:
            raise ConnectionError(f"创建连接失败：{e}")

    @staticmethod
    def close_connection(conn: sqlite3.Connection) -> None:
        """关闭数据库连接"""
        if conn:
            try:
                conn.close()
            except sqlite3.Error as e:
                print(f"⚠️  关闭连接失败：{e}")

    @staticmethod
    def execute_query(cursor: sqlite3.Cursor, sql: str, params: Optional[tuple] = None) -> list:
        """执行查询（仅读操作）"""
        try:
            cursor.execute(sql, params) if params else cursor.execute(sql)
            return cursor.fetchall()
        except sqlite3.Error as e:
            raise RuntimeError(f"查询失败：SQL={sql}，错误={e}")