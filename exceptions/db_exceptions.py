"""数据库服务自定义异常类"""

class LuckyChatDBError(Exception):
    """所有数据库异常的基类（可用于批量捕获）"""
    pass

class DBServiceNotPreloadedError(LuckyChatDBError):
    """数据库服务未预加载异常（未调用init_instance）"""
    def __init__(self, message: str = "❌ 数据库服务未预加载，请先调用 init_instance(db_path)"):
        super().__init__(message)

class DBPreloadFailedError(LuckyChatDBError):
    """数据库预加载失败异常（init_instance时出错）"""
    def __init__(self, message: str = "❌ 数据库预加载失败"):
        super().__init__(message)

class DBConnectionNotInitializedError(LuckyChatDBError):
    """数据库连接未初始化异常（连接对象为空）"""
    def __init__(self, message: str = "❌ 数据库连接未初始化"):
        super().__init__(message)

class DBPoolExhaustedError(LuckyChatDBError):
    """数据库连接未初始化异常（连接对象为空）"""
    def __init__(self, max_connections:int, message: str = "❌ 连接池已耗尽"):
        self.max_connections = max_connections
        full_message = f"{message} (最大连接数: {max_connections})"
        super().__init__(full_message)

class SQLQueryFailedError(LuckyChatDBError):
    """SQL查询执行失败异常"""
    def __init__(self, sql: str, params: tuple, message: str = "❌ SQL 查询失败"):
        self.sql = sql
        self.params = params
        full_message = f"{message}（SQL: {sql}, params: {params}）"
        super().__init__(full_message)