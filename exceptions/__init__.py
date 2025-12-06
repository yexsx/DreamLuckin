"""异常类统一导出入口（支持按模块分组导入）"""

# ====================== 1. 导入所有异常类 ======================
# 数据库异常
from .db_exceptions import (
    LuckyChatDBError,
    DBServiceNotPreloadedError,
    DBPreloadFailedError,
    DBConnectionNotInitializedError,
    SQLQueryFailedError
)

# 解析异常
from .parse_exceptions import (
    ParseBaseError,
    MissingRequiredFieldError,
    InvalidTypeError,
    ParseFileNotFoundError,
    InvalidValueError,
    DateFormatError
)

# ====================== 2. 定义分组（核心：按模块归类） ======================
# 数据库异常分组（仅包含数据库相关）
DB_EXCEPTIONS = (
    LuckyChatDBError,
    DBServiceNotPreloadedError,
    DBPreloadFailedError,
    DBConnectionNotInitializedError,
    SQLQueryFailedError
)

# 解析异常分组（仅包含解析相关）
PARSE_EXCEPTIONS = (
    ParseBaseError,
    MissingRequiredFieldError,
    InvalidTypeError,
    ParseFileNotFoundError,
    InvalidValueError,
    DateFormatError
)

# ====================== 3. 优化__all__（可选：控制import *的行为） ======================
# 若想让 `from exceptions import *` 只导入数据库异常（按需调整），可这样写：
# __all__ = [e.__name__ for e in DB_EXCEPTIONS]
# 若想保留所有异常（默认），则：
__all__ = [e.__name__ for e in DB_EXCEPTIONS + PARSE_EXCEPTIONS]