"""parse_exceptions.py 优化版（推荐）"""
class ParseBaseError(Exception):
    """配置解析异常基类"""
    def __init__(self, message: str):
        super().__init__(message)  # 传递提示语给父类
        self.message = message     # 可选：保存message为实例属性，方便上层获取

class MissingRequiredFieldError(ParseBaseError):
    """缺少必填配置项异常"""
    def __init__(self, message: str = "缺少必填的配置项"):
        super().__init__(message)

class InvalidTypeError(ParseBaseError):
    """配置项类型错误异常"""
    def __init__(self, message: str = "配置项类型不符合要求"):
        super().__init__(message)

class ParseFileNotFoundError(ParseBaseError):  # 重命名：避免覆盖Python内置的FileNotFoundError
    """配置文件不存在异常"""
    def __init__(self, message: str = "配置指定的文件不存在"):
        super().__init__(message)

class InvalidValueError(ParseBaseError):
    """配置项值无效异常"""
    def __init__(self, message: str = "配置项值不符合要求"):
        super().__init__(message)

class DateFormatError(ParseBaseError):
    """日期格式错误异常"""
    def __init__(self, message: str = "日期格式错误（需YYYY-MM-DD）"):
        super().__init__(message)