# DreamLuckin 聊天记录分析系统文档

## 第一部分：项目概述与主程序流程

### 1. 项目概述

DreamLuckin 是一个基于 Python 的微信聊天记录分析系统，主要用于统计和分析微信聊天记录中的特定关键词（口头禅）使用情况。系统支持多种统计模式，能够根据配置的时间范围、联系人列表和关键词进行精准分析，并将结果导出为 JSON 格式。

#### 1.1 核心功能

- **多模式统计**：支持三种统计模式（自己全部消息、自己发给目标、目标发给自己）
- **关键词匹配**：支持包含匹配和精确匹配两种模式
- **上下文回溯**：自动获取关键词消息的前后上下文记录
- **并发优化**：使用异步协程和连接池技术，支持高并发数据库查询
- **群聊支持**：支持群聊消息分析，自动翻译群聊成员昵称
- **结果导出**：分析结果自动保存为 JSON 文件

#### 1.2 技术架构

- **异步编程**：基于 `asyncio` 实现异步数据库操作
- **连接池管理**：使用对象池模式管理数据库连接，提高性能
- **配置驱动**：通过 JSON 配置文件灵活控制分析行为
- **模块化设计**：采用分层架构，职责清晰

#### 1.3 项目结构

```
DreamLuckin/
├── main.py                    # 程序入口
├── chat_analyzer/             # 聊天记录分析器
│   ├── chat_record_analyzer.py
│   └── analyzer_models.py
├── parser/                    # 配置解析器
│   ├── parser.py
│   └── config_models.py
├── services/                  # 数据库服务层
│   ├── base/                  # 基础服务类
│   ├── builder/               # SQL 构建器
│   └── impl/                  # 具体实现
├── io_put/                    # 输入输出处理
│   ├── config_loader.py
│   ├── analyzer_result_saver.py
│   └── dataclass_output.py
├── exceptions/                 # 异常定义
├── log/                       # 日志配置
└── configs/                   # 配置文件目录
    └── config.json
```

### 2. 主程序流程（main.py）

主程序采用异步执行模式，整个流程分为 5 个主要步骤：

#### 2.1 程序启动流程

```python
asyncio.run(main())  # 程序入口
```

#### 2.2 详细执行步骤

**步骤 1：全局日志配置**
- 自动创建 `log_back` 文件夹（如果不存在）
- 生成带时间戳的日志文件（格式：`chat_stat_YYYYMMDD_HHMMSS.log`）
- 配置日志同时输出到控制台和文件

**步骤 2：读取并解析配置文件**
```python
config_dict = ConfigLoader.load_config()  # 加载 JSON 配置
app_config = ConfigParser.parse(config_dict)  # 解析并校验配置
```
- 从 `./configs/config.json` 读取配置（支持自定义路径）
- 解析并校验所有配置项的合法性
- 转换为结构化的 `AppConfig` 对象

**步骤 3：初始化数据库服务**
```python
# 初始化聊天记录数据库连接池（异步）
await ChatRecordDBService.init_pool(
    db_path=app_config.db_config.chat_db_path,
    max_connections=10,
    min_connections=3
)

# 初始化联系人数据库单例（同步）
ContactDBService.init_instance(app_config.db_config.contact_db_path)
```
- 聊天记录数据库：使用异步连接池，支持并发查询
- 联系人数据库：使用同步单例模式

**步骤 4：创建分析器并执行分析**
```python
analyzer = ChatRecordAnalyzer(app_config=app_config)
analyzer_result = await analyzer.run()
```

分析器内部执行流程（6 个子步骤）：
1. **获取映射关系**：根据配置的联系人列表，查询联系人表，建立联系人名称到数据库表名的映射
2. **获取待处理表**：校验目标表是否存在，过滤无效表
3. **处理表数据**：并发查询所有目标表，提取包含关键词的聊天记录
4. **回溯上下文**：为每条核心记录获取前后 N 条上下文消息
5. **聚合分析结果**：将处理结果按联系人聚合为 `AnalyzerResult` 列表
6. **翻译群聊名称**：将群聊消息中的 `wxid_xxx` 替换为对应的昵称

**步骤 5：保存分析结果**
```python
saved_path = save_analyzer_result_to_json(analyzer_result, app_config)
```
- 根据时间戳和关键词生成文件名
- 保存为 JSON 格式到配置的输出目录

#### 2.3 异常处理

程序包含完整的异常处理机制：

- **ParseBaseError**：配置解析/读取失败
- **LuckyChatDBError**：数据库初始化失败
- **AnalyzerBaseException**：统计策略执行失败
- **Exception**：其他未知错误

所有异常都会记录详细日志并退出程序。

#### 2.4 资源清理

在 `finally` 块中确保资源正确释放：
- 关闭联系人数据库连接
- 关闭聊天记录数据库连接池

---

## 第二部分：配置文件参数说明

### 配置文件位置

默认路径：`./configs/config.json`

支持自定义路径：`ConfigLoader.load_config("自定义路径")`

### 配置结构总览

```json
{
  "config_name": "配置名称（可选）",
  "db_config": { ... },
  "stat_mode": { ... },
  "time_config": { ... },
  "pet_phrase_config": { ... },
  "filter_config": { ... },
  "output_config": { ... }
}
```

---

### 1. db_config（数据库配置）

| 参数名 | 类型 | 必填 | 默认值 | 说明 | 取值范围/可选值 | 示例 |
|--------|------|------|--------|------|----------------|------|
| `chat_db_path` | string | 是 | - | 聊天记录数据库文件的完整路径 | 文件必须存在 | `"D:\\...\\message_0_decrypted.db"` |
| `contact_db_path` | string | 是 | - | 联系人数据库文件的完整路径 | 文件必须存在 | `"D:\\...\\contact_decrypted.db"` |
| `max_concurrency` | integer | 否 | `10` | 数据库查询的最大并发数，用于控制协程并发执行的数量 | `1` 到 `20` | `10` |
| `pool_min_connections` | integer | 否 | `4` | 数据库连接池的最小连接数 | 必须 `> 4` 且 `≤ pool_max_connections` | `8` |
| `pool_max_connections` | integer | 否 | `10` | 数据库连接池的最大连接数 | 必须 `≥ pool_min_connections` 且 `< 24` | `20` |

**配置示例**：
```json
{
  "db_config": {
    "contact_db_path": "D:\\programmer\\soul\\DreamLuckin\\configs\\contact_decrypted.db",
    "chat_db_path": "D:\\programmer\\soul\\DreamLuckin\\configs\\message_0_decrypted.db",
    "max_concurrency": 10,
    "pool_min_connections": 8,
    "pool_max_connections": 20
  }
}
```

---

### 2. stat_mode（统计模式配置）

| 参数名 | 类型 | 必填 | 默认值 | 说明 | 取值范围/可选值 | 示例 |
|--------|------|------|--------|------|----------------|------|
| `mode_type` | string | 是 | - | 统计模式类型 | `"self_all"`<br>`"self_to_target"`<br>`"target_to_self"` | `"self_to_target"` |
| `target_contact_list` | array<string> | 条件必填 | - | 目标联系人列表（通过备注名或昵称匹配） | **规则**：<br>- `mode_type = "self_all"` 时：必须为空数组 `[]`<br>- `mode_type = "self_to_target"` 或 `"target_to_self"` 时：必须是非空数组，且所有元素都是非空字符串 | `["成兮昂", "咖喱南", "60%", "许嵩", "KT"]` |

**模式说明**：
- `"self_all"`：统计自己发送的所有消息（不区分联系人）
- `"self_to_target"`：统计自己发送给目标联系人的消息
- `"target_to_self"`：统计目标联系人发送给自己的消息

**匹配规则**：系统会在联系人表中同时匹配 `remark`（备注）和 `nick_name`（昵称）

**配置示例**：
```json
{
  "stat_mode": {
    "mode_type": "self_to_target",
    "target_contact_list": ["成兮昂", "咖喱南", "60%", "许嵩", "KT"]
  }
}
```

---

### 3. time_config（时间配置）

| 参数名 | 类型 | 必填 | 默认值 | 说明 | 取值范围/可选值 | 示例 |
|--------|------|------|--------|------|----------------|------|
| `stat_dimension` | string | 是 | - | 统计时间维度 | `"day"`<br>`"week"`<br>`"month"` | `"month"` |
| `time_range_type` | string | 是 | - | 时间范围类型 | `"recent"`<br>`"custom"` | `"recent"` |
| `recent_num` | integer | 条件必填 | `7` | 最近 N 个时间单位（单位由 `stat_dimension` 决定） | 当 `time_range_type = "recent"` 时必填，`≥ 1` | `12`（最近 12 个月） |
| `custom_start_date` | string | 条件必填 | - | 自定义开始日期 | 当 `time_range_type = "custom"` 时必填，格式：`"YYYY-MM-DD"` | `"2025-01-01"` |
| `custom_end_date` | string | 条件必填 | - | 自定义结束日期 | 当 `time_range_type = "custom"` 时必填，格式：`"YYYY-MM-DD"`，必须晚于或等于 `custom_start_date` | `"2025-12-11"` |

**配置示例（最近模式）**：
```json
{
  "time_config": {
    "stat_dimension": "month",
    "time_range_type": "recent",
    "recent_num": 12
  }
}
```

**配置示例（自定义模式）**：
```json
{
  "time_config": {
    "stat_dimension": "month",
    "time_range_type": "custom",
    "custom_start_date": "2025-01-01",
    "custom_end_date": "2025-12-11"
  }
}
```

---

### 4. pet_phrase_config（口头禅配置）

| 参数名 | 类型 | 必填 | 默认值 | 说明 | 取值范围/可选值 | 示例 |
|--------|------|------|--------|------|----------------|------|
| `pet_phrases` | array<string> | 是 | - | 关键词（口头禅）列表，系统会统计包含这些关键词的消息 | 必须是非空数组，不能全是空字符串，会自动过滤空白字符 | `["好的", "rs4", "通行证"]` |
| `match_type` | string | 否 | `"contains"` | 关键词匹配模式 | `"contains"`：包含匹配<br>`"exact"`：精确匹配 | `"contains"` |
| `context_front_limit` | integer | 否 | `2` | 为每条匹配到的消息获取前 N 条上下文消息 | `0` 到 `10`，设置为 `0` 表示不获取前置上下文 | `2` |
| `context_end_limit` | integer | 否 | `2` | 为每条匹配到的消息获取后 N 条上下文消息 | `0` 到 `10`，设置为 `0` 表示不获取后置上下文 | `3` |

**配置示例**：
```json
{
  "pet_phrase_config": {
    "pet_phrases": ["好的", "rs4", "通行证"],
    "match_type": "contains",
    "context_front_limit": 2,
    "context_end_limit": 3
  }
}
```

---

### 5. filter_config（过滤配置）

| 参数名 | 类型 | 必填 | 默认值 | 说明 | 取值范围/可选值 | 示例 |
|--------|------|------|--------|------|----------------|------|
| `filter_group_chat` | boolean | 否 | `true` | 是否过滤群聊消息 | `true`：只统计单聊消息，过滤所有群聊<br>`false`：包含群聊消息 | `false` |

**说明**：设置为 `false` 时，系统会自动将群聊消息中的 `wxid_xxx` 替换为对应的昵称

**配置示例**：
```json
{
  "filter_config": {
    "filter_group_chat": false
  }
}
```

---

### 6. output_config（输出配置）

| 参数名 | 类型 | 必填 | 默认值 | 说明 | 取值范围/可选值 | 示例 |
|--------|------|--------|--------|------|----------------|------|
| `display_dimension` | string | 否 | `"month"` | 输出结果的统计维度（用于后续可视化，当前版本不影响实际统计） | `"year"`<br>`"month"`<br>`"day"` | `"month"` |
| `export_path` | string | 否 | `"./output/"` | 分析结果 JSON 文件的输出目录 | 必须是字符串类型，如果目录不存在，系统会自动创建 | `"./output/"` |

**文件命名规则**：`{时间戳}_{关键词组合}.json`
- 时间戳格式：`YYYY-MM-DD_HH-MM-SS`
- 关键词组合：取前 3 个关键词，用下划线连接

**配置示例**：
```json
{
  "output_config": {
    "display_dimension": "month",
    "export_path": "./output/"
  }
}
```

---

### 完整配置示例

```json
{
  "config_name": "梦瑞幸配置文件",
  "db_config": {
    "contact_db_path": "D:\\programmer\\soul\\DreamLuckin\\configs\\contact_decrypted.db",
    "chat_db_path": "D:\\programmer\\soul\\DreamLuckin\\configs\\message_0_decrypted.db",
    "max_concurrency": 10,
    "pool_min_connections": 8,
    "pool_max_connections": 20
  },
  "stat_mode": {
    "mode_type": "self_to_target",
    "target_contact_list": ["成兮昂", "咖喱南", "60%", "许嵩", "KT"]
  },
  "time_config": {
    "stat_dimension": "month",
    "recent_num": 12,
    "time_range_type": "recent",
    "custom_start_date": "2025-01-01",
    "custom_end_date": "2025-12-11"
  },
  "pet_phrase_config": {
    "pet_phrases": ["好的", "rs4", "通行证"],
    "match_type": "contains",
    "context_front_limit": 2,
    "context_end_limit": 3
  },
  "filter_config": {
    "filter_group_chat": false
  },
  "output_config": {
    "display_dimension": "month",
    "export_path": "./output/"
  }
}
```

---

## 第三部分：ChatRecordAnalyzer 实现流程

`ChatRecordAnalyzer` 是系统的核心业务类，负责执行完整的聊天记录分析流程。其 `run()` 方法串联了 6 个关键步骤：

### 3.1 整体流程概览

```python
async def run(self) -> List[AnalyzerResult]:
    # 步骤1：获取映射关系
    self.mapping_cache = self._associate_mapping()
    # 步骤2：获取待处理表
    pending_tables = await self._get_pending_tables()
    # 步骤3：处理表数据（并发）
    self.process_result = await self._process_tables(pending_tables)
    # 步骤4：回溯上下文（并发）
    self.backtracked_front_record, self.backtracked_last_record = await self._backtrack_context()
    # 步骤5：聚合分析结果
    self.analyzer_result = self._aggregate_analyzer_results()
    # 步骤6：翻译wxid群聊名称
    self._replace_wxid_with_nickname()
    
    return self.analyzer_result
```

### 3.2 步骤详解

#### 步骤 1：获取映射关系（`_associate_mapping`）

**功能**：建立联系人名称到数据库表名的映射关系

**执行流程**：
1. 从配置读取目标联系人列表和群聊过滤配置
2. 查询联系人表，同时匹配 `remark`（备注）和 `nick_name`（昵称）
3. 对每个匹配到的联系人：
   - 提取 `username` 并计算 MD5 值
   - 生成目标表名：`Msg_{md5_username}`
   - 构造 `ContactRecord` 对象（包含用户名、昵称、类型等）
   - 存入映射缓存
4. 输出未匹配的配置值警告（如果有）
5. 返回映射缓存：`{表名: ContactRecord}`

**关键点**：
- 支持同时匹配备注和昵称（OR 条件）
- 自动识别联系人类型（好友/群聊）
- 提供详细的匹配日志

#### 步骤 2：获取待处理表（`_get_pending_tables`）

**功能**：校验目标表是否存在，过滤无效表

**执行流程**：
1. 从映射缓存获取所有表名
2. 批量校验表存在性（通过查询 `sqlite_sequence` 表）
3. 分别处理：
   - **有效表**：记录表名和总记录数，加入待处理列表
   - **缺失表**：记录警告日志，但不中断流程
4. 如果所有表都缺失，抛出异常
5. 返回有效表名列表（按记录数降序排序）

**关键点**：
- 批量校验减少数据库调用
- 部分表缺失不影响整体流程
- 提供详细的校验日志

#### 步骤 3：处理表数据（`_process_tables`）⭐ 并发优化

**功能**：并发查询所有目标表，提取包含关键词的聊天记录

**执行流程**：
1. 构建公共查询条件：
   - 时间条件（根据 `time_config`）
   - 口头禅条件（根据 `pet_phrase_config`）
   - 命中关键词 SQL 片段
2. 创建信号量限制并发数（`max_concurrency`）
3. 为每个表创建协程任务：
   ```python
   async def process_single_table(tbl_name: str):
       async with semaphore:  # 并发控制
           # 查询数据库获取原始记录
           raw_records = await ChatRecordDBService.get_chat_records_by_phrase_and_time(...)
           # 转换为 ChatRecordCommon 对象
           records_dict = {...}
           return tbl_name, records_dict
   ```
4. 使用 `asyncio.gather()` 并发执行所有任务
5. 整理结果并返回：`{表名: {local_id: ChatRecordCommon}}`

**关键点**：
- 使用 `asyncio.Semaphore` 控制并发数，避免数据库连接池耗尽
- 所有表的查询并发执行，大幅提升性能
- 结果以 `local_id` 为 key 的字典结构存储，便于后续处理

#### 步骤 4：回溯上下文（`_backtrack_context`）⭐ 并发优化

**功能**：为每条核心记录获取前后 N 条上下文消息

**执行流程**：
1. 计算所有核心记录的前后上下文 ID：
   - 前向：`core_id - 1, core_id - 2, ..., core_id - context_front_limit`
   - 后向：`core_id + 1, core_id + 2, ..., core_id + context_end_limit`
2. 创建信号量限制并发数（`max_concurrency`）
3. 为每个核心 ID 创建协程任务：
   ```python
   async def process_context_for_core_id(tbl_name, core_id, front_ids, last_ids):
       async with semaphore:  # 并发控制
           front_ctx = await self._get_and_convert_context_records(tbl_name, front_ids)
           last_ctx = await self._get_and_convert_context_records(tbl_name, last_ids)
           return tbl_name, core_id, front_ctx, last_ctx
   ```
4. 使用 `asyncio.gather()` 并发执行所有上下文查询任务
5. 整理结果并返回：`({表名: {core_id: [前向上下文]}}, {表名: {core_id: [后向上下文]}})`

**关键点**：
- 所有核心 ID 的上下文查询并发执行，充分利用连接池
- 批量查询减少数据库调用次数
- 自动过滤无效 ID（如负数）

#### 步骤 5：聚合分析结果（`_aggregate_analyzer_results`）

**功能**：将各环节处理结果聚合为 `AnalyzerResult` 列表

**执行流程**：
1. 遍历映射缓存中的每个联系人
2. 对于每个联系人：
   - 从 `process_result` 获取基础聊天记录
   - 转换为 `ChatRecordExtend` 对象（包含上下文字段）
   - 从 `backtracked_front_record` 填充前置上下文
   - 从 `backtracked_last_record` 填充后置上下文
3. 构建 `AnalyzerResult` 对象（包含联系人信息和聊天记录列表）
4. 返回完整的分析结果列表

**关键点**：
- 按联系人聚合，便于后续按联系人维度分析
- 完整的上下文信息，便于理解消息语境

#### 步骤 6：翻译群聊名称（`_replace_wxid_with_nickname`）

**功能**：将群聊消息中的 `wxid_xxx` 替换为对应的昵称

**执行流程**：
1. 检查是否需要执行替换（如果过滤群聊则跳过）
2. 检查是否存在群组类型的联系人
3. 获取所有联系人信息，建立 `username → nickname` 映射
4. 遍历所有分析结果：
   - 替换核心记录的 `message_content`
   - 替换前置上下文记录的 `message_content`
   - 替换后置上下文记录的 `message_content`
5. 使用正则表达式匹配 `wxid_xxx:` 前缀并替换

**关键点**：
- 只在 `target_to_self` 模式下替换（因为是自己收到的消息）
- 优先使用 `remark`，其次使用 `nick_name`
- 自动跳过自己发送的消息（`real_sender_id = 1`）

---

## 第四部分：项目设计亮点

### 4.1 双模式数据库服务架构

项目采用了两种不同的数据库服务模式，针对不同的使用场景进行了优化：

#### 4.1.1 同步数据库服务（`LuckyDBBaseServiceSync`）

**设计思想**：单例模式 + 预加载连接

**适用场景**：联系人数据库（查询频率低、数据量小）

**核心特点**：

1. **单例模式**：
   ```python
   _instance: Optional["LuckyDBBaseServiceSync"] = None
   _db_connection: Optional[sqlite3.Connection] = None
   ```
   - 整个程序生命周期只有一个实例
   - 避免重复创建连接，节省资源

2. **预加载机制**：
   ```python
   @classmethod
   def init_instance(cls, db_path: str):
       if cls._instance is not None:
           return cls._instance  # 已初始化直接返回
       cls._instance = super().__new__(cls)
       cls._instance._init_db(db_path)
       return cls._instance
   ```
   - 程序启动时一次性初始化
   - 后续直接使用，无需重复初始化

3. **同步执行**：
   - 使用 `sqlite3` 标准库
   - 操作简单直接，适合低频查询
   - 无需考虑并发问题

**优势**：
- 实现简单，维护成本低
- 资源占用少（单个连接）
- 适合低频、小数据量查询

#### 4.1.2 异步数据库服务（`LuckyDBPoolServiceAsync`）

**设计思想**：连接池模式 + 异步并发

**适用场景**：聊天记录数据库（查询频率高、数据量大、需要并发）

**核心特点**：

1. **连接池管理**：
   ```python
   _pool: Optional[asyncio.Queue] = None
   _max_connections: int = 10
   _min_connections: int = 5
   ```
   - 使用 `asyncio.Queue` 管理连接池
   - 预创建最小连接数，按需扩展到最大连接数
   - 连接复用，避免频繁创建/销毁

2. **异步上下文管理器**：
   ```python
   @asynccontextmanager
   async def acquire_connection(cls):
       conn = await cls.get_connection()
       try:
           yield conn
       finally:
           await cls.release_connection(conn)
   ```
   - 自动获取和释放连接
   - 异常安全，确保连接正确释放
   - 使用方式：`async with cls.acquire_connection() as conn:`

3. **连接有效性检测**：
   ```python
   async def get_connection(cls) -> "PooledConnection":
       conn = await cls._pool.get()
       pooled_conn = PooledConnection(conn, service_cls=cls)
       if await pooled_conn.is_valid():
           return pooled_conn
       # 连接无效则创建新连接替换
       new_conn = await cls._create_connection()
       return PooledConnection(new_conn, service_cls=cls)
   ```
   - 获取连接时自动检测有效性
   - 无效连接自动替换，保证连接质量

4. **只读优化**：
   ```python
   db_uri = f"file:{cls._db_path}?mode=ro"  # 只读模式
   await conn.execute("PRAGMA query_only = 1")  # 强制只读
   await conn.execute("PRAGMA cache_size = -20000")  # 20MB 缓存
   await conn.execute("PRAGMA synchronous = OFF")  # 禁用同步
   ```
   - URI 模式指定只读，防止误操作
   - 优化缓存和同步策略，提升查询性能

5. **资源清理**：
   ```python
   async def close_pool(cls):
       cls._is_initialized = False  # 防止新连接获取
       while True:
           try:
               conn = cls._pool.get_nowait()
               await conn.close()
           except asyncio.QueueEmpty:
               break
   ```
   - 使用 `get_nowait()` 避免阻塞
   - 确保所有连接正确关闭
   - 防止程序无法正常退出

**优势**：
- 支持高并发查询，充分利用数据库性能
- 连接复用，减少资源消耗
- 自动管理连接生命周期，降低出错概率

### 4.2 并发协程优化

项目在 `ChatRecordAnalyzer` 的两个关键方法中使用了并发协程优化，大幅提升了处理性能：

#### 4.2.1 `_process_tables` 方法的并发优化

**问题**：如果顺序处理多个表，总耗时 = 表1耗时 + 表2耗时 + ... + 表N耗时

**解决方案**：使用 `asyncio.Semaphore` + `asyncio.gather()` 实现并发处理

**实现细节**：

```python
# 1. 创建信号量限制并发数
semaphore = asyncio.Semaphore(max_concurrency)

# 2. 定义单个表的处理协程
async def process_single_table(tbl_name: str):
    async with semaphore:  # 获取信号量，控制并发
        # 执行数据库查询
        raw_records = await ChatRecordDBService.get_chat_records_by_phrase_and_time(...)
        # 处理数据
        records_dict = {...}
        return tbl_name, records_dict

# 3. 创建所有任务
tasks = [process_single_table(table_name) for table_name in pending_tables]

# 4. 并发执行所有任务
results = await asyncio.gather(*tasks)
```

**性能提升**：
- 假设有 10 个表，每个表查询耗时 1 秒
- 顺序执行：10 秒
- 并发执行（max_concurrency=10）：约 1 秒（几乎同时完成）
- **性能提升约 10 倍**

**关键设计**：
- `Semaphore` 限制并发数，避免连接池耗尽
- 所有任务并发执行，充分利用 I/O 等待时间
- 结果自动聚合，无需手动管理

#### 4.2.2 `_backtrack_context` 方法的并发优化

**问题**：如果有 100 个核心记录，每个记录需要查询前后上下文，顺序执行会很慢

**解决方案**：将所有上下文查询任务并发执行

**实现细节**：

```python
# 1. 创建信号量
semaphore = asyncio.Semaphore(max_concurrency)

# 2. 定义单个核心ID的上下文处理协程
async def process_context_for_core_id(tbl_name, core_id, front_ids, last_ids):
    async with semaphore:
        # 并发查询前后上下文
        front_ctx = await self._get_and_convert_context_records(tbl_name, front_ids)
        last_ctx = await self._get_and_convert_context_records(tbl_name, last_ids)
        return tbl_name, core_id, front_ctx, last_ctx

# 3. 收集所有任务
context_tasks = []
for table_name in self.process_result.keys():
    for core_local_id in self.process_result[table_name].keys():
        context_tasks.append(
            process_context_for_core_id(table_name, core_local_id, front_ids, last_ids)
        )

# 4. 并发执行所有任务
context_results = await asyncio.gather(*context_tasks)
```

**性能提升**：
- 假设有 100 个核心记录，每个上下文查询耗时 0.1 秒
- 顺序执行：100 × 0.1 = 10 秒
- 并发执行（max_concurrency=10）：约 1 秒（10 个并发批次）
- **性能提升约 10 倍**

**关键设计**：
- 将所有核心 ID 的上下文查询任务统一收集
- 使用 `gather()` 一次性并发执行所有任务
- 通过 `Semaphore` 控制并发数，保护数据库连接池

### 4.3 设计模式总结

| 设计模式 | 应用场景 | 优势 |
|---------|---------|------|
| **单例模式** | 同步数据库服务 | 资源节约，实现简单 |
| **对象池模式** | 异步数据库服务 | 连接复用，支持高并发 |
| **门面模式** | 配置加载、数据转换 | 简化接口，隐藏复杂性 |
| **策略模式** | 统计模式选择 | 灵活扩展，易于维护 |
| **工厂模式** | 分析器创建 | 解耦创建逻辑 |

### 4.4 性能优化总结

1. **数据库层面**：
   - 连接池复用，减少连接创建开销
   - 只读优化，提升查询性能
   - 批量查询，减少数据库调用次数

2. **并发层面**：
   - 协程并发，充分利用 I/O 等待时间
   - 信号量控制，避免资源竞争
   - 异步上下文管理器，确保资源正确释放

3. **算法层面**：
   - 批量校验表存在性
   - 字典结构存储，O(1) 查找
   - 提前过滤无效数据

