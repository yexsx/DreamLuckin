import concurrent.futures
from services.lucky_chat_db_service import LuckyChatDBService


def thread_task(task_name: str, sql: str) -> None:
    """单个线程任务（仅执行基础查询，验证多线程连接）"""
    conn, cursor = None, None
    try:
        print(f"🚀 线程 {task_name} 启动，执行查询...")
        # 每个线程创建独立连接
        conn, cursor = LuckyChatDBService.create_connection()
        # 执行查询（示例：查询消息表前5条数据）
        result = LuckyChatDBService.execute_query(cursor, sql)
        print(f"✅ 线程 {task_name} 执行成功，查询结果条数：{len(result)}")
    except Exception as e:
        print(f"❌ 线程 {task_name} 执行失败：{e}")
    finally:
        # 关闭当前线程连接
        LuckyChatDBService.close_connection(conn)


def main():
    # 1. 初始化配置
    LuckyChatDBService.init_config(config_path="Reference/config.json")

    # 2. 定义2个简单查询任务（多线程并行执行）
    tasks = [
        ("任务1", "select message_content from Msg_c7d86c7f53baf9b37e5df2e0dd0b0305 where local_type = 1"),  # 查前5条消息
        ("任务2", "select message_content from Msg_c7d86c7f53baf9b37e5df2e0dd0b0305 where local_type = 1")  # 查消息总数
    ]

    # 3. 多线程执行（2个线程）
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        for task_name, sql in tasks:
            executor.submit(thread_task, task_name, sql)


if __name__ == "__main__":
    main()