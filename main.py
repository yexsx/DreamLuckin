

def main():

    print("Lucky Luckin")
    # 1.加载原始配置文件（config.json）→
    # 2.调用ConfigParser校验配置 → 生成合法的AppConfig对象（含所有校验后的配置）→
    # 3.初始化DB连接（全局单连接，SQLite单连接足够，避免多连接竞争）→
    # 4.查询sqlite_master获取所有Msg_开头的聊天表名 →
    # 5.初始化策略工厂 → 根据AppConfig中的mode_type创建对应策略（如SelfAllStrategy）→
    # 6.逐个表执行核心流程（单线程 / 多线程可选）：
        # a.调用SQLBuilder拼接完整SQL（时间条件 + 文本过滤 + 口头禅条件）→
        # b.执行SQL查询（单表单查询，获取该表的符合条件的消息）→
        # c.文本预处理（TextPreprocessor）→ 口头禅统计（PetPhraseMatcher）→
    # 7.汇总所有表的统计结果 → 后续输出（CSV / JSON）


if __name__ == "__main__":
    main()