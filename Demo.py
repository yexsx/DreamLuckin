from services import ContactDBService

contact_db_service = ContactDBService.init_instance("D:\programmer\soul\DreamLuckin\configs\contact_decrypted.db")

r = contact_db_service.get_instance()

result = r.execute_query("select * from contact")

print(result)



# TODO
# 1.数据库启动检验是否正确，有concat或message
# 2.实现类封装获取所有映射关系
# 3.main方法读取封装工具类
# 4.实现类自定义业务异常