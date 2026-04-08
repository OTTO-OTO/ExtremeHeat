import pymysql

def connect_to_mysql():
    """连接到MySQL数据库"""
    connecton = ''
    try:
        # 连接到MySQL数据库
        connection = pymysql.connect(
            host='192.168.101.200',  # 替换为MySQL服务器的IP地址或主机名
            port=3307,
            user='czm',          # 替换为数据库用户名
            password='root',  # 替换为数据库密码
            database='spider_data',  # 替换为要连接的数据库名称，如果不需要则省略
            charset='utf8mb4'  # 指定字符集
        )

        with connection:
            with connection.cursor() as cursor:
                # 执行一个查询，例如选择当前数据库
                sql = "SELECT DATABASE();"
                cursor.execute(sql)
                result = cursor.fetchone()
                print("你连接到的数据库是：", result)

    except pymysql.MySQLError as e:
        print("连接失败：", e)

    finally:
        if connection.open:
            connection.close()
            print("MySQL连接已关闭")

if __name__ == "__main__":
    connect_to_mysql()