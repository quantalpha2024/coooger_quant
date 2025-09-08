import time
import pandas as pd
from clickhouse_driver import Client
from datetime import datetime
import numpy as np
import csv
# 连接ClickHouse数据库
ch_client = Client(
    host='cc-3nsqvaflp79lvvs06.clickhouse.ads.aliyuncs.com',
    port=3306,
    user='ft_ck_quant_01',
    password='lI4!vC4%nG4rmI8^iF6smK0',
    database='hk_perpetual',
    secure=False,  # 非SSL连接
    connect_timeout=15  # 设置连接超时
)


def check_database_connection():
    """检查数据库连接是否成功"""
    try:
        # 执行简单查询验证连接
        result = ch_client.execute('SELECT 1')
        print(f"✅ 数据库连接成功! Ping结果: {result[0][0]}")
        return True
    except Exception as e:
        print(f"❌ 连接失败: {str(e)}")
        print("可能原因：")
        print("1. 网络不通或防火墙阻挡")
        print("2. 认证信息错误")
        print("3. 数据库服务不可用")
        return False


def get_table_count():
    """查询数据库中的表数量"""
    try:
        # 执行查询获取表数量
        query = """
                SELECT count() AS table_count
                FROM system.tables
                WHERE database = 'hk_perpetual' \
                """
        result = ch_client.execute(query)
        return result[0][0]
    except Exception as e:
        print(f"❌ 查询失败: {str(e)}")
        return None


def get_table_list():
    """获取完整的表列表"""
    try:
        # 获取所有表名和引擎类型
        query = """
                SELECT *
                FROM system.tables
                WHERE database = 'hk_perpetual'

                """
        result = ch_client.execute(query)

        # 转换为DataFrame
        df = pd.DataFrame(result)

        return df
    except Exception as e:
        print(f"❌ 获取表列表失败: {str(e)}")
        return pd.DataFrame()


def get_t_order():
    """获取交易表信息"""
    try:
        # 获取所有表名和引擎类型
        query = """
                SELECT *
                FROM t_order limit 10

                """
        result = ch_client.execute(query)

        # 转换为DataFrame
        df = pd.DataFrame(result)

        return df
    except Exception as e:
        print(f"❌ 获取交易表失败: {str(e)}")
        return pd.DataFrame()


def get_t_account():
    """获取交易表信息"""
    try:
        # 获取所有表名和引擎类型
        query = """
                SELECT *
                FROM t_account limit 10

                """
        result = ch_client.execute(query)

        # 转换为DataFrame
        df = pd.DataFrame(result)

        return df
    except Exception as e:
        print(f"❌ 获取账户表失败: {str(e)}")
        return pd.DataFrame()


def get_t_d_accountdetail():
    """获取交易表信息（包含列名）"""
    # 第一步：获取表结构（列名）
    col_query = "DESCRIBE TABLE t_d_accountdetail"
    columns_info = ch_client.execute(col_query)
    column_names = [col[0] for col in columns_info]  # 提取列名
    print("表头（列名）:")
    print(column_names)  # 打印列名

    # 第二步：获取实际数据
    data_query = """
                 SELECT *
                 FROM t_d_accountdetail where MemberID in ('094dbc7a-f789-4e9e-b064-3d81dcb79e1c',
'bdf8c5ea-b5ab-4b40-b180-fd5ee2eb0760',
'ea727e8a-3e86-45c2-953e-e086c4606e65',
'0b5b0d55-5caf-4572-8428-8ab26ed89963',
'2af16590-3956-473d-80b5-818bdf78104c',
'9eeaf549-1e57-4ef4-9add-1b79846c61d0'

)
                 """
    result_data = ch_client.execute(data_query)

    # 第三步：创建带列名的DataFrame
    df = pd.DataFrame(result_data, columns=column_names)

    # 可选：打印DataFrame的前几行
    print("\n数据预览:")
    print(df.head(3))  # 只打印前3行避免过多输出
    return df
def get_t_d_trade():
    """获取交易表信息（包含列名）"""
    # 第一步：获取表结构（列名）
    col_query = "DESCRIBE TABLE t_d_trade"
    columns_info = ch_client.execute(col_query)
    column_names = [col[0] for col in columns_info]  # 提取列名
    print("表头（列名）:")
    print(column_names)  # 打印列名
    d=pd.DataFrame()
    for id in ['27630674-cb6a-46cd-97ca-664f5ea8986d',
'094dbc7a-f789-4e9e-b064-3d81dcb79e1c',
'b66b1925-9c36-453e-b67e-49ad7ae484bd',
'1586107a-b674-4a4c-a225-a7b6f06b33ea',
'bdf8c5ea-b5ab-4b40-b180-fd5ee2eb0760',
'ea727e8a-3e86-45c2-953e-e086c4606e65',
'632d31b8-0db0-4dc9-914f-5649cede1be5',
'3c349ca0-cfda-47ed-887f-8efd62621e3d',
'e33b79a9-fb04-4aa0-a621-243427048378',
'e8ed00ff-1196-4186-b7ca-4955a596a030',
'20ee72ed-2f4f-4a7f-a2da-1563652f260e',
'51c85974-553c-4a65-b504-a91b7f504b77',
'fe4b9cae-7e5d-456e-9635-2a0b5f63476c',
'2af16590-3956-473d-80b5-818bdf78104c',
'613cd50e-e42b-4ff7-99e4-bd2155ce7a81',
'bbe5e9bd-8f0a-4d0f-964e-23ce7ff6fdf5',
'aee74f73-ce40-4771-88dd-32edefd65e17',
'9eeaf549-1e57-4ef4-9add-1b79846c61d0',
'ea601caf-2c73-41d1-b4ad-91a5cd6ea910',
'aea6e82b-4e69-4676-96bc-5f6375b1fc33',
'5fd7a139-dbe7-4295-a695-85054371b410',
'109b56ab-eb81-4eb8-88d3-a3409ce56e05']:
       # 第二步：获取实际数据
       data_query = f"""
                 SELECT *
                 FROM t_d_trade where MemberID ='{id}'
                 """
       result_data = ch_client.execute(data_query)

       # 第三步：创建带列名的DataFrame
       df = pd.DataFrame(result_data, columns=column_names)
       d=d._append(df)
    # 可选：打印DataFrame的前几行
    print("\n数据预览:")
    print(d.head(3))  # 只打印前3行避免过多输出
    return d

if __name__ == "__main__":
    print(f"🕒 连接时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"🔗 目标数据库: hk_perpetual")

    # 检查连接
    if check_database_connection():
        # 获取表数量
        table_count = get_table_count()
        if table_count is not None:
            print(f"📊 数据库包含表数量: {table_count}")

            # 获取完整表列表
            tables_df = get_table_list()
            if not tables_df.empty:
                print("\n📋 表列表详情:")
                print(tables_df)
            else:
                print("⚠️ 未找到任何表")
        else:
            print("⚠️ 无法获取表数量信息")
        print("交易表:", get_t_order())
        print("账户表:", get_t_account())
        #d = get_t_d_accountdetail()
        #d.to_csv('7_t_d_accountdetail.csv')
        D=get_t_d_trade()
        D.to_csv('7_t_d_trade.csv')
