import time
import pandas as pd
from clickhouse_driver import Client
from datetime import datetime
import numpy as np
import csv
def calc_pl(df):
    df['CreateTime'] = pd.to_datetime(df['CreateTime'])
    df['Amount'] = df['Amount'].astype(float)
    df['Balance'] = df['Balance'].astype(float)
    df = df.sort_values(by=['CreateTime', 'AccountDetailID'])
    df.index = [i for i in range(len(df))]
    shares = 0
    pl = 1
    for i in range(0, len(df)):
        if df['Source'].iloc[i] == '3' or df['Source'].iloc[i] == '4':  # 申购和赎回，增加或减少份额

            if pl<0.01:
                pl=1
            shares = shares + df['Amount'].iloc[i] / pl
            if i==0:
                df.loc[i, 'pl']=1
            elif df['Balance'].iloc[i] < 1:  # 如果账户资金小于1U，清盘操作
                shares = 0
                df.loc[i, 'shares'] = shares
                df.loc[i, 'pl'] = df.loc[i - 1, 'pl']
            else:
                pl = df['Balance'].iloc[i] / shares
                df.loc[i, 'shares'] = shares
                df.loc[i, 'pl'] = pl
        else:
            if shares == 0:
                try:
                   pl = df.loc[i - 1, 'pl']
                except:
                    pl=1
            else:
                pl = df['Balance'].iloc[i] / shares
            df.loc[i, 'shares'] = shares
            df.loc[i, 'pl'] = pl
    return df
def resample_pl(df,rule="1D"):
    '''
    净值曲线
    '''
    rule = rule.upper()
    if rule[-1:] == 'M':
        rule = rule[:-1] + 'T'
    agg_rule = {
        'pl': 'last'
    }
    cols = [k for k in agg_rule.keys()]
    cols.append('CreateTime')
    temp = df[cols].resample(rule=rule, closed='left', label='left', on='CreateTime').agg(agg_rule)
    temp['pl']=temp['pl'].fillna(method='ffill')
    temp.reset_index(inplace=True)
    return temp
def calc_annualized_return(df):
        if df['pl'].iloc[-1]<0:
            total_return=-1
        else:
            total_return = df['pl'].iloc[-1] -1

        total_return = float(total_return)
        trading_days = (df['CreateTime'].iloc[-1] - df['CreateTime'].iloc[0]).days + 1
        return round((1 + total_return) ** (365 / trading_days) - 1,4)
def calc_max_drawdown(df):
        df['pl']=df['pl'].astype(float)
        if df['pl'].iloc[-1] < 0:
            return -1
        peak = df['pl'].expanding().max()
        dd = (peak - df['pl']) / peak
        return round(dd.max(), 4)
def calculate_calmar_ratio(df):
        if df['pl'].iloc[-1] < 0:
            return 0
        return round(calc_annualized_return(df)/calc_max_drawdown(df),4)
def calc_sortino_ratio(df):
        if df['pl'].iloc[-1] < 0:
           return -1
        df=resample_pl(df)
        annualized_return =calc_annualized_return(df)
        df['daily_returns'] = df['pl'].pct_change().dropna()
        downside_returns = df[df['daily_returns'] < 0]
        downside_risk = downside_returns['daily_returns'].std() * np.sqrt(365)
        sortino = annualized_return / downside_risk
        return round(sortino, 4)

def calc_sharpe_ratio(df):
        df = resample_pl(df)
        df['daily_returns'] = df['pl'].pct_change().dropna()
        excess_returns = df['daily_returns']
        sharpe = np.sqrt(365) * excess_returns.mean() / excess_returns.std()
        return round(sharpe, 4)

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
                FROM t_order
               limit 10

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
                FROM t_account
               limit 10

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
                     FROM t_d_accountdetail LIMIT 1000 \
                     """
    result_data = ch_client.execute(data_query)

    # 第三步：创建带列名的DataFrame
    df = pd.DataFrame(result_data, columns=column_names)

    # 可选：打印DataFrame的前几行
    print("\n数据预览:")
    print(df.head(3))  # 只打印前3行避免过多输出

    data_query=f"""  SELECT DISTINCT MemberID FROM t_d_accountdetail where date(CreateTime) > '2025-07-15' and Balance>50000 """
    result_data = ch_client.execute(data_query)

    # 第三步：创建带列名的DataFrame
    MemberID = pd.DataFrame(result_data,columns=['MemberID'])#, columns=column_names)
    print(MemberID.shape[0])
    result=pd.DataFrame()
    result.index.name='MemberID'
    N=0
    for memberid in [ '27630674-cb6a-46cd-97ca-664f5ea8986d',
    '094dbc7a-f789-4e9e-b064-3d81dcb79e1c',
    '55f43ede-9015-4ad8-ad67-17cabe545bcd',
    '0fbe6a09-1999-412c-9650-133de9e02abe',
    'd869e392-b729-4103-8178-08fe07ee9ad6',
    'e9f9bbbf-8203-4217-9210-67c7763dc5e6',
    '8035d1cd-9097-4ab3-8687-46ec9d5e17c6',
    'e9bbe0b7-9c82-491c-953f-7bf1b3d058be',
    '5288de52-ebdf-426c-a22b-d648de282d14',
    '3cbbb90e-9795-4d0e-bdb0-da1852031df9',
    '44488658-3c4e-4c03-9696-04e909487c90',
    '7729f6d6-552c-4a3e-ab55-010ea4f07b8c',
    'b66b1925-9c36-453e-b67e-49ad7ae484bd',
    '1586107a-b674-4a4c-a225-a7b6f06b33ea',
    '23f14b82-fa8e-48f1-b485-265b11bcbea6',
    'bdf8c5ea-b5ab-4b40-b180-fd5ee2eb0760',
    'ea727e8a-3e86-45c2-953e-e086c4606e65',
    'a891c7ef-bad4-4b14-9382-4a9f56749b8d',
    '5734fbbc-627e-43ec-9127-71c8b3d95c15',
    'fb37fb4a-b481-4d5f-9e51-0c1898feff5c',
    '791e2418-c54e-4dfe-8484-191979490993',
    '56dcd787-d286-4305-82e1-a08810a799f2',
    '68c362d2-40cb-44e2-a121-b5bf17011d39',
    '1d263ef5-1b88-4ba0-9fd4-ff2d12c448e9',
    'eb2eeaf8-3fac-4fdc-81d6-f8f59580adfa',
    '10ca00fc-5ba3-407f-9732-efa80d0510a7',
    'aa555660-5386-401c-a60c-c74d60d9a998',
    '632d31b8-0db0-4dc9-914f-5649cede1be5',
    '95c87e2c-631c-401b-bd94-95b2fbfe0806',
    '0699edbe-ea93-4e96-8e29-cfbc3330d397',
    '42ff82e7-bbe1-49d4-a9c8-6b21f9de2bd4',
    '4a2769fe-61f1-4d76-81e2-c84998f39e62',
    '85673a94-9c24-49c1-9f07-b62b6d0e6ee7',
    'bdf8fa6e-0a80-4857-8b94-8508577c77f4',
    'cb5d23cf-4040-4f6f-8308-1cbd48d70322',
    '3c349ca0-cfda-47ed-887f-8efd62621e3d',
    '1537391f-d0ed-4cc5-b9b3-a5a6f9a3060f',
    '0e8c6847-b51e-4004-8986-12ed4c18d5a1',
    '6e98cbaa-84e3-4796-835c-40f00c5e191e',
    '3b4e1bb7-ae6f-43c0-96a7-a95bbcfa3006',
    '0b5b0d55-5caf-4572-8428-8ab26ed89963',
    'e33b79a9-fb04-4aa0-a621-243427048378',
    '2579772c-7b02-4f19-9a3d-d16a89086ab7',
    'e8ed00ff-1196-4186-b7ca-4955a596a030',
    '20ee72ed-2f4f-4a7f-a2da-1563652f260e',
    '51c85974-553c-4a65-b504-a91b7f504b77',
    '5d9dc683-5220-4c83-8272-7a914f8913f7',
    'a00fa5ae-c2c6-4cf7-987d-f297d0beb55d',
    '206fa667-6fac-4d6f-8550-8618c6e97f8c',
    'bbd14f0d-2927-4434-93e7-49fa44f4a1a0',
    '7400ed60-596c-43a1-a56c-d6908189b096',
    'd847fd91-fa3c-4a63-83e2-57c4dd324208',
    '04ea7356-fbed-474d-8b1b-23ade0a10d42',
    '0899b613-fbe6-47d0-8b66-57218ccb939a',
    'ded41782-53c8-427d-a392-5743d87bbcb5',
    '55ca7072-871c-4d7a-aaa4-84068a6ddc7d',
    '2d8117bd-3286-4b9c-bdd9-634142375d3a',
    '6e71ab13-91a8-4af3-bf0e-c402faac5d54',
    '0861e1c6-f9e5-48dc-b34a-88bff5271467',
    'fe4b9cae-7e5d-456e-9635-2a0b5f63476c',
    'dd9659e9-9c7c-4504-9989-ca557b01ec5d',
    '331249bb-0cfc-46d8-8e1b-0bf5c524815e',
    '5c0ce728-5343-4a5c-b31a-5439a46100fb',
    '2dbfdd1a-abf5-4d12-90fa-539494694e41',
    'b2d0a6aa-39ca-4e61-a622-a7e2e0783173',
    '6abe71d0-f529-4aa3-a43d-f2372e8fdf1c',
    'd72cbdb3-0f28-416a-a08e-5cb52034f442',
    '845d7164-1b4a-44a9-87dd-067bc3f9047b',
    'aa718f48-ad29-4133-b405-f90b9d6c36f0',
    '7eed69e7-dd11-4703-89fd-ce52454fd60c',
    '9d34e6b5-408a-4d07-a7f8-8c5d14b74cb5',
    '2af16590-3956-473d-80b5-818bdf78104c',
    '613cd50e-e42b-4ff7-99e4-bd2155ce7a81',
    'aaea60ba-d8c8-4ff4-96c9-883819e67a50',
    'd3f11703-4ded-4059-90a5-5a7855a0884d',
    'bbe5e9bd-8f0a-4d0f-964e-23ce7ff6fdf5',
    '38275638-1f74-46f5-9d90-7cce3d7b1acd',
    'aee74f73-ce40-4771-88dd-32edefd65e17',
    'fb1711c3-e03f-41d0-80bd-ec082095d3f9',
    'a6d6819f-cc52-4930-b87f-23646471b27c',
    '9eeaf549-1e57-4ef4-9add-1b79846c61d0',
    '60ff1fdc-4cfe-4134-9e6d-112951ab4761',
    'd3c5010d-80d7-49b2-9770-0fb3d193f3b2',
    'bb56d926-8d44-4146-9fcd-64e0032267a5',
    'e925e79e-67cd-4717-a9ca-325cd3f558a6',
    '09d00c76-3e1f-401d-8fd6-745c284f62b1',
    'aa4bd590-1cb0-4c43-b1e6-e6bc87d6db1a',
    '4e79eb20-9efc-4c25-8662-3fdd32370191',
    'c27758ea-04c8-4221-a292-b391f6797118',
    '1344adf1-a486-4fbe-8d46-bc159f69eab3',
    '27f2b154-18aa-405d-a14f-73521d9908d9',
    '52306630-2df4-4bc4-b6dc-5f41c71bfc64',
    '061677c3-8196-4202-86da-c61c75bc4f36',
    'daa68c72-4d68-44d3-b711-74e4fe893607',
    '2bc848d0-bc9f-453f-b0e6-0e498e62ccd5',
    '98c46225-f92c-4185-8c72-b4cb5d3adf81',
    'fd956e38-af19-47af-9584-1296ccb7801a',
    '7a2075e1-2543-4777-aeb9-496d3ab38127',
    '26fc93a8-cf51-4d39-af74-31d100bf8f51',
    'ea601caf-2c73-41d1-b4ad-91a5cd6ea910',
    'b628345e-d35a-4e06-a7f4-81f0ed799f69'] :#MemberID['MemberID']:
            N=N+1
            print(N)
            # 第二步：获取实际数据
            data_query = f"""
                         SELECT CreateTime,AccountDetailID,MemberID,Balance,Source,Amount
                         FROM t_d_accountdetail where MemberID='{memberid}' 
                         """
            result_data = ch_client.execute(data_query)

            # 第三步：创建带列名的DataFrame
            df = pd.DataFrame(result_data, columns=[ 'CreateTime','AccountDetailID','MemberID','Balance','Source','Amount'])
            try:
                if df['CreateTime'].max() <datetime(2025,7,15):
                    continue
            except:
                continue

            df=calc_pl(df)
            print(memberid,df)
            print("\n数据预览:")
            print(df.head(3))  # 只打印前3行避免过多输出
            result.loc[memberid,'start date']=df['CreateTime'].iloc[0]
            result.loc[memberid, 'end date'] = df['CreateTime'].iloc[-1]
            result.loc[memberid, 'annualized_return'] =calc_annualized_return(df)
            result.loc[memberid, 'max_drawdown'] = calc_max_drawdown(df)
            result.loc[memberid, 'sharpe_ratio'] = calc_sharpe_ratio(df)
            result.loc[memberid, 'sortino_ratio'] = calc_sortino_ratio(df)

            #time.sleep(0.01)

        # 可选：打印DataFrame的前几行
        #print("\n数据预览:")
        #print(df.head(3))  # 只打印前3行避免过多输出

    return result



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
        print("交易表:",get_t_order())
        print("账户表:",get_t_account())
        d=get_t_d_accountdetail()
        d.to_csv('t_d_accountdetail.csv')
