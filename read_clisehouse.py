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
    for memberid in ['d3c5010d-80d7-49b2-9770-0fb3d193f3b2',
'c27758ea-04c8-4221-a292-b391f6797118',
'0899b613-fbe6-47d0-8b66-57218ccb939a',
'1344adf1-a486-4fbe-8d46-bc159f69eab3',
'27630674-cb6a-46cd-97ca-664f5ea8986d',
'27f2b154-18aa-405d-a14f-73521d9908d9',
'52306630-2df4-4bc4-b6dc-5f41c71bfc64',
'061677c3-8196-4202-86da-c61c75bc4f36',
'daa68c72-4d68-44d3-b711-74e4fe893607',
'2bc848d0-bc9f-453f-b0e6-0e498e62ccd5',
'aa718f48-ad29-4133-b405-f90b9d6c36f0',
'2d8117bd-3286-4b9c-bdd9-634142375d3a',
'98c46225-f92c-4185-8c72-b4cb5d3adf81',
'38275638-1f74-46f5-9d90-7cce3d7b1acd',
'fd956e38-af19-47af-9584-1296ccb7801a',
'7a2075e1-2543-4777-aeb9-496d3ab38127',
'aa4bd590-1cb0-4c43-b1e6-e6bc87d6db1a',
'27f2b154-18aa-405d-a14f-73521d9908d9',
'38275638-1f74-46f5-9d90-7cce3d7b1acd',
'26fc93a8-cf51-4d39-af74-31d100bf8f51',
'ea601caf-2c73-41d1-b4ad-91a5cd6ea910',
'b628345e-d35a-4e06-a7f4-81f0ed799f69',
'aa718f48-ad29-4133-b405-f90b9d6c36f0',
'613cd50e-e42b-4ff7-99e4-bd2155ce7a81',
'5734fbbc-627e-43ec-9127-71c8b3d95c15',
'c9c03175-99ec-4b0a-9c49-a4c4ae82185a',
'daa68c72-4d68-44d3-b711-74e4fe893607',
'255bda17-051c-4eb6-8b7c-5bec9ae14249',
'52306630-2df4-4bc4-b6dc-5f41c71bfc64',
'061677c3-8196-4202-86da-c61c75bc4f36',
'7a566bda-4f06-43be-949f-1be0c864b396',
'aea6e82b-4e69-4676-96bc-5f6375b1fc33',
'38275638-1f74-46f5-9d90-7cce3d7b1acd',
'613cd50e-e42b-4ff7-99e4-bd2155ce7a81',
'0c4974e2-4efc-4846-9cf4-c67836a548a8',
'38275638-1f74-46f5-9d90-7cce3d7b1acd',
'26fc93a8-cf51-4d39-af74-31d100bf8f51',
'4605ca16-e4f7-494e-9b72-4e5c1985d0c2',
'2535a0c9-983b-4e95-8697-4fa2f8235122',
'5f431985-ef1d-4d25-a409-d2e1cd6a34fe',
'7f436011-a8cf-430a-abb1-0d1aab1844f9',
'b628345e-d35a-4e06-a7f4-81f0ed799f69',
'36d92a9f-0bcc-4481-a3ff-6c076cf673c7',
'27630674-cb6a-46cd-97ca-664f5ea8986d',
'e01bb0a9-ca3e-412e-9c3a-2d4af6f60dee',
'b55e6d49-db67-47a9-8384-e780294c04dc',
'0899b613-fbe6-47d0-8b66-57218ccb939a'] :#MemberID['MemberID']:
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
