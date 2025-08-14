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
    for memberid in ['bdf8c5ea-b5ab-4b40-b180-fd5ee2eb0760',
'0f2e53da-31e3-4105-808b-09ab527c0ede',
'cb93b5c8-fb7c-40f3-a8c2-0b70f70d5b29',
'1ccb2cdc-7008-4a3a-ae1f-a6ae62dc3831',
'3535e019-3455-42e0-b7c8-cc07d3e746f5',
'392f05eb-57b0-46a0-8b2e-b109131d0641',
'2de111d6-15a6-4e52-9a89-36543507d9a7'] :#MemberID['MemberID']:
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
            if df['CreateTime'].max() <datetime(2025,7,15):
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
