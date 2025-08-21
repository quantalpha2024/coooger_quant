import time
import pandas as pd
#from clickhouse_driver import Client
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
if __name__ == '__main__':
    df=pd.read_csv("7_t_d_accountdetail.csv")
    for uid in df.MemberID.unique():
        d=df#.loc[df['MemberID']==uid]
        d=calc_pl(d)
        #df.to_csv("cta.csv")
        print(uid,d)