import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import matplotlib.dates as mdates
df=pd.read_excel('策略实盘交易明细1.xlsx')
print(df)

def resample_pl(df, rule="1D"):
    '''
    净值曲线
    '''
    rule = rule.upper()
    if rule[-1:] == 'M':
        rule = rule[:-1] + 'T'
    agg_rule = {
        '净值': 'last'
    }
    cols = [k for k in agg_rule.keys()]
    cols.append('时间')
    temp = df[cols].resample(rule=rule, closed='left', label='left', on='时间').agg(agg_rule)
    temp['净值'] = temp['净值'].fillna(method='ffill')
    temp.reset_index(inplace=True)
    return temp
def calculate_max_drawdown(df):
    """计算最大回撤"""
    peak = df['净值'].cummax()
    drawdown = (df['净值'] - peak) / peak
    max_drawdown = drawdown.min()
    return abs(max_drawdown)
def calculate_annual_return(df):
        """计算年化收益率"""
        start_val = df['净值'].iloc[0]
        end_val = df['净值'].iloc[-1]
        days = (df['时间'].iloc[-1] - df['时间'].iloc[0]).days
        #print(days)
        annual_return =( (end_val-start_val) / start_val) * (365.25 / days)
        return annual_return
def calculate_sharpe_ratio(df, risk_free_rate=0.0):
        """计算夏普比率"""
        returns = df['净值'].pct_change().dropna()
        excess_returns = returns - risk_free_rate / (365*96)  # 假设收益率
        sharpe = excess_returns.mean() / excess_returns.std() * np.sqrt(365*96)
        return sharpe
print("最大回撤:",format(calculate_max_drawdown(df), '.4%') )
print("年化收益率:",format(calculate_annual_return(df), '.4%') )
print("卡玛比率:",round(calculate_annual_return(df)/calculate_max_drawdown(df),4))
df=resample_pl(df,'15m')
print("夏普率:",round(calculate_sharpe_ratio(df),4))


fig, ax = plt.subplots(figsize=(10, 6))

# 绘制图表
ax.plot(df['时间'],df['净值'])

# 设置时间格式
date_format = mdates.DateFormatter('%Y-%m-%d')  # 定义日期格式
ax.xaxis.set_major_formatter(date_format)

# 自动调整日期标签显示，避免重叠
fig.autofmt_xdate()
plt.show()