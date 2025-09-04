import pandas as pd
import datetime as dt
import matplotlib.pyplot as plt

# 读取数据并预处理
df = pd.read_csv(r"D:\ChromeCoreDownloads\btc_trade.csv", encoding='gbk')
df['日期'] = pd.to_datetime(df['日期'])
fee_rate = 0.00002  # 手续费率

N = 5
# 每组选择的用户数量

# 初始化结果DataFrame
result = pd.DataFrame()
result.index.name = 'date'

# 获取唯一日期列表并按日期排序
unique_dates = sorted(set(df['日期']))

for date in unique_dates:
    try:
        #if date.day!=1:
        #    continue
        # 计算回溯时间窗口
        start_date = date - dt.timedelta(days=365)
        end_date = date - dt.timedelta(days=1)

        # 获取回溯期内的数据
        lookback_data = df[(df['日期'] >= start_date) & (df['日期'] <= end_date)]

        # 计算每个用户在回溯期内的累计收益
        user_cumulative_pnl = (lookback_data.groupby('uid')['收益额']
                               .sum()
                               .reset_index()
                               .rename(columns={'收益额': '累计收益额'}))

        # 按累计收益排序
        user_cumulative_pnl = user_cumulative_pnl.sort_values('累计收益额')
        print(date,user_cumulative_pnl)
        # 选择表现最好和最差的N个用户
        if (date + dt.timedelta(days=1)).day==1:
           top_users = user_cumulative_pnl.loc[user_cumulative_pnl['累计收益额']>100000]['uid'].tolist()
           bottom_users =user_cumulative_pnl.loc[user_cumulative_pnl['累计收益额']<=-50000]['uid'].tolist()

        # 获取选定用户在当日的交易数据
        top_day_data = df[(df['日期'] >= date) &(df['日期'] < (date+ dt.timedelta(days=1)) )& (df['uid'].isin(top_users))]
        bottom_day_data = df[(df['日期'] >= date) &(df['日期'] < (date+ dt.timedelta(days=1)) )& (df['uid'].isin(bottom_users))]

        # 计算组合表现
        result.loc[date, 'long_group_pnl'] = top_day_data['收益额'].sum()  # 多头组收益
        result.loc[date, 'long_group_amount'] = top_day_data['成交额'].sum()  # 多头组成交额
        result.loc[date, 'short_group_pnl'] = bottom_day_data['收益额'].sum()  # 空头组收益
        result.loc[date, 'short_group_amount'] = bottom_day_data['成交额'].sum()  # 空头组成交额

    except Exception as e:
        print(f"处理日期 {date} 时出错: {e}")
        continue

# 计算策略总体收益（多头组收益 - 空头组收益 + 手续费收入）
result['pnl'] = (result['long_group_pnl'] - result['short_group_pnl']) - \
                (result['short_group_amount'] + result['long_group_amount']) * fee_rate

# 按日期排序结果
result = result.sort_index()

# 输出结果
print("策略表现汇总:")
print(result)

# 绘制收益曲线
plt.figure(figsize=(12, 6))
plt.plot(result.index, result['pnl'].cumsum(), linewidth=2)
plt.title('策略累计收益曲线')
plt.xlabel('日期')
plt.ylabel('累计收益')
plt.grid(True, alpha=0.3)
plt.tight_layout()
plt.show()

