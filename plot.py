import pandas as pd
import matplotlib.pyplot as plt
import datetime as dt
import matplotlib.dates as mdates
df=pd.read_csv(r'20250908_d_trade.csv')
print(df.columns)
print(df['TradeTime'])
df['TradeTime']= [dt.datetime.fromtimestamp(x ) for x in df.TradeTime]#pd.to_datetime(df['TradeTime'])
print(df['TradeTime'])
df=df.sort_values(by=['TradeTime'])
plt.plot(df['TradeTime'],df['CloseProfit'].cumsum()-df['Fee'].cumsum())
plt.show()

fig, ax = plt.subplots(figsize=(10, 6))

# 绘制图表
ax.plot(df['TradeTime'],df['CloseProfit'].cumsum()-df['Fee'].cumsum())

# 设置时间格式
date_format = mdates.DateFormatter('%Y-%m-%d')  # 定义日期格式
ax.xaxis.set_major_formatter(date_format)

# 自动调整日期标签显示，避免重叠
fig.autofmt_xdate()
plt.show()