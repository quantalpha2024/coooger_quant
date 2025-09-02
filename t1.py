import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np
plt.rcParams['font.sans-serif'] = ['SimHei']  # 使用黑体
plt.rcParams['axes.unicode_minus'] = False    # 解决负号显示问题
df=pd.read_excel(r'D:\coinw\data\0114-0828每日成交汇总.xlsx')
df['日期']=pd.to_datetime(df['日期'])
print(df.head())
df['总成交量']=df['总成交量'].cumsum()
df['BTC成交量']=df['BTC成交量'].cumsum()
df['ETH成交量']=df['ETH成交量'].cumsum()
df['其他成交量']=df['其他成交量'].cumsum()

for colum in ['已实现盈利','BTC盈亏',
              'ETH盈亏','其他盈亏']:

    df[colum]=df[colum].cumsum()
total_volume = ['总成交量', 'BTC成交量', 'ETH成交量', '其他成交量']
profit=['已实现盈利','BTC盈亏',
              'ETH盈亏','其他盈亏']
for i in range(4):
    fig, ax = plt.subplots(figsize=(10, 6))
    # 绘制图表
    df.plot.scatter(x=total_volume[i], y=profit[i], title='散点图')
    # 设置时间格式
    date_format = mdates.DateFormatter('%Y-%m-%d')  # 定义日期格式
    ax.xaxis.set_major_formatter(date_format)
    # 自动调整日期标签显示，避免重叠
    fig.autofmt_xdate()
    # 计算回归线
    z = np.polyfit(df[total_volume[i]], df[profit[i]], 1)  # 1表示线性回归
    p = np.poly1d(z)  # 创建多项式函数

    # 绘制回归线
    x_line = np.linspace(df[total_volume[i]].min(), df[total_volume[i]].max(), 227)
    y_line = p(x_line)
    df['预测']=z[0]*df[total_volume[i]]+z[1]
    #print(y_line)
    plt.plot(x_line, y_line, "r--", linewidth=2, label='回归线')

    # 添加方程文本
    equation = f"y = {z[0]:.8f}x + {z[1]:.2f}"
    plt.annotate(equation, xy=(0.05, 0.95), xycoords='axes fraction',
                 fontsize=12, bbox=dict(boxstyle="round,pad=0.3", fc="white", alpha=0.8))

    # 添加R²值
    from sklearn.metrics import r2_score

    r2 = r2_score(df['预测'], df[profit[i]])
    print(r2)
    plt.annotate(f"R方 = {r2:.3f}", xy=(0.05, 0.85), xycoords='axes fraction', fontsize=12, bbox=dict(boxstyle="round,pad=0.3", fc="white", alpha=0.8))

    # 添加图例和网格
    plt.legend()
    plt.grid(True, linestyle='--', alpha=0.7)

    plt.tight_layout()
    plt.show()
    df['残差']=df[profit[i]]-df['预测']
    df.plot.scatter(x=total_volume[i], y='残差')
    # 设置时间格式
    date_format = mdates.DateFormatter('%Y-%m-%d')  # 定义日期格式
    ax.xaxis.set_major_formatter(date_format)
    # 自动调整日期标签显示，避免重叠
    fig.autofmt_xdate()
    plt.show()

