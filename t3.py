import datetime

import pandas as pd
import matplotlib.pyplot as plt
df = pd.read_csv('7_t_d_trade.csv',encoding='GBK',index_col=0)
print(df.head())
df['CreateTime']=pd.to_datetime(df['CreateTime'])
df=df.loc[df['CreateTime']>=datetime.datetime(2025,1,1)]
plt.plot(df['CreateTime'],df['累计盈亏']-df['累计盈亏'].iloc[0])
plt.grid(True, alpha=0.3)
plt.tight_layout()
plt.show()
