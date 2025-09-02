import pandas as pd
import matplotlib.pyplot as plt
df=pd.read_excel(r'D:\coooger_quant\38275638-1f74-46f5-9d90-7cce3d7b1acd交易明细.xlsx')
df['交易时间']=pd.to_datetime(df['交易时间'])
print(df)
plt.plot(df['交易时间'],df['累计盈亏'])
plt.show()