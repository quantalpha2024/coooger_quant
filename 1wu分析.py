import pandas as pd
df=pd.read_excel(r'D:\Backup\Downloads\一个月内盈利1wu以上用户成交明细.xlsx')
df['手续费率']=round(df['手续费']/df['成交金额'],4)
d=df.groupby(['手续费率']).apply(lambda x:x[['手续费','平仓盈亏']].sum())
print(d)

d.to_csv('1wu用户手续费影响.csv')