import pandas as pd
import matplotlib.pyplot as plt
cex_df=pd.read_csv(r"D:\ChromeCoreDownloads\2025-09-17-orderbook.csv")
dex_df=pd.read_csv(r"D:\ChromeCoreDownloads\2025-09-17-price.csv")
print(cex_df.head(),dex_df.head())

cex_df['recv_ts']=cex_df['recv_ts'].apply(int)
dex_df['recv_ts']=dex_df['recv_ts'].apply(int)
'''
cex_df['price']=((cex_df['b5q']*cex_df['b5p']+
                 cex_df['b4q']*cex_df['b4p']+
                 cex_df['b3q']*cex_df['b3p']+
                 cex_df['b2q']*cex_df['b2p']+
                 cex_df['b1q']*cex_df['b1p']+
                 cex_df['a1q']*cex_df['a1p']+
                 cex_df['a2p']*cex_df['a2q']+
                 cex_df['a3p']*cex_df['a3p']+
                 cex_df['a4p']*cex_df['a4p']+
                 cex_df['a5q']*cex_df['a5p'])/
                 (cex_df['a1q']+cex_df['a2q']+cex_df['a3q']+cex_df['a4q']+cex_df['a5q']
                  +cex_df['b1q']+cex_df['b2q']+cex_df['b3q']+cex_df['b4q']+cex_df['b5q']))
'''
cex_df['price']=(cex_df['b1p']+cex_df['a1p'])/2
print(cex_df.head(),dex_df.head())

start_ts=max(cex_df['recv_ts'].min(),dex_df['recv_ts'].min())
end_ts=min(cex_df['recv_ts'].max(),dex_df['recv_ts'].max())
end_ts=start_ts+1000000
cex_df=cex_df.loc[(cex_df['recv_ts']>=start_ts)&(cex_df['recv_ts']<=end_ts)]
dex_df=dex_df.loc[(dex_df['recv_ts']>=start_ts)&(dex_df['recv_ts']<=end_ts)]
print(cex_df.head(),dex_df.head())

plt.plot(cex_df['recv_ts'],(cex_df['price']-cex_df['price'].shift())/cex_df['price'].shift(),label='Cex  Price')
plt.plot(dex_df['recv_ts'],(dex_df['price']-dex_df['price'].shift())/dex_df['price'].shift(),label='Dex  Price')
plt.legend()
plt.show()

abc=((dex_df['price']-dex_df['price'].shift())/dex_df['price'].shift()).quantile(0.9)
print(abc)