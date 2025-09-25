import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.ticker import MaxNLocator, MultipleLocator
bz_df=pd.read_csv(r"D:\ChromeCoreDownloads\2025-09-17-orderbook_btc_future_um.csv")
lk_df=pd.read_csv(r"D:\ChromeCoreDownloads\2025-09-17-orderbook_lbank_btc_spot.csv")
print(bz_df.head(),lk_df.head())

bz_df['price']=(bz_df['b1p']+bz_df['a1p'])/2
lk_df['price']=(lk_df['b1p']+lk_df['a1p'])/2
print(bz_df.head(),lk_df.head())

start_ts=max(bz_df['recv_ts'].min(),lk_df['recv_ts'].min())
end_ts=min(bz_df['recv_ts'].max(),lk_df['recv_ts'].max())
end_ts=start_ts+10000000
#start_ts=end_ts-10000
bz_df=bz_df.loc[(bz_df['recv_ts']>=start_ts)&(bz_df['recv_ts']<=end_ts)]
lk_df=lk_df.loc[(lk_df['recv_ts']>=start_ts)&(lk_df['recv_ts']<=end_ts)]
print(bz_df.head(),lk_df.head())
plt.plot(bz_df['recv_ts'],bz_df['price'],label='BZ  Price')
plt.plot(lk_df['recv_ts'],lk_df['price'],label='LK  Price')
plt.gca().xaxis.set_major_locator(MaxNLocator(integer=True))
plt.gca().yaxis.set_major_locator(MaxNLocator(integer=True))
plt.legend()
plt.show()

abc=((bz_df['price']-lk_df['price'])/bz_df['price']).quantile(0.9)
print(abc)