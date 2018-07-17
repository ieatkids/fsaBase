from fsaConfig import MD_FOLDER
from datetime import datetime, timedelta
from utils import dateTools
from itertools import product
import pandas as pd
import numpy as np


CSV_FIELDS=['Id', 'Price', 'Volume', 'B1', 'B2', 'B3', 'B4', 'B5', 
        'A1', 'A2', 'A3', 'A4', 'A5', 'BQ1', 'BQ2', 'BQ3', 'BQ4', 'BQ5', 
        'AQ1', 'AQ2', 'AQ3', 'AQ4', 'AQ5', 'OrderTime', 'TradeTime', 'Type']


def fromCsv(path, timeInterval=0.5, startTime='06:04:06'):
    df = pd.read_csv(path, header=None)
    df = df.iloc[:, 1:]
    df.columns = CSV_FIELDS
    d = df['OrderTime'].iloc[0].split(' ')[0]
    ts0 = datetime.strptime(f'{d} {startTime}', '%Y%m%d %H:%M:%S').timestamp()
    n = int(timedelta(days=1) / timedelta(seconds=timeInterval))

    df['Time'] = df[['OrderTime', 'TradeTime']].max(axis=1).apply(lambda _: datetime.strptime(_, '%Y%m%d %H:%M:%S.%f').timestamp() * 2)
    df['Time'] = df['Time'].apply(np.ceil) / 2
    df['Index'] = ((df['Time'] - ts0) * 2).astype(int)

    df1 = df.pivot_table(index='Index', values=[f'{n}{i}' for n in ['A', 'B', 'AQ', 'BQ'] for i in range(1, 6)], aggfunc='last')
    df1['Open'] = df.pivot_table(index='Index', values='Price', aggfunc='first')['Price']
    df1['Close'] = df.pivot_table(index='Index', values='Price', aggfunc='last')['Price']
    df1['High'] = df.pivot_table(index='Index', values='Price', aggfunc='max')['Price']
    df1['Low'] = df.pivot_table(index='Index', values='Price', aggfunc='min')['Price']
    df1 = df1.reindex(index=range(n), method='ffill')

    df2 = pd.DataFrame()
    df2['Volume'] = df.pivot_table(index='Index', values='Volume', aggfunc='sum')['Volume']
    df2 = df2.reindex(index=range(n)).fillna(0)
    return pd.concat([df1, df2], axis=1)




# def sliceIter(df, unit=0.5):
#     d = dateTools.toDs(df['date_time'][0])
#     ticks = dateTools.getTimeTicks(d, '06:04:00', unit)
#     ticks.append(ticks[-1] + timedelta(seconds=unit))
#     for dt0, dt1 in zip(ticks[:-1], ticks[1:]):
#         sli = df.loc[(df['date_time'] >= dt0)&(df['date_time'] <= dt1)]
#         yield sli


# def toOhlcv(df, unit=0.5):
#     ohlcDf = pd.DataFrame()
#     for sli in sliceIter(df, unit):
#         row = {}
#         if sli.shape[0] > 0:
#             print(sli['date_time'].iloc[0])
#             for _ in ['{}{}'.format(t, i) for t in ['A', 'B', 'AQ', 'BQ'] for i in range(1, 6)]:
#                 row[_] = sli[_.lower()].iloc[-1]
#         else:
#             for _ in ['{}{}'.format(t, i) for t in ['A', 'B', 'AQ', 'BQ'] for i in range(1, 6)]:
#                 row[_] = float('nan')

#         tSli = sli.loc[sli['update_type']==1]   # 切片中的成交数据
#         if tSli.shape[0] > 0:
#             row['Open'] = tSli['trade_px'].iloc[0]
#             row['High'] = tSli['trade_px'].max()
#             row['Low'] = tSli['trade_px'].min()
#             row['Close'] = tSli['trade_px'].iloc[-1]
#             row['Price'] = tSli['trade_px'].iloc[-1]
#             row['Trades'] = tSli.shape[0]
#             row['Amount'] = (tSli['trade_px'] * tSli['trade_volume']).sum()
#             row['Volume'] = tSli['trade_volume'].sum()
#             # tbSli = tSli[tSli['trade_px'] >= tSli['a1']]    # 成交价大于等于卖1时认为是买单
#             # tsSli = tSli[tSli['trade_px'] <= tSli['b1']]    # 成交价小于等于买1时认为是卖单
#         else:
#             for _ in ['Open', 'High', 'Low', 'Close', 'Price']:
#                 row[_] = float('nan')
#             for _ in ['Trades', 'Amount', 'Volume']:
#                 row[_] = 0

#         oSli = sli.loc[sli['update_type']==2]
#         if oSli.shape[0] > 0:
#             row['Orders'] = oSli.shape[0]
#             row['Quantity'] = oSli['trade_volume'].sum()
#         else:
#             for _ in ['Orders', 'Quantity']:
#                 row[_] = 0

#         ohlcDf.append(row, ignore_index=True)
#     ohlcDf = ohlcDf.fillna(method='ffill')
#     return ohlcDf
            



