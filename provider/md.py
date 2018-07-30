from fsaConfig import MD_FOLDER
from datetime import datetime, timedelta
from utils import dateTools
from itertools import product
import pandas as pd
import numpy as np
import h5py


FIELDS = {'id': 'Id', 'trade_px': 'Price', 'trade_volume': 'Volume', 'b1': 'B1', 'b2': 'B2', 'b3': 'B3', 'b4': 'B4', 'b5': 'B5', 'a1': 'A1', 'a2': 'A2', 'a3': 'A3', 'a4': 'A4', 'a5': 'A5', 'bq1': 'BQ1',
          'bq2': 'BQ2', 'bq3': 'BQ3', 'bq4': 'BQ4', 'bq5': 'BQ5', 'aq1': 'AQ1', 'aq2': 'AQ2', 'aq3': 'AQ3', 'aq4': 'AQ4', 'aq5': 'AQ5', 'order_date_time': 'OrderTime', 'trades_date_time': 'TradeTime', 'update_type': 'Type'}


def fromH5(path):
    f = h5py.File(path, 'r')
    colNames = [_.decode('utf-8') for _ in f.attrs['colnames']]
    colDict = dict(zip(colNames, map(lambda _: f['/{}'.format(_)][:], colNames)))
    df = pd.DataFrame(colDict)
    for col in ['order_date_time', 'trades_date_time']:
        df[col] = df[col].apply(lambda _: _.decode('utf-8'))
    f.close()
    df = df.rename(columns=FIELDS)
    return df

def sampling(df, freq=1):
    pass



def fromCsv(path, timeInterval=0.5, startTime='06:04:06'):
    df = pd.read_csv(path, header=None)
    df = df.iloc[:, 1:]
    df.columns = CSV_FIELDS
    d = df['OrderTime'].iloc[0].split(' ')[0]
    ts0 = datetime.strptime(f'{d} {startTime}', '%Y%m%d %H:%M:%S').timestamp()
    n = int(timedelta(days=1) / timedelta(seconds=timeInterval))

    df['Time'] = df[['OrderTime', 'TradeTime']].max(axis=1).apply(
        lambda _: datetime.strptime(_, '%Y%m%d %H:%M:%S.%f').timestamp() * 2)
    df['Time'] = df['Time'].apply(np.ceil) / 2
    df['Index'] = ((df['Time'] - ts0) * 2).astype(int)

    df1 = df.pivot_table(index='Index', values=[f'{n}{i}' for n in [
                         'A', 'B', 'AQ', 'BQ'] for i in range(1, 6)], aggfunc='last')
    df1['Open'] = df.pivot_table(
        index='Index', values='Price', aggfunc='first')['Price']
    df1['Close'] = df.pivot_table(
        index='Index', values='Price', aggfunc='last')['Price']
    df1['High'] = df.pivot_table(
        index='Index', values='Price', aggfunc='max')['Price']
    df1['Low'] = df.pivot_table(
        index='Index', values='Price', aggfunc='min')['Price']
    df1 = df1.reindex(index=range(n), method='ffill')

    df2 = pd.DataFrame()
    df2['Volume'] = df.pivot_table(
        index='Index', values='Volume', aggfunc='sum')['Volume']
    df2 = df2.reindex(index=range(n)).fillna(0)
    return pd.concat([df1, df2], axis=1)
