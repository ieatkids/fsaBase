from fsaConfig import MD_FOLDER
from datetime import datetime, timedelta
from utils import dateTools
from itertools import product
from pathlib2 import Path
import pandas as pd
import numpy as np
import h5py
import shutil


FIELDS = ['Time', 'Price', 'Volume', 'B1', 'B2', 'B3', 'B4', 'B5', 'A1', 'A2', 'A3', 'A4', 'A5', 'BQ1', 'BQ2', 'BQ3', 'BQ4', 'BQ5', 'AQ1', 'AQ2', 'AQ3', 'AQ4', 'AQ5']

TRADE_FIELDS = ['Price', 'Volume']

ORDER_FIELDS = ['B1', 'B2', 'B3', 'B4', 'B5', 'A1', 'A2', 'A3', 'A4', 'A5', 'BQ1', 'BQ2', 'BQ3', 'BQ4', 'BQ5', 'AQ1', 'AQ2', 'AQ3', 'AQ4', 'AQ5']

ALIAS = {'id': 'Id', 'trade_px': 'Price', 'trade_volume': 'Volume', 'b1': 'B1', 'b2': 'B2', 'b3': 'B3', 'b4': 'B4', 'b5': 'B5', 'a1': 'A1', 'a2': 'A2', 'a3': 'A3', 'a4': 'A4', 'a5': 'A5', 'bq1': 'BQ1',
          'bq2': 'BQ2', 'bq3': 'BQ3', 'bq4': 'BQ4', 'bq5': 'BQ5', 'aq1': 'AQ1', 'aq2': 'AQ2', 'aq3': 'AQ3', 'aq4': 'AQ4', 'aq5': 'AQ5', 'order_date_time': 'OrderTime', 'trades_date_time': 'TradeTime', 'update_type': 'Type'}


_testSampledMd = pd.read_csv(Path.cwd() / Path('test', 'sampledMd.csv'))


def getSampledMdFromRawMdH5(path, freq=1):   # 读取raw h5文件并返回sampling之后的数据，freq的单位是秒
    f = h5py.File(path, 'r')
    colNames = [_.decode('utf-8') for _ in f.attrs['colnames']]
    colDict = dict(zip(colNames, map(lambda _: f['/{}'.format(_)][:], colNames)))
    rawDf = pd.DataFrame(colDict)
    rawDf = rawDf.rename(columns=ALIAS)
    f.close()
    rawDf1 = rawDf.loc[rawDf['Type']==1, ORDER_FIELDS + ['OrderTime']]   # 提出Order数据
    rawDf2 = rawDf.loc[rawDf['Type']==2, TRADE_FIELDS + ['TradeTime']]   # 提出Trade数据
    rawDf1['Time'] = rawDf1['OrderTime'].str.decode('utf-8')
    rawDf2['Time'] = rawDf2['TradeTime'].str.decode('utf-8')
    d0 = max(rawDf1['Time'].iloc[0].split(' ')[0], rawDf2['Time'].iloc[0].split(' ')[0])
    t0 = datetime.strptime('{} 00:00:00'.format(d0), '%Y%m%d %H:%M:%S') + timedelta(seconds=freq)
    n = timedelta(days=1) // timedelta(seconds=freq)
    rawDf1['Time'] = rawDf1['Time'].apply(lambda _: datetime.strptime(_, '%Y%m%d %H:%M:%S.%f'))
    rawDf1['Index'] = np.ceil((rawDf1['Time'] - t0) / timedelta(seconds=freq)).astype(int)
    rawDf2['Time'] = rawDf2['Time'].apply(lambda _: datetime.strptime(_, '%Y%m%d %H:%M:%S.%f'))
    rawDf2['Index'] = np.ceil((rawDf2['Time'] - t0) / timedelta(seconds=freq)).astype(int)
    df1 = rawDf1.pivot_table(index='Index', values=ORDER_FIELDS, aggfunc='last')
    df1 = df1.reindex(range(n)).fillna(method='ffill')    # Order数据中缺失值用前一个值填充
    df2 = rawDf2.pivot_table(index='Index', values='Volume', aggfunc='sum')
    df2['Price'] = rawDf2.pivot_table(index='Index', values='Price', aggfunc='last')
    df2 = df2.reindex(range(n))
    df2['Price'] = df2['Price'].fillna(method='ffill')  # Trade数据中的Price缺失值用前一个值填充
    df2['Volume'] = df2['Volume'].fillna(0) # Trade数据中的Volume缺失值用0填充
    df = pd.concat([df1, df2], axis=1)
    df['Time'] = t0 + timedelta(seconds=freq) * np.arange(n)
    df.index = range(n)
    return df[FIELDS]


def getH5Path(source, exchange, instrument, date):
    return str(Path(MD_FOLDER, source, exchange, instrument, '{}.h5'.format(date)))


def classifyRawMdH5(path):
    if Path(path).is_file() and Path(path).suffix == '.h5': # 路径为h5文件，则分类存放
        _, exchange, instrument, date = Path(path).name.split('.')[0].split('_')
        newPath = getH5Path('raw', exchange, instrument, date)
        Path(newPath).parent.mkdir(parents=True, exist_ok=True)
        shutil.move(path, newPath)
    elif Path(path).is_dir():   # 路径为文件夹，则分类存放文件夹下所有文件
        for path_ in Path(path).iterdir():
            classifyRawMdH5(path_)
    else:
        pass


def getSampledMd(exchange, instrument, date, freq=1):
    h5Path = getH5Path('raw', exchange, instrument, date)
    if Path(h5Path).is_file():
        df = getSampledMdFromRawMdH5(h5Path, freq=freq)
        return df
    else:
        raise FileExistsError('h5 file does not exist.')


def getRawMd(exchange, instrument, date):
    pass

