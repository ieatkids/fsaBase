from fsaConfig import MD_FOLDER
from datetime import datetime, timedelta
from utils import dateTools
from library import marketDfFuncs
from itertools import product
from pathlib import Path
import pandas as pd
import numpy as np
import h5py
import shutil


MARKET_FIELDS = ['Time', 'Price', 'Volume', 'BuyVolume', 'SellVolume', 'B1', 'B2', 'B3', 'B4', 'B5', 'A1', 'A2', 'A3', 'A4', 'A5', 'BQ1', 'BQ2', 'BQ3', 'BQ4', 'BQ5', 'AQ1', 'AQ2', 'AQ3', 'AQ4', 'AQ5']

TRADE_FIELDS = ['Price', 'Volume', 'BuyVolume', 'SellVolume']

ORDER_FIELDS = ['B1', 'B2', 'B3', 'B4', 'B5', 'A1', 'A2', 'A3', 'A4', 'A5', 'BQ1', 'BQ2', 'BQ3', 'BQ4', 'BQ5', 'AQ1', 'AQ2', 'AQ3', 'AQ4', 'AQ5']

ALIAS = {'id': 'Id', 'trade_px': 'Price', 'trade_volume': 'Volume', 'b1': 'B1', 'b2': 'B2', 'b3': 'B3', 'b4': 'B4', 'b5': 'B5', 'a1': 'A1', 'a2': 'A2', 'a3': 'A3', 'a4': 'A4', 'a5': 'A5', 'bq1': 'BQ1',
          'bq2': 'BQ2', 'bq3': 'BQ3', 'bq4': 'BQ4', 'bq5': 'BQ5', 'aq1': 'AQ1', 'aq2': 'AQ2', 'aq3': 'AQ3', 'aq4': 'AQ4', 'aq5': 'AQ5', 'order_date_time': 'OrderTime', 'trades_date_time': 'TradeTime', 'update_type': 'Type'}


_sampleMarketDf = pd.read_csv(Path.cwd() / Path('sampleData', 'marketDf.csv'), parse_dates=['Time'])


def _getH5Path(source, k, d):   # 获取h5文件存放路径
    instrument, exchange = k.split('.')
    if source in ['raw', 'order', 'trade']:
        return str(Path(MD_FOLDER, source, exchange, instrument, d).with_suffix('.h5'))
    elif source == 'market':
        return str(Path(MD_FOLDER, source, exchange, d).with_suffix('.h5'))
    else:
        raise IOError(f"data source '{source}' does not exist.")


def _getRawDfFromRawH5(k, d):   # 从原始数据获取raw data
    h5Path = _getH5Path('raw', k, d)
    f = h5py.File(h5Path, 'r')
    colNames = [_.decode('utf-8') for _ in f.attrs['colnames']]
    colDict = dict(zip(colNames, map(lambda _: f['/{}'.format(_)][:], colNames)))
    f.close()
    rawDf = pd.DataFrame(colDict).rename(columns=ALIAS)
    rawDf['OrderTime'] = rawDf['OrderTime'].str.decode('utf-8')
    rawDf['TradeTime'] = rawDf['TradeTime'].str.decode('utf-8')
    rawDf['OrderTime'] = rawDf['OrderTime'].apply(lambda _: datetime.strptime(_, '%Y%m%d %H:%M:%S.%f'))
    rawDf['TradeTime'] = rawDf['TradeTime'].apply(lambda _: datetime.strptime(_, '%Y%m%d %H:%M:%S.%f'))
    wgtDf = pd.DataFrame()  # 按成交价格把Volume分为Buy和Sell
    wgtDf['BuyWeight'] = (rawDf['Price'] - rawDf['B1']) / (rawDf['A1'] - rawDf['B1'])
    wgtDf.loc[wgtDf['BuyWeight'] > 1] = 1
    wgtDf.loc[wgtDf['BuyWeight'] < 0] = 0
    rawDf['BuyVolume'] = rawDf['Volume'] * wgtDf['BuyWeight']
    rawDf['SellVolume'] = rawDf['Volume'] - rawDf['BuyVolume']
    return rawDf


def _getOrderAndTradeDfFromRawH5(k, d):  # 从原始数据中分离order data和trade data
    rawDf = _getRawDfFromRawH5(k, d)
    orderDf = rawDf.loc[rawDf['Type']==1, ORDER_FIELDS + ['OrderTime']]   # 提出order data
    tradeDf = rawDf.loc[rawDf['Type']==2, TRADE_FIELDS + ['TradeTime']]   # 提出trade data
    orderDf = orderDf.rename(columns={'OrderTime': 'Time'})
    tradeDf = tradeDf.rename(columns={'TradeTime': 'Time'})
    orderDf = orderDf[['Time'] + ORDER_FIELDS].reset_index()
    tradeDf = tradeDf[['Time'] + TRADE_FIELDS].reset_index()
    return orderDf, tradeDf


def _getMarketDfFromRawH5(k, d, freq=1):   # 从raw data获取market data，freq的单位是秒
    orderDf, tradeDf = _getOrderAndTradeDfFromRawH5(k, d)
    t0 = max(orderDf['Time'][0], tradeDf['Time'][0]).date()
    t0 = datetime(t0.year, t0.month, t0.day) + timedelta(seconds=freq)
    n = timedelta(days=1) // timedelta(seconds=freq)
    orderDf['Index'] = np.ceil((orderDf['Time'] - t0) / timedelta(seconds=freq)).astype(int)
    tradeDf['Index'] = np.ceil((tradeDf['Time'] - t0) / timedelta(seconds=freq)).astype(int)
    df1 = orderDf.pivot_table(index='Index', values=ORDER_FIELDS, aggfunc='last')
    df1 = df1.reindex(range(n)).fillna(method='ffill')    # Order数据中缺失值用前一个值填充
    df2 = tradeDf.pivot_table(index='Index', values=['Volume', 'BuyVolume', 'SellVolume'], aggfunc='sum')
    df2['Price'] = tradeDf.pivot_table(index='Index', values='Price', aggfunc='last')
    df2 = df2.reindex(range(n))
    df2['Price'] = df2['Price'].fillna(method='ffill')  # Trade数据中的Price缺失值用前一个值填充
    df2 = df2.fillna(0) # Trade数据中的Volume缺失值用0填充
    marketDf = pd.concat([df1, df2], axis=1)
    marketDf['Time'] = t0 + timedelta(seconds=freq) * np.arange(n)
    marketDf = marketDf[MARKET_FIELDS].reset_index()
    for name in ['Freq', 'Spread', 'MidPrc', 'MicPrc']: # 添加一些常用的字段
        marketDf[name] = getattr(marketDfFuncs, name)(marketDf)
    return marketDf


def classifyRawMdH5(path):  # 分类存放raw data
    if Path(path).exists() and Path(path).suffix == '.h5': # 路径为h5文件，则分类存放
        _, exchange, instrument, d = Path(path).name.split('.')[0].split('_')
        newPath = _getH5Path('raw', f'{instrument}.{exchange}', d)
        Path(newPath).parent.mkdir(parents=True, exist_ok=True)
        shutil.move(path, newPath)
    elif Path(path).is_dir():   # 路径为文件夹，则分类存放文件夹下所有文件
        for path_ in Path(path).iterdir():
            classifyRawMdH5(path_)
    else:
        pass


def getMarketDf(k, d, freq=1):    # 统一的market data接口，freq的单位是秒
    if Path(_getH5Path('market', k, d)).exists(): # 预留，从转存好的数据读取
        marketDf = None
    elif Path(_getH5Path('raw', k, d)).exists():  # 从raw数据读取
        marketDf =  _getMarketDfFromRawH5(k, d, freq=freq)
    else:
        print(f'Data does not exist.')
        marketDf = None
    return marketDf

def getOrderDf(k, d):   # 统一的order data接口
    if Path(_getH5Path('order', k, d)).exists(): # 预留，从转存好的数据读取
        orderDf = None
    elif Path(_getH5Path('raw', k, d)).exists(): # 从raw数据读取
        orderDf, _ = _getOrderAndTradeDfFromRawH5(k, d)
    else:
        print(f'Data does not exist')
        orderDf = None
    return orderDf


def getTradeDf(k, d):   # 统一的trade data接口
    if Path(_getH5Path('trade', k, d)).exists(): # 预留，从转存好的数据读取
        tradeDf = None
    elif Path(_getH5Path('raw', k, d)).exists(): # 从raw数据读取
        _, tradeDf = _getOrderAndTradeDfFromRawH5(k, d)
    else:
        print(f'Data does not exist')
        tradeDf = None
    return tradeDf

