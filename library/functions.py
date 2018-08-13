from utils import arrayTools
from datetime import datetime, timedelta
import numpy as np

"""
>>> from provider import histData
>>> df = histData._sampleMarketDf
"""

# df: marketDf


# 0. 与时间无关的函数
def Freq(df):   # 时间间隔
    freq = (df['Time'][1] - df['Time'][0]) / timedelta(seconds=1)
    return np.ones(df.shape[0]) * freq


def Nans(df):   # 一列nan，用于函数报错时填充
    return np.ones(df.shape[0]) * float('nan')


# 1. 过去函数

def TradeThr(df):
    pass


# 2. 现在函数

def BQMax(df):
    return df[['BQ{}'.format(i) for i in range(1, 6)]].max(axis=1)


def AQMax(df):
    return df[['AQ{}'.format(i) for i in range(1, 6)]].max(axis=1)


def BQMean(df):
    return df[['BQ{}'.format(i) for i in range(1, 6)]].mean(axis=1)


def AQMean(df):
    return df[['AQ{}'.format(i) for i in range(1, 6)]].mean(axis=1)


def Spread(df):
    ret = df['A1'] - df['B1']
    return ret.ravel()


def MidPrc(df):
    ret = df[['B1', 'A1']].mean(axis=1)
    return ret.ravel()


def MicPrc(df):
    ret = (df['B1'] * df['AQ1'] + df['A1'] *
           df['BQ1']) / (df['BQ1'] + df['AQ1'])
    return ret.ravel()


def BookPrs(df):
    ret = (df['BQ1'] - df['AQ1']) / (df['BQ1'] + df['AQ1'])
    return ret.ravel()


def NetVol(df):
    ret = df['BuyVolume'] - df['SellVolume']
    return ret.ravel()


# 3. 将来函数

def Chg(df, p, t):  # t秒之后的价格p的变化
    freq = (timedelta(days=1) / df.shape[0]).seconds
    window = int(t / freq)
    ret = df[p].shift(-window) - df[p]
    return ret.ravel()


def Ret(df, p, t):  # t秒之后的价格p的变化率
    window = int(t / df['Freq'][0])
    ret = df[p].shift(-window) / df[p] - 1
    return ret.ravel()
