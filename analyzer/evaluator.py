from provider import histData
from library import marketDfFuncs
from fsaConfig import MD_FOLDER
from utils import dateTools, listTools
import pandas as pd


def _splitV(v): # 将字符串分解为函数名和参数列表
    """
    >>> _splitV('Ret_MidPrc_60')
    ('Ret', ['MidPrc', 60])
    """
    funcName = v.split('_')[0]
    params = []
    for p in v.split('_')[1:]:
        try:
            params.append(eval(p))
        except NameError:
            params.append(p)
    return funcName, params


def _getV(df, v):   # 把字符串换算为array
    if v in df.columns:
        ret = df[v].ravel()
    else:
        func, params = _splitV(v)
        if func in dir(marketDfFuncs):
            ret = getattr(marketDfFuncs, func)(df, *params)
        else:
            ret = getattr(marketDfFuncs, 'Nans')(df)
    return ret


class MarketDfEvaluator():
    def __init__(self, freq=1):
        self.cache = {}
        self.freq = freq

    def getMarketDf(self, k, d):
        if (k, d) not in self.cache:
            self.cache[(k, d)] = histData.getMarketDf(k, d, freq=self.freq)
        return self.cache[(k, d)]
    
    def getDf(self, **kws):
        """
        >>> ev = MarketDfEvaluator()
        >>> df = ev.getDf(k='btcusd.bitstamp', d='20180730', v=['Ret_MidPrc_20', 'NetVol', 'BookPrs'])
        >>> print(df.head())
           Ret_MidPrc_20  NetVol  BookPrs
        0            NaN     0.0      NaN
        1            NaN     0.0      NaN
        2            NaN     0.0      NaN
        3            NaN     0.0      NaN
        4            NaN     0.0      NaN
        >>> df = ev.getDf(d='20180730', kv=['btcusd.bitstamp:Price', 'btceur.bitstamp:Price'])
        >>> print(df.tail())
               btcusd.bitstamp:Price  btceur.bitstamp:Price
        86395                8209.73                8209.73
        86396                8209.73                8209.73
        86397                8209.73                8209.73
        86398                8216.74                8216.74
        86399                8216.74                8216.74
        """
        dfs = []
        if 'kv' in kws:
            for d in dateTools.drToD(kws['d']):
                df_ = pd.DataFrame()
                for kv in listTools.box(kws['kv']):
                    k, v = kv.split(':')
                    marketDf = self.getMarketDf(k, d)
                    if marketDf is not None:
                        df_[kv] = _getV(marketDf, v)
                dfs.append(df_)
        elif listTools.isSubset(['k', 'v'], kws.keys()):
            for d in dateTools.drToD(kws['d']):
                marketDf = self.getMarketDf(kws['k'], d)
                df_ = pd.DataFrame()
                if marketDf is not None:
                    for v in listTools.box(kws['v']):
                        df_[v] = _getV(marketDf, v)
                dfs.append(df_)
        else:
            pass
        df = pd.concat(dfs).reset_index(drop=True)     
        return df

    def addToQueue(self, **kws):
        pass

    # def evalXY(self, k, d, xv, yv):
    #     df = self.getDf(k, d, [xv, yv])
    #     df = df.dropna(axis=0, how='any')
    #     if df.shape[0] > 1000:  # 如果行数太少就没有参考意义
    #         ret = {'K': k, 'D': d, 'XV': xv, 'YV': yv, 'Samples': df.shape[0]}
    #         ret['IC'] = df.corr().loc[yv, xv]
    #         ret['']