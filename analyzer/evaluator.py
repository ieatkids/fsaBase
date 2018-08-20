import sqlite3
import seaborn
import pandas as pd
import numpy as np
from matplotlib import pyplot as plt
from itertools import product
from analyzer import incubator
from fsaConfig import MD_FOLDER
from library import functions
from provider import histData
from utils import dateTools, listTools, funcTools


class Evaluator:
    def __init__(self, freq=1):
        self.cache = {}
        self.freq = freq
        self.xyDf = pd.DataFrame()

    def toDB(self):
        pass

    def getMarketDf(self, k, d):
        if (k, d) not in self.cache:
            self.cache[(k, d)] = histData.getMarketDf(k, d, freq=self.freq)
        return self.cache[(k, d)]
    
    def getDf(self, **kws):
        """
        >>> ev = Evaluator()
        >>> df = ev.getDf(k='btcusd.bitstamp', d='20180808', v=['MicPrc|MidPrc|sub', 'NetVol', 'BookPrs'])
        >>> print(df.tail())
               MicPrc|MidPrc|sub    NetVol   BookPrs
        86395           0.448537  0.000000  0.934453
        86396           0.448537  0.006206  0.934453
        86397           0.448537  0.002412  0.934453
        86398           1.284447  0.000000  0.930759
        86399           1.284447  0.000000  0.930759
        >>> df = ev.getDf(d='20180810', kv=['btcusd.bitstamp:Price', 'btceur.bitstamp:Price'])
        >>> print(df.tail())
               btcusd.bitstamp:Price|lag_1  btceur.bitstamp:Price|lag_1
        86395                      8160.21                      8160.21
        86396                      8160.21                      8160.21
        86397                      8160.21                      8160.21
        86398                      8160.21                      8160.21
        86399                      8160.21                      8160.21
        """
        dfs = []
        if listTools.isSubset(['d', 'kv'], kws.keys()):
            for d in dateTools.drToD(kws['d']):
                df_ = pd.DataFrame()
                for kv in listTools.box(kws['kv']):
                    k, v = kv.split(':')
                    marketDf = self.getMarketDf(k, d)
                    if marketDf is not None:
                        df_[kv] = incubator.AlphaTree.fromPostfixExpr(v).getArray(marketDf)
                dfs.append(df_)
        elif listTools.isSubset(['d', 'k', 'v'], kws.keys()):
            for d in dateTools.drToD(kws['d']):
                marketDf = self.getMarketDf(kws['k'], d)
                df_ = pd.DataFrame()
                if marketDf is not None:
                    for v in listTools.box(kws['v']):
                        df_[v] = incubator.AlphaTree.fromPostfixExpr(v).getArray(marketDf)
                dfs.append(df_)
        else:
            pass
        df = pd.concat(dfs).reset_index(drop=True)
        return df

    @staticmethod
    def getTaskQueue(**kws):
        if listTools.isSubset(['d', 'xkv', 'ykv'], kws.keys()):
            d = dateTools.drToD(kws['d'])
            xkv = listTools.box(kws['xkv'])
            ykv = listTools.box(kws['ykv'])
            queue = list(map(lambda _: ':'.join(_), product(d, xkv, ykv)))
        elif listTools.isSubset(['d', 'k', 'xv', 'yv'], kws.keys()):
            d = dateTools.drToD(kws['d'])
            k = listTools.box(kws['k'])
            xv = listTools.box(kws['xv'])
            yv = listTools.box(kws['yv'])
            queue = list(map(lambda _: ':'.join(_), product(d, k, xv, k, yv)))
        else:
            queue = []
        return queue

    def evalXY(self, **kws):
        queue = Evaluator.getTaskQueue(**kws)
        xyDf = pd.DataFrame(columns=['Hash', 'D', 'YK', 'XK', 'YV', 'XV', 'IC', 'Samples', 'XMax', 'XMin', 'XMean', 'XStd', 'XBot1', 'XTop1', 'YBot1', 'YTop1'])
        @funcTools.monitor
        def _evalXY(task):
            d, xk, xv, yk, yv = task.split(':')
            xkv = f'{xk}:{xv}'
            ykv = f'{yk}:{yv}'
            df = self.getDf(d=d, kv=[xkv, ykv]).dropna(axis=0, how='any')
            if df.shape[0] > 10000: # 如数据中NaN太多就没有参考价值。
                x = df[xkv].ravel()
                y = df[ykv].ravel()
                newRow = dict(zip(['D', 'XK', 'XV', 'YK', 'YV'], task.split(':')))
                newRow['Hash'] = hash(task) # 计算hash值方便去重
                newRow['IC'] = np.corrcoef(x, y)[0][1]
                newRow['Samples'] = df.shape[0]
                newRow['XMax'] = x.max()
                newRow['XMin'] = x.min()
                newRow['XMean'] = x.mean()
                newRow['XStd'] = x.std()
                newRow['XBot1'] = np.percentile(x, 1)
                newRow['XTop1'] = np.percentile(x, 99)
                newRow['YBot1'] = y[x <= newRow['XBot1']].mean()
                newRow['YTop1'] = y[x >= newRow['XTop1']].mean()
            else:
                newRow = {}
            return newRow
        while queue:
            task = queue.pop(0)
            xyDf = xyDf.append(_evalXY(task), ignore_index=True)
        self.xyDf = pd.concat([self.xyDf, xyDf], ignore_index=True).drop_duplicates(subset='Hash')
        return xyDf

    def pivotXY(self, **kws):
        indexCols = ['XK', 'XV', 'YK', 'YV']
        xyDf = self.xyDf
        for k, v in kws.items():
            xyDf = xyDf.loc[xyDf[k.upper()]==v]
            indexCols.remove(k.upper())
        pivotDf = xyDf.pivot_table(index=indexCols, values=['IC', 'Samples', 'YBot1', 'YTop1'])
        icStd = xyDf.pivot_table(index=indexCols, values=['IC'], aggfunc='std')['IC'].ravel()
        icStd[icStd==0] = float('nan')
        pivotDf['IR'] = pivotDf['IC'] / icStd
        return pivotDf