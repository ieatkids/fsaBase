import sqlite3
import pandas as pd
import numpy as np
import seaborn as sns
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
        self.xDf = pd.DataFrame()

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
                cols = {kv: incubator.getArray(self.getMarketDf(kv.split(':')[0], d), kv.split(':')[1]) for kv in listTools.box(kws['kv'])}
                dfs.append(pd.DataFrame(cols))
        elif listTools.isSubset(['d', 'k', 'v'], kws.keys()):
            for d in dateTools.drToD(kws['d']):
                cols = {v: incubator.getArray(self.getMarketDf(kws['k'], d), v) for v in listTools.box(kws['v'])}
                dfs.append(pd.DataFrame(cols))
        else:
            pass
        df = pd.concat(dfs).reset_index(drop=True)
        return df

    def evalX(self, **kws):
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
        xDf = pd.DataFrame(columns=['Hash', 'D', 'YK', 'XK', 'YV', 'XV', 'IC', 'Samples', 'XMax', 'XMin', 'XMean', 'XStd', 'XBot1', 'XTop1', 'YBot1', 'YTop1'])
        @funcTools.monitor
        def _loop(task):
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
            xDf = xDf.append(_loop(task), ignore_index=True)
        self.xDf = pd.concat([self.xDf, xDf], ignore_index=True).drop_duplicates(subset='Hash')
        return xDf

    def pivotX(self, **kws):
        """
        >>> ev = Evaluator()
        >>> ev.evalX(d='20180810', k='btcusd.bitstamp', xv='BookPrs', yv='Ret_MidPrc_60')
        <BLANK LINE>
        >>> ev.evalX(d='20180809 to 20180811', xkv=['btcusd.bitstamp:NetVol', 'btcusd.bitstamp:MicPrc|MidPrc|div'], ykv='btcusd.bitstamp:MaxDeviRet_MidPrc_60')
        """
        indexCols = ['XK', 'XV', 'YK', 'YV']
        xDf = self.xDf
        for k, v in kws.items():
            xDf = xDf.loc[xDf[k.upper()]==v]
            indexCols.remove(k.upper())
        pivotDf = xDf.pivot_table(index=indexCols, values=['IC', 'Samples', 'YBot1', 'YTop1'])
        icStd = xDf.pivot_table(index=indexCols, values=['IC'], aggfunc='std')['IC'].ravel()
        icStd[icStd==0] = float('nan')
        pivotDf['IR'] = pivotDf['IC'] / icStd
        return pivotDf

    def jointPlot(self, **kws):
        """
        >>> ev = Evaluator()
        >>> ev.jointPlot(d='20180810', k='btcusd.bitstamp', v=['MicPrc|MidPrc|div', 'MaxDeviRet_MidPrc_60'])
        <BLANK LINE>
        >>> ev.jointPlot(d='20180810', kv=['btceur.bitstamp:MicPrc|MidPrc|div', 'btcusd.bitstamp:MaxDeviRet_MidPrc_60'])
        """
        df = self.getDf(**kws).dropna(axis=0, how='any')
        fig = sns.jointplot(x=df.iloc[:, 0], y=df.iloc[:, 1], kind='hex').fig
        if kws.get('show', True):
            fig.show()
        return fig

    def pctPlot(self, **kws):
        """
        >>> ev = Evaluator()
        >>> ev.pctPlot(d='20180810', k='btcusd.bitstamp', v=['BookPrs', 'MaxDeviRet_MidPrc_60'])
        <BLANK LINE>
        >>> ev.pctPlot(d='20180810', kv=['btceur.bitstamp:MicPrc|MidPrc|div', 'btcusd.bitstamp:MaxDeviRet_MidPrc_60'])
        """
        df = self.getDf(**kws).dropna(axis=0, how='any')
        pcts = [1, 5, 10, 90, 95, 99]
        x = df.iloc[:, 0].ravel()
        y = df.iloc[:, 1].ravel()
        fig = plt.figure()
        bars = list(map(lambda p: y[x <= np.percentile(x, p)].mean() if p < 50 else y[x >= np.percentile(x, p)].mean(), pcts))
        sns.barplot(x=pcts, y=bars, palette="rocket", ax=fig.add_subplot(211))
        yBot = y[x <= np.percentile(x, 1)]
        yTop = y[x >= np.percentile(x, 99)]
        sns.distplot(yBot, ax=fig.add_subplot(212), color='g')
        sns.distplot(yTop, ax=fig.add_subplot(212), color='r')
        if kws.get('show', True):
            fig.show()
        return fig

    def distPlot(self, **kws):
        pass