import sqlite3
import pandas as pd
import numpy as np
import seaborn as sns
from pathlib2 import Path
from matplotlib import pyplot as plt
from itertools import product
from analyzer import incubator
from fsaConfig import MD_FOLDER
from library import functions
from provider import histData
from utils import dateTools, listTools, funcTools


PERFORM_DB_PATH = Path(MD_FOLDER, 'features.db').as_posix()


class Evaluator:
    def __init__(self, freq=1):
        self.cache = {}
        self.freq = freq
        self.featureDf = pd.DataFrame()

    def toDB(self):
        con = sqlite3.connect(PERFORM_DB_PATH)
        for yk in self.featureDf['YK'].unique().tolist():
            self.featureDf.loc[self.featureDf['YK'] == yk].to_sql(
                yk, con, if_exists='replace', index=False, index_label='Hash')
        con.close()

    def fromDB(self, **kws):
        assert 'yk' in kws or 'xv' in kws, ValueError('pass')
        con = sqlite3.connect(PERFORM_DB_PATH)
        tails = []
        if 'yk' in kws:
            univ = listTools.box(kws.pop('yk'))
        else:
            univ = pd.read_sql("SELECT name FROM sqlite_master", con)[
                'name'].tolist()
        if 'd' in kws and 'to' in kws['d']:
            tails.append(
                f"d >= {kws['d'].split(' to ')[0]} and d < {kws['d'].split(' to ')[1]}")
        for k, v in kws.items():
            tails.append(f"{k} = '{v}'")
        sql = "SELECT * FROM '{}'"
        if tails:
            sql += " WHERE " + " AND ".join(tails)
        featureDf = pd.concat(map(lambda yk: pd.read_sql(
            sql.format(yk), con), univ), ignore_index=True)
        self.featureDf = pd.concat([self.featureDf, featureDf], ignore_index=True)
        con.close()
        return featureDf

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
                cols = {kv: incubator.getArray(self.getMarketDf(
                    kv.split(':')[0], d), kv.split(':')[1]) for kv in listTools.box(kws['kv'])}
                dfs.append(pd.DataFrame(cols))
        elif listTools.isSubset(['d', 'k', 'v'], kws.keys()):
            for d in dateTools.drToD(kws['d']):
                cols = {v: incubator.getArray(self.getMarketDf(
                    kws['k'], d), v) for v in listTools.box(kws['v'])}
                dfs.append(pd.DataFrame(cols))
        else:
            pass
        df = pd.concat(dfs).reset_index(drop=True)
        return df

    def evalFeature(self, **kws):
        d = dateTools.drToD(kws['d'])
        if listTools.isSubset(['d', 'xkv', 'ykv'], kws.keys()):
            xkv = listTools.box(kws['xkv'])
            ykv = listTools.box(kws['ykv'])
            queue = list(map(lambda _: ':'.join(_), product(d, xkv, ykv)))
        elif listTools.isSubset(['d', 'k', 'xv', 'yv'], kws.keys()):
            k = listTools.box(kws['k'])
            xv = listTools.box(kws['xv'])
            yv = listTools.box(kws['yv'])
            queue = list(map(lambda _: ':'.join(_), product(d, k, xv, k, yv)))
        elif 'json' in kws:
            pass
        else:
            queue = []
        featureDf = pd.DataFrame(columns=['Hash', 'D', 'YK', 'XK', 'YV', 'XV', 'IC', 'Samples',
                                    'XMax', 'XMin', 'XMean', 'XStd', 'XBot1', 'XTop1', 'YBot1', 'YTop1'])

        @funcTools.monitor
        def _loop(task):
            d_, xk_, xv_, yk_, yv_ = task.split(':')
            xkv_ = f'{xk_}:{xv_}'
            ykv_ = f'{yk_}:{yv_}'
            df = self.getDf(d=d_, kv=[xkv_, ykv_]).dropna(axis=0, how='any')
            if df.shape[0] > 10000:  # 如数据中NaN太多就没有参考价值。
                x = df[xkv_].ravel()
                y = df[ykv_].ravel()
                newRow = dict(
                    zip(['D', 'XK', 'XV', 'YK', 'YV'], task.split(':')))
                newRow['Hash'] = hash(task)  # 计算hash值方便去重
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
            featureDf = featureDf.append(_loop(task), ignore_index=True)
        self.featureDf = pd.concat(
            [self.featureDf, featureDf], ignore_index=True).drop_duplicates(subset='Hash')
        if kws.get('save', True):
            self.toDB()
        return featureDf

    def predictY(self, **kws):
        if 'json' in kws:
            pass
        elif listTools.isSubset(['d', 'k', 'xv', 'yv'], kws.keys()):
            d = dateTools.drToD(kws['d'])
            xkv = [f'{kws["k"]}:{v}' for v in listTools.box(kws['xv'])]
            ykv = f'{kws["k"]}:{kws["yv"]}'
        elif listTools.isSubset(['d', 'xkv', 'ykv'], kws.keys()):
            d = dateTools.drToD(kws['d'])
            xkv = listTools.box(kws['xkv'])
            ykv = kws['ykv']
        else:
            d = []
            xkv = []
            ykv = None

        traningDays = kws.get('trainingDays', 10)
        testingDays = kws.get('testingDays', 1)
        model = None
        lastDate = None

        def _loop(model, lastDate):
            pass

        while d:
            curDate = d.pop(0)
            pass

    def pivotFeature(self, **kws):
        """
        >>> ev = Evaluator()
        >>> ev.evalFeature(d='20180810', k='btcusd.bitstamp', xv='BookPrs', yv='Ret_MidPrc_60')
        <BLANK LINE>
        >>> ev.evalFeature(d='20180809 to 20180811', xkv=['btcusd.bitstamp:NetVol', 'btcusd.bitstamp:MicPrc|MidPrc|div'], ykv='btcusd.bitstamp:MaxDeviRet_MidPrc_60')
        """
        indexCols = ['XK', 'XV', 'YK', 'YV']
        featureDf = self.featureDf
        for k, v in kws.items():
            featureDf = featureDf.loc[featureDf[k.upper()] == v]
            indexCols.remove(k.upper())
        pivotDf = featureDf.pivot_table(index=indexCols, values=[
                                  'IC', 'Samples', 'YBot1', 'YTop1'])
        try:
            icStd = featureDf.pivot_table(index=indexCols, values=[
                                    'IC'], aggfunc='std')['IC'].ravel()
            icStd[icStd == 0] = float('nan')
            pivotDf['IR'] = pivotDf['IC'] / icStd
        except:
            pivotDf['IR'] = float('nan')
        return pivotDf

    def linePlot(self, **kws):
        """
        >>> ev = Evaluator()
        >>> ev.linePlot(k='btcusd.bitstamp', d='20180810', v=['Price', 'MA_600', 'EMA_1200'])
        <BLANK LINE>
        >>> ev.linePlot(d='20180810', kv=['btcusd.bitstamp:Price', 'btceur.bitstamp:Price'])
        """
        df = self.getDf(**kws).dropna(axis=0, how='any')
        fig = df.plot().figure
        if kws.get('show', True):
            plt.show()
        return fig

    def jointPlot(self, **kws):
        """
        >>> ev = Evaluator()
        >>> ev.jointPlot(d='20180810', k='btcusd.bitstamp', v=['MACD_12_24_9', 'Ret_MidPrc_60'])
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
        >>> ev.pctPlot(d='20180810', kv=['btceur.bitstamp:MicPrc|MidPrc|div', 'btcusd.bitstamp:Ret_MidPrc_60'])
        """
        df = self.getDf(**kws).dropna(axis=0, how='any')
        pcts = [1, 5, 10, 90, 95, 99]
        x = df.iloc[:, 0].ravel()
        y = df.iloc[:, 1].ravel()
        fig = plt.figure()
        bars = list(map(lambda p: y[x <= np.percentile(x, p)].mean(
        ) if p < 50 else y[x >= np.percentile(x, p)].mean(), pcts))
        sns.barplot(x=pcts, y=bars, palette="rocket", ax=fig.add_subplot(211))
        yBot = y[x <= np.percentile(x, 1)]
        yTop = y[x >= np.percentile(x, 99)]
        sns.distplot(yBot, ax=fig.add_subplot(212), color='g')
        sns.distplot(yTop, ax=fig.add_subplot(212), color='r')
        if kws.get('show', True):
            fig.show()
        return fig

    def distPlot(self, **kws):
        df = self.getDf(**kws).dropna(axis=0, how='any')
        fig = sns.distplot(df).figure
        if kws.get('show', True):
            fig.show()
        return fig
