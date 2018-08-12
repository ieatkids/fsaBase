from provider import histData
from analyzer import incubator
from library import functions
from fsaConfig import MD_FOLDER
from utils import dateTools, listTools
import pandas as pd


class Evaluator:
    def __init__(self, freq=1):
        self.cache = {}
        self.freq = freq

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
        >>> df = ev.getDf(d='20180730', kv=['btcusd.bitstamp:Price|lag_1', 'btceur.bitstamp:Price|lag_1'])
        >>> print(df.tail())
               btcusd.bitstamp:Price|lag_1  btceur.bitstamp:Price|lag_1
        86395                      8160.21                      8160.21
        86396                      8160.21                      8160.21
        86397                      8160.21                      8160.21
        86398                      8160.21                      8160.21
        86399                      8160.21                      8160.21
        """
        dfs = []
        if 'kv' in kws:
            for d in dateTools.drToD(kws['d']):
                df_ = pd.DataFrame()
                for kv in listTools.box(kws['kv']):
                    k, v = kv.split(':')
                    marketDf = self.getMarketDf(k, d)
                    if marketDf is not None:
                        df_[kv] = incubator.AlphaTree.fromSuffixExpr(v).getValue(marketDf)
                dfs.append(df_)
        elif listTools.isSubset(['k', 'v'], kws.keys()):
            for d in dateTools.drToD(kws['d']):
                marketDf = self.getMarketDf(kws['k'], d)
                df_ = pd.DataFrame()
                if marketDf is not None:
                    for v in listTools.box(kws['v']):
                        df_[v] = incubator.AlphaTree.fromSuffixExpr(v).getValue(marketDf)
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