from gplearn import genetic
from analyzer import evaluator
import numpy as np
import matplotlib.pyplot as plt

k = 'btcusd.bitstamp'
d='20180730'
xv = ['Price', 'Volume', 'BuyVolume', 'SellVolume', 'MidPrc', 'MicPrc', 'BookPrs', 'B1', 'A1', 'BQ1', 'AQ1']
yv = 'Ret_MidPrc_20'
ev = evaluator.MarketDfEvaluator()
df = ev.getDf(k=k, d=d, v=xv+[yv])
df = df.dropna(axis=0, how='any')
x = np.asarray(df[xv])
y = np.asarray(df[yv])



def test(x, y):
    print(f'x shape: {x.shape}\t y shape: {y.shape}')
    reg = genetic.SymbolicTransformer(
        init_method='grow',
        init_depth=(5, 10),
        metric='pearson',
        function_set=('add', 'sub', 'mul', 'div', 'inv', 'sqrt', 'log', 'abs', 'neg', 'max', 'min')
        )
    reg.fit(x, y)
    print(f'formula: {reg}')


if __name__ == '__main__':
    test(x, y)

