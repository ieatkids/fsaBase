from provider import md
from utils import arrayTools

"""
>>> df = md._testSampledMd
"""

def MidPrc(df):
    ret = df[['B1', 'A1']].mean(axis=1)
    return ret.ravel()


def MicPrc(df):
    ret = (df['B1'] * df['AQ1'] + df['A1'] * df['BQ1']) / (df['BQ1'] + df['AQ1'])
    return ret.ravel()


def BookPrs(df):
    ret = (df['BQ1'] - df['AQ1']) / (df['BQ1'] + df['AQ1'])
    return ret.ravel()

