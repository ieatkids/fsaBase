from provider import histData
from library import marketDfFuncs
from fsaConfig import MD_FOLDER
from utils import dateTools


class Evaluator():
    def __init__(self, freq=1):
        self.cache = {}
        self.freq = freq

    def getMarketDf(self, k, d):
        self.cache[(k, d)] = histData.getMarketDf(k, d, freq=self.freq)

    

