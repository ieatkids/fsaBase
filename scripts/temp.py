from provider import md
from fsaConfig import MD_FOLDER
from pathlib2 import Path
import pandas as pd
import h5py


filePath = 'D:\\md\\rawMd\\exch_bitstamp_bchusd_20180730.h5'

FREQS = [1, 60]


class RawDataTransformer():
    @staticmethod
    def getH5Path(fileName):
        _, exchange, _, date = fileName.split('_')
        h5Path = Path(MD_FOLDER, exchange, date)
        return str(h5Path)

    @staticmethod
    def create(filePath):
        pass

    @staticmethod
    def append(filePath):
        pass












