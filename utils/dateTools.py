from datetime import datetime, timedelta
import numpy as np

DATE_FORMAT = '%Y%m%d'


TIME_FORMAT = '%Y%m%d %H:%M:%S'


TIME_UNIT = {'w': 'weeks', 'd': 'days', 'h': 'hours', 'm': 'minutes', 's': 'seconds', 'ms': 'milliseconds'}


# d: datestr. e.g. '20180102'
# dt： datetime.datetime
# ts: datetime.datetime.timestamp
# td: datetime.timedelta


def toDs(dt):       # str to datetime
    return dt.strftime(DATE_FORMAT)


def toDt(d: str):   # datetime to str
    return datetime.strptime(d, DATE_FORMAT)


def getCurDate():   # 获取当前日期
    return toDs(datetime.now())
    

def getDateShift(d: str, shift: int):
    return toDs(toDt(d) + timedelta(days=shift))


def getNextDate(d=None):    # 获取次日日期
    d = d or getCurDate()
    return getDateShift(d, 1)


def getPrevDate(d=None):    # 获取前日日期
    d = d or getCurDate()
    return getDateShift(d, -1)


def getDatesCount(d0:str, d1:str, includeEndDate=False):    # 计算天数
    n = (toDt(d1) - toDt(d0)).days
    if includeEndDate:
        n += 1
    return n


def getTimeTicks(d, t, step=0.5):   # 获取当天标准化时间
    dt = datetime.strptime(f'{d} {t}', TIME_FORMAT)
    unit = timedelta(seconds=step)
    n = int(timedelta(days=1) / unit)
    return list(map(lambda _: dt + _ * unit, range(n)))


