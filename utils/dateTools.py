from datetime import datetime, timedelta

DATE_FORMAT = '%Y%m%d'

TIME_FORMAT_S = '%Y%m%d %H:%M:%S'

TIME_FORMAT_F = '%Y%m%d %H:%M:%S.%f'

# 主要有三种日期格式
# d: dateStr. e.g. '20180102'
# dt： datetime.datetime
# dr: dateRange. e.g. '20180101 to 20180103'


def dToDt(d: str):  # dateStr转换为datetime
    return datetime.strptime(d, DATE_FORMAT)


def dtToD(dt):  # datetime转换为dateStr
    return dt.strftime(DATE_FORMAT)


def drToDt(dr):  # dateRange转换为datetime，不包含结束日期
    if ' to ' in dr:
        d0, d1 = dr.split(' to ')
        dt0 = dToDt(d0)
        dt1 = dToDt(d1)
        return [dt0 + timedelta(days=i) for i in range((dt1 - dt0).days)]
    else:
        return [dToDt(dr)]


def drToD(dr):  # dateRange转换为dateStr，不包含结束日期
    return [dtToD(dt) for dt in drToDt(dr)]


def getCurD():  # 获取当前日期
    return dtToD(datetime.today())


def getDtShift(dt, offset):
    return dt + timedelta(days=offset)


def getDShift(d, offset):
    return dtToD(getDtShift(dToDt(d), offset))


def getPrevDs(d=None):
    d = d or getCurD()
    return getDShift(d, -1)


def getNextD(d=None):
    d = d or getCurD()
    return getDShift(d, 1)


def getDCount(dr):
    return len(drToD(dr))


