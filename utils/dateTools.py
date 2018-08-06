from datetime import datetime, timedelta

DATE_FORMAT = '%Y%m%d'

TIME_FORMAT_S = '%Y%m%d %H:%M:%S'

TIME_FORMAT_F = '%Y%m%d %H:%M:%S.%f'

# 主要有三种日期格式
# ds: dateStr. e.g. '20180102'
# dt： datetime.datetime
# dr: dateRange. e.g. '20180101 to 20180103'


def dsToDt(ds: str):  # dateStr转换为datetime
    return datetime.strptime(ds, DATE_FORMAT)


def dtToDs(dt):  # datetime转换为dateStr
    return dt.strftime(DATE_FORMAT)


def drToDt(dr):  # dateRange转换为datetime，不包含结束日期
    ds0, ds1 = dr.split(' to ')
    dt0 = dsToDt(ds0)
    dt1 = dsToDt(ds1)
    return [dt0 + timedelta(days=i) for i in range((dt1 - dt0).days)]


def drToDs(dr):  # dateRange转换为dateStr，不包含结束日期
    return [dtToDs(dt) for dt in drToDt(dr)]


def getCurDs():  # 获取当前日期
    return dtToDs(datetime.today())


def getDsShift(ds, offset):
    return dtToDs(dsToDt(ds) + timedelta(days=offset))


def getPrevDs(ds=None):
    ds = ds or getCurDs()
    return getDsShift(ds, -1)


def getNextDs(ds=None):
    ds = ds or getCurDs()
    return getDsShift(ds, 1)


def getDsCount(dr):
    return len(drToDs(dr))


