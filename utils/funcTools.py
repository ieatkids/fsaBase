from functools import wraps
from datetime import datetime


def timer(func):    # 计时器
    @wraps(func)
    def decorator(*args, **kwargs):
        print('[Timer] Calling {}...'.format(func.__qualname__))
        t = datetime.now()
        ret = func(*args, **kwargs)
        print('[Timer] Elapsed time: {} seconds'.format((datetime.now() - t).seconds))
        return ret
    return decorator


def monitor(func):  # 监视器，用来处理一些loop中容易报错的函数，报错时返回None
    @wraps(func)
    def decorator(*args, **kwargs):
        try:
            ret = func(*args, **kwargs)
        except Exception as e:
            print('[Monitor] Error occured. {}'.format(e.__str__()))
            ret = None
        return ret
    return decorator
