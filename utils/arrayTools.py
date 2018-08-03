import numpy as np


def rollingWindow(arr, window):
    shape = arr.shape[:-1] + (arr.shape[-1] - window + 1, window)
    strides = arr.strides + (arr.strides[-1], )
    rw = np.lib.stride_tricks.as_strided(arr, shape=shape, strides=strides)
    return rw

def rollingWindowPadding(arr, window, padding=None):
    if padding is None:
        arr = np.concatenate([[arr[0]] * (window - 1), arr])
    else:
        arr = np.concatenate([[padding] * (window - 1), arr])
    return rollingWindow(arr, window)









