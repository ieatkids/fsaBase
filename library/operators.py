import numpy as np
from utils import arrayTools

# Binary Operators

def add(left, right):
    ret = left + right
    return ret.ravel()

def sub(left, right):
    ret = left - right
    return ret.ravel()

def mul(left, right):
    ret = left * right
    return ret.ravel()

def div(left, right):
    ret = left / right
    return ret.ravel()

# Unary Operators

def sqrt(left):
    ret = left ** .5
    return ret.ravel()

def log(left):
    ret = np.log(left)
    return ret.ravel()

def rollingMax(left, n):
    rw = arrayTools.rollingWindowPadding(left, n, padding=left[0])
    ret = rw.max(axis=1)
    return ret

def rollingMin(left, n):
    rw = arrayTools.rollingWindowPadding(left, n, padding=left[0])
    ret = rw.min(axis=1)
    return ret

def rollingMean(left, n):
    rw = arrayTools.rollingWindowPadding(left, n, padding=left[0])
    ret = rw.mean(axis=1)
    return ret

def rollingStd(left, n):
    rw = arrayTools.rollingWindowPadding(left, n, padding=left[0])
    ret = rw.std(axis=1)
    return ret

def lag(left, n):
    ret = left.shift(n)
    return ret.ravel()

def delta(left, n):
    ret = left.shift(n) - left
    return ret.ravel()

def ret(left, n):
    ret = left.shift(n) / left - 1
    return ret.ravel()
