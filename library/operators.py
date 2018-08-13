import numpy as np
from utils import arrayTools

# Binary Operators

def add(left, right):
    return left + right

def sub(left, right):
    return left - right

def mul(left, right):
    return left * right

def div(left, right):
    return left / right

# Unary Operators

def sqrt(left):
    return left ** .5

def log(left):
    return np.log(left)

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
    return np.concatenate([[left[0]] * n, left[:-n]])

# def delta(left, n):
#     return lag(left, n) - left

# def ret(left, n):
#     left[left==0] = float('nan')
#     return lag(left, n) / left - 1
