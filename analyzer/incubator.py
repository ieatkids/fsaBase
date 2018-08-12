from library import operators, functions
from inspect import getfullargspec
import pandas as pd


# expr: 逆波兰表达式
# 用|分隔开variables和operators，其中variables首字母为大写，可以为column名或函数表达式

COLUMNS = ['Price', 'Volume', 'BuyVolume', 'SellVolume', 'B1', 'B2', 'B3', 'B4', 'B5', 'A1',
          'A2', 'A3', 'A4', 'A5', 'BQ1', 'BQ2', 'BQ3', 'BQ4', 'BQ5', 'AQ1', 'AQ2', 'AQ3', 'AQ4', 'AQ5']

DIMENTIONS = {'Price': (1, 0), 'Volume': (0, 1), 'BuyVolume': (0, 1), 'SellVolume': (0, 1), 'B1': (1, 0), 'B2': (1, 0), 'B3': (1, 0), 'B4': (1, 0), 'B5': (1, 0), 'A1': (1, 0), 'A2': (1, 0), 'A3': (
    1, 0), 'A4': (1, 0), 'A5': (1, 0), 'BQ1': (0, 1), 'BQ2': (0, 1), 'BQ3': (0, 1), 'BQ4': (0, 1), 'BQ5': (0, 1), 'AQ1': (0, 1), 'AQ2': (0, 1), 'AQ3': (0, 1), 'AQ4': (0, 1), 'AQ5': (0, 1)}



def _splitToken(token):
    name = token.split('_')[0]
    params = []
    for p in token.split('_')[1:]:
        try:
            params.append(eval(p))
        except NameError:
            params.append(p)
    return name, params


def _isVariable(token):
    return token[0].isupper()


def _isColumn(token):
    return token in COLUMNS


def _isOperator(token):
    return token.split('_')[0] in dir(operators)


def _isFunction(token):
    return token.split('_')[0] in dir(functions)


def _isUnaryOperator(token):
    name = token.split('_')[0]
    return all([name in dir(operators), 'right' not in getfullargspec(getattr(operators, token.split('_')[0])).args])


def getExprValue(df, expr):
    stack = []
    tokens = expr.split('|')
    df_ = df[[_ for _ in tokens if _ in df.columns]].copy()
    for token in [_ for _ in tokens if _isFunction(_)]:
        name, params = _splitToken(token)
        df_[token] = getattr(functions, name)(df, *params)
    step = 0
    while tokens:
        token = tokens.pop(0)
        if _isVariable(token):
            stack.append(token)
        else:
            name, params = _splitToken(token)
            left = df_[stack.pop()]
            if _isUnaryOperator(token):
                df_[f'Cache{step}'] = getattr(operators, name)(left, *params)
            else:
                right = df_[stack.pop()]
                df_[f'Cache{step}'] = getattr(
                    operators, name)(left, right, *params)
            stack.append(f'Cache{step}')
            step += 1
    return df_[stack[0]].ravel()


class AlphaTree:
    def __init__(self, value, left=None, right=None):
        self.value = value
        self.left = left
        self.right = right

    @property
    def depth(self):
        leftDepth = self.left.depth if self.left else 0
        rightDepth = self.right.depth if self.right else 0
        return max(leftDepth, rightDepth) + 1

    @property
    def expr(self):
        return '|'.join(self.postOrder())

    @property
    def dimension(self):
        pass

    @staticmethod
    def fromExpr(expr):
        tokens = expr.split('|')
        stack = []
        while tokens:
            token = tokens.pop(0)
            if _isVariable(token):
                stack.append(AlphaTree(token))
            else:
                if _isUnaryOperator(token):
                    left = stack.pop()
                    stack.append(AlphaTree(token, left))
                else:
                    right = stack.pop()
                    left = stack.pop()
                    stack.append(AlphaTree(token, left, right))
        assert len(stack) == 1, SyntaxError('表达式有误')
        return stack[0]

    def preOrder(self):
        l = []

        def _preOrder(root):
            if root is None:
                return
            else:
                l.append(root.value)
                _preOrder(root.left)
                _preOrder(root.right)
        _preOrder(self)
        return l

    def inOrder(self):
        l = []

        def _inOrder(root):
            if root is None:
                return
            else:
                _inOrder(root.left)
                l.append(root.value)
                _inOrder(root.right)
        _inOrder(self)
        return l

    def postOrder(self):
        l = []

        def _postOrder(root):
            if root is None:
                return
            else:
                _postOrder(root.left)
                _postOrder(root.right)
                l.append(root.value)
        _postOrder(self)
        return l

    def levelOrder(self):
        l = []
        curLevel = [self]
        while curLevel:
            l.append([n.value for n in curLevel])
            nextLevel = []
            for n in curLevel:
                if n.left is not None:
                    nextLevel.append(n.left)
                if n.right is not None:
                    nextLevel.append(n.right)
            curLevel = nextLevel
        return l

    def getValue(self, df):
        return getExprValue(df, self.expr)
