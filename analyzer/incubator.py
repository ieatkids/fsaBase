from library import operators, functions
from inspect import getfullargspec
import pandas as pd


# COLUMNS = ['Price', 'Volume', 'BuyVolume', 'SellVolume', 'B1', 'B2', 'B3', 'B4', 'B5', 'A1',
#           'A2', 'A3', 'A4', 'A5', 'BQ1', 'BQ2', 'BQ3', 'BQ4', 'BQ5', 'AQ1', 'AQ2', 'AQ3', 'AQ4', 'AQ5']

# DIMENTIONS = {'Price': (1, 0), 'Volume': (0, 1), 'BuyVolume': (0, 1), 'SellVolume': (0, 1), 'B1': (1, 0), 'B2': (1, 0), 'B3': (1, 0), 'B4': (1, 0), 'B5': (1, 0), 'A1': (1, 0), 'A2': (1, 0), 'A3': (
#     1, 0), 'A4': (1, 0), 'A5': (1, 0), 'BQ1': (0, 1), 'BQ2': (0, 1), 'BQ3': (0, 1), 'BQ4': (0, 1), 'BQ5': (0, 1), 'AQ1': (0, 1), 'AQ2': (0, 1), 'AQ3': (0, 1), 'AQ4': (0, 1), 'AQ5': (0, 1)}



def _splitToken(token):
    name = token.split('_')[0]
    params = []
    for p in token.split('_')[1:]:
        try:
            params.append(eval(p))
        except NameError:
            params.append(p)
    return name, params


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
    def SuffixExpr(self):
        return '|'.join(self.postOrder())

    @property
    def dimension(self):
        pass

    @staticmethod
    def fromSuffixExpr(expr):
        tokens = expr.split('|')
        stack = []
        while tokens:
            token = tokens.pop(0)
            if token[0].isupper():
                stack.append(AlphaTree(token))
            else:
                if 'right' in getfullargspec(getattr(operators, token.split('_')[0])).args:
                    right = stack.pop()
                    left = stack.pop()
                    stack.append(AlphaTree(token, left, right))
                else:
                    left = stack.pop()
                    stack.append(AlphaTree(token, left))
        assert len(stack) == 1, SyntaxError('Suffix expression error.')
        return stack[0]

    def getValue(self, df):
        if self.depth == 1:
            if self.value in df.columns:
                return df[self.value].ravel()
            else:
                name, params = _splitToken(self.value)
                return getattr(functions, name)(df, *params)
        else:
            name, params = _splitToken(self.value)
            if self.right is None:
                return getattr(operators, name)(self.left.getValue(df), *params)
            else:
                return getattr(operators, name)(self.left.getValue(df), self.right.getValue(df), *params)

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