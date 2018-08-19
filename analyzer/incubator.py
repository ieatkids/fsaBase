from library import operators, functions
from inspect import getfullargspec
import pandas as pd


# COLUMNS = ['Price', 'Volume', 'BuyVolume', 'SellVolume', 'B1', 'B2', 'B3', 'B4', 'B5', 'A1',
#           'A2', 'A3', 'A4', 'A5', 'BQ1', 'BQ2', 'BQ3', 'BQ4', 'BQ5', 'AQ1', 'AQ2', 'AQ3', 'AQ4', 'AQ5']

# DIMENTIONS = {'Price': (1, 0), 'Volume': (0, 1), 'BuyVolume': (0, 1), 'SellVolume': (0, 1), 'B1': (1, 0), 'B2': (1, 0), 'B3': (1, 0), 'B4': (1, 0), 'B5': (1, 0), 'A1': (1, 0), 'A2': (1, 0), 'A3': (
#     1, 0), 'A4': (1, 0), 'A5': (1, 0), 'BQ1': (0, 1), 'BQ2': (0, 1), 'BQ3': (0, 1), 'BQ4': (0, 1), 'BQ5': (0, 1), 'AQ1': (0, 1), 'AQ2': (0, 1), 'AQ3': (0, 1), 'AQ4': (0, 1), 'AQ5': (0, 1)}



def _splitToken(token): # 
    name = token.split('_')[0]
    params = []
    for p in token.split('_')[1:]:
        try:
            params.append(eval(p))
        except NameError:
            params.append(p)
    return name, params


class AlphaTree:    # Alpha的基本格式
    def __init__(self, value, left=None, right=None):
        self.value = value
        self.insertLeft(left)
        self.insertRight(right)

    def insertLeft(self, value):
        self.left = AlphaTree(value) if isinstance(value, str) else value

    def insertRight(self, value):
        self.right = AlphaTree(value) if isinstance(value, str) else value

    def isLeaf(self):
        return all(self.left is None, self.right is None)
    
    def __repr__(self):
        return f'AlphaTree(Postfix Expression:{self.asPostfix})'

    def __str__(self):
        blank = ' ' * max([len(_) for _ in self.preOrder()])
        curLevel = [self]
        l = []
        i = 0
        while curLevel:
            margin = 2 ** (self.height - i - 1) - 1
            space = 2 ** (self.height - i) - 1
            curLine = []
            nextLevel = []
            for node in curLevel:
                if node is None:
                    node = AlphaTree(blank)
                curLine.append(f'{blank}{node.value}'[-len(blank):])
                nextLevel.append(node.left)
                nextLevel.append(node.right)
            l.append(f'{blank * margin}{(blank * space).join(curLine)}')
            if any(nextLevel):
                curLevel = nextLevel
                i += 1
            else:
                break
        return '\n'.join(l)
    
    def __eq__(self, other):
        return self.asPostfix == other.asPostfix

    @property
    def height(self):
        leftHeight = 0 if self.left is None else self.left.height
        rightHeight = 0 if self.right is None else self.right.height
        return max(leftHeight, rightHeight) + 1

    @property
    def asPostfix(self):
        return '|'.join(self.postOrder())

    @property
    def dimension(self):
        pass

    @staticmethod
    def fromPostfixExpr(postfixExpr):
        tokens = postfixExpr.split('|')
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
        assert len(stack) == 1, SyntaxError('Postfix expression error.')
        return stack[0]

    def getValue(self, df):
        if self.isLeaf():
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

        def _preOrder(node):
            if node is None:
                return
            else:
                l.append(node.value)
                _preOrder(node.left)
                _preOrder(node.right)
        _preOrder(self)
        return l

    def inOrder(self):
        l = []

        def _inOrder(node):
            if node is None:
                return
            else:
                _inOrder(node.left)
                l.append(node.value)
                _inOrder(node.right)
        _inOrder(self)
        return l

    def postOrder(self):
        l = []

        def _postOrder(node):
            if node is None:
                return
            else:
                _postOrder(node.left)
                _postOrder(node.right)
                l.append(node.value)
        _postOrder(self)
        return l

    def levelOrder(self):
        l = []
        curLevel = [self]
        while curLevel:
            l.append([n.value for n in curLevel])
            nextLevel = []
            for n in curLevel:
                if n.left:
                    nextLevel.append(n.left)
                if n.right:
                    nextLevel.append(n.right)
            curLevel = nextLevel
        return l