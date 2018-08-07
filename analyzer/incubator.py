from dataclasses import dataclass
from itertools import chain

@dataclass
class BinaryTree:
    value: str or None = None
    left: str or None = None
    right: str or None = None

    @property
    def depth(self):
        leftDepth = self.left.depth if self.left else 0
        rightDepth = self.right.depth if self.right else 0
        return max(leftDepth, rightDepth) + 1

    @property
    def expr(self):
        if self.left is None and self.right is None:
            return self.value
        else:
            if self.right is None:
                return f'{self.left.expr}|{self.value}'
            else:
                return f'{self.left.expr}|{self.right.expr}|{self.value}'

    def fromExpr(self, expr):
        pass



a = BinaryTree('A')

b = BinaryTree('B')

c = BinaryTree('-', a, b)

d = BinaryTree('+', a, b)

e = BinaryTree('/', c, d)




