class ListQueue(list):  # 队列，用来控制循环
    def put(self, items):
        self += items
    
    def get(self):
        self.pop(0)
    
    @property
    def size(self):
        return self.__len__()

    def isEmpty(self):
        return self.size == 0


class ListStack(list):  # 栈
    def push(self, item):
        self.append(item)

    def peek(self):
        return self[-1]

    @property
    def size(self):
        return self.__len__()

    def isEmpty(self):
        return self.size == 0


def box(items):
    if isinstance(items, (tuple, list)):
        return list(items)
    else:
        return [items]


def isSubset(l1, l2):
    return set(l1) <= set(l2)