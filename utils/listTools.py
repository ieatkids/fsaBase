class ListQueue(list):  # 队列，用来控制循环
    def put(self, items):
        self += items
    
    def get(self):
        self.pop(0)
    
    @staticmethod
    def size(self):
        return self.__len__()

    def isEmpty(self):
        return self.size == 0


class ListStack(list):  # 栈
    def put(self, item):
        self.append(item)

    def get(self):
        self.pop()

    @staticmethod
    def size(self):
        return self.__len__()

    def isEmpty(self):
        return self.size == 0


def box(items):
    if isinstance(items, (tuple, list)):
        return list(items)
    else:
        return [items]




