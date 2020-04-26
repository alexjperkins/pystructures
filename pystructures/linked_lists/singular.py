"""Singularly linked list."""


import functools
import itertools
import operator


SUB1 = lambda x: x - 1


class Node:
    """Linked list node."""

    def __init__(self, value):
        self._value = value
        self._next = None

    def _append(self, value):
        """Inserts a new value to right."""
        if not self._next:
            self._next = self.__class__(value=value)
            return self._next

        return self._next._append(value=value)

    def _traverse(self, i):
        """Recursively traverses through linked list."""
        yield self
        if self._next is not None and i > 0:
            yield from self._next._traverse(SUB1(i))

    def __str__(self):
        return f'Node: {self._value}'

    def __repr__(self):
        return f'Node: {self._value}'


class SingularyLinkedList:
    """Singularly linked list."""

    def __init__(self, node_type=Node):
        self._head = None
        self._tail = None
        self.__length = 0
        self.__node_type = node_type

    def __len__(self):
        return self.__length

    def __iter__(self):
        return self.traverse(len(self))

    def _build_node(self, *args, **kwargs):
        """Builds a node from `self.__node_type`."""
        return self.__node_type(*args, **kwargs)

    @staticmethod
    def augment_length(*, operation):
        """Decorator for augmenting the linked list length dynamically."""
        def decorator(func):
            """Decorator."""
            @functools.wraps(func)
            def wrapped(self, *args, **kwargs):
                """Wrapped func."""
                result = func(self, *args, **kwargs)
                self.__length = operation(self.__length, 1)
                return result
            return wrapped
        return decorator

    @augment_length(operation=operator.add)
    def append(self, value):
        """Append a node with `value` a linked list."""
        if not self._head:
            self._head = self._tail = self._build_node(value)
            return self._head

        self._tail = self._head._append(value=value)
        return self._tail

    def traverse(self, i=0):
        """Traverses the linked list upto `i`."""
        if self._head is None:
            return

        yield from self._head._traverse(i)

    def get(self, index):
        """Gets the node at the position `index`."""
        if index == -1:
            return next(itertools.islice(self, len(self)-1, len(self)))

        if index > len(self) or index < 0:
            raise IndexError(
                "Incorrect Index or LinkedLinked contains no values"
            )

        return next(itertools.islice(self, index, index+1))

    def set(self, index, value):
        """Sets the value at the position `index`."""
        node = self.get(index=index)
        node._value = value
        return node

    @augment_length(operation=operator.sub)
    def pop_by_index(self, index=-1):
        """Deletes node via `index`."""
        if index == -1:
            res = self.pop_by_index(index=len(self) - 1)
            self.__length += 1  # since augment length called twice under recursion.
            return res

        if index > 0:
            _prev = self.get(index=index-1)
            _current = _prev._next

            if _current._next is None:
                _prev._next = None
                self._tail = _prev
                return _current

            _prev._next = _current._next
            return _current

        _current = self.get(index=index)
        self._head = _current._next
        return _current

    def pop_by_value(self, value):
        """Deletes node via `value`."""
        index, node = self.search(value=value)
        if node is None:
            return node

        return self.pop_by_index(index=index)

    def _insert(self, index, value, operation):
        """Inserts `node` given the operation."""
        return operation(index, value)

    def insert_left(self, index, value):
        """Insert to the left."""

        if index == -1:
            return self.insert_left(len(self)-1, value)

        node = self._build_node(value=value)

        if index == 0:
            node._next = self.get(index=index+1)
            self._head = node
            return node

        def _left(index, value):
            """`Left` operation."""
            nonlocal node
            new = node
            prev = self.get(index-1)
            _next = self.get(index)

            prev._next = new
            new._next = _next
            return new

        return self._insert(index, value, operation=_left)

    def insert_right(self, index, value):
        """Insert to the right."""
        node = self._build_node(value=value)

        if index == len(self) - 1:
            current = self.get(index=-1)
            current._next = node
            return node

        def _right(index, value):
            """`Right` operation."""
            nonlocal node
            new = node
            prev = self.get(index)
            _next = self.get(index+1)

            prev._next = new
            new._next = _next
            return new

        return self._insert(index, value, operation=_right)

    def search(self, value):
        """Searches the linked list for the `value`, returns None if not found."""
        for idx, node in enumerate(self):
            if node._value == value:
                return idx, node

        return 0, None

    def pprint(self, end=' --> '):
        """Prints the entire linked list."""
        print("HEAD", end=end)
        for node in self:
            print(node, end=end)
        print("TAIL", end="")
