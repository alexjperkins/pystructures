"""Hash tables: using Linked Lists as buckets."""


import functools

from ..linked_lists import SingularyLinkedList, Node


class HashNode(Node):
    """Hash specific Node, also stores an extra `key` attribute."""
    def __init__(self, key, *args,  **kwargs):
        self._key = key
        super().__init__(*args, **kwargs)

    @property
    def pair(self):
        """Key Value pair property."""
        return self.key, self.value


class HashTable:
    """Hash table that uses a linked list as a `bucket`."""
    def  __init__(self, size=8, bucket_type=SingularyLinkedList):
        self._size = size
        self._table = [None] * self._size
        self.__bucket_type = bucket_type
        self.__bucket_count = 0

    def insert(self, key, value):
        """Inserts `value` into the hash table with `key`."""
        if self.search(key) is None:
            idx = self.get_index(key)
            if self.table[idx] is None:
                self.__bucket_count += 1
                self.table[idx] = self._build_bucket()

            self._table[idx].append(value)
            return value

        raise KeyError(f"Key: {key} already exists")

    def search(self, key):
        """Searches the given node."""
        bucket = self._table[self.get_index(key)]
        if bucket is not None:
            for item in bucket:
                if item.key == key:
                    return item.value

        return bucket

    def _build_bucket(self):
        """Factory method for `self._bucket_type`"""
        return self._bucket_type(node_type=HashNode)

    @property
    def fullness(self):
        """How `full` the hash table is: how many buckets are initialized in table over size."""
        return self.bucket_count // self._size)

    @property
    def bucket_count(self):
        """The number of buckets initialized."""
        return self.__bucket_count

    @bucket_count.setter
    def bucket_count(self, value):
        """Setter for bucket_count: abstraction for table resizing."""
        if self.fullness > 0.66:
            self.__resize()

        self.__bucket_count = value

    @functools.lru_cache(maxsize=256)
    def get_index(self, key):
        """Hash func."""
        return hash(key) % self._size

    def __resize(self):
        """Resizes the table."""
        tmp = self._table
        new = [None] * (len(self) * 2)
        self._table = new
        for item in tmp:
            if item:
                for node in item:
                    self.insert(*node.pair)
