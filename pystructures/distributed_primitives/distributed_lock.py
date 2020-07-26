import asyncio
import itertools
import functools
import random
from typing import Callable, Union, Tuple

from kazoo.client import KazooClient, WatchedEvent


class DistributedLock:
    def __init__(
            self, client: KazooClient, root: str, node: str, callback: Callable
    ):
        self.zk = client
        self._root = root
        self._node = node
        self._callback = callback
        self._current = None

    def acquire(self) -> bool:
        children = self.zk.get_children(self._root)
        print(f"ZNode Children: {children}")
        self._current = self.zk.create(
            f"{self._root}/{self._node}", ephemeral=True, sequence=True
        )
        print(f"Created znode: {self._current}")
        if not children:
            # acquire the lock
            return True

        # add watcher
        znode_to_watch = sorted(children)[-1]

        # lame ass kazoo uses gevents, so callback breaks
        self.zk.get(
            f"{self._root}/{znode_to_watch}", watch=lambda e: self._callback() # wrap to catch param
        )

        print(f"Adding watcher to: {znode_to_watch}")
        return False

    __enter__ = acquire

    def __exit__(self, *args):
        self.zk.delete(self._current)
        self._current = None


async def func_with_lock(client, root, node, monotonic):
    callback = functools.partial(func_with_lock, client, root, node, monotonic)
    with DistributedLock(client, root, node, callback) as acquired:
        if acquired is True:
            print("acquired lock")
            to_wait = random.randint(1, 20)
            print(f"{next(monotonic)}: acquiring lock for: {to_wait}")
            await asyncio.sleep(to_wait)

        return


if __name__ == "__main__":
    zk = KazooClient('localhost:2181')
    zk.start()
    print("Initializing Zookeeper Client...")
    monotonic = itertools.count()
    loop = asyncio.get_event_loop()

    loop.create_task(func_with_lock(zk, 'lock', 'node', monotonic))
    loop.create_task(func_with_lock(zk, 'lock', 'node', monotonic))
    print("Running Loop...")
    loop.run_forever()
