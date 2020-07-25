import abc
import asyncio
import itertools
import functools
import logging
import random
import time

from asyncio import wrap_future, wait_for
from concurrent.futures import Future
from typing import Callable, Union, Tuple

from kazoo.client import KazooClient, WatchedEvent


logger = logging.getLogger(__file__)


class IAsyncLock(abc.ABC):
    @abc.abstractmethod
    async def acquire(self) -> None:
        pass

    @abc.abstractmethod
    async def release(self) -> None:
        pass


class AsyncDistributedQueueLock(IAsyncLock):
    def __init__(
            self, client: KazooClient, root: str, node: str
    ):
        self.zk = client
        self._root = root
        self._node = node
        self._current = None

    async def acquire(self) -> None:
        #  TODO: this is not consistent, another client could create a child
        #  in before these
        children = self.zk.get_children(self._root)
        self._current = self.zk.create(
            f"{self._root}/{self._node}", ephemeral=True, sequence=True
        )
        logger.info("CREATED CHILD ZNODE : %s", self._current)
        if not children:
            logger.info("%s ACQUIRED LOCK", self._current)
            return None

        # add future callback to watcher
        future = Future()
        znode_to_watch = sorted(children)[-1]
        logger.info("WATCHER ADDED TO: %s",  znode_to_watch)
        self.zk.get(
            f"{self._root}/{znode_to_watch}",
            watch=lambda e: future.set_result(True)
        )

        # wait for lock, non-blocking
        then = time.time()
        await wait_for(wrap_future(future), None)
        now = time.time()
        logger.info(
            "%s ACQUIRED LOCK: WAITED %d SECONDS",
            self._current, now - then
        )
        return None

    def release(self) -> None:
        self.zk.delete(self._current)

    __aenter__ = acquire

    def __aexit__(self, *args):
        self.release()
        self._current = None


async def func_with_lock(client, root, node, monotonic) -> None:
    async with AsyncDistributedQueueLock(client, root, node):
        to_wait = random.randint(1, 20)
        print(f"{next(monotonic)}: acquiring lock for: {to_wait}")
        await asyncio.sleep(to_wait)
        return None


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
