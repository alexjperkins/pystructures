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
    async def acquire(self) -> bool:
        pass

    @abc.abstractmethod
    async def release(self) -> None:
        pass


class AsyncDistributedQueueLock(IAsyncLock):
    def __init__(
        self,
        client: KazooClient,
        root: str, node: str, timeout: Union[None, int]
    ):
        self.zk = client
        self._root = root
        self._node = node
        self._timeout = timeout
        self._current: Union[None, str] = None

    async def acquire(self) -> bool:
        children = self.zk.get_children(self._root)
        self._current = self.zk.create(
            f"{self._root}/{self._node}", ephemeral=True, sequence=True
        )
        logger.info("CREATED CHILD ZNODE : %s", self._current)
        if not children:
            logger.info("WORKER `%s` ACQUIRED LOCK", self._current)
            return True

        future = Future()
        znode_to_watch = sorted(children)[-1]
        logger.info("WATCHER ADDED TO: %s", znode_to_watch)
        self.zk.get(
            f"{self._root}/{znode_to_watch}",
            watch=lambda e: future.set_result(True)
        )

        then = time.time()
        try:
            await wait_for(wrap_future(future), self._timeout)
        except asyncio.TimeoutError:
            logger.warning(
                "TIMEOUT WAITING FOR LOCK EXCEEDED:  %d", self._timeout
            )
            return False
        else:
            now = time.time()
            logger.info(
                "%s ACQUIRED LOCK: WAITED %d SECONDS",
                self._current, now - then
            )
            return True

    def release(self) -> None:
        self.zk.delete(self._current)

    __aenter__ = acquire

    async def __aexit__(self, *args):
        self.release()
        self._current = None


# Example use case
async def func_with_lock(client, root, node, timeout, monotonic) -> None:
    async with AsyncDistributedQueueLock(client, root, node, timeout) as acquired:  # NOQA
        if acquired:
            # mimic work
            to_wait = random.randint(1, 2)
            print(f"PROCESS {next(monotonic)}: acquiring lock for: {to_wait}")
            await asyncio.sleep(to_wait)

        return None


if __name__ == "__main__":
    # This assumes a running `Zookeeper Ensemble`
    zk = KazooClient('localhost:2181')
    zk.start()
    print("Initializing Zookeeper Client...")
    monotonic = itertools.count()
    loop = asyncio.get_event_loop()

    async def counter():
        monotonic = itertools.count()
        while True:
            print("CLOCK: ", next(monotonic))
            await asyncio.sleep(1)

    loop.create_task(func_with_lock(zk, 'lock', 'node', 30, monotonic))
    loop.create_task(func_with_lock(zk, 'lock', 'node', 30, monotonic))
    loop.create_task(func_with_lock(zk, 'lock', 'node', 30, monotonic))
    loop.create_task(counter())  # clock

    print("Running Loop...")
    loop.run_forever()
