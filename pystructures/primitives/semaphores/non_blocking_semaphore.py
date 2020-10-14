import logging


logger = logging.getLogger(__file__)


class NonBlockingSemaphore:
    def __init__(self, *, resources: int):
        self._max_resources = resources
        self._resources = resources

    @property
    def resources(self):
        return self._resources

    @resources.setter
    def resources(self, val):
        if (
            val > self._max_resources or
            val < 0
        ):
            raise ValueError(
               "Invalid number of resources."
            )

    def acquire(self) -> bool:
        try:
            self.resources -= 1
        except ValueError as error:
            logger.info(error)
            return False
        else:
            return True

    def release(self) -> bool:
        try:
            self.resources += 1
        except ValueError as error:
            logger.info(error)
            return False
        else:
            return True

    __enter__ = acquire

    def __exit__(self, t, v, tb):
        self.release()
