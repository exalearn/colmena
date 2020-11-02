"""Base class for the Method Server"""

from abc import ABCMeta, abstractmethod
from multiprocessing import Process
from typing import Optional
import logging

from colmena.exceptions import KillSignalException, TimeoutException
from colmena.redis.queue import MethodServerQueues

logger = logging.getLogger(__name__)


class BaseMethodServer(Process, metaclass=ABCMeta):
    """Abstract class that executes requests across distributed resources.

    Clients submit requests to the server by pushing them to a Redis queue,
    and then receives results from a second queue.

    Different implementations vary in how the queue is processed.

    Start the method server by first instantiating it and then calling :meth:`start`
    to launch the server in a separate process.

    The method server is shutdown by pushing a ``None`` to the inputs queue,
    signaling that no new tests will be incoming. The remaining tasks will
    continue to be pushed to the output queue.
    """

    def __init__(self, queues: MethodServerQueues, timeout: Optional[int] = None):
        """
        Args:
            queues (MethodServerQueues): Queues for the method server
            timeout (int): Timeout, if desired
        """
        super().__init__()
        self.queues = queues
        self.timeout = timeout

    @abstractmethod
    def process_queue(self):
        """Evaluate a single task from the queue"""
        pass

    def listen_and_launch(self):
        logger.info('Begin pulling from task queue')
        while True:
            try:
                self.process_queue()
            except KillSignalException:
                logger.info('Kill signal received')
                return
            except TimeoutException:
                logger.info('Timeout while waiting on task queue')
                return

    @abstractmethod
    def _cleanup(self):
        """Close out any resources needed by the method server"""
        pass

    def run(self) -> None:
        """Launch the thread and start running tasks

        Blocks until the inputs queue is closed and all tasks have completed"""
        logger.info(f"Started method server {self.__class__.__name__} on {self.ident}")

        # Loop until queue has closed
        self.listen_and_launch()

        # Shutdown any needed functions
        self._cleanup()
