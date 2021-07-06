"""Base class for the Task Server"""

from abc import ABCMeta, abstractmethod
from multiprocessing import Process
from typing import Optional
import logging

from colmena.exceptions import KillSignalException, TimeoutException
from colmena.redis.queue import TaskServerQueues

logger = logging.getLogger(__name__)


class BaseTaskServer(Process, metaclass=ABCMeta):
    """Abstract class for the Colmena Task Server, which manages the execution
    of different asks

    Clients submit task requests to the server by pushing them to a Redis queue,
    and then receive results from a second queue.

    Different implementations vary in how the queue is processed.

    Start the task server by first instantiating it and then calling :meth:`start`
    to launch the server in a separate process.

    The task server can be stopped by pushing a ``None`` to the task queue,
    signaling that no new tasks will be incoming. The remaining tasks will
    continue to be pushed to the output queue.
    """

    def __init__(self, queues: TaskServerQueues, timeout: Optional[int] = None):
        """
        Args:
            queues (TaskServerQueues): Queues for the task server
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
        """Close out any resources needed by the task server"""
        pass

    def run(self) -> None:
        """Launch the thread and start running tasks

        Blocks until the inputs queue is closed and all tasks have completed"""
        logger.info(f"Started task server {self.__class__.__name__} on {self.ident}")

        # Loop until queue has closed
        self.listen_and_launch()

        # Shutdown any needed functions
        self._cleanup()
