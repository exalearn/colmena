"""Base class for the Task Server"""
import os
import platform

from abc import ABCMeta, abstractmethod
from multiprocessing import Process
from time import perf_counter
from typing import Optional, Callable
import logging

from colmena.exceptions import KillSignalException, TimeoutException
from colmena.redis.queue import TaskServerQueues
from colmena.models import Result, FailureInformation
from colmena.proxy import resolve_proxies_async

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


def run_and_record_timing(func: Callable, result: Result) -> Result:
    """Run a function and also return the runtime

    Args:
        func: Function to invoke
        result: Result object describing task request
    Returns:
        Result object with the serialized result
    """
    # Mark that compute has started on the worker
    result.mark_compute_started()

    # Unpack the inputs
    result.time_deserialize_inputs = result.deserialize()

    # Start resolving any proxies in the input asynchronously
    start_time = perf_counter()
    resolve_proxies_async(result.args)
    resolve_proxies_async(result.kwargs)
    result.time_async_resolve_proxies = perf_counter() - start_time

    # Execute the function
    start_time = perf_counter()
    success = True
    try:
        output = func(*result.args, **result.kwargs)
    except BaseException as e:
        output = None
        success = False
        result.failure_info = FailureInformation.from_exception(e)
    finally:
        end_time = perf_counter()

    # Store the results
    result.set_result(output, end_time - start_time)
    if not success:
        result.success = False

    # Add the worker information into the tasks, if available
    if result.task_info is None:
        result.task_info = {}
    for tag in ['PARSL_WORKER_RANK', 'PARSL_WORKER_POOL_ID']:
        if tag in os.environ:
            result.task_info[tag] = os.environ[tag]
    result.task_info['executor'] = platform.node()

    # Re-pack the results
    result.time_serialize_results = result.serialize()

    return result
