"""Base classes for the Task Server and associated functions"""
import os
import platform

from abc import ABCMeta, abstractmethod
from concurrent.futures import Future
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
    """Abstract class for the Colmena Task Server, which manages the execution of tasks

    Start the task server by first instantiating it and then calling :meth:`start` to launch the server in a separate process.
    Clients submit task requests to the server by pushing them to a Redis queue, and then receive results from a second queue.

    The task server can be stopped by pushing a ``None`` to the task queue, signaling that no new tasks will be incoming.
    The remaining tasks will continue to be pushed to the output queue.

    ## Implementing a Task Server

    Different implementations vary in how the queue is processed.

    Each implementation must provide the :meth:`process_queue` function is responsible for executing tasks supplied on the tasks queue
    and ensuring completed results are written back to the result queue on completion.
    Tasks must first be wrapped in the :meth:`run_and_record_timing` decorator function to capture the runtime information.

    Implementations should also provide a `_cleanup` function that releases any resources reserved by the task server.
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
    def process_queue(self, topic: str, task: Result):
        """Execute a single task from the task queue

        Args:
            topic: Which task queue this result came from
            task: Task description
        """
        pass

    def listen_and_launch(self):
        logger.info('Begin pulling from task queue')
        while True:
            # Get a result from the queue
            topic, task = self.queues.get_task(self.timeout)
            logger.info(f'Received request for {task.method} with topic {topic}')

            # Provide it to the workflow system to be executed
            try:
                self.process_queue(topic, task)
            except KillSignalException:
                logger.info('Kill signal received')
                return
            except TimeoutException:
                logger.info('Timeout while waiting on task queue')
                return

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


class FutureBasedTaskServer(BaseTaskServer, metaclass=ABCMeta):
    """Base class for workflow engines that use Python's native Future object

    Implementations need to specify a function, :meth:`_submit`, that creates the Future and
    `FutureBasedTaskServer`'s implementation of :meth:`process_queue` will add a
    callback to submit the output to the result queue.
    Note that implementations are still responsible for adding the :meth:`run_and_record_timing` decorator.
    """

    def _perform_callback(self, future: Future, result: Result, topic: str):
        """Send a completed result back to queue. Used as a callback for complete tasks

        Args:
            future: Future created by FuncX
            result: Initial result object. Used if the future throws an exception
            topic: Topic used to send back to the user
        """

        task_exc = future.exception()

        # If it was, send back a modified copy of the input structure
        if future.exception() is not None:
            # Mark it as unsuccessful and capture the exception information
            result.success = False
            result.failure_info = FailureInformation.from_exception(task_exc)
        else:
            # If not, the result object is the one we need
            result = future.result()

        # Put them back in the pipe with the proper topic
        self.queues.send_result(result, topic)

    @abstractmethod
    def _submit(self, task: Result) -> Future:
        """Submit the task to the workflow engine

        Args:
            task: Task description
        Returns:
            Future for the result object
        """
        pass

    def process_queue(self, topic: str, task: Result):
        # Launch the task
        future = self._submit(task)

        # Create the callback
        future.add_done_callback(lambda x: self._perform_callback(x, task, topic))


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
    worker_info = {}
    # TODO (wardlt): Move this information into a separate, parsl-specific wrapper
    for tag in ['PARSL_WORKER_RANK', 'PARSL_WORKER_POOL_ID']:
        if tag in os.environ:
            worker_info[tag] = os.environ[tag]
    worker_info['hostname'] = platform.node()
    result.worker_info = worker_info

    # Re-pack the results
    result.time_serialize_results = result.serialize()

    return result
