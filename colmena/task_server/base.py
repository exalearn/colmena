"""Base classes for the Task Server and associated functions"""
import logging
from inspect import isgeneratorfunction
from abc import ABCMeta, abstractmethod
from concurrent.futures import Future
from multiprocessing import Process
from typing import Collection, Optional, Callable, Union

from colmena.exceptions import KillSignalException, TimeoutException
from colmena.models.methods import ColmenaMethod, PythonGeneratorMethod, PythonMethod
from colmena.models import Result, FailureInformation
from colmena.queue.base import ColmenaQueues

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

    def __init__(self, queues: ColmenaQueues, method_names: Collection[str], timeout: Optional[int] = None):
        """
        Args:
            queues (TaskServerQueues): Queues for the task server
            timeout (int): Timeout for reading from the task queue, if desired
        """
        super().__init__()
        self.queues = queues
        self.timeout = timeout
        self.method_names = set(method_names)

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
            try:
                # Get a result from the queue
                topic, task = self.queues.get_task(self.timeout)
                logger.info(f'Received request for {task.method} with topic {topic}')

                # Make sure the method name is valid
                if task.method in self.method_names:
                    # Provide it to the workflow system to be executed
                    self.process_queue(topic, task)
                else:
                    task.success = False
                    task.failure_info = FailureInformation.from_exception(
                        ValueError(f'Method name "{task.method}" not recognized. Options: {", ".join(self.method_names)}')
                    )
                    self.queues.send_result(task)

            except KillSignalException:
                logger.info('Kill signal received')
                return
            except TimeoutException:
                logger.info('Timeout while waiting on task queue')
                return

    def _cleanup(self):
        """Close out any resources needed by the task server"""
        pass

    def _setup(self):
        """Start any resources needed by the task server after it has started in a new process"""
        pass

    def run(self) -> None:
        """Launch the thread and start running tasks

        Blocks until the inputs queue is closed and all tasks have completed"""
        logger.info(f"Started task server {self.__class__.__name__} on {self.ident}")
        self.queues.set_role('server')

        # Perform any setup operations
        self._setup()

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

    def perform_callback(self, future: Future, result: Result, topic: str):
        """Send a completed result back to queue. Used as a callback for complete tasks

        Args:
            future: Future for a task
            result: Initial result object. Used if the future throws an exception
            topic: Topic used to send back to the user
        """

        task_exc = future.exception()

        # If it was, send back a modified copy of the input structure
        if task_exc is not None:
            # Mark it as unsuccessful and capture the exception information
            result.success = False
            result.failure_info = FailureInformation.from_exception(task_exc)
        else:
            # If not, the result object is the one we need
            result = future.result()

        result.mark_task_received()

        # Put them back in the pipe with the proper topic
        self.queues.send_result(result)

    @abstractmethod
    def _submit(self, task: Result, topic: str) -> Optional[Future]:
        """Submit the task to the workflow engine

        Args:
            task: Task description
            topic: Topic for the task
        Returns:
            Future for the result object, if any that needs a "return to user" callback is created
        """
        pass

    def process_queue(self, topic: str, task: Result):
        # Launch the task
        future = self._submit(task, topic)

        # Create the callback
        if future is not None:
            future.add_done_callback(lambda x: self.perform_callback(x, task, topic))


def convert_to_colmena_method(function: Union[Callable, ColmenaMethod]) -> ColmenaMethod:
    """Wrap user-supplified functions in the task model wrapper, if needed

    Args:
        function: User-provided function
    Returns:
        Function as appropriate subclasses of Colmena Task wrapper
    """

    if isinstance(function, ColmenaMethod):
        return function
    elif isgeneratorfunction(function):
        return PythonGeneratorMethod(function)
    else:
        return PythonMethod(function)
