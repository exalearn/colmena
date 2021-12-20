"""Parsl task server and related utilities"""
import os
import logging
from functools import partial
from queue import Queue
from threading import Thread
from concurrent.futures import wait, Future
from time import sleep
from typing import Optional, List, Callable, Tuple, Dict, Union, Set

import parsl
from parsl import python_app, ThreadPoolExecutor
from parsl.config import Config
from parsl.app.python import PythonApp
from parsl.dataflow.futures import AppFuture

from colmena.models import Result, FailureInformation
from colmena.task_server.base import BaseTaskServer
from colmena.redis.queue import TaskServerQueues
from colmena.task_server.base import run_and_record_timing

logger = logging.getLogger(__name__)


@python_app(executors=['_output_workers'])
def output_result(queues: TaskServerQueues, topic: str, result_obj: Result):
    """Submit the function result to the Redis queue

    Args:
        queues: Queues used to communicate with Redis
        topic: Topic to assign in output queue
        result_obj: Result object containing the inputs, to be sent back with outputs
    """
    return queues.send_result(result_obj, topic=topic)


class _ErrorHandler(Thread):
    """Keeps track of the Parsl futures and reports back errors"""

    def __init__(self, future_queue: Queue, timeout: float = 5):
        """
        Args:
            future_queue (Queue): A queue on which to receive
            timeout (float): How long to wait before checking for new futures
        """
        super().__init__(daemon=True, name='error_handler')
        self.future_queue = future_queue
        self.timeout = timeout
        self.kill = False

    def run(self) -> None:
        # Initialize the list of futures
        logger.info('Starting the error-handler thread')
        futures: Set[Future] = set()

        # Continually look for new jobs
        while not self.kill:
            # Pull from the queue until empty
            #  This operation assumes we have only one thread reading from the queue
            #  Otherwise, the queue could become empty between checking status and then pulling
            while not self.future_queue.empty():
                futures.add(self.future_queue.get())

            # If no futures, wait for the timeout
            if len(futures) == 0:
                sleep(self.timeout)
            else:
                done, not_done = wait(futures, timeout=self.timeout)

                # If there are entries that are complete, check if they have errors
                for task in done:
                    # Check if an exception was raised
                    exc = task.exception()
                    if exc is None:
                        logger.debug(f'Task completed: {task}')
                        continue
                    logger.warning(f'Task {task} with an exception: {exc}')

                    # Pull out the result objects
                    queues: TaskServerQueues = task.task_def['args'][0]
                    topic: str = task.task_def['args'][1]
                    method_task = task.task_def['depends'][0]
                    task_exc = method_task.exception()
                    result_obj: Result = method_task.task_def['args'][0]

                    # Mark it as unsuccessful and capture the exception information
                    result_obj.success = False
                    result_obj.failure_info = FailureInformation.from_exception(task_exc)

                    # Send it to the client
                    queues.send_result(result_obj, topic=topic)

                # Display run information
                if len(done) > 0:
                    logger.debug(f'Cleared {len(done)} futures from Parsl task queue')

                # Loop through the incomplete tasks
                futures = not_done


class ParslTaskServer(BaseTaskServer):
    """Task server based on Parsl

    Create a Parsl task server by first creating a resource configuration following
    the recommendations in `the Parsl documentation
    <https://parsl.readthedocs.io/en/stable/userguide/configuring.html>`_.
    Then instantiate a task server with a list of Python functions,
    configurations defining on which Parsl executors each function can run,
    and the Parsl resource configuration.
    The executor(s) for each function can be defined with a combination
    of per method specifications

    .. code-block:: python

        ParslTaskServer([(f, {'executors': ['a']})], queues, config)

    and also using a default executor

    .. code-block:: python

        ParslTaskServer([f], queues, config, default_executors=['a'])

    Further configuration options for each method can be defined
    in the list of methods.

    **Technical Details**

    The task server stores each of the supplied methods as Parsl "PythonApp" classes.
    Tasks are launched using these PythonApps after being received on the queue.
    The Future provided when requesting the method invocation is then passed
    to second PythonApp that pushes the result of the function to the output
    queue after it completes.
    That second, "output_result," function runs on threads of the same
    process as this task server.
    There is also a separate thread that monitors for Futures that yield an error
    before the "output_result" function and sends back the error messages.
    """

    def __init__(self, methods: List[Union[Callable, Tuple[Callable, Dict]]],
                 queues: TaskServerQueues,
                 config: Config,
                 timeout: Optional[int] = None,
                 default_executors: Union[str, List[str]] = 'all',
                 num_output_workers: int = 4):
        """

        Args:
            methods (list): List of methods to be served.
                Each element in the list is either a function or a tuple where the first element
                is a function and the second is a dictionary of the arguments being used to create
                the Parsl ParslApp see `Parsl documentation
                <https://parsl.readthedocs.io/en/stable/stubs/parsl.app.app.python_app.html#parsl.app.app.python_app>`_.
            queues (TaskServerQueues): Queues for the task server
            config: Parsl configuration
            timeout (int): Timeout, if desired
            default_executors: Executor or list of executors to use by default.
            num_output_workers: Maximum number of output workers push results to the Redis queue. Will not create
                more workers than CPUs
        """
        super().__init__(queues, timeout)

        # Insert _output_workers to the thread count
        executors = config.executors.copy()
        executors.append(ThreadPoolExecutor(label='_output_workers', max_threads=num_output_workers))
        config.executors = executors

        # Get a list of default executors that _does not_ include the output workers
        if default_executors == 'all':
            default_executors = [e.label for e in executors if e.label != "_output_workers"]

        # Store the Parsl configuration
        self.config = config

        # Assemble the list of methods
        self.methods_ = {}
        for method in methods:
            # Get the options or use the defaults
            if isinstance(method, (tuple, list)):
                if len(method) != 2:
                    raise ValueError('Method description should a tuple of length 2')
                function, options = method
            else:
                function = method
                options = {'executors': default_executors}
                logger.info(f'Using default executors for {function.__name__}: {default_executors}')

            # Make the Parsl app
            name = function.__name__

            # Wrap the function in the timer class
            wrapped_function = partial(run_and_record_timing, function)
            app = PythonApp(wrapped_function, **options)

            # Store it
            self.methods_[name] = app
        logger.info(f'Defined {len(self.methods_)} methods: {", ".join(self.methods_.keys())}')

        # If only one method, store a default method
        self.default_method_ = list(self.methods_.keys())[0] if len(self.methods_) == 1 else None
        if self.default_method_ is not None:
            logger.info(f'There is only one method, so we are using {self.default_method_} as a default')

        # Create a thread to check if tasks completed successfully
        self.task_queue: Optional[Queue] = None
        self.error_checker: Optional[_ErrorHandler] = None

    def process_queue(self):
        """Evaluate a single task from the queue"""

        # Get a result from the queue
        topic, result = self.queues.get_task(self.timeout)
        logger.info(f'Received request for {result.method} with topic {topic}')

        # Determine which method to run
        if self.default_method_ and result.method is None:
            method = self.default_method_
        else:
            method = result.method

        # Submit the application
        future = self.submit_application(method, result)
        # TODO (wardlt): Implement "resubmit if task returns a new future."
        #  Requires waiting on two streams: input_queue and the queues

        # Pass the future of that operation to the output queue
        result_future = output_result(self.queues, topic, future)
        logger.debug('Pushed task to Parsl')

        # Pass the task to the "error handler"
        self.task_queue.put(result_future)

    def submit_application(self, method_name: str, result: Result) -> AppFuture:
        """Submit an application to run via Parsl

        Args:
            method_name (str): Name of the method to invoke
            result: Task description
        """
        return self.methods_[method_name](result)

    def _cleanup(self):
        """Close out any resources needed by the task server"""
        # Wait until all tasks have finished
        dfk = parsl.dfk()
        dfk.wait_for_current_tasks()
        logger.info(f"All tasks have completed for {self.__class__.__name__} on {self.ident}")

        logger.info("Shutting down the error handling thread")
        self.error_checker.kill = True

    def run(self) -> None:
        # Launch the Parsl workflow engine
        parsl.load(self.config)
        logger.info(f"Launched Parsl DFK. Process id: {os.getpid()}")

        # Create the error checker
        self.task_queue = Queue()
        self.error_checker = _ErrorHandler(self.task_queue)
        self.error_checker.start()

        # Start the loop
        super().run()
