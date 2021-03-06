"""Parsl method server and related utilities"""
import os
import logging
from functools import partial
from queue import Queue
from threading import Thread
from concurrent.futures import wait, Future
from time import sleep, perf_counter
from typing import Optional, List, Callable, Tuple, Dict, Union, Set
from traceback import TracebackException

import parsl
from parsl import python_app, ThreadPoolExecutor
from parsl.config import Config
from parsl.app.python import PythonApp
from parsl.dataflow.futures import AppFuture

from colmena.method_server.base import BaseMethodServer
from colmena.redis.queue import MethodServerQueues
from colmena.models import Result

logger = logging.getLogger(__name__)


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

    # Execute the function
    start_time = perf_counter()
    success = True
    try:
        output = func(*result.args, **result.kwargs)
    except Exception as e:
        output = None
        success = False
        if result.task_info is None:
            result.task_info = {}
        result.task_info['exception'] = str(e)
        tb = TracebackException.from_exception(e)
        result.task_info['traceback'] = "".join(tb.format())
    finally:
        end_time = perf_counter()

    # Store the results
    result.set_result(output, end_time - start_time)
    if not success:
        result.success = False

    # Re-pack the results
    result.time_serialize_results = result.serialize()

    return result


@python_app(executors=['_output_workers'])
def output_result(queues: MethodServerQueues, topic: str, result_obj):
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
                    logger.debug(f'Task {task} with an exception: {exc}')

                    # Pull out the result objects
                    queues: MethodServerQueues = task.task_def['args'][0]
                    topic: str = task.task_def['args'][1]
                    method_task = task.task_def['depends'][0]
                    result_obj: Result = method_task.task_def['args'][1]
                    result_obj.success = False
                    queues.send_result(result_obj, topic=topic)

                # Display run information
                if len(done) > 0:
                    logger.debug(f'Cleared {len(done)} futures from Parsl task queue')

                # Loop through the incomplete tasks
                futures = not_done


class ParslMethodServer(BaseMethodServer):
    """Method server based on Parsl

    Create a Parsl method server by first creating a resource configuration following
    the recommendations in `the Parsl documentation
    <https://parsl.readthedocs.io/en/stable/userguide/configuring.html>`_.
    Then instantiate a method server with a list of functions,
    configurations defining on which Parsl executors each function can run,
    and the Parsl resource configuration.
    The executor(s) for each function can be defined with a combination
    of per method specifications

    .. code-block:: python

        ParslMethodServer([(f, {'executors': ['a']})], queues, config)

    and also using a default executor

    .. code-block:: python

        ParslMethodServer([f], queues, config, default_executors=['a'])

    Further configuration options for each method can be defined
    in the list of methods.

    **Technical Details**

    The method server stores each of the supplied methods as Parsl "PythonApp" classes.
    Tasks are launched using these PythonApps after being received on the queue.
    The Future provided when requesting the method invocation is then passed
    to second PythonApp that pushes the result of the function to the output
    queue after it completes.
    That second, "output_result," function runs on threads of the same
    process as this method server.
    """

    def __init__(self, methods: List[Union[Callable, Tuple[Callable, Dict]]],
                 queues: MethodServerQueues,
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
            queues (MethodServerQueues): Queues for the method server
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
            if isinstance(method, tuple):
                if len(method) != 2:
                    raise ValueError('Method description should a tuple of length 2')
                function, options = method
            else:
                function = method
                options = {'executors': default_executors}

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
        # TODO (wardlt): Objects are deserialized here, serialized again and then sent to the worker.
        #  We could implement a method to still read the task but ignore serialization
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
        """Close out any resources needed by the method server"""
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
