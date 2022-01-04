"""Parsl task server and related utilities"""
import os
import logging
from concurrent.futures import Future
from functools import partial, update_wrapper
from typing import Optional, List, Callable, Tuple, Dict, Union

import parsl
from parsl import ThreadPoolExecutor
from parsl.config import Config
from parsl.app.python import PythonApp

from colmena.models import Result
from colmena.redis.queue import TaskServerQueues
from colmena.task_server.base import run_and_record_timing, FutureBasedTaskServer

logger = logging.getLogger(__name__)


class ParslTaskServer(FutureBasedTaskServer):
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
            wrapped_function = update_wrapper(wrapped_function, function)
            app = PythonApp(wrapped_function, **options)

            # Store it
            self.methods_[name] = app
        logger.info(f'Defined {len(self.methods_)} methods: {", ".join(self.methods_.keys())}')

        # If only one method, store a default method
        self.default_method_ = list(self.methods_.keys())[0] if len(self.methods_) == 1 else None
        if self.default_method_ is not None:
            logger.info(f'There is only one method, so we are using {self.default_method_} as a default')

    def _submit(self, task: Result) -> Future:
        # Determine which method to run
        if self.default_method_ and task.method is None:
            method = self.default_method_
        else:
            method = task.method

        # Submit the application
        future = self.methods_[method](task)
        logger.debug('Pushed task to Parsl')
        # TODO (wardlt): Implement "resubmit if task returns a new future." or the ability to launch Parsl workflows with >1 step

        return future

    def _cleanup(self):
        """Close out any resources needed by the task server"""
        # Wait until all tasks have finished
        dfk = parsl.dfk()
        dfk.wait_for_current_tasks()
        logger.info(f"All tasks have completed for {self.__class__.__name__} on {self.ident}")

    def run(self) -> None:
        # Launch the Parsl workflow engine
        parsl.load(self.config)
        logger.info(f"Launched Parsl DFK. Process id: {os.getpid()}")

        # Start the loop
        super().run()
