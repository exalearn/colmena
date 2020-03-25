import time
import logging
from functools import partial
from typing import Any, Optional, List, Callable, Tuple, Dict, Union

import parsl
from parsl import python_app
from parsl.app.python import PythonApp
from parsl.dataflow.futures import AppFuture

from pipeline_prototype.method_server.base import BaseMethodServer
from pipeline_prototype.redis.queue import MethodServerQueues
from pipeline_prototype.models import Result

logger = logging.getLogger(__name__)


def run_and_record_timing(func: Callable, *args, **kwargs) -> Tuple[Any, float]:
    """Run a function and also return the runtime

    Args:
        func: Function to invoke
    Returns:
        - Output of `func(*args, **kwargs)`
        - Runtime in seconds
    """
    start_time = time.perf_counter()
    output = func(*args, **kwargs)
    end_time = time.perf_counter()
    return output, end_time - start_time


@python_app(executors=['local_threads'])
def output_result(queues: MethodServerQueues, result_obj: Result, wrapped_output: Tuple[Any, float]):
    """Submit the function result to the Redis queue

    Args:
        queues: Queues used to communicate with Redis
        result_obj: Result object containing the inputs, to be sent back with outputs
        wrapped_output: Result from invoking the function and the inputs
    """
    value, runtime = wrapped_output
    result_obj.set_result(value, runtime)
    return queues.send_result(result_obj)


# TODO (wardlt): How do we send back errors? Errors currently cause apps to hang
class ParslMethodServer(BaseMethodServer):
    """Method server based on Parsl

    Create a Parsl method server by first configuring Parsl following
    the recommendations in `the Parsl documentation
    <https://parsl.readthedocs.io/en/stable/userguide/configuring.html>`_.
    Then instantiate a method server with a list of functions and
    configurations defining on which Parsl executors each function can run.
    The executor(s) for each function can be defined with a combination
    of per method specifications

    .. code-block:: python

        ParslMethodServer([(f, {'executors': ['a']})])

    and also using a default executor

    .. code-block:: python

        ParslMethodServer([f], default_executors=['a'])

    Further configuration options for each method can be defined
    in the list of methods.

    Technical Details
    -----------------

    The method server stores each of the supplied methods as Parsl "PythonApp" classes.
    Tasks are launched using these PythonApps after being received on the queue.
    The Future provided when requesting the method invocation is then passed
    to second PythonApp that pushes the result of the function to the output
    queue after it completes.
    That second, "output_result," function runs on threads of the same
    process as this method server.
    """

    def __init__(self, methods: List[Union[Callable, Tuple[Callable, Dict]]],
                 queues: MethodServerQueues, timeout: Optional[int] = None,
                 default_executors: Union[str, List[str]] = 'all'):
        """

        Args:
            methods (list): List of methods to be served.
                Each element in the list is either a function or a tuple where the first element
                is a function and the second is a dictionary of the arguments being used to create
                the Parsl ParslApp see `Parsl documentation
                <https://parsl.readthedocs.io/en/stable/stubs/parsl.app.app.python_app.html#parsl.app.app.python_app>`_.
            queues (MethodServerQueues): Queues for the method server
            timeout (int): Timeout, if desired
            default_executors: Executor or list of executors to use by default.
        """
        super().__init__(queues, timeout)

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
                if default_executors == 'all':
                    logger.warning(f'Method {function.__name__} may run on the method server\'s local threads.'
                                   ' Consider specifying default_executors')

            # Make the Parsl app
            name = function.__name__

            # Wrap the function in the timer class
            wrapped_function = partial(run_and_record_timing, function)
            app = PythonApp(wrapped_function, **options)

            # Store it
            self.methods_[name] = app
        logger.info(f'Defined {len(self.methods_)} methods: {", ".join(self.methods_.keys())}')

        # If only one method, store a default method
        self.default_method_ = list(self.methods_.keys())[0] if len(self.methods_) else None
        if self.default_method_ is not None:
            logger.info(f'There is only one method, so we are using {self.default_method_} as a default')

    def process_queue(self):
        """Evaluate a single task from the queue"""

        # Get a result from the queue
        # TODO (wardlt): Objects are deserialized here, serialized again and then sent to the worker.
        #  We could implement a method to still read the task but ignore serialization
        result = self.queues.get_task(self.timeout)
        logger.info(f'Received inputs {result}')

        # Determine which method to run
        #  TODO (wardlt): This logic is not really
        if self.default_method_ and result.method is None:
            method = self.default_method_
        else:
            method = result.method

        # Submit the application
        future = self.submit_application(method, *result.args, **result.kwargs)
        # TODO (wardlt): Implement "resubmit if task returns a new future."
        #  Requires waiting on two streams: input_queue and the queues

        # Pass the future of that operation to the output queue
        #  Note that we do not hold on to the future. No need to wait for them as of yet (see above TODO)
        output_result(self.queues, result, future)
        logger.debug(f'Pushed task to Parsl')

    def submit_application(self, method_name, *args, **kwargs) -> AppFuture:
        """Submit an application to run via Parsl

        Args:
            method_name (str): Name of the method to invoke
            *args: Positional arguments
            **kwargs: Keyword arguments
        """
        return self.methods_[method_name](*args, **kwargs)

    def _cleanup(self):
        """Close out any resources needed by the method server"""
        # Wait until all tasks have finished
        dfk = parsl.dfk()
        dfk.wait_for_current_tasks()
        logger.info(f"All tasks have completed for {self.__class__.__name__} on {self.ident}")
