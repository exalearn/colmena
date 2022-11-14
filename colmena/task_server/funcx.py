"""Task server based on FuncX

FuncX provides the ability to execute functions on remote "endpoints" that provide access to computational resources (e.g., cloud providers, HPC).
Tasks and results are communicated to/from the endpoint through a cloud service secured using Globus Auth."""

import logging
from functools import partial, update_wrapper
from typing import Dict, Callable, Optional, Tuple
from concurrent.futures import Future

from funcx import FuncXClient
from funcx.sdk.executor import FuncXExecutor

from colmena.task_server.base import run_and_record_timing, FutureBasedTaskServer
from colmena.queue.python import PipeQueue

from colmena.models import Result

logger = logging.getLogger(__name__)


class FuncXTaskServer(FutureBasedTaskServer):
    """Task server that uses FuncX to execute tasks on remote systems

    Create a FuncXTaskServer by providing a dictionary of functions along with a FuncX endpoint ID
    mapped to the `endpoint <https://funcx.readthedocs.io/en/latest/endpoints.html>`_
    on which it should run. The task server will wrap the provided function
    in an interface that tracks execution information (e.g., runtime) and
    `registers <https://funcx.readthedocs.io/en/latest/sdk.html#registering-functions>`_
    the wrapped function with FuncX.
    You must also provide a :class:`FuncXClient` that the task server can use to authenticate with the
    FuncX web service.

    The task server works using the :class:`FuncXExecutor` to communicate with FuncX via a websocket.
    `FuncXExecutor` receives completed work, and we use callbacks on the Python :class:`Future` objects
    to send that completed work back to the task queue.
    """

    def __init__(self, methods: Dict[Callable, str],
                 funcx_client: FuncXClient,
                 queues: PipeQueue,
                 timeout: Optional[int] = None):
        """
        Args:
            methods: Map of functions to the endpoint on which it will run
            funcx_client: Authenticated FuncX client
            queues: Queues used to communicate with thinker
            timeout: Timeout for requests from the task queue
        """
        super(FuncXTaskServer, self).__init__(queues, timeout)

        # Store the FuncX client
        self.fx_client = funcx_client

        # Create a function with the latest version of the wrapper function
        self.registered_funcs: Dict[str, Tuple[Callable, str]] = {}  # Function name -> (funcX id, endpoints)
        for func, endpoint in methods.items():
            # Make a wrapped version of the function
            func_name = func.__name__
            new_func = partial(run_and_record_timing, func)
            update_wrapper(new_func, func)

            # Store the FuncX information for the function
            self.registered_funcs[func_name] = (new_func, endpoint)

        # Create the executor and queue of tasks to be submitted back to the user
        self.fx_exec = FuncXExecutor(self.fx_client)

    def _submit(self, task: Result) -> Future:
        # Lookup the appropriate function ID and endpoint
        func, endp_id = self.registered_funcs[task.method]

        # Submit it to FuncX to be executed
        future: Future = self.fx_exec.submit(func, task, endpoint_id=endp_id)
        logger.info(f'Submitted {task.method} to run on {endp_id}')
        return future

    def _cleanup(self):
        self.fx_exec.shutdown()
