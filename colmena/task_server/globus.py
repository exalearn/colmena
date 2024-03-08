"""Task server based on Globus Compute

Globus Compute provides the ability to execute functions on remote "endpoints" that provide access to
computational resources (e.g., cloud providers, HPC).
Tasks and results are communicated to/from the endpoint through a cloud service secured using Globus Auth."""

import logging
from typing import Dict, Callable, Optional, Tuple
from concurrent.futures import Future

from globus_compute_sdk import Client, Executor

from colmena.task_server.base import convert_to_colmena_method, FutureBasedTaskServer
from colmena.queue.python import PipeQueues

from colmena.models import Result

logger = logging.getLogger(__name__)


class GlobusComputeTaskServer(FutureBasedTaskServer):
    """Task server that uses Globus Compute to execute tasks on remote systems

    Create a task server by providing a dictionary of functions
    mapped to the `endpoint <https://funcx.readthedocs.io/en/latest/endpoints.html>`_
    on which each should run. The task server will wrap the provided function
    in an interface that tracks execution information (e.g., runtime) and
    `registers <https://funcx.readthedocs.io/en/latest/sdk.html#registering-functions>`_
    the wrapped function with Globus Compute.
    You must also provide a Globus Compute :class:`~globus_compute_sdk.client.Client`
    that the task server will use to authenticate with the web service.

    The task server works using Globus Compute's :class:`~globus_compute_sdk.executor.Executor`
    to communicate to the web service over a web socket.
    The functions used by the executor are registered when you create the task server,
    and the Executor is launched when you start the task server.
    """

    def __init__(self,
                 methods: Dict[Callable, str],
                 funcx_client: Client,
                 queues: PipeQueues,
                 timeout: Optional[int] = None,
                 batch_size: int = 128):
        """
        Args:
            methods: Map of functions to the endpoint on which it will run
            funcx_client: Authenticated Globus Compute client
            queues: Queues used to communicate with thinker
            timeout: Timeout for requests from the task queue
            batch_size: Maximum number of task request to receive before submitting
        """
        # Store the client that has already been authenticated.
        self.fx_client = funcx_client
        self.fx_exec: Optional[Executor] = None

        # Create a function with the latest version of the wrapper function
        self.registered_funcs: Dict[str, Tuple[str, str]] = {}  # Function name -> (funcX id, endpoints)
        for func, endpoint in methods.items():
            # Register a wrapped version of the function
            task = convert_to_colmena_method(func)
            func_fxid = self.fx_client.register_function(task)

            # Store the information for the function
            self.registered_funcs[task.name] = (func_fxid, endpoint)

        self._batch_options = dict(
            batch_size=batch_size,
        )

        # Initialize the outputs
        super().__init__(queues, self.registered_funcs.keys(), timeout)

    def perform_callback(self, future: Future, result: Result, topic: str):
        # Check if the failure was due to a ManagerLost
        #  TODO (wardlt): Remove when we have retry support in Globus Compute
        exc = future.exception()
        if 'Task failure due to loss of manager' in str(exc):
            logger.info('Caught an task that failed due to a lost manager. Resubmitting')
            self.process_queue(topic, result)
        else:
            super().perform_callback(future, result, topic)

    def _submit(self, task: Result, topic: str) -> Future:
        # Lookup the appropriate function ID and endpoint
        func, endp_id = self.registered_funcs[task.method]
        task.mark_start_task_submission()

        # set the executor's endpoint before submitting the task
        self.fx_exec.endpoint_id = endp_id
        logger.info(f'Submitting function {func} to run on {endp_id}')

        # Submit it to funcX to be executed
        future = self.fx_exec.submit_to_registered_function(func, kwargs={'result': task})

        logger.info(f'Submitted {task.method} to run on {endp_id}')
        return future

    def _setup(self):
        # Create an executor to asynchronously transmit funcX tasks and receive results
        self.fx_exec = Executor(funcx_client=self.fx_client,
                                batch_size=self._batch_options['batch_size'])

    def _cleanup(self):
        self.fx_exec.shutdown(cancel_futures=True)
