import logging
from threading import Thread
from typing import Any, Optional, List, Callable

import parsl
from parsl import python_app
from parsl.app.app import AppBase

from pipeline_prototype.execptions import KillSignalException, TimeoutException
from pipeline_prototype.redis.queue import MethodServerQueues
from pipeline_prototype.models import Result

logger = logging.getLogger(__name__)


@python_app(executors=['local_threads'])
def output_result(queues: MethodServerQueues, result_obj: Result, output_param: Any):
    result_obj.set_result(output_param)
    return queues.send_result(result_obj)


# TODO (wardlt): Have the MethodServer class manage creating Parsl apps?
# TODO (wardlt): How do we send back errors? Errors currently cause apps to hang

class MethodServer(Thread):
    """Abstract class that executes requests across distributed resources.

    Clients submit requests to the server by pushing them to a Redis queue,
    and then receives results from a second queue

    Users must implement the :meth:`run_simulation` method, which must return
    a ``ParslFuture`` object.

    The method server is shutdown by pushing a ``None`` to the inputs queue,
    signaling that no new tests will be incoming. The remaining tasks will
    continue to be pushed to the output queue.

    The method server, itself, implements the thread interface. So, you can start it
    """

    def __init__(self, queues: MethodServerQueues, timeout: Optional[int] = None):
        """
        Args:
            queues (MethodServerQueues): Queues for the method server
            timeout (int): Timeout, if desired
        """
        super().__init__()
        self.queues = queues
        self.timeout = timeout

    def process_queue(self):
        """Evaluate a single task from the queue"""
        result = self.queues.get_task(self.timeout)
        logger.info(f'Received inputs {result}')

        # Run the application
        future = self.run_application(result.method, *result.args, **result.kwargs)
        # TODO (wardlt): Implement "resubmit if task returns a new future."
        #  Requires waiting on two streams: input_queue and the queues

        # Pass the future of that operation to the output queue
        #  Note that we do not hold on to the future. No need to wait for them as of yet (see above TODO)
        output_result(self.queues, result, future)
        logger.debug(f'Pushed task to Parsl')

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

    def run_application(self, method_name, *args, **kwargs):
        """Run an application

        Args:
            method_name (str): Name of the method to invoke
            *args: Positional arguments
            **kwargs: Keyword arguments
        """
        raise NotImplementedError()

    def run(self) -> None:
        """Launch the thread and start running tasks

        Blocks until the inputs queue is closed and all tasks have completed"""
        logger.info(f"Started method server {self.__class__.__name__} on {self.ident}")

        # Loop until queue has closed
        self.listen_and_launch()

        # Wait until all tasks have finished
        dfk = parsl.dfk()
        dfk.wait_for_current_tasks()
        logger.info(f"All tasks have completed for {self.__class__.__name__} on {self.ident}")


class MultiMethodServer(MethodServer):
    """Server that can handle multiple methods"""

    def __init__(self, queues: MethodServerQueues, methods: List[Callable], timeout: Optional[int] = None):
        super(MultiMethodServer, self).__init__(queues, timeout)
        self._methods = dict((n.__name__, n) for n in methods)  # Store them in a dict for fast lookup

        # Make sure each method is a ParslApp
        for name, func in self._methods.items():
            if not isinstance(func, AppBase):
                raise ValueError(f'Function "{name}" is not a Parsl app')

    def run_application(self, method_name, *args, **kwargs):
        return self._methods[method_name](*args, **kwargs)
