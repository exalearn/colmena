import os
import logging
from threading import Thread

import parsl
from parsl import python_app

logger = logging.getLogger(__name__)


@python_app(executors=['local_threads'])
def output_result(output_queue, input_param, output_param):
    output_queue.put((input_param, output_param))


class MethodServer(Thread):
    """Abstract class that executes requests across distributed resources.

    Clients submit requests to the server by pushing them to a Redis queue,
    and then receives results from a second queue

    Users must implement the :meth:`run_simulation` method, which must return
    a ``ParslFuture`` object.

    The method server is shutdown by pushing a ``None`` to the input queue,
    signaling that no new tests will be incoming. The remaining tasks will
    continue to be pushed to the output queue.

    The method server, itself, implements the thread interface. So, you can start it
    """

    def __init__(self, input_queue, output_queue):
        super().__init__()
        self.input_queue = input_queue
        self.output_queue = output_queue

    @python_app(executors=['local_threads'])
    def listen_and_launch(self):
        logger.info('Begin pulling from task queue')
        while True:
            param = self.input_queue.get()
            logger.debug(f'Received inputs {param}')

            # Check for stop command
            if param == 'null' or param is None:
                logging.info('None received. No longer listening for new tasks')
                break

            # Run the application
            future = self.run_application(param)
            # TODO (wardlt): Implement "resubmit if task renders a new future."
            #  Requires waiting on two streams: input_queue and the output_queue

            # Pass the future of that operation to the output queue
            #  Note that we do not hold on to the future. No need to wait for them as of yet (see above TODO)
            output_result(self.output_queue, param, future)
            logger.debug(f'Pushed task to Parsl')
        return

    def run_application(self, params):
        raise NotImplementedError()

    def run(self) -> None:
        """Launch the thread and start running tasks

        Blocks until the input queue is closed and all tasks have completed"""
        logger.info(f"Started method server {self.__class__.__name__} on {self.ident}")

        # Loop until queue has closed
        self.listen_and_launch(self).result()

        # Wait until all tasks have finished
        dfk = parsl.dfk()
        dfk.wait_for_current_tasks()
        logger.info(f"All tasks have completed for {self.__class__.__name__} on {self.ident}")
