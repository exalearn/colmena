"""Launches a random-search program"""
from pipeline_prototype.method_server import MethodServer
from pipeline_prototype.redis_q import ClientQueues, MethodServerQueues
from parsl.executors import HighThroughputExecutor, ThreadPoolExecutor
from parsl.providers import LocalProvider
from parsl.config import Config
from parsl import python_app
from threading import Thread
from random import uniform
from math import inf
import argparse
import logging
import parsl


# Hard code the function to be optimized
@python_app(executors=["htex"])
def target_fun(x: float) -> float:
    return (x - 1) * (x - 2)


# The Thinker and Doer Classes
class Thinker(Thread):
    """Tool that monitors results of simulations and calls for new ones, as appropriate"""

    def __init__(self, queues: ClientQueues, n_guesses: int = 10):
        """
        Args:
            queues (ClientQueues): Communicator to the MethodServer
            n_guesses (int): Number of guesses the Thinker can make
        """
        super().__init__()
        self.n_guesses = n_guesses
        self.queues = queues
        self.logger = logging.getLogger(self.__class__.__name__)

    def run(self):
        # Make a bunch of guesses
        best_answer = inf
        for _ in range(self.n_guesses):
            # Add a new guess
            self.queues.send_inputs(uniform(0, 10))
            self.logger.info("Added task to queue")

            # Get a value
            result = self.queues.get_result()
            self.logger.info(f"Received value: {result}")

            # Update the best answer
            best_answer = min(best_answer, result.value)

        # Write the best answer to disk
        with open('answer.out', 'w') as fp:
            print(best_answer, file=fp)


class Doer(MethodServer):
    """Class the manages running the function to be optimized"""

    def run_application(self, method, *args, **kwargs):
        return target_fun(*args)


if __name__ == '__main__':
    # User inputs
    parser = argparse.ArgumentParser()
    parser.add_argument("--redishost", default="127.0.0.1",
                        help="Address at which the redis server can be reached")
    parser.add_argument("--redisport", default="6379",
                        help="Port on which redis is available")
    args = parser.parse_args()

    # Set up the logging
    logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)

    # Write the configuration
    config = Config(
        executors=[
            HighThroughputExecutor(
                address="localhost",
                label="htex",
                # Max workers limits the concurrency exposed via mom node
                max_workers=2,
                worker_port_range=(10000, 20000),
                provider=LocalProvider(
                    init_blocks=1,
                    max_blocks=1,
                ),
            ),
            ThreadPoolExecutor(label="local_threads", max_threads=4)
        ],
        strategy=None,
    )
    parsl.load(config)

    # Connect to the redis server
    client_queues = ClientQueues(args.redishost, args.redisport)
    server_queues = MethodServerQueues(args.redishost, args.redisport)

    # Create the method server and task generator
    doer = Doer(server_queues)
    thinker = Thinker(client_queues)
    logging.info('Created the method server and task generator')

    try:
        # Launch the servers
        #  The method server is a Thread, so that it can access the Parsl DFK
        #  The task generator is a Thread, so that all debugging methods get cast to screen
        doer.start()
        thinker.start()
        logging.info('Launched the servers')

        # Wait for the task generator to complete
        thinker.join()
        logging.info('Task generator has completed')
    finally:
        client_queues.send_kill_signal()

    # Wait for the method server to complete
    doer.join()
