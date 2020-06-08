"""Perform a multi-step AI workflow"""
from generator_demo import Generator

from colmena.method_server import ParslMethodServer
from colmena.redis.queue import ClientQueues, make_queue_pairs
from sklearn.gaussian_process import GaussianProcessRegressor, kernels
from parsl.executors import HighThroughputExecutor, ThreadPoolExecutor
from parsl.providers import LocalProvider
from parsl.config import Config
from parsl import python_app
from threading import Thread
from random import uniform
from typing import List
import numpy as np
import argparse
import logging
import parsl


# TODO (wardlt): Handle piping results to next calculation on the server side, avoid serializing to/from client

# Hard code the function to be optimized
def target_fun(x: float) -> float:
    return (x - 1) * (x - 2) * (x - 7) * (x + 1)


# Make the generative model and its corresponding MethodServer
def generate(gen: Generator, n: int) -> List[float]:
    return gen.generate(n)


# Create the scorer: Runs a GPR
def score(gpr: GaussianProcessRegressor, x: np.ndarray):
    return gpr.predict(x, return_std=True)


# Make the selector. Determines which calculation to run next
def select(best_so_far: float, pred_y: np.ndarray, pred_std: np.ndarray) -> int:
    import numpy as np
    ei = (best_so_far - pred_y) / pred_std
    return np.argmax(ei)


# Make the thread that performs all of the thinking
class Thinker(Thread):
    """Tool that monitors results of simulations and calls for new ones, as appropriate"""

    def __init__(self, queues: ClientQueues, n_guesses: int = 10):
        """
        Args:
            n_guesses (int): Number of guesses the Thinker can make
            queues (ClientQueues): Queues for communicating with generator
        """
        super().__init__()
        self.n_guesses = n_guesses
        self.queues = queues
        self.logger = logging.getLogger(self.__class__.__name__)

    def run(self):
        """Connects to the Redis queue with the results and pulls them"""

        # Make a random guess to start
        self.queues.send_inputs(uniform(0, 10), method='target_fun')
        self.logger.info('Submitted initial random guess')
        train_X = []
        train_y = []

        # Initialize the GPR and generator
        gpr = GaussianProcessRegressor(normalize_y=True, kernel=kernels.RBF() * kernels.ConstantKernel())
        generator = Generator()

        # Make guesses based on expected improvement
        for _ in range(self.n_guesses - 1):
            # Wait for the result
            result = self.queues.get_result()
            self.logger.info(f'Received result: {(result.args, result.value)}')
            train_X.append(result.args)
            train_y.append(result.value)

            # Update the generator and  the entry generator
            generator.partial_fit(*result.args, result.value)
            gpr.fit(train_X, train_y)

            # Generate a random assortment of potential next points to sample
            self.queues.send_inputs(generator, 64, method='generate')
            result = self.queues.get_result()
            sample_X = result.value

            # Compute the expected improvement for each point
            self.queues.send_inputs(gpr, sample_X, method='score')
            result = self.queues.get_result()
            pred_y, pred_std = result.value

            # Select the best point
            best_y = np.min(train_y)
            self.queues.send_inputs(best_y, pred_y, pred_std, method='select')
            result = self.queues.get_result()
            chosen_ix = result.value

            # Run the sample with the highest EI
            self.queues.send_inputs(*sample_X[chosen_ix], method='target_fun')

        # Write the best answer to disk
        with open('answer.out', 'w') as fp:
            print(np.min(train_y), file=fp)


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
    parsl.set_stream_logger(level=logging.INFO)

    # Connect to the redis server
    client_queues, server_queues = make_queue_pairs(args.redishost, args.redisport,
                                                    clean_slate=True, use_pickle=True)

    # Create the method server and task generator
    doer = ParslMethodServer([
        target_fun, generate, score, select
    ], server_queues, default_executors=['htex'])
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
