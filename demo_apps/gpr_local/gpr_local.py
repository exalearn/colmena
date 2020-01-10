"""Perform GPR Active Learning where the model is trained / ran on the local thread"""
from pipeline_prototype.method_server import MethodServer
from pipeline_prototype.redis_q import RedisQueue
from sklearn.gaussian_process import GaussianProcessRegressor, kernels
from parsl.executors import HighThroughputExecutor, ThreadPoolExecutor
from parsl.providers import LocalProvider
from parsl.config import Config
from parsl import python_app
from threading import Thread
from random import uniform
import numpy as np
import argparse
import logging
import parsl


# Hard code the function to be optimized
@python_app(executors=["htex"])
def target_fun(x: float) -> float:
    return (x - 1) * (x - 2) * (x - 7) * (x + 1)


# The Thinker and Doer Classes
class Thinker(Thread):
    """Tool that monitors results of simulations and calls for new ones, as appropriate"""

    def __init__(self, input_queue: RedisQueue, output_queue: RedisQueue, n_guesses: int = 10):
        """
        Args:
            n_guesses (int): Number of guesses the Thinker can make
            input_queue (RedisQueue): Queue to push new simulation commands
            redis_port (int): Port at which the redis server can be reached
        """
        super().__init__()
        self.n_guesses = n_guesses
        self.input_queue = input_queue
        self.output_queue = output_queue
        self.logger = logging.getLogger(self.__class__.__name__)

    def run(self):
        """Connects to the Redis queue with the results and pulls them"""

        # Make a random guess to start
        self.input_queue.put(uniform(0, 10))
        self.logger.info('Submitted initial random guess')
        train_X = []
        train_y = []

        # Use the initial guess to train a GPR
        gpr = GaussianProcessRegressor(normalize_y=True, kernel=kernels.RBF() * kernels.ConstantKernel())
        x, y = self.output_queue.get()
        train_X.append([x])
        train_y.append(y)

        # Make guesses based on expected improvement
        for _ in range(self.n_guesses - 1):
            # Update the GPR with the available training data
            gpr.fit(train_X, train_y)

            # Generate a random assortment of potential next points to sample
            sample_X = np.random.uniform(size=(64, 1), low=0, high=10)

            # Compute the expected improvement for each point
            pred_y, pred_std = gpr.predict(sample_X, return_std=True)
            best_so_far = np.min(train_y)
            ei = (best_so_far - pred_y) / pred_std

            # Run the sample with the highest EI
            best_ei = sample_X[np.argmax(ei), 0]
            self.input_queue.put(best_ei)
            self.logger.info(f'Sent new guess based on EI: {best_ei}')

            # Wait for the result to complete
            _, result = self.output_queue.get()
            self.logger.info('Received result')

            # Add the result to the training set for the GPR
            train_X.append([best_ei])
            train_y.append(result)

        # Write the best answer to disk
        with open('answer.out', 'w') as fp:
            print(np.min(train_y), file=fp)


class Doer(MethodServer):
    """Class the manages running the function to be optimized"""

    def run_application(self, i):
        return target_fun(i)


if __name__ == '__main__':
    # User input
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
    input_queue = RedisQueue(args.redishost, port=int(args.redisport), prefix='input')
    input_queue.connect()
    input_queue.flush()

    output_queue = RedisQueue(args.redishost, port=int(args.redisport), prefix='output')
    output_queue.connect()
    output_queue.flush()

    # Create the method server and task generator
    doer = Doer(input_queue, output_queue)
    thinker = Thinker(input_queue, output_queue)
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
        input_queue.put('null')  # Send the "exit" to the method server

        # Wait for the method server to complete
        doer.join()
    finally:
        input_queue.put("null")  # Closes the "Doer"
