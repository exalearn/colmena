"""Launches a random-search program"""
from pipeline_prototype.method_server import MpiMethodServer
from pipeline_prototype.redis_q import RedisQueue
from parsl.executors import HighThroughputExecutor, ThreadPoolExecutor
from parsl.providers import LocalProvider
from parsl.config import Config
from parsl import python_app
from multiprocessing import Process
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


@python_app(executors=['local_threads'])
def output_result(output_queue, param, output):
    output_queue.put((param, output))
    return param, output


# The Thinker and Doer Classes
class Thinker(Process):
    """Tool that monitors results of simulations and calls for new ones, as appropriate"""

    def __init__(self, input_queue: RedisQueue,
                 output_queue: RedisQueue,
                 n_guesses: int = 10,
                 log_file: str = 'thinker.log'):
        """
        Args:
            n_guesses (int): Number of guesses the Thinker can make
            input_queue (RedisQueue): Queue to push new simulation commands
            redis_port (int): Port at which the redis server can be reached
            task_prefix (str): Name of the queue holding the results of simulations
        """
        super().__init__()
        self.n_guesses = n_guesses
        self.input_queue = input_queue
        self.output_queue = output_queue
        self.log_file = log_file

    def run(self):
        """Connects to the Redis queue with the results and pulls them"""
        # Open the logger
        logger = logging.getLogger('thinker')
        handler = logging.FileHandler(self.log_file, 'w')
        handler.setFormatter(
            logging.Formatter("%(asctime)s.%(msecs)03d %(name)s:%(lineno)d [%(levelname)s]  %(message)s")
        )
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        logger.info(f'Launched thinking process: {self.pid}')

        # Make a bunch of guesses
        best_answer = inf
        for _ in range(self.n_guesses):
            # Add a new guess
            self.input_queue.put(uniform(0, 10))

            # Get a result
            _, result = self.output_queue.get()

            # Update the best answer
            best_answer = min(best_answer, result)

        # Write the best answer to disk
        with open('answer.out', 'w') as fp:
            print(best_answer, file=fp)


class Doer(MpiMethodServer):
    """Class the manages running the function to be optimized"""

    def run_application(self, i):
        """Compute the target function"""
        print('Running application...')
        x = self.launch_method('target_fun', i)
        print('Target function launched:', x)
        y = self.launch_method('output_result', self.output_queue, i, x)
        return y


if __name__ == '__main__':
    # User input
    parser = argparse.ArgumentParser()
    parser.add_argument("--redishost", default="127.0.0.1",
                        help="Address at which the redis server can be reached")
    parser.add_argument("--redisport", default="6379",
                        help="Port on which redis is available")
    args = parser.parse_args()

    # Write the configuration
    config = Config(
        executors=[
            HighThroughputExecutor(
                label="htex",
                # Max workers limits the concurrency exposed via mom node
                max_workers=2,
                worker_port_range=(10000, 20000),
                provider=LocalProvider(
                    init_blocks=1,
                    max_blocks=1,
                ),
            ),
            ThreadPoolExecutor(label="local_threads")
        ],
        strategy=None,
    )
    parsl.load(config)

    # Connect to the redis server
    input_queue = RedisQueue(args.redishost, port=int(args.redisport), prefix='input')
    input_queue.connect()

    output_queue = RedisQueue(args.redishost, port=int(args.redisport), prefix='output')
    output_queue.connect()

    # Create the task server, launch it in another thread
    #  Using Threads because they let this object access the Parsl dfk
    doer = Doer(input_queue, output_queue, methods_list=[target_fun, output_result], load_default=False)
    doer_proc = Thread(target=doer.main_loop)
    doer_proc.start()

    # Create the thinker
    #  Starts in another process, as it does not need access to the DFK
    thinker = Thinker(input_queue, output_queue)
    thinker.start()

    try:
        thinker.join()
    finally:
        input_queue.put("null")  # Closes the "Doer"
