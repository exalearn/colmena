"""Example used in the Colmena quickstart"""
import sys
import logging
from math import inf

from parsl import HighThroughputExecutor
from parsl.config import Config

from colmena.task_server import ParslTaskServer
from colmena.queue import ColmenaQueue, PipeQueue
from colmena.thinker import BaseThinker, agent


def target_function(x: float) -> float:
    return x ** 2


def task_generator(best_to_date: float) -> float:
    from random import random
    return best_to_date + random() - 0.5


if __name__ == "__main__":
    # Set up the logging
    logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                        level=logging.INFO,
                        handlers=[logging.FileHandler('runtime.log'),
                                  logging.StreamHandler(sys.stdout)])

    # Make the queues
    queues = PipeQueue(topics=['generate', 'simulate'], keep_inputs=True)

    # Define the worker configuration
    config = Config(executors=[HighThroughputExecutor()])

    doer = ParslTaskServer([target_function, task_generator], queues, config)

    # Define the thinker
    class Thinker(BaseThinker):

        def __init__(self, queue):
            super().__init__(queue)
            self.remaining_guesses = 10
            self.parallel_guesses = 4
            self.best_guess = 10
            self.best_result = inf

        @agent
        def consumer(self):
            for _ in range(self.remaining_guesses):
                # Update the current guess with the
                result = self.queues.get_result(topic='simulate')
                if result.value < self.best_result:
                    self.best_result = result.value
                    self.best_guess = result.args[0]

        @agent
        def producer(self):
            while not self.done.is_set():
                # Make a new guess
                self.queues.send_inputs(self.best_guess, method='task_generator', topic='generate')

                # Get the result, push new task to queue
                result = self.queues.get_result(topic='generate')
                self.logger.info(f'Created a new guess: {result.value:.2f}')
                self.queues.send_inputs(result.value, method='target_function', topic='simulate')

    thinker = Thinker(queues)
    logging.info('Created the task server and task generator')

    try:
        # Launch the servers
        doer.start()
        thinker.start()
        logging.info('Launched the servers')

        # Wait for the task generator to complete
        thinker.join()
        logging.info('Task generator has completed')
    finally:
        queues.send_kill_signal()

    # Wait for the task server to complete
    doer.join()

    # Print the output result
    logging.info(f'Result: f({thinker.best_guess:.2f}) = {thinker.best_result:.2f}')
