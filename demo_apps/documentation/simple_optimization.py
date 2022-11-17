"""Example from the README"""

##  Building the Thinker
from random import random

from colmena.thinker import BaseThinker, result_processor, task_submitter, ResourceCounter
from colmena.queue import PipeQueues
from colmena.models import Result

# Build queues to connect Thinker and Doer
queues = PipeQueues()


class Thinker(BaseThinker):

    def __init__(self, queues, num_workers: int, num_guesses=100):
        super().__init__(queues, ResourceCounter(num_workers))
        self.best_result = None
        self.answer = -10  # A (bad) starting guess
        self.num_guesses = num_guesses

    @task_submitter()
    def submit_task(self):
        """Submit a new guess close to the current best whenever a node is free"""
        self.queues.send_inputs(self.answer - 1 + 2 * random(), method='simulate')

    @result_processor()
    def store_result(self, result: Result):
        """Update best guess whenever a simulation finishes"""
        assert result.success, result.failure_info
        # Update the best result
        if self.best_result is None or result.value > self.best_result:
            self.answer = result.args[0]
            self.best_result = result.value
        self.rec.release()  # Mark that a node is now free

        # Determine if we are done
        self.num_guesses -= 1
        if self.num_guesses <= 0:
            self.done.set()


thinker = Thinker(queues, 8)

### Building the doer
from parsl.configs.htex_local import config  # Configuration to run locally
from colmena.task_server import ParslTaskServer


# Define your function
def simulate(x: float) -> float:
    return - x ** 2 + 4


# Make the Doer
doer = ParslTaskServer([simulate], queues, config)

## Running the application
# Launch the Thinker and doer
doer.start()
thinker.start()

# Wait until it finishes
thinker.join()
queues.send_kill_signal()

# Done!
print(f'Answer: f({thinker.answer:.2f}) = {thinker.best_result:.2f}')
