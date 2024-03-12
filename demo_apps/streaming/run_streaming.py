from functools import partial, update_wrapper
from threading import Event, Lock
from typing import Generator, List, Tuple
from collections import deque
from argparse import ArgumentParser
from datetime import datetime
from pathlib import Path
import logging
import heapq
import json
import sys

from parsl import Config, HighThroughputExecutor

from colmena.models import Result
from colmena.models.methods import PythonGeneratorMethod
from colmena.queue import ColmenaQueues
from colmena.queue.redis import RedisQueues
from colmena.task_server.parsl import ParslTaskServer
from colmena.thinker import BaseThinker, agent, result_processor, ResourceCounter, task_submitter


def generate_inputs(
        best_observations: List[float],
        weights: Tuple[float, float],
        num_steps: int,
        batch_size: int = 8,
        learning_rate: float = 0.1,
        startup_time: float = 10,
        batch_time: float = 5
) -> Generator[List[float], None, Tuple[float, ...]]:
    """Train a function which mimics the best observations,
    yielding batches of examples each training set

    Args:
        best_observations: A list of the best observations
        weights: Current weights of the generator
        num_steps: Number of training steps
        batch_size: Number of examples to produce per batch
        learning_rate: Rate at which to change the weights
        startup_time: Time to sleep at the beginning of the function
        batch_time: Time to sleep per training batch

    Yields:
        Batches of entries produced during training

    Returns:
        Updated weights for the generator
    """
    from time import sleep
    import numpy as np

    sleep(startup_time)

    # Get the statistics of the training points
    has_data = len(best_observations) > 0
    best_mean = best_std = None
    if has_data:
        best_mean = np.mean(best_observations)
        best_std = np.std(best_observations)

    # Gradually adjust generator to mimic them
    output = list(weights)
    for step in range(num_steps):
        sleep(batch_time)  # Emulate an expensive process

        # Generate samples
        batch = np.random.normal(*output, size=(batch_size,))
        yield batch.tolist()

        # Update the weights
        if has_data:
            output[0] += learning_rate * (best_mean - np.mean(batch))
            output[1] += learning_rate * (best_std - np.std(batch))

    return tuple(output)


def evaluate(x: float, run_time: float = 0.5) -> float:
    """Evaluate an entry

    Args:
        x: Entry to evaluate
        run_time: How long to sleep
    Returns:
        Score
    """
    from time import sleep
    sleep(run_time)
    return 4 - x ** 2


class Thinker(BaseThinker):
    """Thinker which runs MCMC"""

    def __init__(self,
                 queues: ColmenaQueues,
                 num_workers: int,
                 num_evaluations: int,
                 out_dir: Path):
        super().__init__(queues, resource_counter=ResourceCounter(num_workers, task_types=['generation', 'simulation']))
        self.out_dir = out_dir

        # Generator state
        self.weights = (2., 4)  # The actual maximum is at 0

        # Queue and signal used to communicate generated results
        self.task_queue = deque[float](maxlen=32)
        self.new_results = Event()

        # Storage for the best results
        self.num_best = 10
        self.remaining_evals = num_evaluations
        self.best_results: List[Tuple[float, float]] = []  # Elements are (y, x)
        self.best_results_lock = Lock()

        # Partition computational resources
        self.rec.reallocate(None, 'generation', 1)  # One to generation
        self.rec.reallocate(None, 'simulation', 'all')  # Remaining to simulation

    @task_submitter(task_type='generation')
    def submit_generation(self):
        """Submit a task to create more samples"""

        # Submit a task with the best results
        with self.best_results_lock:
            best_observations = list(x[1] for x in self.best_results)
        self.queues.send_inputs(
            best_observations,
            self.weights,
            method='generate_inputs',
            topic='generation'
        )
        self.logger.info(f'Submitted generation using {len(best_observations)} entries')

    @result_processor(topic='generation')
    def process_generation(self, result: Result):
        """Process either the generated parts of task or new weights"""

        if not result.success:
            raise ValueError(f'Failed: {result.failure_info.traceback}')

        if result.complete:
            # It's the new weights, store them and trigger a new simulation task
            self.weights = result.value
            self.rec.release('generation', 1)
            self.logger.info('Updated weights and triggered new generation task')
        else:
            # It's new samples. Store them and signal more are ready
            samples = result.value
            self.task_queue.extendleft(samples)  # Put them on the beginning of the list
            self.new_results.set()  # Signal to the submission thread
            self.logger.info(f'Added {len(samples)} to task queue. Queue length: {len(self.task_queue)}')

        with self.out_dir.joinpath('generation-tasks.json').open('a') as fp:
            print(result.json(), file=fp)

    @task_submitter(task_type='simulation')
    def submit_evaluation(self):
        """Submit simulation tasks from the front of the queue"""

        # Block until tasks are available
        if len(self.task_queue) == 0:
            self.new_results.clear()
            self.new_results.wait()
            self.logger.info('No tasks are available. Waiting...')

        # Submit the one from the front of the queue
        sample = self.task_queue.popleft()  # Pull the most-recent task
        self.queues.send_inputs(
            sample,
            method='evaluate',
            topic='simulation'
        )

    @result_processor(topic='simulation')
    def process_evaluation(self, result: Result):
        """Store the result of evaluating a point"""

        if not result.success:
            raise ValueError(f'Failed: {result.failure_info.traceback}')

        # Trigger another submission
        self.rec.release('simulation', 1)

        # Update the heap of the best results
        y = result.value
        with self.best_results_lock:
            # Use a hash which keeps the tasks with the largest y
            if len(self.best_results) < self.num_best:
                heapq.heappush(self.best_results, (y, result.args[0]))
            else:
                heapq.heappushpop(self.best_results, (y, result.args[0]))

        # End if no evaluations left
        self.remaining_evals -= 1
        if self.remaining_evals <= 0:
            self.done.set()
        else:
            self.logger.info(f'{self.remaining_evals} simulations left in budget')

        with self.out_dir.joinpath('simulation-tasks.json').open('a') as fp:
            print(result.json(), file=fp)


if __name__ == "__main__":
    # Define the run
    parser = ArgumentParser()
    parser.add_argument('--num-evaluations', type=int, default=256, help='Total number of samples to evaluate')
    parser.add_argument('--num-workers', type=int, default=4, help='Number of parallel workers')
    parser.add_argument('--generator-batches', type=int, default=8, help='Number of batches per generation task')
    parser.add_argument('--generator-batch-size', type=int, default=8, help='Number of samples to generate per batch')
    parser.add_argument('--generator-timings', type=float, nargs=2, default=(10, 5),
                        help='Time to start and produce a batch for the generator task. Units: s')
    parser.add_argument('--evaluate-time', type=float, default=1.5, help='Time to evaluate a sample. Unit: s')
    args = parser.parse_args()

    # Make the output directory
    out_dir = Path('runs') / f'stream-{datetime.now().strftime("%d%m%y-%H%M%S")}'
    out_dir.mkdir(parents=True)
    with open(out_dir / 'params.json', 'w') as fp:
        run_params = args.__dict__
        json.dump(run_params, fp)

    # Make the thinker and task server
    queues = RedisQueues(topics=['generation', 'simulation'], serialization_method='pickle')
    thinker = Thinker(queues=queues, num_workers=args.num_workers, num_evaluations=args.num_evaluations, out_dir=out_dir)

    # Define the target functions
    eval_fun = partial(evaluate, run_time=args.evaluate_time)
    update_wrapper(eval_fun, evaluate)

    gen_start, gen_batch = args.generator_timings
    gen_fun = PythonGeneratorMethod(
        function=partial(generate_inputs, num_steps=args.generator_batches, batch_size=args.generator_batch_size,
                         startup_time=gen_start, batch_time=gen_batch),
        name='generate_inputs',
        store_return_value=True,
        streaming_queue=queues
    )

    # Make the task server
    config = Config(executors=[HighThroughputExecutor(max_workers=args.num_workers)], run_dir=str(out_dir / 'parsl'))
    doer = ParslTaskServer(queues=queues, methods=[eval_fun, gen_fun], config=config)

    # Set up the logging
    my_logger = logging.getLogger('main')
    col_logger = logging.getLogger('colmena')
    stdout_handler = logging.StreamHandler(sys.stdout)
    file_handler = logging.FileHandler(out_dir / 'run.log')
    for logger in [my_logger, col_logger, thinker.logger]:
        for hnd in [stdout_handler, file_handler]:
            hnd.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
            logger.addHandler(hnd)
        logger.setLevel(logging.INFO)
    my_logger.info(f'Running in {out_dir}')

    try:
        # Launch the servers
        doer.start()
        thinker.start()
        my_logger.info('Launched the servers')

        # Wait for the task generator to complete
        thinker.join()
        my_logger.info('Task generator has completed')
    finally:
        queues.send_kill_signal()

    # Wait for the task server to complete
    doer.join()
