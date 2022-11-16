"""Perform GPR Active Learning where one threads continually re-prioritizes a list of
simulations to run and a second thread sebmits """
from colmena.queue.base import ColmenaQueue
from colmena.queue.python import PipeQueue
from colmena.thinker import BaseThinker, agent
from colmena.task_server import ParslTaskServer
from sklearn.gaussian_process import GaussianProcessRegressor, kernels
from sklearn.preprocessing import MinMaxScaler
from sklearn.pipeline import Pipeline
from parsl.executors import HighThroughputExecutor, ThreadPoolExecutor
from parsl.providers import LocalProvider
from functools import partial, update_wrapper
from parsl.config import Config
from threading import Lock, Event
from datetime import datetime
from typing import List, Tuple
import numpy as np
import argparse
import logging
import json
import sys
import os


# Hard code the function to be optimized
def ackley(x: np.ndarray, a=20, b=0.2, c=2 * np.pi, mean_rt=0, std_rt=0.1) -> np.ndarray:
    """The Ackley function (http://www.sfu.ca/~ssurjano/ackley.html)

    Args:
        x (ndarray): Points to be evaluated. Can be a single or list of points
        a (float): Parameter of the Ackley function
        b (float): Parameter of the Ackley function
        c (float): Parameter of the Ackley function
        mean_rt (float): ln(Mean runtime in seconds)
        std_rt (float): ln(Standard deviation of runtime in seconds)
    Returns:
        y (ndarray): Output of the Ackley function
    """

    # Simulate this actually taking awhile
    import numpy as np
    import time
    runtime = np.random.lognormal(mean_rt, std_rt)
    time.sleep(runtime)

    # Make x an array
    x = np.array(x)

    # Get the dimensionality of the problem
    if x.ndim == 0:
        x = x[None, None]
    elif x.ndim == 1:
        x = x[None, :]
    d = x.shape[1]
    y = - a * np.exp(-b * np.sqrt(np.sum(x ** 2, axis=1) / d)) - np.exp(np.cos(c * x).sum(axis=1) / d) + a + np.e
    return y[0]


def reprioritize_queue(database: List[Tuple[np.ndarray, float]],
                       gpr: GaussianProcessRegressor,
                       queue: np.ndarray,
                       opt_delay: float = 0) -> np.ndarray:
    """Reprioritize the task queue

    Args:
        database: Inputs and outputs of completed simulations
        gpr: Gaussian-process regression model
        queue: Existing task queue
        opt_delay: Minimum run time of this function
    Returns:
        Re-ordered priorities of queue
    """
    import numpy as np
    from time import sleep

    sleep(opt_delay)

    # Update the GPR with the available training data
    train_X, train_y = zip(*database)
    gpr.fit(np.vstack(train_X), train_y)

    # Run GPR on the existing task queue
    pred_y, pred_std = gpr.predict(queue, return_std=True)
    best_so_far = np.min(train_y)
    ei = (best_so_far - pred_y) / pred_std

    # Argument sort the EI score, ordered with largest tasks first
    return np.argsort(-1 * ei)


class Thinker(BaseThinker):
    """Tool that monitors results of simulations and calls for new ones, as appropriate"""

    def __init__(self, queues: ColmenaQueue,  output_dir: str, dim: int = 2,
                 n_guesses: int = 100, batch_size: int = 10, opt_delay: float = 0,
                 search_space_size: int = 1000):
        """
        Args:
            output_dir (str): Output path
            dim (int): Dimensionality of optimization space
            batch_size (int): Number of simulations to run in parallel
            n_guesses (int): Number of guesses the Thinker can make
            queues (ClientQueues): Queues for communicating with task server
        """
        super().__init__(queues)
        self.n_guesses = n_guesses
        self.queues = queues
        self.batch_size = batch_size
        self.dim = dim
        self.output_path = os.path.join(output_dir, 'results.json')
        self.opt_delay = opt_delay

        # Make an initial task queue and database
        sampled_space = np.random.uniform(size=(search_space_size, self.dim), low=-32.768, high=32.768)
        self.task_queue = [x for x in sampled_space]
        self.database = []

        # Synchronization bits
        self.has_data = Event()
        self.queue_lock = Lock()
        self.done = Event()

    @agent
    def simulation_worker(self):
        """Dispatch tasks and update"""
        # Send out the initial tasks
        for _ in range(self.batch_size):
            self.queues.send_inputs(self.task_queue.pop(), method='ackley', topic='doer')

        # Pull and re-submit
        while not self.done.is_set():
            # Get a result
            result = self.queues.get_result(topic='doer')

            # Immediately send out a new one
            with self.queue_lock:
                self.queues.send_inputs(self.task_queue.pop(), method='ackley', topic='doer')

            # Add the old task to the database
            self.database.append((result.args[0], result.value))

            # Append it to the output deck
            with open(self.output_path, 'a') as fp:
                print(result.json(exclude={'inputs'}), file=fp)

            # If have required amount, terminate program
            if len(self.database) == self.n_guesses:
                logging.info('Done running new calculations')
                self.done.set()

            # Mark that we have some data now
            self.has_data.set()

    @agent
    def thinker_worker(self):
        """Reprioritize task list"""

        # Make the GPR model
        gpr = Pipeline([
            ('scale', MinMaxScaler(feature_range=(-1, 1))),
            ('gpr', GaussianProcessRegressor(normalize_y=True, kernel=kernels.RBF() * kernels.ConstantKernel()))
        ])

        # Wait until we have data
        self.has_data.wait()
        logging.info('Task reprioritization worker has started')

        while not self.done.is_set():
            # Send out an update task
            with self.queue_lock:
                self.queues.send_inputs(self.database, gpr, self.task_queue,
                                        method='reprioritize_queue',
                                        topic='thinker')

            # Wait until it is complete
            result = self.queues.get_result(topic='thinker')
            new_order = result.value

            # Update the queue (requires locking)
            with self.queue_lock:
                logging.info('Reordering task queue')
                # Copy out the old values
                current_queue = self.task_queue.copy()
                self.task_queue.clear()

                # Note how many of the tasks have been started
                num_started = len(new_order) - len(current_queue)
                logging.info(f'{num_started} jobs have completed in the meanwhile')

                # Compute the new position of tasks
                new_order -= num_started

                # Re-submit tasks to the queue
                for i in new_order:
                    if i < 0:  # Task has already been sent out
                        continue
                    self.task_queue.append(current_queue[i])
                logging.info(f'New queue contains {len(self.task_queue)} tasks')


if __name__ == '__main__':
    # User inputs
    parser = argparse.ArgumentParser()
    parser.add_argument("--num-guesses", "-n", help="Total number of guesses", type=int, default=100)
    parser.add_argument("--num-parallel", "-p", help="Number of guesses to evaluate in parallel (i.e., the batch size)",
                        type=int, default=os.cpu_count())
    parser.add_argument("--dim",  help="Dimensionality of the Ackley function", type=int, default=4)
    parser.add_argument('--runtime', help="Average runtime for the target function", type=float, default=2)
    parser.add_argument('--runtime-var', help="Average runtime for the target function", type=float, default=0.1)
    parser.add_argument('--opt-delay', help="Minimum runtime for the optimization function", type=float, default=2.)
    args = parser.parse_args()

    # Make queue to connect Thinker and Task Server
    queues = PipeQueue(keep_inputs=True, topics=['doer', 'thinker'])

    # Make the output directory
    out_dir = os.path.join('runs',
                           f'interleaved-N{args.num_guesses}-P{args.num_parallel}'
                           f'-{datetime.now().strftime("%d%m%y-%H%M%S")}')
    os.makedirs(out_dir, exist_ok=False)
    with open(os.path.join(out_dir, 'params.json'), 'w') as fp:
        run_params = args.__dict__
        run_params['file'] = os.path.basename(__file__)
        json.dump(run_params, fp)

    # Set up the logging
    logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                        level=logging.INFO,
                        handlers=[logging.FileHandler(os.path.join(out_dir, 'runtime.log')),
                                  logging.StreamHandler(sys.stdout)])

    # Write the configuration
    config = Config(
        executors=[
            HighThroughputExecutor(
                address="localhost",
                label="simulation",
                max_workers=args.num_parallel,
                cores_per_worker=0.0001,
                worker_port_range=(10000, 20000),
                provider=LocalProvider(
                    init_blocks=1,
                    max_blocks=1,
                ),
            ),
            HighThroughputExecutor(
                address="localhost",
                label="task_generator",
                max_workers=1,
                provider=LocalProvider(
                    init_blocks=1,
                    max_blocks=1,
                ),
            ),
            ThreadPoolExecutor(label="local_threads", max_threads=4)
        ],
        strategy=None,
    )
    config.run_dir = os.path.join(out_dir, 'run-info')

    # Create the task server and task generator
    my_ackley = partial(ackley, mean_rt=args.runtime, std_rt=args.runtime_var)
    update_wrapper(my_ackley, ackley)

    my_rep = partial(reprioritize_queue, opt_delay=args.opt_delay)
    update_wrapper(my_rep, reprioritize_queue)
    doer = ParslTaskServer([(my_ackley, {'executors': ['simulation']}),
                            (my_rep, {'executors': ['task_generator']})],
                           queues, config)
    thinker = Thinker(queues, out_dir, dim=args.dim, n_guesses=args.num_guesses,
                      batch_size=args.num_parallel)
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

    # Print out the result
    train_X, train_y = zip(*thinker.database)
    best_ind = np.argmin(train_y)
    train_X = np.array(train_X)
    logging.info(f'Done! Best result {np.array2string(train_X[best_ind, :], precision=2)} = {train_y[best_ind]:.2f}')

    # Wait for the task server to complete
    doer.join()
