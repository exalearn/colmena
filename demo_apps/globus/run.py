"""Perform GPR Active Learning where Bayesian optimization is used to select a new
calculation as soon as one calculation completes"""
from colmena.queue.base import ColmenaQueues
from colmena.queue.python import PipeQueues
from colmena.task_server.globus import GlobusComputeTaskServer
from colmena.thinker import BaseThinker, agent
from globus_compute_sdk import Client
from sklearn.gaussian_process import GaussianProcessRegressor, kernels
from sklearn.preprocessing import MinMaxScaler
from sklearn.pipeline import Pipeline
from functools import partial, update_wrapper
from datetime import datetime
import proxystore as ps
from time import sleep
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


class Thinker(BaseThinker):
    """Tool that monitors results of simulations and calls for new ones, as appropriate"""

    def __init__(self, queues: ColmenaQueues, output_dir: str, dim: int = 2,
                 n_guesses: int = 100, batch_size: int = 10, opt_delay: float = 0):
        """
        Args:
            output_dir (str): Output path
            dim (int): Dimensionality of optimization space
            batch_size (int): Number of simulations to run in parallel
            n_guesses (int): Number of guesses the Thinker can make
            queues (ClientQueues): Queues for communicating with task server
            opt_delay (float): Minimum runtime for the optimizer algorithm
        """
        super().__init__(queues)
        self.n_guesses = n_guesses
        self.queues = queues
        self.batch_size = batch_size
        self.dim = dim
        self.output_path = os.path.join(output_dir, 'results.json')
        self.opt_delay = opt_delay

    @agent
    def operate(self):
        """Connects to the Redis queue with the results and pulls them"""

        # Make a random guesses to start
        for i in range(self.batch_size):
            self.queues.send_inputs(np.random.uniform(-32.768, 32.768, size=(self.dim,)).tolist(), method='ackley')
        self.logger.info('Submitted initial random guesses to queue')
        train_X = []
        train_y = []

        # Use the initial guess to train a GPR
        gpr = Pipeline([
            ('scale', MinMaxScaler(feature_range=(-1, 1))),
            ('gpr', GaussianProcessRegressor(normalize_y=True, kernel=kernels.RBF() * kernels.ConstantKernel()))
        ])

        # Make guesses based on expected improvement
        while len(train_X) < self.n_guesses:
            # Wait for a result to complete
            result = self.queues.get_result()
            with open(self.output_path, 'a') as fp:
                print(result.json(), file=fp)

            # Print the error and halt if one occurred
            if not result.success:
                raise ValueError(f'Task failed:\n{result.failure_info.traceback}')

            # Update the dataset
            train_X.append(result.args)
            train_y.append(result.value)

            # Sleep to simulate a more expensive optimizer
            sleep(self.opt_delay)

            # Update the GPR with the available training data
            gpr.fit(np.vstack(train_X), train_y)

            # Generate a random assortment of potential next points to sample
            sample_X = np.random.uniform(size=(self.batch_size * 1024, self.dim), low=-32.768, high=32.768)

            # Compute the expected improvement for each point
            pred_y, pred_std = gpr.predict(sample_X, return_std=True)
            best_so_far = np.min(train_y)
            ei = (best_so_far - pred_y) / pred_std

            # Run the sample with the highest EI
            best_ind = np.argmax(ei)
            self.logger.info(f'Selected best samples. EI: {ei[best_ind]}')
            best_ei = sample_X[best_ind, :]
            self.queues.send_inputs(best_ei.tolist(), method='ackley')


if __name__ == '__main__':
    # User inputs
    parser = argparse.ArgumentParser()
    parser.add_argument("--num-guesses", "-n", help="Total number of guesses", type=int, default=16)
    parser.add_argument("--num-parallel", "-p", help="Number of guesses to evaluate in parallel (i.e., the batch size)",
                        type=int, default=2)
    parser.add_argument("--dim", help="Dimensionality of the Ackley function", type=int, default=4)
    parser.add_argument('--opt-delay', help="Minimum runtime for the optimizer", type=float, default=0.0)
    parser.add_argument('--runtime', help="Natural logarithm of average runtime for the target function", type=float, default=2)
    parser.add_argument('--runtime-var', help="Natural logarithm of runtime variance for the target function", type=float, default=0.1)
    parser.add_argument('--proxystore-threshold', help="Threshold (bytes) for using ProxyStore with a Redis backend for moving inputs/results. This requires the FuncX endpoint and driver being on the same system.", type=int, default=None)
    parser.add_argument('endpoint', help='FuncX endpoing on which to execute tasks', type=str)
    args = parser.parse_args()

    # Make the output directory
    out_dir = os.path.join('runs',
                           f'streaming-N{args.num_guesses}-P{args.num_parallel}'
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

    if args.proxystore_threshold is not None:
        ps.store.init_store(
            ps.store.STORES.REDIS,
            name='default',
            hostname=args.redishost,
            port=args.redisport
        )
        serialization_method = 'pickle'
    else:
        serialization_method = 'json'

    # Connect to the redis server
    queues = PipeQueues()

    # Log in to FuncX
    gc_client = Client()

    # Create the task server and task generator
    my_ackley = partial(ackley, mean_rt=args.runtime, std_rt=args.runtime_var)
    update_wrapper(my_ackley, ackley)
    doer = GlobusComputeTaskServer({my_ackley: args.endpoint}, gc_client, queues)
    thinker = Thinker(queues, out_dir, dim=args.dim, n_guesses=args.num_guesses,
                      batch_size=args.num_parallel, opt_delay=args.opt_delay)
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
