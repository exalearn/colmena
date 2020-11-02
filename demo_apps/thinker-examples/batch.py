"""Perform GPR Active Learning where simulations are sent in batches"""
from colmena.method_server import ParslMethodServer
from colmena.redis.queue import ClientQueues, make_queue_pairs
from sklearn.gaussian_process import GaussianProcessRegressor, kernels
from sklearn.preprocessing import MinMaxScaler
from sklearn.pipeline import Pipeline
from parsl.executors import HighThroughputExecutor, ThreadPoolExecutor
from parsl.providers import LocalProvider
from functools import partial, update_wrapper
from parsl.config import Config
from threading import Thread
from datetime import datetime
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


class Thinker(Thread):
    """Tool that monitors results of simulations and calls for new ones, as appropriate"""

    def __init__(self, queues: ClientQueues,  output_dir: str, dim: int = 2,
                 n_guesses: int = 100, batch_size: int = 10):
        """
        Args:
            output_dir (str): Output path
            dim (int): Dimensionality of optimization space
            batch_size (int): Number of simulations to run in parallel
            n_guesses (int): Number of guesses the Thinker can make
            queues (ClientQueues): Queues for communicating with method server
        """
        super().__init__(daemon=True)
        self.n_guesses = n_guesses
        self.queues = queues
        self.batch_size = batch_size
        self.dim = dim
        self.logger = logging.getLogger(self.__class__.__name__)
        self.output_path = os.path.join(output_dir, 'results.json')

    def run(self):
        """Connects to the Redis queue with the results and pulls them"""

        # Make a random guess to start
        for i in range(self.batch_size):
            self.queues.send_inputs(np.random.uniform(-32.768, 32.768, size=(self.dim,)).tolist())
        self.logger.info('Submitted initial random guesses to queue')
        train_X = []
        train_y = []

        # Use the initial guess to train a GPR
        gpr = Pipeline([
            ('scale', MinMaxScaler(feature_range=(-1, 1))),
            ('gpr', GaussianProcessRegressor(normalize_y=True, kernel=kernels.RBF() * kernels.ConstantKernel()))
        ])

        with open(self.output_path, 'a') as fp:
            for _ in range(self.batch_size):
                result = self.queues.get_result()
                print(result.json(), file=fp)
                train_X.append(result.args)
                train_y.append(result.value)

        # Make guesses based on expected improvement
        for _ in range(self.n_guesses // self.batch_size - 1):
            # Update the GPR with the available training data
            gpr.fit(np.vstack(train_X), train_y)

            # Generate a random assortment of potential next points to sample
            sample_X = np.random.uniform(size=(self.batch_size * 1024, self.dim), low=-32.768, high=32.768)

            # Compute the expected improvement for each point
            pred_y, pred_std = gpr.predict(sample_X, return_std=True)
            best_so_far = np.min(train_y)
            ei = (best_so_far - pred_y) / pred_std

            # Run the samples with the highest EI
            best_inds = np.argsort(ei)[-self.batch_size:]
            self.logger.info(f'Selected {len(best_inds)} best samples. EI: {ei[best_inds]}')
            for i in best_inds:
                best_ei = sample_X[i, :]
                self.queues.send_inputs(best_ei.tolist())
            self.logger.info('Sent all of the inputs')

            # Wait for the value to complete
            with open(self.output_path, 'a') as fp:
                for _ in range(self.batch_size):
                    result = self.queues.get_result()
                    print(result.json(), file=fp)
                    train_X.append(result.args)
                    train_y.append(result.value)


if __name__ == '__main__':
    # User inputs
    parser = argparse.ArgumentParser()
    parser.add_argument("--redishost", default="127.0.0.1",
                        help="Address at which the redis server can be reached")
    parser.add_argument("--redisport", default="6379",
                        help="Port on which redis is available")
    parser.add_argument("--num-guesses", "-n", help="Total number of guesses", type=int, default=100)
    parser.add_argument("--num-parallel", "-p", help="Number of guesses to evaluate in parallel (i.e., the batch size)",
                        type=int, default=os.cpu_count())
    parser.add_argument("--dim",  help="Dimensionality of the Ackley function", type=int, default=4)
    parser.add_argument('--runtime', help="Average runtime for the target function", type=float, default=10)
    parser.add_argument('--runtime-var', help="Average runtime for the target function", type=float, default=4)
    args = parser.parse_args()

    # Connect to the redis server
    client_queues, server_queues = make_queue_pairs(args.redishost, args.redisport, serialization_method='json')

    # Make the output directory
    out_dir = os.path.join('runs',
                           f'batch-N{args.num_guesses}-P{args.num_parallel}'
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
                label="htex",
                # Max workers limits the concurrency exposed via mom node
                max_workers=args.num_parallel,
                cores_per_worker=0.0001,
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
    config.run_dir = os.path.join(out_dir, 'run-info')

    # Create the method server and task generator
    my_ackley = partial(ackley, mean_rt=np.log(args.runtime), std_rt=np.log(args.runtime_var))
    update_wrapper(my_ackley, ackley)
    doer = ParslMethodServer([my_ackley], server_queues, config, default_executors=['htex'])
    thinker = Thinker(client_queues, out_dir, dim=args.dim, n_guesses=args.num_guesses,
                      batch_size=args.num_parallel)
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
