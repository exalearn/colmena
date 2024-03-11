from functools import partial, update_wrapper
from argparse import ArgumentParser
from datetime import datetime
from typing import Callable
from pathlib import Path
import logging
import json
import sys

from parsl import Config, HighThroughputExecutor
from scipy.stats import multivariate_normal
import numpy as np

from colmena.models import Result
from colmena.queue import PipeQueues, ColmenaQueues
from colmena.task_server.parsl import ParslTaskServer
from colmena.thinker import BaseThinker, agent, result_processor


def compute_logp(x: np.ndarray, target_dist: Callable[[np.ndarray], float]) -> float:
    """Compute the log probability density of a sample being from the target distribution

    Args:
        x: Sampled point
        target_dist: Target distribution
    Returns:
        logp
    """
    return target_dist(x)


class Thinker(BaseThinker):
    """Thinker which runs MCMC"""

    def __init__(self,
                 queues: ColmenaQueues,
                 dimensionality: int,
                 num_samples: int,
                 num_walkers: int):
        super().__init__(queues)
        self.num_samples = num_samples
        self.num_walkers = num_walkers
        self.dimensionality = dimensionality
        self.samples = []
        self.current_x = np.random.sample((self.num_walkers, self.dimensionality)) * 2 - 1  # [-1, 1]
        self.current_logp = np.zeros((self.num_walkers,)) + np.inf

    @agent(startup=True)
    def startup(self):
        """Submit initial samples"""

        # Submit them to run
        for i, x in enumerate(self.current_x):
            self.queues.send_inputs(x, method='compute_logp', task_info={'walker': i})
        self.logger.info(f'Submitted an initial {len(self.current_x)} walkers')

    @result_processor()
    def step(self, result: Result):
        """Make a new step"""

        # Move if accepting
        walker_id = result.task_info['walker']
        new_logp, old_logp = result.value, self.current_logp[walker_id]
        accept = np.exp(new_logp - old_logp) < np.random.random()
        if accept:
            self.current_x[walker_id, :] = result.args[0]

        # Submit a new sample, if not done
        if not self.done.is_set():
            self.queues.send_inputs(self.current_x[walker_id] + np.random.random((self.dimensionality,)) * 2 - 1,
                                    method='compute_logp', task_info={'walker': walker_id})

        # Store, then stop if done
        self.samples.append(self.current_x[walker_id])
        self.logger.info(f'Completed {len(self.samples)}/{self.num_samples}')
        if len(self.samples) > self.num_samples:
            self.done.set()


if __name__ == "__main__":
    # Define the run
    parser = ArgumentParser()
    parser.add_argument('--num-walkers', type=int, default=8, help='Number of chains to sample in parallel')
    parser.add_argument('--num-samples', type=int, default=256, help='Total number of samples to draw')
    parser.add_argument('--dimensionality', type=int, default=8, help='Dimensionality of the distribution')
    args = parser.parse_args()

    # Define the output function, which is just a unit MVN with the target dimensionality
    dist = multivariate_normal(mean=[0.] * args.dimensionality)
    target_fn = partial(compute_logp, target_dist=dist.logpdf)
    update_wrapper(target_fn, compute_logp)

    # Make the output directory
    out_dir = Path('runs') / f'mcmc-N{args.num_samples}-P{args.num_walkers}-d={args.dimensionality}-{datetime.now().strftime("%d%m%y-%H%M%S")}'
    out_dir.mkdir(parents=True)
    with open(out_dir / 'params.json', 'w') as fp:
        run_params = args.__dict__
        json.dump(run_params, fp)

    # Make the thinker and task server
    queues = PipeQueues()
    thinker = Thinker(queues=queues, dimensionality=args.dimensionality, num_samples=args.num_samples, num_walkers=args.num_walkers)
    config = Config(executors=[HighThroughputExecutor()], run_dir=str(out_dir / 'parsl'))
    doer = ParslTaskServer(queues=queues, methods=[target_fn], config=config)

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

    # Save the result
    np.save(out_dir / 'samples.npy', np.concatenate(thinker.samples))
    my_logger.info('Saved samples to disk')

    # Wait for the task server to complete
    doer.join()
