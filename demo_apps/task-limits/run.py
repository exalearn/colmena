import argparse
import json
import logging
import os
import sys
import time

import numpy as np
from scipy.stats import truncnorm

from datetime import datetime
from typing import Any

from parsl import HighThroughputExecutor
from parsl.addresses import address_by_hostname
from parsl.config import Config
from parsl.launchers import AprunLauncher, SimpleLauncher
from parsl.providers import LocalProvider, CobaltProvider

from colmena.method_server import ParslMethodServer
from colmena.redis.queue import make_queue_pairs, ClientQueues
from colmena.thinker import BaseThinker, agent


def get_args():
    parser = argparse.ArgumentParser(
            formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('--config', default=None,
                        help='JSON Config file to override argparse defaults')
    parser.add_argument('--redis-host', default='localhost',
                        help='Redis server IP')
    parser.add_argument('--redis-port', default='6379',
                        help='Redis server port')
    parser.add_argument('--local-host', action='store_true', default=False,
                        help='Launch jobs on local host')
    parser.add_argument('--task-input-size', type=float, default=1,
                        help='Data amount to send to tasks [MB]')
    parser.add_argument('--task-output-size', type=float, default=1,
                        help='Data amount to return from tasks [MB]')
    parser.add_argument('--task-count', type=int, default=100,
                        help='Number of task to complete')
    parser.add_argument('--worker-count', type=int, default=10,
                        help='Number of tasks per node')
    parser.add_argument('--task-length', type=float, default=1, 
                       help='Length of task in seconds')
    parser.add_argument('--task-length-std', type=float, default=0.1,
                       help='Standard deviation of task length, expressed as a fraction of task length')
    parser.add_argument('--use-value-server', action='store_true', default=False,
                        help='Use the value server for sending data to worker')
    parser.add_argument('--value-server-threshold', type=float, default=1,
                        help='Threshold object size for value server [MB]')
    parser.add_argument('--reuse-data', action='store_true', default=False,
                        help='Send the same input to each task')
    parser.add_argument('--output-dir', type=str, default='runs',
                        help='output dir')

    args = parser.parse_args()

    if args.config is not None:
        with open(args.config) as f:
            for key, value in json.load(f).items():
                if key in args:
                    setattr(args, key, value)
                else:
                    logging.error('Unknown key {} in {}'.format(
                            key, args.config))
    
    return args


def empty_array(size: int) -> np.ndarray:
    return np.empty(int(1000 * 1000 * size / 4), dtype=np.float32)


def target_function(data: np.ndarray, output_size: int, runtime: float) -> np.ndarray:
    import numpy as np
    import time
    time.sleep(runtime)
    return np.empty(int(1000 * 1000 * output_size / 4), dtype=np.float32)


class Thinker(BaseThinker):

    def __init__(self,
                 queue: ClientQueues,
                 task_input_size: int,
                 task_output_size: int,
                 task_count: int,
                 length_mean: float,
                 length_std: float,
                 parallel_tasks: int,
                 out_dir: str
                 ):
        """
        Args:
            queue
            task_input_size: Size of input data (MB)
            task_output_size: Size of output data (MB)
            task_count: Number of tasks to run
            length_mean: Average length of tasks (s)
            length_std: Standard deviation of task length (s)
            parallel_tasks: Number of tasks to run in parallel
            out_dir: Task output directory
        """
        super().__init__(queue)
        self.task_input_size = task_input_size
        self.task_output_size = task_output_size
        self.length_mean = length_mean
        self.length_std = length_std
        self.task_count = task_count
        self.parallel_tasks = parallel_tasks
        self.count = 0
        self.time_dist = truncnorm(0, np.inf, scale=length_std, loc=length_mean)
        self.out_dir = out_dir
    
    def submit(self):
        """Submit a new task to queue"""
        input_data = empty_array(self.task_input_size)
        self.queues.send_inputs(
            input_data, self.task_output_size, self.time_dist.rvs(),
            method='target_function', topic='generate')

    @agent
    def resubmitter(self):
        with open(os.path.join(self.out_dir, 'results.json'), 'w') as fp:
            while self.count < self.task_count:
                result = self.queues.get_result(topic='generate')
                self.submit()
                print(result.json(exclude={'inputs', 'value'}), file=fp)
                self.count += 1
                self.logger.info(f'Completed task {self.count}/{self.task_count}')
                
            for i in range(self.parallel_tasks):
                result = self.queues.get_result(topic='generate')
                print(result.json(exclude={'inputs', 'value'}), file=fp)
                self.logger.info(f'Retrieved remaining task {i+1}/{self.parallel_tasks}')
                

    @agent(startup=False)
    def startup(self):
        """Submit the initial tasks"""
        for _ in range(self.parallel_tasks):
            self.submit()


if __name__ == "__main__":
    args = get_args()

    # Save the configuration
    out_dir = os.path.join(args.output_dir, datetime.utcnow().strftime('%Y-%m-%d_%H-%M-%S'))
    os.makedirs(out_dir, exist_ok=True)

    # Set up the logging
    logging.basicConfig(
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        level=logging.INFO,
        handlers=[logging.FileHandler(os.path.join(out_dir, 'runtime.log')),
                  logging.StreamHandler(sys.stdout)]
    )

    logging.info('Args: {}'.format(args))

    value_server_threshold = args.value_server_threshold * 1000 * 1000 if args.use_value_server else None

    # Make the queues
    client_queues, server_queues = make_queue_pairs(
        args.redis_host,
        args.redis_port,
        topics=['generate'],
        serialization_method='pickle',
        keep_inputs=False,
        value_server_threshold=value_server_threshold
    )


    # Define the worker configuration
    if args.local_host:
        node_count = 1
        executors = [HighThroughputExecutor(max_workers=args.worker_count)]
    else:
        node_count = int(os.environ.get('COBALT_JOBSIZE', 1))
        executors=[
            HighThroughputExecutor(
                address=address_by_hostname(),
                label='workers',
                max_workers=args.worker_count,
                cores_per_worker=1e-6,
                provider=LocalProvider(
                    nodes_per_block=node_count,
                    init_blocks=1,
                    min_blocks=0,
                    max_blocks=1,
                    launcher=AprunLauncher(overrides='-d 64 --cc depth'),
                    worker_init='module load miniconda-3\nconda activate /lus/theta-fs0/projects/CSC249ADCD08/edw/env\n'
                ),
            ),
        ]

    config = Config(executors=executors, run_dir=out_dir)

    doer = ParslMethodServer([target_function], server_queues, config)

    thinker = Thinker(
        queue=client_queues,
        task_input_size=args.task_input_size,
        task_output_size=args.task_output_size,
        task_count=args.task_count,
        parallel_tasks=args.worker_count * node_count,
        length_mean=args.task_length,
        length_std=args.task_length * args.task_length_std,
        out_dir=out_dir
    )
    
    # Save the configuration
    with open(os.path.join(out_dir, 'config.json'), 'w') as fp:
        params = args.__dict__.copy()
        params['parallel_tasks'] = args.worker_count * node_count
        params['queue'] = os.environ.get('COBALT_QUEUE')
        json.dump(params, fp)

    logging.info('Created the method server and task generator')
    logging.info(thinker)

    start_time = time.time()

    try:
        # Launch the servers
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

    # Print the output result
    logging.info('Finished. Runtime = {}s'.format(time.time() - start_time))
