"""Evaluate the effect of task duration and size on throughput"""
from platform import node
from datetime import datetime
from time import perf_counter
from typing import TextIO
import argparse
import json
import logging
import os
import sys
import time

import numpy as np
from proxystore.connectors.redis import RedisConnector
from proxystore.store import Store, register_store
from scipy.stats import truncnorm
from parsl import HighThroughputExecutor
from parsl.config import Config

from colmena.models import Result
from colmena.queue import ColmenaQueues
from colmena.queue.redis import RedisQueues
from colmena.task_server.parsl import ParslTaskServer
from colmena.thinker import BaseThinker, result_processor, task_submitter, ResourceCounter


logger = logging.getLogger('main')


def get_args():
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('--config', default=None,
                        help='JSON Config file to override argparse defaults')
    parser.add_argument('--local-host', action='store_true', default=False,
                        help='Launch jobs on local host')
    parser.add_argument('--task-input-size', type=float, default=1,
                        help='Data amount to send to tasks [MB]')
    parser.add_argument('--task-output-size', type=float, default=1,
                        help='Data amount to return from tasks [MB]')
    parser.add_argument('--task-count', type=int, default=100,
                        help='Number of task to complete per node')
    parser.add_argument('--worker-count', type=int, default=10,
                        help='Number of processes per node')
    parser.add_argument('--task-length', type=float, default=1,
                        help='Length of task in seconds')
    parser.add_argument('--task-length-std', type=float, default=0.1,
                        help='Standard deviation of task length, expressed as a fraction of task length')
    parser.add_argument('--use-proxystore', action='store_true', default=False,
                        help='Use the proxystore for sending data to worker')
    parser.add_argument('--proxystore-threshold', type=float, default=1,
                        help='Threshold object size for proxystore [MB]')
    parser.add_argument('--redis-host', default='localhost',
                        help='Host for Redis instance used by Proxystire')
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
                    logging.warning('Unknown key {} in {}'.format(key, args.config))

    return args


def target_function(data: bytes, output_size: float, runtime: float) -> bytes:
    import time
    from random import randbytes
    time.sleep(runtime)
    assert len(data) > 0  # Run a method which requires loading the entire dataset
    return randbytes(int(output_size * 1024 * 1024))


class Thinker(BaseThinker):

    def __init__(self,
                 queue: ColmenaQueues,
                 task_input_size: float,
                 task_output_size: float,
                 task_count: int,
                 length_mean: float,
                 length_std: float,
                 parallel_tasks: int,
                 output_file: TextIO):
        """
        Args:
            queue
            task_input_size: Size of input data (MB)
            task_output_size: Size of output data (MB)
            task_count: Number of tasks to run
            length_mean: Average length of tasks (s)
            length_std: Standard deviation of task length (s)
            parallel_tasks: Number of tasks to run in parallel
            output_file: File to write completed results
        """
        super().__init__(queue, resource_counter=ResourceCounter(parallel_tasks))
        self.task_output_size = task_output_size
        self.parallel_tasks = parallel_tasks
        self.output_file = output_file

        # Define the list of tasks
        time_dist = truncnorm(0, np.inf, scale=length_std, loc=length_mean)

        self.task_queue = [
            (time_dist.rvs(), int(task_input_size * 1024 * 1024))
            for _ in range(task_count)
        ]

    @task_submitter()
    def submit(self):
        """Submit a new task if resources are available"""
        runtime, task_size = self.task_queue.pop()
        input_data = np.empty(task_size, bool)
        self.queues.send_inputs(
            input_data, self.task_output_size, runtime,
            method='target_function')
        if len(self.task_queue) == 0:
            self.done.set()

    @result_processor
    def resubmitter(self, result: Result):
        assert result.success, result.failure_info.traceback
        self.rec.release()

        # Force access to the data
        read_time = perf_counter()
        data_size = len(result.value)
        read_time = perf_counter() - read_time
        result.task_info['read_time'] = read_time
        result.task_info['read_size'] = data_size

        # Store
        print(result.json(exclude={'inputs', 'value'}), file=self.output_file, flush=False)


if __name__ == "__main__":
    args = get_args()

    # Make the output directory
    out_dir = os.path.join(args.output_dir, f"{node()}-{datetime.utcnow().strftime('%Y-%m-%d_%H-%M-%S')}")
    os.makedirs(out_dir, exist_ok=True)

    proxystore_threshold = args.proxystore_threshold * 1000 * 1000 if args.use_proxystore else None

    # Prepare to store the run parameters
    run_params = args.__dict__.copy()

    # Make a proxy store, if needed
    store = None
    if args.use_proxystore:
        #  TODO: Set up to use your target proxystore connector
        store = Store(
            name='store',
            connector=RedisConnector(hostname=args.redis_host, port=6379),
            metrics=True
        )
        register_store(store)
        run_params['store_config'] = str(store)

    # Make the queues
    queues = RedisQueues(
        topics=['generate'],
        serialization_method='pickle',
        keep_inputs=False,
        proxystore_name=store.name if store is not None else None,
        proxystore_threshold=proxystore_threshold
    )  # We use Redis as it's the only queue which can handle large data as of now

    # Define the worker configuration
    if args.local_host:
        node_count = 1
        config = Config(
            executors=[HighThroughputExecutor(max_workers_per_node=args.worker_count)],
            run_dir=out_dir
        )
    else:
        # TODO: Fill in with configuration for your supercomputer. The following is for Polaris
        from parsl.providers import LocalProvider
        from parsl.launchers import MpiExecLauncher
        from parsl.addresses import address_by_interface

        with open(os.environ.get('PBS_NODEFILE')) as fp:
            node_count = len(fp.readlines())
        executors = [
            HighThroughputExecutor(
                address=address_by_interface('bond0'),
                label='workers',
                max_workers=args.worker_count,
                cores_per_worker=1e-6,
                provider=LocalProvider(
                    nodes_per_block=node_count,
                    init_blocks=1,
                    min_blocks=0,
                    max_blocks=1,
                    launcher=MpiExecLauncher(bind_cmd="--cpu-bind", overrides="--depth=64 --ppn 1"),
                    worker_init='module load miniconda-3\nconda activate /lus/theta-fs0/projects/CSC249ADCD08/edw/env\n'
                ),
            ),
        ]

        config = Config(executors=executors, run_dir=out_dir)

    run_params['parsl_config'] = str(config)
    doer = ParslTaskServer([target_function], queues, config)

    # Make the thinker
    thinker = Thinker(
        queue=queues,
        task_input_size=args.task_input_size,
        task_output_size=args.task_output_size,
        task_count=args.task_count,
        parallel_tasks=args.worker_count * node_count,
        length_mean=args.task_length,
        length_std=args.task_length * args.task_length_std,
        output_file=open(os.path.join(out_dir, 'results.json'), 'w')
    )

    # Set up the logging
    col_logger = logging.getLogger('colmena')
    stdout_handler = logging.StreamHandler(sys.stdout)
    file_handler = logging.FileHandler(os.path.join(out_dir, 'run.log'))
    for logger in [col_logger, thinker.logger]:
        for hnd in [stdout_handler, file_handler]:
            hnd.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
            logger.addHandler(hnd)
        logger.setLevel(logging.INFO)
    logger.info(f'Running in {out_dir}')

    logger.info('Args: {}'.format(args))

    # Save the configuration
    with open(os.path.join(out_dir, 'config.json'), 'w') as fp:
        run_params['parallel_tasks'] = args.worker_count * node_count
        json.dump(run_params, fp, indent=2)

    logger.info('Created the method server and task generator')
    logger.info(thinker)

    start_time = time.time()

    try:
        # Launch the servers
        doer.start()
        thinker.start()
        logger.info('Launched the servers')

        # Wait for the task generator to complete
        thinker.join()
    finally:
        queues.send_kill_signal()
        thinker.output_file.close()

    # Wait for the method server to complete
    doer.join()

    # Exit the proxystore
    if store is not None:
        store.close(clear=True)
        logger.info('Closed the proxystore')

    # Print the output result
    logger.info('Finished. Runtime = {}s'.format(time.time() - start_time))
