import argparse
import json
import logging
import os
import sys
import time

import numpy as np

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
    parser.add_argument('--task-interval', type=float, default=0.001,
                        help='Interval between new task generation [s]')
    parser.add_argument('--task-count', type=int, default=100,
                        help='Number of task to generate')
    parser.add_argument('--worker-count', type=int, default=10,
                        help='workers to use (worker/node=worker-count//node')
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


def target_function(data: np.ndarray, output_size: int) -> np.ndarray:
    import numpy as np
    import time

    time.sleep(5)  # simulate more imports/setup
    # Check that ObjectProxy acts as the wrapped np object
    assert isinstance(data, np.ndarray), 'got type {}'.format(data)
    time.sleep(0.005)  # simulate more computation
    return np.empty(int(1000 * 1000 * output_size / 4), dtype=np.float32)


class Thinker(BaseThinker):

    def __init__(self,
                 queue: ClientQueues,
                 task_input_size: int,
                 task_output_size: int,
                 task_count: int,
                 task_interval: float,
                 use_value_server: bool,
                 reuse_data: bool,
                 ):
        super().__init__(queue)
        self.task_input_size = task_input_size
        self.task_output_size = task_output_size
        self.task_count = task_count
        self.task_interval = task_interval
        self.use_value_server = use_value_server
        self.reuse_data = reuse_data
        self.count = 0

    def __repr__(self):
        return ("SyntheticDataThinker(\n" + 
                "    task_input_size={}\n".format(self.task_input_size) +
                "    task_output_size={}\n".format(self.task_output_size) +
                "    task_count={}\n".format(self.task_count) +
                "    task_interval={}\n)".format(self.task_interval)
        )

    @agent
    def consumer(self):
        for _ in range(self.task_count):
            result = self.queues.get_result(topic='generate')
            self.logger.info('Got result: {}'.format(str(result).replace('\n', ' ')))

    @agent
    def producer(self):
        if self.reuse_data:
            input_data = empty_array(self.task_input_size)
        while not self.done.is_set():
            if not self.reuse_data:
                input_data = empty_array(self.task_input_size)
            self.queues.send_inputs(input_data,
                    self.task_output_size, method='target_function',
                    topic='generate')
            self.count += 1
            if self.count >= self.task_count:
                break
            time.sleep(self.task_interval)


if __name__ == "__main__":
    args = get_args()

    out_dir = os.path.join(args.output_dir, 
            datetime.utcnow().strftime('%Y-%m-%d_%H-%M-%S'))
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
        executors = [HighThroughputExecutor(max_workers=args.worker_count)]
    else:
        node_count = int(os.environ.get('COBALT_JOBSIZE', 1))
        executors=[
            HighThroughputExecutor(
                address=address_by_hostname(),
                label='workers',
                max_workers=args.worker_count,
                cores_per_worker=max(1, args.worker_count // node_count),
                provider=LocalProvider(
                    nodes_per_block=node_count,
                    init_blocks=1,
                    max_blocks=1,
                    launcher=AprunLauncher('-d 64 --cc depth'),
                    worker_init='module load miniconda-3\nconda activate colmena\n'
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
        task_interval=args.task_interval,
        use_value_server=args.use_value_server,
        reuse_data=args.reuse_data,
    )

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

