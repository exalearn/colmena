import argparse
import logging
import os
import sys
import time
from datetime import datetime
from typing import Any

import numpy as np
from proxystore.connectors.file import FileConnector
from proxystore.connectors.globus import GlobusEndpoints
from proxystore.connectors.globus import GlobusConnector
from proxystore.connectors.protocols import Connector
from proxystore.connectors.redis import RedisConnector
from proxystore.store import Store
from proxystore.store import register_store
from globus_compute_sdk import Client
from parsl import HighThroughputExecutor
from parsl.config import Config

from colmena.queue.python import PipeQueues
from colmena.queue.base import ColmenaQueues
from colmena.task_server import ParslTaskServer
from colmena.task_server.base import BaseTaskServer
from colmena.task_server.globus import GlobusComputeTaskServer
from colmena.thinker import agent
from colmena.thinker import BaseThinker


def get_args():
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    backend_group = parser.add_mutually_exclusive_group(required=True)
    backend_group.add_argument(
        '--globus',
        action='store_true',
        help='Use the Globus Compute Colmena Task Server',
    )
    backend_group.add_argument(
        '--parsl',
        action='store_true',
        help='Use the Parsl Colmena Task Server',
    )

    task_group = parser.add_argument_group()
    task_group.add_argument(
        '--redis-host',
        default='localhost',
        help='Redis server IP',
    )
    task_group.add_argument(
        '--redis-port',
        default='6379',
        help='Redis server port',
    )
    task_group.add_argument(
        '--input-size',
        type=float,
        default=1,
        help='Data amount to send to tasks [MB]',
    )
    task_group.add_argument(
        '--output-size',
        type=float,
        default=1,
        help='Data amount to return from tasks [MB]',
    )
    task_group.add_argument(
        '--interval',
        type=float,
        default=0.001,
        help='Interval between new task generation [s]',
    )
    task_group.add_argument(
        '--count',
        type=int,
        default=100,
        help='Number of task to generate',
    )
    task_group.add_argument(
        '--sleep-time',
        type=int,
        default=0,
        help='Optional sleep time for each task',
    )
    task_group.add_argument(
        '--reuse-data',
        action='store_true',
        default=False,
        help='Send the same input to each task',
    )
    task_group.add_argument(
        '--output-dir',
        type=str,
        default='runs',
        help='output directory',
    )

    globus_compute = parser.add_argument_group()
    globus_compute.add_argument(
        '--endpoint',
        required='--globus' in sys.argv,
        help='Globus compute endpoint for task execution',
    )

    parsl_group = parser.add_argument_group()
    parsl_group.add_argument(
        '--workers',
        type=int,
        default=10,
        help='# workers to use (workers/node)',
    )

    ps_group = parser.add_argument_group()
    ps_backend_group = parser.add_mutually_exclusive_group(required=False)
    ps_backend_group.add_argument(
        '--ps-file',
        action='store_true',
        help='Use the ProxyStore file backend.',
    )
    ps_backend_group.add_argument(
        '--ps-globus',
        action='store_true',
        help='Use the ProxyStore Globus backend.',
    )
    ps_backend_group.add_argument(
        '--ps-redis',
        action='store_true',
        help='Use the ProxyStore redis backend.',
    )
    ps_group.add_argument(
        '--ps-threshold',
        type=float,
        default=0.1,
        help='Threshold object size for ProxyStore [MB]',
    )
    ps_group.add_argument(
        '--ps-file-dir',
        required='--ps-file' in sys.argv,
        help='Temp directory to store proxied object in.',
    )
    ps_group.add_argument(
        '--ps-globus-config',
        required='--ps-globus' in sys.argv,
        help='Globus Endpoint config file to use with ProxyStore.',
    )

    return parser.parse_args()


def empty_array(size: int) -> np.ndarray:
    return np.empty(int(1000 * 1000 * size / 4), dtype=np.float32)


def target_function(
    data: np.ndarray,
    output_size: int,
    sleep_time: int = 0,
) -> np.ndarray:
    import numpy as np
    from time import sleep

    # Check that proxy acts as the wrapped np object
    assert isinstance(data, np.ndarray), f'got type {data}'

    sleep(sleep_time)  # simulate additional work

    return np.empty(int(1000 * 1000 * output_size / 4), dtype=np.float32)


class Thinker(BaseThinker):
    def __init__(
        self,
        queue: ColmenaQueues,
        input_size: int,
        output_size: int,
        task_count: int,
        interval: float,
        sleep_time: int,
        reuse_data: bool,
    ):
        super().__init__(queue)
        self.input_size = input_size
        self.output_size = output_size
        self.task_count = task_count
        self.interval = interval
        self.sleep_time = sleep_time
        self.reuse_data = reuse_data
        self.count = 0

    def __repr__(self):
        return (
            f'{self.__class__.__name__}(\n'
            f'    input_size={self.input_size}\n'
            f'    output_size={self.output_size}\n'
            f'    task_count={self.task_count}\n'
            f'    interval={self.interval}\n'
            f'    sleep_time={self.sleep_time}\n'
            f'    reuse_data={self.reuse_data}\n)'
        )

    @agent
    def consumer(self):
        for _ in range(self.task_count):
            result = self.queues.get_result(topic='generate')
            self.logger.info(
                'Got result: {}'.format(str(result).replace('\n', ' ')),
            )

    @agent
    def producer(self):
        if self.reuse_data:
            input_data = empty_array(self.input_size)
        while not self.done.is_set():
            if not self.reuse_data:
                input_data = empty_array(self.input_size)
            self.queues.send_inputs(
                input_data,
                self.output_size,
                self.sleep_time,
                method='target_function',
                topic='generate',
            )
            self.count += 1
            if self.count >= self.task_count:
                break
            time.sleep(self.interval)


if __name__ == '__main__':
    args = get_args()

    out_dir = os.path.join(
        args.output_dir,
        datetime.utcnow().strftime('%Y-%m-%d_%H-%M-%S'),
    )
    os.makedirs(out_dir, exist_ok=True)

    # Set up the logging
    logging.basicConfig(
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        level=logging.INFO,
        handlers=[
            logging.FileHandler(os.path.join(out_dir, 'runtime.log')),
            logging.StreamHandler(sys.stdout),
        ],
    )

    logging.info(f'Args: {args}')

    connector: Connector[Any] | None = None
    ps_name: str | None = None
    if args.ps_file:
        ps_name = 'file'
        connector = FileConnector(args.ps_file_dir)
    elif args.ps_globus:
        ps_name = 'globus'
        endpoints = GlobusEndpoints.from_json(args.ps_globus_config)
        connector = GlobusConnector(endpoints, timeout=60)
    elif args.ps_redis:
        ps_name = 'redis'
        connector = RedisConnector(args.redis_host, args.redis_port)

    store: Store[Any] | None = None
    if connector is not None and ps_name is not None:
        store = Store(ps_name, connector, metrics=True)
        register_store(store)

    # Make the queues
    queues = PipeQueues(
        topics=['generate'],
        serialization_method='pickle',
        keep_inputs=False,
        proxystore_name=ps_name,
        proxystore_threshold=int(args.ps_threshold * 1000 * 1000),
    )

    # Create the task server
    doer: BaseTaskServer
    if args.globus:
        fcx = Client()
        doer = GlobusComputeTaskServer(
            {target_function: args.endpoint},
            fcx,
            queues,
        )
    elif args.parsl:
        # Define the worker configuration
        executors = [HighThroughputExecutor(max_workers=args.workers)]
        config = Config(executors=executors, run_dir=out_dir)
        doer = ParslTaskServer([target_function], queues, config)
    else:
        raise ValueError(f'No such task server')

    thinker = Thinker(
        queue=queues,
        input_size=args.input_size,
        output_size=args.output_size,
        task_count=args.count,
        interval=args.interval,
        sleep_time=args.sleep_time,
        reuse_data=args.reuse_data,
    )

    logging.info('Created the task server and task generator')
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
        queues.send_kill_signal()

    # Wait for the task server to complete
    doer.join()

    if store is not None:
        store.close()

    # Print the output result
    logging.info(f'Finished. Runtime = {time.time() - start_time}s')
