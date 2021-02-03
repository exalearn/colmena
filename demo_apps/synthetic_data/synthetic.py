import argparse
import json
import logging
import os
import sys
import time

from typing import Any

from parsl import HighThroughputExecutor
from parsl.addresses import address_by_hostname
from parsl.config import Config
from parsl.launchers import AprunLauncher, SimpleLauncher
from parsl.providers import LocalProvider, CobaltProvider

from colmena.method_server import ParslMethodServer
from colmena.redis.queue import make_queue_pairs, ClientQueues
from colmena.thinker import BaseThinker, agent
from colmena import value_server


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
    parser.add_argument('--task-input-size', type=int, default=1,
                        help='Data amount to send to tasks [MB]')
    parser.add_argument('--task-output-size', type=int, default=1,
                        help='Data amount to return from tasks [MB]')
    parser.add_argument('--task-interval', type=float, default=0.001,
                        help='Interval between new task generation [s]')
    parser.add_argument('--task-count', type=int, default=100,
                        help='Number of task to generate')
    parser.add_argument('--worker-count', type=int, default=10,
                        help='Workers to use')

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


def target_function(data, output_size):
    return data


class Thinker(BaseThinker):

    def __init__(self,
                 queue: ClientQueues,
                 task_input_size: int,
                 task_output_size: int,
                 task_count: int,
                 task_interval: float,
                 ):
        super().__init__(queue)
        self.task_input_size = task_input_size
        self.task_output_size = task_output_size
        self.task_count = task_count
        self.task_interval = task_interval
        self.count = 0
        self.data_sent = 0
        self.data_received = 0

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
            self.data_received += sys.getsizeof(result.value)
            print('Got result: ', result)

        self.logger.info(
            'Thinker sent {} MB and recieved {} MB over {} tasks.'.format(
            self.data_sent / 1000, self.data_received / 1000, self.count)
        )

    @agent
    def producer(self):
        #while not self.done.is_set():
        for i in range(self.task_count):
            #input_data = bytearray(1000 * self.task_input_size)
            #input_data_key = hash(input_data)
            input_data = i
            ref = value_server.put(input_data)
            self.data_sent += sys.getsizeof(input_data)
            self.queues.send_inputs(ref, self.task_output_size,
                    method='target_function', topic='generate')
            self.count += 1
            #if self.count >= self.task_count:
            #    break
            time.sleep(self.task_interval)


if __name__ == "__main__":
    # Set up the logging
    logging.basicConfig(
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        level=logging.INFO,
        handlers=[logging.FileHandler('runtime.log'),
                  logging.StreamHandler(sys.stdout)]
    )

    args = get_args()

    # Make the queues
    client_queues, server_queues = make_queue_pairs(
        args.redis_host,
        args.redis_port,
        topics=['generate'],
        serialization_method='pickle'
    ) 

    # Connect to value server on client. The workers will connect automatically
    # using the host/port that we pass as environment variables
    value_server.init_value_server(args.redis_host, args.redis_port)

    # Define the worker configuration
    if args.local_host:
        executors = [HighThroughputExecutor(max_workers=args.worker_count)]
    else:
        node_count = int(os.environ.get('COBALT_JOBSIZE', 1))
        executors=[
            HighThroughputExecutor(
                address=address_by_hostname(),
                label='workers',
                max_workers=node_count,
                provider=LocalProvider(
                    nodes_per_block=node_count,
                    init_blocks=1,
                    max_blocks=1,
                    launcher=AprunLauncher('-d 64 --cc depth'),
                    worker_init='''
module load miniconda-3
conda activate colmena
export {}={}
export {}={}
'''.format(value_server.VALUE_SERVER_HOST_ENV_VAR, args.redis_host, value_server.VALUE_SERVER_PORT_ENV_VAR, args.redis_port)
                ),
            ),
        ]

    config = Config(executors=executors)

    doer = ParslMethodServer([target_function], server_queues, config)

    thinker = Thinker(
        queue=client_queues,
        task_input_size=args.task_input_size,
        task_output_size=args.task_output_size,
        task_count=args.task_count,
        task_interval=args.task_interval,
    )

    logging.info('Created the method server and task generator')
    logging.info(thinker)

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
    logging.info('Finished')

