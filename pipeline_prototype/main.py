import argparse

import parsl
from parsl.executors import ThreadPoolExecutor
from parsl.executors import HighThroughputExecutor
from parsl.providers import LocalProvider
from parsl.config import Config

from pipeline_prototype.redis_q import RedisQueue

from pipeline_prototype.method_server import MpiMethodServer


def cli_run():
    parser = argparse.ArgumentParser()
    parser.add_argument("--redishost", default="127.0.0.1",
                        help="Address at which the redis server can be reached")
    parser.add_argument("--redisport", default="6379",
                        help="Port on which redis is available")
    parser.add_argument("-d", "--debug", action='store_true',
                        help="Count of apps to launch")
    parser.add_argument("-m", "--mac", action='store_true',
                        help="Configure for Mac")
    args = parser.parse_args()

    if args.debug:
        parsl.set_stream_logger()

    if args.mac:
        config = Config(
            executors=[
                ThreadPoolExecutor(label="theta_mpi_launcher"),
                ThreadPoolExecutor(label="local_threads")
            ],
            strategy=None,
        )
    else:
        config = Config(
            executors=[
                HighThroughputExecutor(
                    label="theta_mpi_launcher",
                    # Max workers limits the concurrency exposed via mom node
                    max_workers=2,
                    provider=LocalProvider(
                        init_blocks=1,
                        max_blocks=1,
                    ),
                ),
                ThreadPoolExecutor(label="local_threads")
            ],
            strategy=None,
        )
    parsl.load(config)

    print('''This program creates an "MPI Method Server" that listens on an input queue and write on an output queue:

        input_queue --> mpi_method_server --> output_queue

To send it a request, add an entry to the input queue:
     run "pipeline-pump -p N" where N is an integer request
To access a result, remove it from the outout queue:
     run "pipeline-pull" (blocking) or "pipeline-pull -t T" (T an integer) to time out after T seconds
    TODO: Timeout does not work yet!
''')

    # input_queue --> mpi_method_server --> output_queue

    input_queue = RedisQueue(args.redishost, port=int(
        args.redisport), prefix='input')
    input_queue.connect()
    output_queue = RedisQueue(args.redishost, port=int(
        args.redisport), prefix='output')
    output_queue.connect()
    # value_server = RedisQueue(args.redishost, port=int(args.redisport), prefix='value')
    # value_server.connect()

    mms = MpiMethodServer(input_queue, output_queue)
    mms.main_loop()

    # Next up, we likely want to add the ability to create a value server and connect it to a method server, e.g.:
    # vs = value_server.ValueServer(output_queue)

    print("All done")


if __name__ == "__main__":
    cli_run()
