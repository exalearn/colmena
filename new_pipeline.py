import argparse
from multiprocessing import Queue
import redis
import time
import os

import parsl
from parsl import python_app, bash_app
from parsl.executors import ThreadPoolExecutor
from parsl.executors import HighThroughputExecutor
from parsl.providers import LocalProvider
from parsl.config import Config
from parsl.data_provider.files import File
from concurrent.futures import Future

from redis_q import RedisQueue

config_mac = Config(
    executors=[
        ThreadPoolExecutor(label="theta_mpi_launcher"),
        ThreadPoolExecutor(label="local_threads")
    ],
    strategy=None,
)

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

# Simulate will run some commands on bash, this can be made to run MPI applications via aprun
# Here, simulate will put a random number from range(0-32767) into the output file.
@bash_app(executors=['theta_mpi_launcher'])
def simulate(params, delay=1, outputs=[], stdout=parsl.AUTO_LOGNAME, stderr=parsl.AUTO_LOGNAME):
    return f'''sleep {delay};
echo "Running at ", $PWD
echo "Running some serious MPI application"
set -x
echo "aprun mpi_application {params} -o {outputs[0]}"
echo $RANDOM > {outputs[0]}
'''

# Output the param and output kv pair on the output queue.
# This app runs on the Parsl local side on threads.
@python_app(executors=['local_threads'])
def output_result(output_queue, param, inputs=[]):
    print(f"Outputting data for {param}")
    with open(inputs[0]) as f:
        simulated_output = int(f.readline().strip())
        output_queue.put((param, simulated_output))
    return param, simulated_output

# We listen on a Python multiprocessing Queue as an example
# we launch the simulate pipeline with the params that arrive over this queue
# Listen on the input queue for params, run a task for the param, and output the result on output queue
@python_app(executors=['local_threads'])
def listen_and_launch(input_queue, output_queue, task_list):
    while True:
        param = input_queue.get()
        print(f"QUEUE : Got task params {param}")
        if param is None:
            break
        future = chain_of_tasks(param, output_queue)
        task_list.append(future)
    return task_list


def make_outdir(path):
    # Make outputs directory if it does not already exist
    if not os.path.exists(path):
        os.makedirs(path)


# Calls a sequence of apps
# simulate  ---> output_result
# (remote)     (local threads)
def chain_of_tasks(i, output_queue):
    print(f"Chain of tasks called with {i} \n")
    outdir = 'outputs'
    make_outdir(outdir)
    x = simulate(i, delay=1 + i % 2, outputs=[File(f'{outdir}/simulate_{i}.out')])
    y = output_result(output_queue, i, inputs=[x.outputs[0]])
    return y


def main_loop(input_queue, output_queue, block=False):

    external_task_list = []

    m = listen_and_launch(input_queue, output_queue, external_task_list)

    if block is True:
        print("** BLOCKING on queue **")
    else:
        # Trigger listener process to exit via a stop message.
        print("** NOT blocking on queue **")
        queue.put(None)

    for task in external_task_list:
        current = task
        print(task)
        while True:
            x = current.result()
            if isinstance(x, Future):
                current = x
            else:
                break

    dfk = parsl.dfk()
    dfk.wait_for_current_tasks()


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("--redishost", default="127.0.0.1",
                        help="Address at which the redis server can be reached")
    parser.add_argument("--redisport", default="6379",
                        help="Port on which redis is available")
    parser.add_argument("-b", "--block", action='store_false',
                        help="Set blocking behavior where the pipeline will block until a None message is passed through the queue")
    parser.add_argument("-d", "--debug", action='store_true',
                        help="Count of apps to launch")
    parser.add_argument("-m", "--mac", action='store_true',
                        help="Configure for Mac")
    args = parser.parse_args()

    if args.debug:
        parsl.set_stream_logger()
    if args.mac:
        parsl.load(config_mac)
    else:
        parsl.load(config)

    input_queue = RedisQueue(args.redishost, port=int(args.redisport), prefix='input')
    input_queue.connect()
    output_queue = RedisQueue(args.redishost, port=int(args.redisport), prefix='output')
    output_queue.connect()
    main_loop(input_queue, output_queue, block=args.block)
    print("All done")
