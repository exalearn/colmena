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

# Update a cache with the param and output kv pair.
# Here the cache is a simple python dict, it could be anything, Redis, mongo, file...
# This app runs on the Parsl local side on threads.


@python_app(executors=['local_threads'])
def update_cache(cache, param, inputs=[]):
    print(f"Updating cache with data for {param}")
    with open(inputs[0]) as f:
        simulated_output = int(f.readline().strip())
        cache[param] = simulated_output
    return param, simulated_output

# If the simulated output was even, we launch more simulate task chains.


@python_app(executors=['local_threads'])
def eval_and_launch(cache, kv_pair):
    print(f"Evaluating results from {kv_pair}")
    param, simulated_output = kv_pair
    # We could also do some error case checking.
    if simulated_output % 2 == 0:
        print(f"Launching simulate {simulated_output*2} based on even output!\n")
        x = chain_of_tasks(simulated_output * 2, cache)
    else:
        x = None
    return x

# We listen on a Python multiprocessing Queue as an example
# we launch the simulate pipeline with the params that arrive over this queue
# Listen on the queue for params, and launch the chain of tasks with the param


@python_app(executors=['local_threads'])
def listen_and_launch(queue, task_list, cache_handle):
    while True:
        param = queue.get()
        print(f"QUEUE : Got task params {param}")
        if param is None:
            break
        future = chain_of_tasks(param, cache_handle)
        task_list.append(future)
    return task_list


def make_outdir(path):
    # Make outputs directory if it does not already exist
    if not os.path.exists(path):
        os.makedirs(path)


# Calls a sequence of apps
# simulate  ---> update_cache ---> eval_and_launch
# (remote)     (local threads)     (local threads)
def chain_of_tasks(i, cache_handle):
    print(f"Chain of tasks called with {i} \n")
    outdir = 'outputs'
    make_outdir(outdir)
    x = simulate(i, delay=1 + int(i) % 2, outputs=[File(f'{outdir}/simulate_{i}.out')])
    y = update_cache(cache_handle, i, inputs=[x.outputs[0]])
    z = eval_and_launch(cache_handle, y)
    return z


def main_loop(queue, block=False):

    external_task_list = []
    cache_handle = {}
    active_tasks = []

    m = listen_and_launch(queue, external_task_list, cache_handle)
    # Here we spawn 4 chains of tasks
    # Each chain may launch a single child chain and so on.
    # This will terminate since each generation can launches children with p=1/2
    for i in range(4):
        z = chain_of_tasks(i, cache_handle)
        active_tasks.append(z)

    # Here we will simulate sending a task in via the queue
    # mechanism
    time.sleep(0.5)
    queue.put(100)

    # I think I might have found a limitation in parsl dfk wait_for_current_tasks
    # which is why we have this silly function, that checks for child tasks to complete
    for task in active_tasks:
        current = task
        print(task)
        while True:
            x = current.result()
            if isinstance(x, Future):
                current = x
            else:
                break

    print("External_task_list : ", external_task_list)
    if block is True:
        print("** BLOCKING on queue **")
    else:
        # Trigger listener process to exit via a stop message.
        queue.put(None)

    print(m.result())
    print("Done with internally launched tasks")
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
    parser.add_argument("-b", "--block", action='store_true',
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

    redis_queue = RedisQueue(args.redishost, port=int(args.redisport))
    redis_queue.connect()
    main_loop(redis_queue, block=args.block)
    print("All done")
