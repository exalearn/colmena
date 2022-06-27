"""Tests for the Parsl implementation of the task server"""
from typing import Tuple, List

from parsl import HighThroughputExecutor
from parsl.config import Config
from pytest import fixture, mark
import proxystore as ps

from colmena.models import ResourceRequirements
from .test_base import EchoTask, FakeMPITask
from colmena.redis.queue import ClientQueues, make_queue_pairs
from colmena.task_server.parsl import ParslTaskServer


def f(x):
    return x + 1


def capitalize(y: List[str], x: str, **kwargs):
    return x.upper(), [i.lower() for i in y]


def bad_task(x):
    raise MemoryError()


def count_nodes(x, _resources: ResourceRequirements):
    return _resources.node_count


# Make the Parsl configuration. Use LocalThreads for Mac and Windows compatibility
@fixture()
def config(tmpdir):
    return Config(
        executors=[
            HighThroughputExecutor()
        ],
        strategy=None,
        run_dir=str(tmpdir),
    )


# Make a proxy store for larger objects
@fixture()
def store():
    return ps.store.init_store(ps.store.STORES.REDIS, name='store', hostname='localhost', port=6379, stats=True)


@fixture(autouse=True)
def server_and_queue(config, store) -> Tuple[ParslTaskServer, ClientQueues]:
    """Make a simple task server"""
    client_q, server_q = make_queue_pairs('localhost', clean_slate=True,
                                          proxystore_name='store', proxystore_threshold=5000,
                                          serialization_method='pickle')
    server = ParslTaskServer([f, capitalize, bad_task, EchoTask(), FakeMPITask(), count_nodes], server_q, config)
    yield server, client_q
    if server.is_alive():
        client_q.send_kill_signal()
        server.join(timeout=30)


@mark.timeout(30)
def test_kill(server_and_queue):
    """Make sure the server shutdown signal works. Should not hang"""
    server, queue = server_and_queue
    queue.send_kill_signal()

    # Make sure it kills the server
    queue.send_kill_signal()
    server.start()
    server.join()

    assert server.exitcode == 0


@mark.timeout(30)
def test_run_simple(server_and_queue):
    """Make sure the run and stop """
    server, queue = server_and_queue

    # Start the service
    server.start()

    # Send a result and then check results
    queue.send_inputs(1, method='f')
    result = queue.get_result()
    assert result.success
    assert result.value == 2
    assert result.time_running > 0
    assert result.time_deserialize_inputs > 0
    assert result.time_serialize_results > 0
    assert result.time_compute_started is not None
    assert result.time_result_sent is not None


@mark.timeout(30)
def test_timeout(server_and_queue):
    """Make sure the timeout works"""
    server, _ = server_and_queue
    server.timeout = 1

    # Make sure the server dies
    server.start()
    server.join()


@mark.timeout(30)
def test_error_handling(server_and_queue):
    server, queue = server_and_queue

    # Start the server
    server.start()

    # Send a result and then get the error message back
    queue.send_inputs(None, method='f')
    result = queue.get_result()
    assert result.args == (None,)
    assert result.value is None
    assert not result.success
    assert result.failure_info is not None
    assert result.time_running is not None

    # Send a task that kills the worker
    queue.send_inputs(None, method='bad_task')
    result = queue.get_result()
    assert 'MemoryError' in result.failure_info.exception


@mark.timeout(30)
def test_bash(server_and_queue):
    server, queue = server_and_queue

    # Get ready to receive tasks
    server.start()

    # Send a standard task
    queue.send_inputs(1, method='echotask')
    result = queue.get_result()
    assert result.success, result.failure_info
    assert result.value == '1\n'
    assert result.keep_inputs
    assert result.additional_timing['exec_execution'] > 0
    assert result.inputs == ((1,), {})

    # Send an MPI task
    queue.send_inputs(1, method='fakempitask')
    result = queue.get_result()
    assert result.success, result.failure_info
    assert result.value == '-N 1 -n 1 --cc depth echo -n 1\n'  # We're actually testing that it makes the correct command string
    assert result.keep_inputs
    assert result.additional_timing['exec_execution'] > 0
    assert result.inputs == ((1,), {})

    # Send an MPI task
    queue.send_inputs(1, method='fakempitask', resources=ResourceRequirements(node_count=2, cpu_processes=4))
    result = queue.get_result()
    assert result.success, result.failure_info
    assert result.value == '-N 8 -n 4 --cc depth echo -n 1\n'
    assert result.keep_inputs
    assert result.additional_timing['exec_execution'] > 0
    assert result.inputs == ((1,), {})


@mark.timeout(30)
def test_resources(server_and_queue):
    server, queue = server_and_queue

    # Get ready to receive tasks
    server.start()

    # Launch a test with the default values
    queue.send_inputs(1, method='count_nodes')
    result = queue.get_result()
    assert result.success
    assert result.value == 1  # Default node count is 1

    # Launch a test with a user-specified value
    queue.send_inputs(1, method='count_nodes', resources={'node_count': 2})
    result = queue.get_result()
    assert result.success
    assert result.value == 2


@mark.timeout(30)
def test_proxy(server_and_queue, store):
    """Test a task that uses proxies"""

    # Start the server
    server, queue = server_and_queue
    server.start()

    # Send a big task
    big_string = "a" * 10000
    little_string = "A" * 10000
    queue.send_inputs([little_string], big_string, method='capitalize')
    result = queue.get_result()
    assert result.success, result.failure_info.exception
    assert len(result.proxy_timing) == 2  # There are two proxies to resolve

    # Proxy the results ahead of time
    little_proxy = store.proxy(little_string)

    queue.send_inputs([little_proxy], big_string, method='capitalize')
    result = queue.get_result()
    assert result.success, result.failure_info.exception
    assert len(result.proxy_timing) == 2

    # Try it with a kwarg
    queue.send_inputs(['a'], big_string, input_kwargs={'little': little_proxy}, method='capitalize',
                      keep_inputs=False)  # TODO (wardlt): test does not work with keep-inputs=True
    result = queue.get_result()
    assert result.success, result.failure_info.exception
    assert len(result.proxy_timing) == 2
