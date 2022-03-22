"""Tests for the Parsl implementation of the task server"""
from typing import Tuple

from parsl import ThreadPoolExecutor
from parsl.config import Config
from pytest import fixture, raises, mark

from colmena.exceptions import KillSignalException, TimeoutException
from colmena.redis.queue import ClientQueues, make_queue_pairs
from colmena.task_server.parsl import ParslTaskServer


def f(x):
    return x + 1


def bad_task(x):
    raise MemoryError()


# Make the Parsl configuration. Use LocalThreads for Mac and Windows compatibility
@fixture()
def config():
    return Config(
        executors=[
            ThreadPoolExecutor(label="local_threads", max_threads=4)
        ],
        strategy=None,
    )


# Make a simple task server
@fixture(autouse=True)
def server_and_queue(config) -> Tuple[ParslTaskServer, ClientQueues]:
    client_q, server_q = make_queue_pairs('localhost', clean_slate=True)
    server = ParslTaskServer([f, bad_task], server_q, config)
    yield server, client_q
    if server.is_alive():
        server.terminate()


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
