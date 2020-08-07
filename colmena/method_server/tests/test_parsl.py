"""Tests for the Parsl implementation of the method server"""
import parsl
from typing import Tuple
from parsl.config import Config
from parsl import ThreadPoolExecutor

from colmena.exceptions import KillSignalException, TimeoutException
from colmena.redis.queue import ClientQueues, make_queue_pairs

from colmena.method_server.parsl import ParslMethodServer
from pytest import fixture, raises, mark


def f(x):
    return x + 1


# Make the Parsl configuration. Use LocalThreads for Mac and Windows compatibility
config = Config(
    executors=[
        ThreadPoolExecutor(label="local_threads", max_threads=4)
    ],
    strategy=None,
)
parsl.load(config)


# Make a simple method server
@fixture(autouse=True)
def server_and_queue() -> Tuple[ParslMethodServer, ClientQueues]:
    client, server = make_queue_pairs('localhost', clean_slate=True)
    return ParslMethodServer([f], server), client


@mark.timeout(30)
def test_kill(server_and_queue):
    """Make sure the server shutdown signal works. Should not hang"""
    server, queue = server_and_queue
    queue.send_kill_signal()

    # Make sure it throws the correct exception
    with raises(KillSignalException):
        server.process_queue()

    # Make sure it kills the server
    queue.send_kill_signal()
    server.run()


@mark.timeout(30)
def test_run_simple(server_and_queue):
    """Make sure the run and stop """
    server, queue = server_and_queue

    # Send a result and then a
    queue.send_inputs(1)
    server.process_queue()
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

    # Make sure it throws the correct exception
    with raises(TimeoutException):
        server.process_queue()

    # Make sure it kills the server
    server.run()


@mark.timeout(30)
def test_error_handling(server_and_queue):
    server, queue = server_and_queue

    # Send a result and then get the error message back
    queue.send_inputs(None)
    server.process_queue()
    result = queue.get_result()
    assert result.args == (None,)
    assert not result.success
    assert result.time_running is None
