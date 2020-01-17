"""Tests for the Parsl implementation of the method server"""
import parsl
from parsl.config import Config
from parsl import python_app, ThreadPoolExecutor

from pipeline_prototype.execptions import KillSignalException, TimeoutException
from pipeline_prototype.redis.queue import ClientQueues, make_queue_pairs
from pydantic.validators import Tuple

from pipeline_prototype.method_server.parsl import MethodServer
from pytest import fixture, raises, mark


@python_app()
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


class TestMethodServer(MethodServer):
    def run_application(self, method_name, *args, **kwargs):
        return f(*args, **kwargs)

# Make a simple method server
@fixture
def server_and_queue() -> Tuple[MethodServer, ClientQueues]:
    client, server = make_queue_pairs('localhost', clean_slate=True)
    return TestMethodServer(server), client


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


def test_run_simple(server_and_queue):
    """Make sure the run and stop """
    server, queue = server_and_queue

    # Send a result and then a
    queue.send_inputs(1)
    server.process_queue()
    result = queue.get_result()
    assert result.value == 2


def test_timeout(server_and_queue):
    """Make sure the timeout works"""
    server, _ = server_and_queue
    server.timeout = 1

    # Make sure it throws the correct exception
    with raises(TimeoutException):
        server.process_queue()

    # Make sure it kills the server
    server.run()
