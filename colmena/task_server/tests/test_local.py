"""Test local task server"""

from pytest import fixture

from colmena.queue.python import PipeQueues
from colmena.task_server.local import LocalTaskServer


def f(x):
    return x


@fixture
def queues():
    return PipeQueues()


@fixture
def server(queues):
    server = LocalTaskServer(queues, methods=[f], num_workers=2)
    server.start()
    yield server
    queues.send_kill_signal()
    server.join()


def test_local_service(server, queues):
    queues.send_inputs(1, method='f')

    result = queues.get_result(timeout=4)
    assert result.success, result.failure_info.traceback
    assert result.value == 1
