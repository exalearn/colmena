from concurrent.futures import Future
from time import sleep
from uuid import uuid4

from pytest_mock import MockFixture
from pytest import fixture, mark

from colmena.models import Result
from colmena.redis.queue import make_queue_pairs
from colmena.task_server.funcx import FuncXTaskServer


class FakeClient:
    """Faked FuncXClient that allows you to register functions"""

    def __init__(self):
        self.my_funcs = {}

    def register_function(self, new_func, function_name: str, description: str, searchable: bool):
        uuid = uuid4()
        self.my_funcs[uuid] = (new_func, function_name)
        return uuid


class FakeExecutor:
    """Faked FuncXExecutor that generates "futures" but does not communicate with FuncX"""

    def __init__(self, client: FakeClient, **kwargs):
        self.funcs = client.my_funcs

    def submit(self, func_id: str, task: Result, endpoint_id: str):
        new_future = Future()
        result = self.funcs[func_id][0](task)
        new_future.set_result(result)
        return new_future

    def shutdown(self):
        pass


@fixture()
def mock_funcx(mocker: MockFixture):
    mocker.patch('colmena.task_server.funcx.FuncXExecutor', FakeExecutor)


@mark.timeout(10)
def test_mocked_server(mock_funcx):
    # Create the task server with a single, no-op function
    client = FakeClient()
    client_q, server_q = make_queue_pairs('localhost', clean_slate=True)

    def func(x):
        if x is None:
            raise MemoryError()
        return x
    fts = FuncXTaskServer({func: 'fake_endp'}, client, server_q)
    fts.start()

    # Make sure it registered exactly one function, named
    assert len(client.my_funcs) == 1
    assert list(client.my_funcs.values())[0][1] == 'func'

    # Submit a task to the queue and see how it works
    try:
        # Send a task that will execute properly
        client_q.send_inputs(1, method='func')
        sleep(1)
        result = client_q.get_result()
        assert result.success
        assert result.value == 1

        # Send a task that will throw a memory error
        client_q.send_inputs(None, method='func')
        sleep(1)
        result = client_q.get_result()
        assert not result.success
        assert 'MemoryError' in result.failure_info.exception

    finally:
        client_q.send_kill_signal()
        fts.join()
