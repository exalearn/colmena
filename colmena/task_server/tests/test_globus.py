from concurrent.futures import Future
from time import sleep
from uuid import uuid4

from pytest_mock import MockFixture
from pytest import fixture, mark

from colmena.queue.python import PipeQueues
from colmena.task_server.globus import GlobusComputeTaskServer

my_funcs: dict = {}


class FakeClient:
    """Faked Client that allows you to register functions"""

    def register_function(self, new_func, function_name: str = None, **kwargs):
        global my_funcs
        uuid = uuid4()
        if function_name is None:
            function_name = new_func.__name__
        my_funcs[uuid] = (new_func, function_name)
        return uuid


# TODO (wardlt): This mocked version does not mimic the real versions' multi-threaded nature, which
#  means it does not suffer the same problems when being copied over to a subprocess
class FakeExecutor:
    """Faked FuncXExecutor that generates "futures" but does not communicate with FuncX"""

    def __init__(self, *args, **kwargs):
        pass

    def submit_to_registered_function(self, func: str, kwargs: dict):
        # Get the function from the global registry
        global my_funcs
        func = my_funcs[func][0]
        task = kwargs['result']
        new_future = Future()
        result = func(task)
        new_future.set_result(result)
        return new_future

    def shutdown(self):
        pass


@fixture()
def mock_globus(mocker: MockFixture):
    mocker.patch('colmena.task_server.globus.Executor', FakeExecutor)


@mark.timeout(10)
def test_mocked_server(mock_globus):
    # Create the task server with a single, no-op function
    client = FakeClient()
    queues = PipeQueues()

    def func(x):
        if x is None:
            raise MemoryError()
        return x

    fts = GlobusComputeTaskServer({func: 'fake_endp'}, client, queues)
    fts.start()

    # Submit a task to the queue and see how it works
    try:
        # Send a task that will execute properly
        queues.send_inputs(1, method='func')
        sleep(1)
        result = queues.get_result()
        assert result.success
        assert result.value == 1

        # Send a task that will throw a memory error
        queues.send_inputs(None, method='func')
        sleep(1)
        result = queues.get_result()
        assert not result.success
        assert 'MemoryError' in result.failure_info.exception

    finally:
        queues.send_kill_signal()
        fts.join()
