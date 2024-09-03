from typing import Tuple, Generator
from parsl import ThreadPoolExecutor
from parsl.config import Config
from colmena.queue.base import ColmenaQueues
from colmena.queue.python import PipeQueues
from colmena.task_server.parsl import ParslTaskServer

from pytest import fixture, mark

# Make global state for the retry task
RETRY_COUNT = 0


def retry_task(success_idx: int) -> bool:
    """Task that will succeed (return True) every `success_idx` times."""
    global RETRY_COUNT

    # If we haven't reached the success index, raise an error.
    if RETRY_COUNT < success_idx:
        RETRY_COUNT += 1
        raise ValueError('Retry')

    # Reset the retry count
    RETRY_COUNT = 0
    return True


@fixture
def reset_retry_count():
    """Reset the retry count before each test."""
    global RETRY_COUNT
    RETRY_COUNT = 0


@fixture()
def config(tmpdir):
    """Make the Parsl configuration."""
    return Config(
        executors=[
            ThreadPoolExecutor(max_threads=2)
        ],
        strategy=None,
        run_dir=str(tmpdir / 'run'),
    )


@fixture
def server_and_queue(config) -> Generator[Tuple[ParslTaskServer, ColmenaQueues], None, None]:
    queues = PipeQueues()
    server = ParslTaskServer([retry_task], queues, config)
    yield server, queues
    if server.is_alive():
        queues.send_kill_signal()
        server.join(timeout=30)


@mark.timeout(10)
def test_retry_policy_max_retries_zero(server_and_queue, reset_retry_count):
    """Test the retry policy with max_retries=0"""

    # Start the server
    server, queue = server_and_queue
    server.start()

    # The task will fail every other time (setting success_idx=1)
    success_idx = 1

    for i in range(4):
        # The task will fail every other time (setting success_idx=1)
        queue.send_inputs(success_idx, method='retry_task', max_retries=0)
        result = queue.get_result()
        assert result.success == (i % 2 == 1)
        if i % 2 == 1:
            assert result.value
            assert result.failure_info is None
        else:
            assert not result.success
            assert 'Retry' in str(result.failure_info.exception)


@mark.timeout(10)
@mark.parametrize(('success_idx', 'max_retries'), [(0, 0), (1, 1), (4, 10)])
def test_retry_policy_max_retries(server_and_queue, reset_retry_count, success_idx: int, max_retries: int):
    """Test the retry policy.

    This test checks the following cases:
    - A task that always succeeds (success_idx=0, max_retries=0)
    - A task that succeeds after one retry (success_idx=1, max_retries=1)
    - A task that succeeds after four retries (and extra max_retries) (success_idx=4, max_retries=10)
    """

    # Start the server
    server, queue = server_and_queue
    server.start()

    # The task will fail every other time (setting success_idx=1)
    # However, we set max_retries=1, so it should succeed after the first try
    queue.send_inputs(success_idx, method='retry_task', max_retries=max_retries)
    result = queue.get_result()
    assert result is not None
    assert result.success
    assert result.value
    assert result.failure_info is None
    assert result.retries == success_idx
    assert result.max_retries == max_retries
