from pipeline_prototype.redis_q import RedisQueue, ClientQueues, MethodServerQueues
import pytest


@pytest.fixture
def queue() -> RedisQueue:
    """An empty queue"""
    q = RedisQueue('localhost')
    q.connect()
    q.flush()
    return q


def test_connect_error():
    """Test the connection detection features"""
    queue = RedisQueue('localhost')
    assert not queue.is_connected
    with pytest.raises(ConnectionError) as exc:
        queue.put('test')
    assert 'Not connected' in str(exc)
    with pytest.raises(ConnectionError) as exc:
        queue.get()
    assert 'Not connected' in str(exc)


def test_push_pull(queue):
    """Test a basic push/pull pair"""
    assert queue.is_connected
    queue.put('hello')
    assert queue.get() == 'hello'


def test_flush(queue):
    """Make sure flushing works"""
    queue.put('oops')
    queue.flush()
    assert queue.get(timeout=1) is None


def test_client_method_pair():
    """Make sure method client/server can talk and back and forth"""
    client = ClientQueues('localhost')
    server = MethodServerQueues('localhost')

    # Ensure client and server are talking to the same queue
    assert client.outbound.prefix == server.inbound.prefix
    assert client.inbound.prefix == server.outbound.prefix

    # Push inputs to method server and make sure it is received
    client.send_inputs(1)
    task = server.get_task()
    assert task.inputs == 1
    assert task.time_input_received is not None
    assert task.time_created < task.time_input_received

    # Test sending the value back
    task.set_result(2)
    server.send_result(task)
    result = client.get_result()
    assert result.value == 2
    assert result.time_result_received > result.time_result_completed
