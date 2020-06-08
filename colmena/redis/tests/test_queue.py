from colmena.redis.queue import RedisQueue, ClientQueues, MethodServerQueues, make_queue_pairs
import pytest


class Test:
    x = None


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
    with pytest.raises(ConnectionError):
        queue.put('test')
    with pytest.raises(ConnectionError):
        queue.get()


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
    assert task.args == (1,)
    assert task.time_input_received is not None
    assert task.time_created < task.time_input_received

    # Test sending the value back
    task.set_result(2)
    server.send_result(task)
    result = client.get_result()
    assert result.value == 2
    assert result.time_result_received > result.time_result_completed


def test_methods():
    """Test sending a method name"""
    client, server = make_queue_pairs('localhost')

    # Push inputs to method server and make sure it is received
    client.send_inputs(1, method='test')
    task = server.get_task()
    assert task.args == (1,)
    assert task.method == 'test'
    assert task.kwargs == {}


def test_kwargs():
    """Test sending function keyword arguments"""
    client, server = make_queue_pairs('localhost')
    client.send_inputs(1, input_kwargs={'hello': 'world'})
    task = server.get_task()
    assert task.args == (1,)
    assert task.kwargs == {'hello': 'world'}


def test_pickling_error():
    """Test communicating results that need to be pickled fails without correct setting"""
    client, server = make_queue_pairs('localhost')

    # Attempt to push a non-JSON-able object to the queue
    with pytest.raises(TypeError):
        client.send_inputs(Test())


def test_pickling():
    """Test communicating results that need to be pickled fails without correct setting"""
    client, server = make_queue_pairs('localhost', use_pickle=True)

    # Attempt to push a non-JSONable object to the queue
    client.send_inputs(Test())
    task = server.get_task()
    assert task.args[0].x is None

    # Set the value
    # Test sending the value back
    x = Test()
    x.x = 1
    task.set_result(x)
    server.send_result(task)
    result = client.get_result()
    assert result.args[0].x is None
    assert result.value.x == 1
