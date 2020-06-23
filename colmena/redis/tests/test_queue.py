from colmena.redis.queue import RedisQueue, ClientQueues, MethodServerQueues, make_queue_pairs
import pytest


class Test:
    x = None


@pytest.fixture
def queue() -> RedisQueue:
    """An empty queue"""
    q = RedisQueue('localhost', topics=['priority'])
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

    # Test without a topic
    queue.put('hello')
    assert queue.get() == ('default', 'hello')

    # Test with a specified topic
    queue.put('hello', topic='priority')
    assert queue.get() == ('priority', 'hello')

    # Test with an unspecified topic
    with pytest.raises(AssertionError):
        queue.put('hello', 'not_a_topic')


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
    topic, task = server.get_task()
    assert topic == 'default'
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
    _, task = server.get_task()
    assert task.args == (1,)
    assert task.method == 'test'
    assert task.kwargs == {}


def test_kwargs():
    """Test sending function keyword arguments"""
    client, server = make_queue_pairs('localhost')
    client.send_inputs(1, input_kwargs={'hello': 'world'})
    _, task = server.get_task()
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
    _, task = server.get_task()
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


def test_filtering():
    """Test filtering tasks by topic"""
    client, server = make_queue_pairs('localhost', clean_slate=True, topics=['priority'])

    # Simulate a result being sent through the method server
    client.send_inputs("hello", topic="priority")
    topic, task = server.get_task()
    assert topic == "priority"
    task.set_result(1)
    server.send_result(task, topic)

    # Make sure it does not appear if we pull only from "default"
    output = client.get_result(timeout=1, topic='default')
    assert output is None

    # Make sure it works if we specify the topic
    output = client.get_result(topic='priority')
    assert output is not None

    # Make sure it works if we do not specify anything
    server.send_result(task, topic)
    output = client.get_result()
    assert output is not None
