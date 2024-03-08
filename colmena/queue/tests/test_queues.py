"""Tests across different queue implementations"""
from multiprocessing import Pool

from pytest import fixture, raises, mark
from redis import Redis, ConnectionError

from colmena.exceptions import TimeoutException, KillSignalException
from colmena.queue.base import ColmenaQueues
from colmena.queue.python import PipeQueues
from colmena.queue.redis import RedisQueues

# Determine which classes to test

has_redis = True
try:
    client = Redis()
    client.ping()
except ConnectionError:
    has_redis = False

to_test = [PipeQueues]
if has_redis:
    to_test.append(RedisQueues)


@fixture(params=to_test)
def queue(request) -> ColmenaQueues:
    return request.param(['a', 'b'])


def test_topics(queue):
    assert queue.topics == {'default', 'a', 'b'}


@mark.timeout(5)
def test_flush(queue):
    # Test that it flushes out the input queue
    queue.send_inputs(1, method='method')
    queue.flush()

    with raises(TimeoutException):
        queue.get_task(timeout=0.1)

    # Test that it will flush a result
    queue.send_inputs(1, method='method')
    topic, result = queue.get_task()
    queue.send_result(result)
    queue.flush()

    with raises(TimeoutException):
        queue.get_result(topic, timeout=0.1)


@mark.parametrize('topic', ['default', 'a'])
@mark.timeout(5)
def test_basic(queue, topic):
    """Make sure topics get passed back-and-forth correctly"""

    # Send a result
    with Pool(1) as pool:
        queue.send_inputs(1, method='method', topic=topic)
        my_topic, result = pool.apply(queue.get_task)
        assert my_topic == topic

        # Send it back
        result.deserialize()
        result.set_result(1, 1)
        result.serialize()
        pool.apply(queue.send_result, (result,))

        # Make sure it does not appear in b
        with raises(TimeoutException):
            queue.get_result(timeout=0.1, topic='b')
        queue.get_result(topic=topic)


@mark.timeout(5)
def test_kill_signal(queue):
    queue.send_kill_signal()
    with raises(KillSignalException):
        queue.get_task()
