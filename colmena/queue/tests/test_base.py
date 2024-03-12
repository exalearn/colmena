"""Test the base class using the thread queue implemetnation"""
from pytest import raises, fixture, warns

from colmena.models import SerializationMethod
from colmena.queue.base import ColmenaQueues
from colmena.queue.python import PipeQueues


class Test:
    """Class used in a serialization test"""
    x = None


@fixture()
def queue() -> ColmenaQueues:
    return PipeQueues(['a', 'b'])


def test_role(queue):
    """Test defining queue roles"""
    assert queue.role == 'any'

    queue.set_role('client')
    assert queue.role == 'client'

    with raises(ValueError):
        queue.set_role('asdf')

    # No warning here
    queue.send_inputs('test')

    # Make sure a warning is created
    with warns() as w:
        queue.get_task()
    assert 'get_task' in w[0].message.args[0]


def test_requests(queue):
    """Test sending requests"""

    # Send a request with all the fixin's
    queue.send_inputs('ab', method='test_method', input_kwargs={'1': 1}, task_info={'info': 'important'}, topic='a')

    # Make sure it comes through
    topic, request = queue.get_task()
    request.deserialize()
    assert request.serialization_method == queue.serialization_method
    assert request.keep_inputs == queue.keep_inputs
    assert topic == 'a'
    assert request.args == ('ab',)
    assert request.kwargs == {'1': 1}
    assert request.method == 'test_method'
    assert request.task_info == {'info': 'important'}

    # Make sure we can change the defaults
    queue.send_inputs('cd', method='test_method', keep_inputs=not queue.keep_inputs)
    topic, request = queue.get_task()
    request.deserialize()
    assert request.keep_inputs != queue.keep_inputs


def test_results(queue):
    """Test receiving results"""

    # Send a request and get it
    queue.send_inputs('ab')
    topic, request = queue.get_task()
    request.deserialize()
    request.set_result(1)
    assert topic == 'default'

    # Set a result value and send it back
    request.serialize()
    queue.send_result(request)
    result = queue.get_result(topic=topic)
    assert result.value == 1


def test_serialization(queue):
    """Test communicating results that must be pickled"""
    # Attempt to push a non-JSONable object to the queue
    queue.send_inputs(Test(), method='test_method', keep_inputs=True)
    topic, task = queue.get_task()
    assert isinstance(task.inputs[0][0], str)
    task.deserialize()
    assert task.args[0].x is None

    # Set the value
    # Test sending the value back
    x = Test()
    x.x = 1
    task.set_result(x)
    task.serialize()
    queue.send_result(task)
    result = queue.get_result(topic=topic)
    assert result.args[0].x is None
    assert result.value.x == 1


def test_clear_inputs(queue):
    """Test clearing the inputs after storing the result"""

    # Sent a method request
    queue.keep_inputs = False
    queue.send_inputs(1)
    _, result = queue.get_task()
    result.deserialize()
    result.set_result(1)

    # Make sure the inputs were deleted
    assert result.args == ()

    # Make sure we can override it, if desired
    queue.send_inputs(1, keep_inputs=True)
    _, result = queue.get_task()
    result.deserialize()
    result.set_result(1)

    assert result.args == (1,)


def test_pickling_error(queue):
    """Test communicating results that need to be pickled fails without correct setting"""
    queue.serialization_method = SerializationMethod.JSON
    # Attempt to push a non-JSON-able object to the queue
    with raises(TypeError):
        queue.send_inputs(Test())


def test_task_info(queue):
    """Make sure task info gets passed along"""

    # Sent a method request
    queue.send_inputs(1, task_info={'id': 'test'})
    topic, result = queue.get_task()
    result.deserialize()
    result.set_result(1)

    # Send it back
    result.serialize()
    queue.send_result(result)
    result = queue.get_result()
    assert result.task_info == {'id': 'test'}


def test_resources(queue):
    # Test with defaults
    queue.send_inputs(1, method='test')
    topic, result = queue.get_task()
    assert result.resources.node_count == 1

    # Test with non-defaults
    queue.send_inputs(1, resources={'node_count': 2})
    topic, result = queue.get_task()
    assert result.resources.node_count == 2


def test_event_count(queue):
    # Sent a method request
    task_id = queue.send_inputs(1)
    assert queue.active_count == 1
    assert not queue.wait_until_done(timeout=1)

    # Make sure the task ID does not change
    topic, task = queue.get_task()
    assert task_id == task.task_id

    # Sent the task back
    task.set_result(1)
    print(queue._active_tasks)
    print(task)
    queue.send_result(task)
    queue.get_result()
    assert queue.active_count == 0
    assert queue.wait_until_done(timeout=1)

    # Send another and make sure the event is reset
    queue.send_inputs(1)
    assert not queue.wait_until_done(timeout=1)
