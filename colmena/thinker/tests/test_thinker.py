"""Test the Thinker class"""
from threading import Event
from time import sleep

from pytest import fixture, mark

from colmena.models import Result
from colmena.redis.queue import make_queue_pairs
from colmena.thinker import BaseThinker, agent, result_processor


class ExampleThinker(BaseThinker):

    def __init__(self, queues, flag: Event, daemon: bool):
        super().__init__(queues, daemon=daemon)
        self.flag = flag
        self.func_ran = False
        self.last_value = None

    @agent(critical=False)
    def function(self):
        self.func_ran = True

    @agent
    def critical_function(self):
        self.flag.wait(timeout=3)

    @result_processor
    def process_results(self, result: Result):
        self.last_value = result.value


@fixture()
def queues():
    return make_queue_pairs('localhost')


def test_detection():
    assert hasattr(ExampleThinker.function, '_colmena_agent')
    assert not getattr(ExampleThinker.function, '_colmena_critical')
    assert hasattr(ExampleThinker.critical_function, '_colmena_agent')
    assert getattr(ExampleThinker.critical_function, '_colmena_critical')
    assert hasattr(ExampleThinker.process_results, '_colmena_agent')
    assert len(ExampleThinker.list_agents()) == 3
    assert 'function' in [a.__name__ for a in ExampleThinker.list_agents()]


@mark.timeout(5)
def test_run(queues):
    # Make the server and thinker
    client, server = queues
    flag = Event()
    th = ExampleThinker(client, flag, daemon=True)

    # Launch it and wait for it to run
    th.start()

    # Make sure function ran, and the thinker did not stop
    sleep(.1)
    assert th.func_ran
    assert not flag.is_set()
    assert th.is_alive()
    assert not th.done.is_set()

    # Push a result to the queue and make sure it was received
    server.send_result(Result(inputs=((1,), {}), value=4))
    sleep(.1)
    assert th.last_value == 4

    # Set the "finish" flag
    flag.set()
    sleep(2)
    assert not th.is_alive()
