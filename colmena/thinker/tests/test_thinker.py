"""Test the Thinker class"""
from threading import Event
from time import sleep

from pytest import fixture, mark

from colmena.redis.queue import make_queue_pairs
from colmena.thinker import BaseThinker, agent


class ExampleThinker(BaseThinker):

    def __init__(self, queues, flag: Event):
        super().__init__(queues)
        self.flag = flag
        self.func_ran = False

    @agent(critical=False)
    def function(self):
        self.func_ran = True

    @agent
    def critical_function(self):
        self.flag.wait(timeout=3)


@fixture()
def queues():
    return make_queue_pairs('localhost')


def test_detection():
    assert hasattr(ExampleThinker.function, '_colmena_agent')
    assert not getattr(ExampleThinker.function, '_colmena_critical')
    assert hasattr(ExampleThinker.critical_function, '_colmena_agent')
    assert getattr(ExampleThinker.critical_function, '_colmena_critical')
    assert len(ExampleThinker.list_agents()) == 2
    assert 'function' in [a.__name__ for a in ExampleThinker.list_agents()]


@mark.timeout(5)
def test_run(queues):
    # Make the server and thinker
    client, server = queues
    flag = Event()
    th = ExampleThinker(client, flag)

    # Launch it and wait for it to run
    th.start()

    # Make sure the function ran, and the machine did not close
    sleep(.1)
    assert th.func_ran
    assert not flag.is_set()
    assert th.is_alive()
    assert not th.done.is_set()

    # Set the "finish" flag
    flag.set()
    sleep(.1)
    assert not th.is_alive()
    assert th.done.is_set()
