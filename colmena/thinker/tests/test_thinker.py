"""Test the Thinker class"""
from threading import Event
from time import sleep

from pytest import fixture, mark

from colmena.models import Result
from colmena.redis.queue import make_queue_pairs
from colmena.thinker import BaseThinker, agent, result_processor, task_submitter, event_responder
from colmena.thinker.resources import ResourceCounter


class ExampleThinker(BaseThinker):

    def __init__(self, queues, rec, flag: Event, daemon: bool):
        super().__init__(queues, rec, daemon=daemon)
        self.flag = flag
        self.func_ran = False
        self.last_value = None
        self.submitted = None
        self.event = Event()
        self.event_responded = False

    @agent(critical=False)
    def function(self):
        self.func_ran = True

    @agent
    def critical_function(self):
        self.flag.wait(timeout=3)

    @result_processor
    def process_results(self, result: Result):
        self.last_value = result.value

    @task_submitter(n_slots=1)
    def submit_task(self):
        assert self.rec.available_slots(None) == 0
        self.submitted = True

    @event_responder(event_name='event')
    def responder(self):
        self.event_responded = True
        self.event.clear()


@fixture()
def queues():
    return make_queue_pairs('localhost')


def test_detection():
    assert hasattr(ExampleThinker.function, '_colmena_agent')
    assert not getattr(ExampleThinker.function, '_colmena_critical')
    assert hasattr(ExampleThinker.critical_function, '_colmena_agent')
    assert getattr(ExampleThinker.critical_function, '_colmena_critical')
    assert hasattr(ExampleThinker.process_results, '_colmena_agent')
    assert hasattr(ExampleThinker.submit_task, '_colmena_agent')
    assert hasattr(ExampleThinker.responder, '_colmena_agent')
    assert len(ExampleThinker.list_agents()) == 5
    assert 'function' in [a.__name__ for a in ExampleThinker.list_agents()]


@mark.timeout(5)
def test_run(queues):
    # Make the server and thinker
    client, server = queues
    flag = Event()
    rec = ResourceCounter(1, [])
    rec.acquire(None, 1)
    th = ExampleThinker(client, rec, flag, daemon=True)

    # Launch it and wait for it to run
    th.start()

    # Make sure function ran, and the thinker did not stop
    sleep(.1)
    assert th.func_ran
    assert not flag.is_set()
    assert th.is_alive()
    assert not th.done.is_set()

    # Test task processor: Push a result to the queue and make sure it was received
    server.send_result(Result(inputs=((1,), {}), value=4))
    sleep(.1)
    assert th.last_value == 4

    # Test task submitter: Release the nodes and see if it submits
    assert not th.submitted
    th.rec.release(None, 1)
    sleep(0.1)
    assert th.is_alive()
    assert th.submitted
    assert rec.available_slots(None) == 0

    # Test event responder: Trigger events and see if it triggers
    assert not th.event.is_set()
    assert not th.event_responded
    th.event.set()
    sleep(0.1)
    assert th.is_alive()
    assert th.event_responded
    assert not th.event.is_set()

    # Set the "finish" flag
    flag.set()
    sleep(2)
    assert not th.is_alive()
