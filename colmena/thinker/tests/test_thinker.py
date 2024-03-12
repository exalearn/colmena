"""Test the Thinker class"""
import logging
from threading import Event
from time import sleep

from pytest import fixture, mark

from colmena.models import Result
from colmena.queue.python import PipeQueues
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
        self.n_slots = 1
        self.teardown_result = None

    def prepare_agent(self):
        if self.agent_name == "setup_teardown":
            self.local_details.payload = 'hello'

    def tear_down_agent(self):
        if self.agent_name == "setup_teardown":
            self.teardown_result = self.local_details.payload + "_goodbye"

    @agent(startup=True)
    def startup_function(self):
        self.func_ran = True

    @agent(startup=True)
    def setup_teardown(self):
        """Agent that tests the setup and teardown logic"""
        pass

    @agent
    def function(self):
        self.flag.wait()

    @result_processor
    def process_results(self, result: Result):
        self.last_value = result.value

    @task_submitter(n_slots='n_slots')  # Look it up from the class attribute at runtime
    def submit_task(self):
        assert self.rec.available_slots(None) >= 0
        self.submitted = True
        sleep(0.1)  # Used to prevent a race condition with responder. By the time this event finishes

    @event_responder(event_name='event', reallocate_resources=True, max_slots='n_slots',
                     gather_from=None, gather_to="event", disperse_to="event")
    def responder(self):
        self.rec.acquire("event", 1)
        self.event_responded = True
        self.rec.release("event", 1)


@fixture()
def queues():
    return PipeQueues([])


def test_detection():
    # Test that the wrappers set the appropriate attributes
    assert hasattr(ExampleThinker.startup_function, '_colmena_agent')
    assert getattr(ExampleThinker.startup_function, '_colmena_startup')
    assert hasattr(ExampleThinker.function, '_colmena_agent')
    assert not getattr(ExampleThinker.function, '_colmena_startup')
    assert hasattr(ExampleThinker.process_results, '_colmena_agent')
    assert hasattr(ExampleThinker.submit_task, '_colmena_agent')
    assert hasattr(ExampleThinker.responder, '_colmena_agent')

    # Test detecting the agents
    assert len(ExampleThinker.list_agents()) == 6
    assert 'function' in [a.__name__ for a in ExampleThinker.list_agents()]


def test_logger_name(queues):
    """Test the name of loggers"""

    class SimpleThinker(BaseThinker):
        pass

    # See if the default name holds
    thinker = SimpleThinker(queues)
    assert 'SimpleThinker' in thinker.logger.name

    # See if we can provide it its own name
    thinker = SimpleThinker(queues, logger_name='my_logger')
    assert thinker.logger.name == 'my_logger'


@mark.timeout(5)
def test_logger_timings_event(queues, caplog):
    class TestThinker(BaseThinker):
        event: Event = Event()

        @event_responder(event_name='event')
        def a(self):
            self.done.set()

        @event_responder(event_name='event')
        def b(self):
            self.done.set()

    # Start the thinker
    with caplog.at_level(logging.INFO):
        thinker = TestThinker(queues, daemon=True)
        thinker.start()
        sleep(0.5)
        thinker.event.set()
        thinker.join(timeout=1)
    assert thinker.done.is_set()
    assert any('Runtime' in record.msg for record in caplog.records if '.a' in record.name)
    assert sum('All responses to event complete' in record.msg for record in caplog.records) == 1


@mark.timeout(5)
def test_logger_timings_process(queues, caplog):
    class TestThinker(BaseThinker):

        @result_processor()
        def process(self, _):
            self.done.set()

    # Start the thinker
    thinker = TestThinker(queues, daemon=True)
    thinker.start()

    # Spoof a result completing
    queues.send_inputs(1, method='test')
    topic, result = queues.get_task()
    result.set_result(1, 1)
    with caplog.at_level(logging.INFO):
        queues.send_result(result)

        # Wait then check the logs
        sleep(0.5)

    assert thinker.done.is_set()
    assert any('Runtime' in record.msg for record in caplog.records if '.process' in record.name), caplog.record_tuples


@mark.timeout(5)
def test_logger_timings_submitter(queues, caplog):
    class TestThinker(BaseThinker):

        @task_submitter()
        def submit(self):
            self.done.set()

    # Start the thinker
    with caplog.at_level(logging.INFO):
        thinker = TestThinker(queues, ResourceCounter(1), daemon=True)
        thinker.start()
        sleep(0.5)

    assert thinker.done.is_set()
    assert any('Runtime' in record.msg for record in caplog.records if '.submit' in record.name), caplog.record_tuples


@mark.timeout(5)
def test_run(queues):
    """Test the behavior of all agents"""
    # Make the server and thinker
    flag = Event()
    rec = ResourceCounter(1, ["event"])
    rec.acquire(None, 1)
    th = ExampleThinker(queues, rec, flag, daemon=True)

    # Launch it and wait for it to run
    th.start()

    # Make sure startup function ran, and the thinker did not stop
    sleep(.1)
    assert th.func_ran
    assert not flag.is_set()
    assert th.is_alive()
    assert not th.done.is_set()

    # Make sure the setup and teardown logic worked
    assert th.teardown_result == "hello_goodbye"

    # Test task processor: Push a result to the queue and make sure it was received
    queues.send_inputs(1)
    topic, task = queues.get_task()
    task.set_result(4)
    queues.send_result(task)
    sleep(0.1)
    assert th.last_value == 4

    # Test task submitter: Release the nodes and see if it submits
    assert not th.submitted
    th.rec.release(None, 1)
    sleep(0.1)
    assert th.is_alive()
    assert th.submitted
    assert rec.available_slots(None) == 0

    # Test event responder: Trigger event, see if it triggers and acquires resources
    assert len(th.barriers) == 1
    assert th.barriers['event'].n_waiting == 0
    assert not th.event.is_set()
    assert not th.event_responded
    th.event.set()
    sleep(0.1)  # Give enough time for this thread to start up before releasing resources
    for _ in range(3):
        th.rec.release(None, 1)  # Repeat as submit_task/responder are competing for resources
    sleep(0.1)
    assert th.is_alive()
    assert th.event_responded
    assert th.rec.available_slots("event") >= 1
    assert not th.event.is_set()

    # Set the "finish" flag after sending a result out
    queues.send_inputs(1)
    flag.set()
    sleep(1)
    assert th.is_alive()

    # The system should not exit until all results are back
    topic, task = queues.get_task()
    task.set_result(4)
    queues.send_result(task)
    assert th.queues.wait_until_done(timeout=2)
    sleep(0.1)
    assert not th.is_alive()


@mark.timeout(5)
def test_exception(queues):
    """Verify that thinkers stop properly with an exception"""

    # Make a thinker that will fail on startup
    class BadThinker(BaseThinker):

        def __init__(self, queues):
            super().__init__(queues)
            self.flag = Event()
            self.was_set = False

        @event_responder(event_name='flag')
        def do_nothing(self):
            self.was_set = False
            pass

        @agent(startup=True)
        def fail(self):
            raise ValueError()

    # Will only exit within a timeout if the exception is properly set
    thinker = BadThinker(queues)
    thinker.run()
