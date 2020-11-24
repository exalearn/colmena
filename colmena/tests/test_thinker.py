"""Test the Thinker class"""
import logging

from pytest import fixture

from colmena.redis.queue import make_queue_pairs
from colmena.thinker import BaseThinker, agent


class ExampleThinker(BaseThinker):

    @agent
    def function(self):
        return True


@fixture()
def queues():
    return make_queue_pairs('localhost')


def test_detection():
    assert hasattr(ExampleThinker.function, '_colmena_agent')
    assert len(ExampleThinker.list_agents()) == 1
    assert ExampleThinker.list_agents()[0].__name__ == 'function'


def test_run(queues, caplog):
    # Make the server
    client, server = queues
    th = ExampleThinker(client)
    with caplog.at_level(logging.INFO):
        th.run()
    assert th.done.is_set()

    # Check the messages from the end
    assert 'ExampleThinker.function'.lower() == caplog.record_tuples[-5][0]
    assert 'Launched all 1 functions' in caplog.messages[-4]
    assert 'Thread completed without' in caplog.messages[-2]
    assert 'ExampleThinker completed' in caplog.messages[-1]
