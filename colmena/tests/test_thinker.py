"""Test the Thinker class"""
import logging

from colmena.thinker import BaseThinker, agent


class ExampleThinker(BaseThinker):

    @agent
    def function(self):
        my_logger = self._make_logger('function')
        my_logger.info('Started function')
        return True


def test_detection():
    assert hasattr(ExampleThinker.function, '_colmena_agent')
    assert len(ExampleThinker.list_agents()) == 1
    assert ExampleThinker.list_agents()[0].__name__ == 'function'


def test_run(caplog):
    th = ExampleThinker()
    with caplog.at_level(logging.INFO):
        th.run()

    # Check the messages from the end
    assert 'ExampleThinker.function'.lower() == caplog.record_tuples[-4][0]
    assert 'Launched all 1 functions' in caplog.messages[-3]
    assert 'Thread completed without' in caplog.messages[-2]
    assert 'ExampleThinker completed' in caplog.messages[-1]
