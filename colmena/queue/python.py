"""Queues built on Python's native libraries"""
from multiprocessing.connection import Connection
from typing import Collection, Union, Optional, Dict, Tuple
from multiprocessing import Pipe

from colmena.exceptions import TimeoutException, KillSignalException
from colmena.queue.base import ColmenaQueues
from colmena.models import SerializationMethod


class PipeQueues(ColmenaQueues):
    """Queues using Python's implementation
    of `multiprocessing Pipes <https://docs.python.org/3/library/multiprocessing.html#exchanging-objects-between-processes>`_"""

    def __init__(self,
                 topics: Collection[str] = (),
                 serialization_method: Union[str, SerializationMethod] = SerializationMethod.PICKLE,
                 keep_inputs: bool = True,
                 proxystore_name: Optional[Union[str, Dict[str, str]]] = None,
                 proxystore_threshold: Optional[Union[int, Dict[str, int]]] = None):
        super().__init__(topics, serialization_method, keep_inputs, proxystore_name, proxystore_threshold)

        # Make the queues
        self.request_queue = Pipe(duplex=False)
        self.result_queues = dict((t, Pipe(duplex=False)) for t in self.topics)

    def _send_request(self, message: str, topic: str):
        self.request_queue[1].send((topic, message))

    def _get_request(self, timeout: int = None) -> Optional[Tuple[str, str]]:
        if self.request_queue[0].poll(timeout):
            msg = self.request_queue[0].recv()
            if msg[1] == "null":
                raise KillSignalException()
            return msg
        else:
            raise TimeoutException()

    def _send_result(self, message: str, topic: str):
        assert len(message) < 32 * 1024 * 1024, "Messages larger than 32 MiB may fail"
        self.result_queues[topic][1].send(message)

    def _get_result(self, topic: str, timeout: int = None) -> str:
        if self.result_queues[topic][0].poll(timeout):
            return self.result_queues[topic][0].recv()
        else:
            raise TimeoutException()

    def flush(self):
        def _flush(pipe: Tuple[Connection, Connection]):
            """Pull from a queue until it is empty"""
            while pipe[0].poll():
                pipe[0].recv()

        _flush(self.request_queue)
        for v in self.result_queues.values():
            _flush(v)
