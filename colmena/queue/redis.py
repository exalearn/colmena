"""Queues that use Redis"""
from typing import Collection, Optional, Union, Dict, Any, Tuple
from uuid import uuid4
import logging

import redis

from colmena.exceptions import TimeoutException, KillSignalException
from colmena.models import SerializationMethod

from colmena.queue.base import ColmenaQueues

logger = logging.getLogger(__name__)


def _error_if_unconnected(f):
    def wrapper(queue: 'RedisQueues', *args, **kwargs) -> Any:
        if not queue.is_connected:
            raise ConnectionError('Not connected. Did you call `.connect()`?')
        return f(queue, *args, **kwargs)

    return wrapper


class RedisQueues(ColmenaQueues):
    """A basic redis queue for communications used by the task server

    A queue is defined by its prefix and a "topic" designation.
    The full list of available topics is defined when creating the queue,
    and simplifies writing software that waits for only certain types
    of messages without needing to manage several "queue" objects.
    By default, the :meth:`get` methods for the queue
    listen on all topics and the :meth:`put` method pushes to the default topic.
    You can put messages into certain "topical" queue and wait for responses
    that are from a single topic.

    The queue only connects when the `connect` method is called to avoid
    issues with passing an object across processes."""

    def __init__(self,
                 topics: Collection[str],
                 hostname: str = '127.0.0.1',
                 port: int = 6379,
                 prefix: str = uuid4(),
                 serialization_method: Union[str, SerializationMethod] = SerializationMethod.PICKLE,
                 keep_inputs: bool = True,
                 proxystore_name: Optional[Union[str, Dict[str, str]]] = None,
                 proxystore_threshold: Optional[Union[int, Dict[str, int]]] = None):
        """
        Args:
            hostname (str): Hostname of the Redis server
            port (int): Port on which to access Redis
            prefix (str): Name of the Redis queue
            topics: Names of topics that are known for this queue
            serialization_method: Method used to serialize task inputs and results
            keep_inputs: Whether to return task inputs with the result object
            proxystore_name (str, dict): Name of ProxyStore backend to use for all
                topics or a mapping of topic to ProxyStore backend for specifying
                backends for certain tasks. If a mapping is provided but a topic is
                not in the mapping, ProxyStore will not be used.
            proxystore_threshold (int, dict): Threshold in bytes for using
                ProxyStore to transfer objects. Optionally can pass a dict
                mapping topics to threshold to use different threshold values
                for different topics. None values in the mapping will exclude
                ProxyStore use with that topic.

        """
        super().__init__(topics, serialization_method, keep_inputs, proxystore_name, proxystore_threshold)
        self.hostname = hostname
        self.port = port
        self.redis_client = None
        self.prefix = prefix
        assert not any("{" in t for t in self.topics)
        self.connect()

    def __setstate__(self, state):
        self.__dict__ = state

        # If you find the RedisClient placeholder,
        #  attempt to reconnect
        if self.redis_client == 'connected':
            self.redis_client = None
            self.connect()

    def __getstate__(self):
        state = super().__getstate__()

        # If connected, remove the unpicklable RedisClient and
        #  put a placeholder instead
        if self.is_connected:
            state['redis_client'] = 'connected'
        return state

    def connect(self):
        """Connect to the Redis server"""
        try:
            if not self.redis_client:
                self.redis_client = redis.StrictRedis(
                    host=self.hostname, port=self.port, decode_responses=True)
                self.redis_client.ping()  # Ping is needed to detect if connection failed
        except redis.exceptions.ConnectionError:
            logger.warning(f"ConnectionError while trying to connect to Redis@{self.hostname}:{self.port}")
            raise

    def disconnect(self):
        """Disconnect from the server

        Useful if sending the connection object to another process"""
        self.redis_client = None

    def _send_message(self, message, queue):
        """Sending a message to a certain redis queue

        Args:
            message: Message to send
            queue: Name of the queue
        """
        try:
            assert len(message) < 512 * 1024 * 1024, "Value too large for Redis. Task will fail"
            output = self.redis_client.rpush(queue, message)
            if not isinstance(output, int):
                raise ValueError(f'Message failed to push to {queue}')
        except redis.exceptions.ConnectionError:
            logger.warning(f"ConnectionError while trying to connect to Redis@{self.hostname}:{self.port}. Is Redis running?")
            raise

    def _get_message(self, queue: str, timeout: Optional[float]) -> str:
        """Get a message from a queue

        Args:
            queue: Name of the queue
            timeout: Timeout
        """

        # Redis uses 0 for non-blocking values
        if timeout is None:
            timeout = 0
        elif timeout < 1:
            logger.warning('Redis does not support timeouts less than 1s')
            timeout = 1

        try:
            output = self.redis_client.blpop(queue, timeout=int(timeout))
            if output is None:
                raise TimeoutException()
            return output[1]
        except redis.exceptions.ConnectionError:
            logger.warning(f"ConnectionError while trying to connect to Redis@{self.hostname}:{self.port}")
            raise

    @_error_if_unconnected
    def _send_request(self, message: str, topic: str):
        queue = f'{self.prefix}_requests'
        # Send it to the task server
        self._send_message(topic + message, queue)

    @_error_if_unconnected
    def _get_request(self, timeout: float = None) -> Tuple[str, str]:
        queue = f'{self.prefix}_requests'

        # Get the message
        message = self._get_message(queue, timeout)

        # The task data is JSON-encoded, so the first time a { appears is the beginning of the result
        #  the string before it is the topic
        if message.endswith("null"):
            raise KillSignalException()
        request_start = message.index("{")
        topic, request = message[:request_start], message[request_start:]

        return topic, request

    @_error_if_unconnected
    def _send_result(self, message: str, topic: str):
        queue = f'{self.prefix}_{topic}_result'
        self._send_message(message, queue)

    @_error_if_unconnected
    def _get_result(self, topic: str, timeout: int = None) -> str:
        queue = f'{self.prefix}_{topic}_result'
        return self._get_message(queue, timeout)

    @_error_if_unconnected
    def flush(self):
        """Flush the Redis queue"""

        all_queues = [f'{self.prefix}_requests']
        for topic in self.topics:
            all_queues.append(f'{self.prefix}_{topic}_result')
        try:
            self.redis_client.delete(*all_queues)
        except AttributeError:
            raise Exception("Queue is empty/flushed")
        except redis.exceptions.ConnectionError:
            logger.warning(f"ConnectionError while trying to connect to Redis@{self.hostname}:{self.port}")
            raise

    @property
    def is_connected(self):
        return self.redis_client is not None
