"""Wrappers for Redis queues."""

import json
import logging
from typing import Optional, Any

import redis

from pipeline_prototype.models import Result

logger = logging.getLogger(__name__)


def _error_if_unconnected(f):
    def wrapper(queue: 'RedisQueue', *args, **kwargs) -> Any:
        if not queue.is_connected:
            raise ConnectionError('Not connected')
        return f(queue, *args, **kwargs)
    return wrapper


class RedisQueue(object):
    """A basic redis queue for communications used by the method server

    The queue only connects when the `connect` method is called to avoid
    issues with passing an object across processes."""

    def __init__(self, hostname: str, port: int = 6379, prefix='pipeline'):
        """
        Args:
            hostname (str): Hostname of the Redis server
            port (int): Port on which to access Redis
            prefix (str): Name of the Redis queue
        """
        self.hostname = hostname
        self.port = port
        self.redis_client = None
        self.prefix = prefix

    def connect(self):
        """Connect to the Redis server"""
        try:
            if not self.redis_client:
                self.redis_client = redis.StrictRedis(
                    host=self.hostname, port=self.port, decode_responses=True)
                self.redis_client.ping() # Ping is needed to detect if connection failed
        except redis.exceptions.ConnectionError:
            print("ConnectionError while trying to connect to Redis@{}:{}".format(self.hostname,
                                                                                  self.port))
            raise

    @_error_if_unconnected
    def get(self, timeout: int = None) -> Optional[str]:
        """Get an item from the redis queue

        Args:
            timeout (int): Timeout for the blocking get in seconds
        Returns:
            (str) Value from the redis queue or ``None`` if timeout is hit
        """
        try:
            if timeout is None:
                return self.redis_client.blpop(self.prefix)[1]  # First entry is the queue name
            else:
                return self.redis_client.blpop(self.prefix, timeout=int(timeout))
        except redis.exceptions.ConnectionError:
            print(f"ConnectionError while trying to connect to Redis@{self.hostname}:{self.port}")
            raise

    @_error_if_unconnected
    def put(self, input_data: str):
        """Push data to a Redis queue

        Args:
            input_data (str): Message to be sent

        payload : dict
            Dict of task information to be stored
        """

        # Send it to the method server
        try:
            self.redis_client.rpush(self.prefix, input_data)
        except redis.exceptions.ConnectionError:
            logger.warning(f"ConnectionError while trying to connect to Redis@{self.hostname}:{self.port}")
            raise

    @_error_if_unconnected
    def flush(self):
        """Flush the Redis queue"""
        try:
            self.redis_client.delete(self.prefix)
        except AttributeError:
            raise Exception("Queue is empty/flushed")
        except redis.exceptions.ConnectionError:
            logger.warning(f"ConnectionError while trying to connect to Redis@{self.hostname}:{self.port}")
            raise

    @property
    def is_connected(self):
        return self.redis_client is not None


class ClientQueues:
    """Wraps communication with the MethodServer"""

    def __init__(self, hostname: str, port: int = 6379, name: Optional[str] = None):
        """
        Args:
            hostname (str): Hostname of the Redis server
            port (int): Port on which to access Redis
            name (int): Name of the MethodServer
        """

        # Make the queues
        self.outbound = RedisQueue(hostname, port, 'inputs' if name is None else f'{name}_inputs')
        self.inbound = RedisQueue(hostname, port, 'inputs' if name is None else f'{name}_results')

        # Attempt to connect
        self.outbound.connect()
        self.inbound.connect()

    def send_inputs(self, input_data: Any):
        """Send inputs to be computed

        Args:
            input_data (Any): Inputs to be computed
        """

        # Create a new Result object
        result = Result(input_data)

        # Push the serialized value to the method server
        self.outbound.put(result.json(exclude_unset=True))

    def get_result(self, timeout: Optional[int] = None) -> Optional[Result]:
        """Get a value from the MethodServer

        Args:
            timeout (int): Timeout for waiting for a value
        Returns:
            (Result) Result from a computation, or ``None`` if timeout is met
        """

        # Get a value
        message = self.inbound.get(timeout=timeout)
        logging.debug(f'Received value: {message}')

        # If None, return because this is a timeout issue
        if message is None:
            return message

        # Parse the value and mark it as complete
        result_obj = Result.parse_raw(message)
        result_obj.mark_result_received()
        return result_obj

    def send_kill_signal(self):
        """Send the kill signal to the method server"""
        self.outbound.put("null")


class MethodServerQueues:
    """Communication wrapper for the MethodServer itself"""

    def __init__(self, hostname: str, port: int = 6379, name: Optional[str] = None, clean_slate: bool = True):
        """
        Args:
            hostname (str): Hostname of the Redis server
            port (int): Port on which to access Redis
            name (str): Name of the MethodServer
            clean_slate (bool): Whether to flush the queues before launching
        """

        # Make the queues
        self.inbound = RedisQueue(hostname, port, 'inputs' if name is None else f'{name}_inputs')
        self.outbound = RedisQueue(hostname, port, 'inputs' if name is None else f'{name}_results')

        # Attempt to connect
        self.outbound.connect()
        self.inbound.connect()

        # Flush, if desired
        if clean_slate:
            self.outbound.flush()
            self.inbound.flush()

    def get_task(self, timeout: int = None) -> Optional[Result]:
        """Get a task object

        Args:
            timeout (int): Timeout for waiting for a task
        Returns:
            (Result) Computation to run or ``None``, which means a kill signal was received
        """

        # Pull a record off of the queue
        message = self.inbound.get(timeout)

        # Return the kill signal
        # TODO (wardlt): Should we raise a TimeoutError when the timeout occurs?
        if message == "null" or message is None:
            return None

        # Get the message
        task = Result.parse_raw(message)
        task.mark_input_received()
        return task

    def send_result(self, result: Result):
        """Send a value to a client

        Args:
            (Result): Result object to communicate back
        """
        self.outbound.put(result.json())
