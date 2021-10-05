"""Wrappers for Redis queues."""

import logging
from typing import Optional, Any, Tuple, Dict, Iterable, Union

import redis

import proxystore as ps

from colmena.exceptions import TimeoutException, KillSignalException
from colmena.models import Result, SerializationMethod

logger = logging.getLogger(__name__)


def _error_if_unconnected(f):
    def wrapper(queue: 'RedisQueue', *args, **kwargs) -> Any:
        if not queue.is_connected:
            raise ConnectionError('Not connected')
        return f(queue, *args, **kwargs)
    return wrapper


def make_queue_pairs(hostname: str, port: int = 6379, name='method',
                     serialization_method: Union[str, SerializationMethod] = SerializationMethod.JSON,
                     keep_inputs: bool = True,
                     clean_slate: bool = True,
                     topics: Optional[Iterable[str]] = None,
                     value_server_threshold: Optional[int] = None,
                     value_server_hostname: Optional[str] = None,
                     value_server_port: Optional[int] = None)\
        -> Tuple['ClientQueues', 'TaskServerQueues']:
    """Make a pair of queues for a server and client

    Args:
        hostname (str): Hostname of the Redis server
        port (int): Port on which to access Redis
        name (str): Name of the MethodServer
        serialization_method (SerializationMethod): serialization type for inputs/outputs
        keep_inputs (bool): Whether to keep the inputs after the method has finished executing
        clean_slate (bool): Whether to flush the queues before launching
        topics ([str]): List of topics used when having the client filter different types of tasks
        value_server_threshold (int): Input/output objects larger than this threshold
            (in bytes) will be stored in the value server. If None, value server
            is ignored
        value_server_hostname (str): Optional redis server hostname for value server.
            If the value server is being used but this option is not provided,
            the redis server for the task queues will be used.
        value_server_port (int): See `value_server_hostname`
    Returns:
        (ClientQueues, TaskServerQueues): Pair of communicators set to use the correct channels
    """

    return (ClientQueues(hostname, port, name, serialization_method, keep_inputs,
                         topics, value_server_threshold, value_server_hostname,
                         value_server_port),
            TaskServerQueues(hostname, port, name, topics=topics, clean_slate=clean_slate))


class RedisQueue:
    """A basic redis queue for communications used by the task server

    A queue is defined by its prefix and a "topic" designation.
    The full list of available topics is defined when creating the queue,
    and simplifies writing software that waits for only certain types
    of messages without needing to manage several "queue" objects.
    By default, the :meth:`get` methods for the queue
    listen on all topics and the :meth:`put` method pushes to the default topic.
    You can put messages into certain "topical" queues and wait for responses
    that are from a single topic.

    The queue only connects when the `connect` method is called to avoid
    issues with passing an object across processes."""

    def __init__(self, hostname: str, port: int = 6379, prefix='pipeline',
                 topics: Optional[Iterable[str]] = None):
        """
        Args:
            hostname (str): Hostname of the Redis server
            port (int): Port on which to access Redis
            prefix (str): Name of the Redis queue
            topics ([str]): List of special topics
        """
        self.hostname = hostname
        self.port = port
        self.redis_client = None
        self.prefix = prefix

        # Create the list of accepted topics
        if topics is None:
            topics = set()
        else:
            topics = set(topics)
        topics.add("default")
        assert not any('_' in t for t in topics), "Topic names may not contain underscores"
        self._all_queues = [f'{prefix}_{t}' for t in topics]

    def __setstate__(self, state):
        self.__dict__ = state

        # If you find the RedisClient placeholder,
        #  attempt to reconnect
        if self.redis_client == 'connected':
            self.redis_client = None
            self.connect()

    def __getstate__(self):
        state = self.__dict__.copy()

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
        """Disconnet from the server

        Useful if sending the connection object to another process"""
        self.redis_client = None

    @_error_if_unconnected
    def get(self, timeout: int = None, topic: Optional[str] = None) -> Optional[Tuple[str, str]]:
        """Get an item from the redis queue

        Args:
            timeout (int): Timeout for the blocking get in seconds
            topic (str): Which topical queue to read from. If ``None``, wait for all topics
        Returns:
            If timeout occurs, output is ``None``. Else:
            - (str) Topic of the item
            - (str) Value from the redis queue
        """

        # Get the topic(s)
        if topic is None:
            queues = self._all_queues
        else:
            queues = f'{self.prefix}_{topic}'
            assert queues in self._all_queues, f"Unrecognized topic: {topic}"

        # Wait for the result
        try:
            if timeout is None:
                queue, result = self.redis_client.blpop(queues)
            else:
                output = self.redis_client.blpop(queues, timeout=int(timeout))
                if output is None:
                    return output
                queue, result = output

            # Strip off the prefix for the queue
            return queue.split("_")[-1], result
        except redis.exceptions.ConnectionError:
            print(f"ConnectionError while trying to connect to Redis@{self.hostname}:{self.port}")
            raise

    @_error_if_unconnected
    def put(self, input_data: str, topic: str = 'default'):
        """Push data to a Redis queue

        Args:
            input_data (str): Message to be sent
            topic (str): Topic of the queue
        """

        # Make the queue name
        queue = f'{self.prefix}_{topic}'
        assert queue in self._all_queues, f'Unrecognized topic: {topic}'

        # Send it to the task server
        try:
            assert len(input_data) < 512 * 1024 * 1024, "Value too large for Redis. Task will fail"
            output = self.redis_client.rpush(queue, input_data)
            if not isinstance(output, int):
                raise ValueError(f'Message failed to push to {queue}')
        except redis.exceptions.ConnectionError:
            logger.warning(f"ConnectionError while trying to connect to Redis@{self.hostname}:{self.port}")
            raise

    @_error_if_unconnected
    def flush(self):
        """Flush the Redis queue"""
        for queue in self._all_queues:
            try:
                self.redis_client.delete(queue)
            except AttributeError:
                raise Exception("Queue is empty/flushed")
            except redis.exceptions.ConnectionError:
                logger.warning(f"ConnectionError while trying to connect to Redis@{self.hostname}:{self.port}")
                raise

    @property
    def is_connected(self):
        return self.redis_client is not None


class ClientQueues:
    """Provides communication of method requests and results with the task server

    This queue wraps communication with the underlying Redis queue and also handles communicating
    requests using the :class:`Result` messaging format.
    Method requests are encoded in the Result format along with any options (e.g., serialization method)
    for the communication and automatically serialized.
    Results are automatically de-serialized upon receipt.

    The queue also generates stores task performance results, such as the timestamp for when messages were created
    and runtime for serialization.
    """

    def __init__(self, hostname: str, port: int = 6379, name: Optional[str] = None,
                 serialization_method: Union[str, SerializationMethod] = SerializationMethod.JSON,
                 keep_inputs: bool = True,
                 topics: Optional[Iterable] = None,
                 value_server_threshold: Optional[int] = None,
                 value_server_hostname: Optional[str] = None,
                 value_server_port: Optional[int] = None):
        """
        Args:
            hostname (str): Hostname of the Redis server
            port (int): Port on which to access Redis
            name (int): Name of the MethodServer
            serialization_method (SerializationMethod): Method used to store the input
            keep_inputs (bool): Whether to keep inputs after method results are stored
            topics ([str]): List of topics used when having the client filter different types of tasks
            value_server_threshold (int): Threshold (bytes) to store objects in
                value server. If None, the value server is not used.
            value_server_hostname (str): Optional redis server hostname for the
                value server. If not provided and the value server is used,
                defaults to `hostname`.
            value_server_port (str): Optional redis server port for the
                value server. If not provided and the value server is used,
                defaults to `port`.
        """

        # Store the result communication options
        self.serialization_method = serialization_method
        self.keep_inputs = keep_inputs
        self.value_server_threshold = value_server_threshold

        if self.value_server_threshold is not None:
            if self.serialization_method.lower() != 'pickle':
                raise ValueError('Serialization method must be pickle to use the value server')
            if value_server_hostname is None:
                value_server_hostname = hostname
            if value_server_port is None:
                value_server_port = port
            ps.store.init_store(
                ps.store.STORES.REDIS,
                name='redis',
                hostname=value_server_hostname,
                port=value_server_port
            )
            logger.debug('Initialized value server using Redis server at '
                         f'{value_server_hostname}:{value_server_port}')

        self.value_server_hostname = value_server_hostname
        self.value_server_port = value_server_port

        # Make the queues
        self.outbound = RedisQueue(hostname, port, 'inputs' if name is None else f'{name}_inputs', topics=topics)
        self.inbound = RedisQueue(hostname, port, 'results' if name is None else f'{name}_results', topics=topics)

        # Attempt to connect
        self.outbound.connect()
        self.inbound.connect()

    def send_inputs(self, *input_args: Any, method: str = None,
                    input_kwargs: Optional[Dict[str, Any]] = None,
                    keep_inputs: Optional[bool] = None,
                    topic: str = 'default',
                    task_info: Optional[Dict[str, Any]] = None):
        """Send inputs to be computed

        Args:
            *input_args (Any): Positional arguments to a function
            method (str): Name of the method to run. Optional
            input_kwargs (dict): Any keyword arguments for the function being run
            keep_inputs (bool): Whether to override the
            topic (str): Topic for the queue, which sets the topic for the result.
            task_info (dict): Any information used for task tracking
        """

        # Make fake kwargs, if needed
        if input_kwargs is None:
            input_kwargs = dict()

        # Determine whether to override the default "keep_inputs"
        _keep_inputs = self.keep_inputs
        if keep_inputs is not None:
            _keep_inputs = keep_inputs

        # Create a new Result object
        result = Result(
            (input_args, input_kwargs),
            method=method,
            keep_inputs=_keep_inputs,
            serialization_method=self.serialization_method,
            task_info=task_info,
            value_server_hostname=self.value_server_hostname,
            value_server_port=self.value_server_port,
            value_server_threshold=self.value_server_threshold
        )

        # Push the serialized value to the task server
        result.time_serialize_inputs = result.serialize()
        self.outbound.put(result.json(exclude_unset=True), topic=topic)
        logger.info(f'Client sent a {method} task with topic {topic}')

    def get_result(self, timeout: Optional[int] = None, topic: Optional[str] = None) -> Optional[Result]:
        """Get a value from the MethodServer

        Args:
            timeout (int): Timeout for waiting for a value
            topic (str): What topic of task to wait for. Set to ``None`` to pull all topics
        Returns:
            (Result) Result from a computation, or ``None`` if timeout is met
        """

        # Get a value
        output = self.inbound.get(timeout=timeout, topic=topic)
        logging.debug(f'Received value: {str(output)[:50]}')

        # If None, return because this is a timeout
        if output is None:
            return output
        topic, message = output

        # Parse the value and mark it as complete
        result_obj = Result.parse_raw(message)
        result_obj.time_deserialize_results = result_obj.deserialize()
        result_obj.mark_result_received()

        # Some logging
        logger.info(f'Client received a {result_obj.method} result with topic {topic}')

        return result_obj

    def send_kill_signal(self):
        """Send the kill signal to the task server"""
        self.outbound.put("null")


class TaskServerQueues:
    """Communication wrapper for the task server

    Handles receiving tasks
    """

    def __init__(self, hostname: str, port: int = 6379, name: Optional[str] = None,
                 clean_slate: bool = True, topics: Optional[Iterable[str]] = None):
        """
        Args:
            hostname (str): Hostname of the Redis server
            port (int): Port on which to access Redis
            name (str): Name of the MethodServer
            clean_slate (bool): Whether to flush the queues before launching
            topics ([str]): List of topics used when having the client filter different types of tasks
        """

        # Make the queues
        self.inbound = RedisQueue(hostname, port, 'inputs' if name is None else f'{name}_inputs', topics=topics)
        self.outbound = RedisQueue(hostname, port, 'results' if name is None else f'{name}_results', topics=topics)

        # Attempt to connect
        self.outbound.connect()
        self.inbound.connect()

        # Flush, if desired
        if clean_slate:
            self.outbound.flush()
            self.inbound.flush()

    def get_task(self, timeout: int = None) -> Tuple[str, Result]:
        """Get a task object

        Args:
            timeout (int): Timeout for waiting for a task
        Returns:
            - (str) Topic of the calculation. Used in defining which queue to use to send the results
            - (Result) Task description
        Raises:
            TimeoutException: If the timeout on the queue is reached
            KillSignalException: If the queue receives a kill signal
        """

        # Pull a record off of the queue
        output = self.inbound.get(timeout)

        # Return the kill signal
        if output is None:
            raise TimeoutException('Listening on task queue timed out')
        elif output[1] == "null":
            raise KillSignalException('Kill signal received on task queue')
        topic, message = output
        logger.debug(f'Received a task message with topic {topic} inbound queue')

        # Get the message
        task = Result.parse_raw(message)
        task.mark_input_received()
        return topic, task

    def send_result(self, result: Result, topic: str = 'default'):
        """Send a value to a client

        Args:
            result (Result): Result object to communicate back
            topic (str): Topic of the calculation
        """
        result.mark_result_sent()
        self.outbound.put(result.json(), topic=topic)
