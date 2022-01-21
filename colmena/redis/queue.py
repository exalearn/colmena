"""Wrappers for Redis queues."""

import logging
from collections import defaultdict
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
                     proxystore_name: Optional[Union[str, Dict[str, str]]] = None,
                     proxystore_threshold: Optional[Union[int, Dict[str, int]]] = None)\
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
        proxystore_name (str, dict): Name of ProxyStore backend to use for all
            topics or a mapping of topic to ProxyStore backend for specifying
            backends for certain tasks. If a mapping is provided but a topic is
            not in the mapping, ProxyStore will not be used.
        proxystore_threshold (int, dict): Threshold in bytes for using
            ProxyStore to transfer objects. Optionally can pass a dict
            mapping topics to threshold to use different threshold values
            for different topics. None values in the mapping will exclude
            ProxyStore use with that topic.
    Returns:
        (ClientQueues, TaskServerQueues): Pair of communicators set to use the correct channels
    """

    return (ClientQueues(hostname, port, name, serialization_method, keep_inputs,
                         topics, proxystore_name, proxystore_threshold),
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
                 proxystore_name: Optional[Union[str, Dict[str, str]]] = None,
                 proxystore_threshold: Optional[Union[int, Dict[str, int]]] = None):
        """
        Args:
            hostname (str): Hostname of the Redis server
            port (int): Port on which to access Redis
            name (int): Name of the MethodServer
            serialization_method (SerializationMethod): Method used to store the input
            keep_inputs (bool): Whether to keep inputs after method results are stored
            topics ([str]): List of topics used when having the client filter different types of tasks
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

        # Store the result communication options
        self.serialization_method = serialization_method
        self.keep_inputs = keep_inputs

        # Create {topic: proxystore_name} mapping
        if isinstance(proxystore_name, str):
            self.proxystore_name = defaultdict(lambda: proxystore_name)
        elif isinstance(proxystore_name, dict):
            self.proxystore_name = defaultdict(lambda: None)
            self.proxystore_name.update(proxystore_name)
        elif proxystore_name is None:
            self.proxystore_name = defaultdict(lambda: None)
        else:
            raise ValueError(f'Unexpected type {type(proxystore_name)} for proxystore_name')

        # Create {topic: proxystore_threshold} mapping
        if isinstance(proxystore_threshold, int):
            self.proxystore_threshold = defaultdict(lambda: proxystore_threshold)
        elif isinstance(proxystore_threshold, dict):
            self.proxystore_threshold = defaultdict(lambda: None)
            self.proxystore_threshold.update(proxystore_threshold)
        elif proxystore_threshold is None:
            self.proxystore_threshold = defaultdict(lambda: None)
        else:
            raise ValueError(f'Unexpected type {type(proxystore_threshold)} for proxystore_threshold')

        # Verify that ProxyStore backends exist
        for ps_name in set(self.proxystore_name.values()):
            if ps_name is None:
                continue
            store = ps.store.get_store(ps_name)
            if store is None:
                raise ValueError(
                    f'ProxyStore backend with name "{ps_name}" was not '
                    'found. This is likely because the store needs to be '
                    'initialized prior to initializing the Colmena queues.'
                )

        if topics is None:
            _topics = set()
        else:
            _topics = set(topics)
        _topics.add("default")

        # Log the ProxyStore configuration
        for topic in _topics:
            ps_name = self.proxystore_name[topic]
            ps_threshold = self.proxystore_threshold[topic]

            if ps_name is None or ps_threshold is None:
                logger.debug(f'Topic {topic} will not use ProxyStore')
            else:
                logger.debug(
                    f'Topic {topic} will use ProxyStore backend "{ps_name}" '
                    f'with a threshold of {ps_threshold} bytes'
                )

        # Make the queues
        self.topics = topics
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

        # Gather ProxyStore info if we are using it with this topic
        proxystore_kwargs = {}
        if (
            self.proxystore_name[topic] is not None and
            self.proxystore_threshold[topic] is not None
        ):
            store = ps.store.get_store(self.proxystore_name[topic])
            # proxystore_kwargs contains all the information we would need to
            # reconnect to the ProxyStore backend on any worker
            proxystore_kwargs.update({
                'proxystore_name': self.proxystore_name[topic],
                'proxystore_threshold': self.proxystore_threshold[topic],
                # Pydantic prefers to not have types as attributes so we
                # get the string corresponding to the type of the store we use
                'proxystore_type': ps.store.STORES.get_str_by_type(type(store)),
                'proxystore_kwargs': store.kwargs
            })

        # Create a new Result object
        result = Result(
            (input_args, input_kwargs),
            method=method,
            keep_inputs=_keep_inputs,
            serialization_method=self.serialization_method,
            task_info=task_info,
            **proxystore_kwargs
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
        if self.topics is not None:
            for topic in self.topics:
                self.outbound.put("null", topic=topic)
        else:
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
