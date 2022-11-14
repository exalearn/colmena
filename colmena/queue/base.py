"""Base classes for queues and related functions"""
import warnings
from abc import abstractmethod
from enum import Enum
from functools import update_wrapper
from typing import Optional, Tuple, Callable, Any, Collection, Union, Dict
import logging

import proxystore as ps

from colmena.models import Result, SerializationMethod

logger = logging.getLogger(__name__)


class QueueRole(str, Enum):
    """Role a queue is used for"""

    ANY = 'any'
    SERVER = 'server'
    CLIENT = 'client'


def _allowed_roles(allowed_role: Collection[QueueRole] = QueueRole.ANY) -> Callable:
    """Decorator which warns users if a call is made on a queue set to the wrong role

    Args:
        allowed_role: Role that this function is allowed to take
    Returns:
        Function wrapper
    """

    def wrapper(func: Optional[Callable[['BaseQueue', Any], Any]] = None):
        def inner_wrapper(queue: 'BaseQueue', *args, **kwargs):
            my_role = queue.role
            if allowed_role != 'ANY' and my_role != allowed_role:
                warnings.warn(f'Called {func.__name__}, which is only allowed for {allowed_role} queues, on a {my_role} queue')
            return func(queue, *args, **kwargs)

        update_wrapper(inner_wrapper, func)
        return inner_wrapper

    return wrapper


class BaseQueue:
    """Base class for a queue used in Colmena.

    Follows the basic ``get`` and ``put`` semantics of most queues,
    with the addition of a "topic" used by Colmena to separate
    task requests or objects used for different purposes."""

    def __init__(self,
                 topics: Collection[str],
                 serialization_method: Union[str, SerializationMethod] = SerializationMethod.JSON,
                 keep_inputs: bool = False,
                 proxystore_name: Optional[Union[str, Dict[str, str]]] = None,
                 proxystore_threshold: Optional[Union[int, Dict[str, int]]] = None):
        """
        Args:
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

        # Store the list of topics and other simple options
        self.topics = set(topics)
        self.topics.add('default')
        self.keep_inputs = keep_inputs
        self.serialization_method = serialization_method
        self.role = QueueRole.ANY

        # Create {topic: proxystore_name} mapping
        self.proxystore_name = {t: None for t in self.topics}
        if isinstance(proxystore_name, str):
            self.proxystore_name = {t: proxystore_name for t in topics}
        elif isinstance(proxystore_name, dict):
            self.proxystore_name.update(proxystore_name)
        elif proxystore_name is not None:
            raise ValueError(f'Unexpected type {type(proxystore_name)} for proxystore_name')

        # Create {topic: proxystore_threshold} mapping
        self.proxystore_threshold = {t: None for t in self.topics}
        if isinstance(proxystore_threshold, int):
            self.proxystore_threshold = {t: proxystore_threshold for t in self.topics}
        elif isinstance(proxystore_threshold, dict):
            self.proxystore_threshold.update(proxystore_threshold)
        elif proxystore_threshold is not None:
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
                    'initialized prior to initializing the Colmena queue.'
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

    def set_role(self, role: Union[QueueRole, str]):
        """Define the role of this queue.

        Controls whether users will be warned away from performing actions that are disallowed by
        a certain queue role, such as sending results from a client or issuing requests from a server"""
        role = QueueRole(role)
        self.role = role

    @_allowed_roles(QueueRole.SERVER)
    def get_result(self, topic: str = 'default', timeout: Optional[float] = None) -> Optional[Result]:
        """Get a value from the MethodServer

        Args:
            topic: Which topic of task to wait for
            timeout: Timeout for waiting for a value
        Returns:
            (Result) Result from a computation
        Raises:
            TimeoutException if the timeout is met
        """

        # Get a value
        message = self._get_result(timeout=timeout, topic=topic)
        logging.debug(f'Received value: {str(message)[:25]}')

        # Parse the value and mark it as complete
        result_obj = Result.parse_raw(message)
        result_obj.time_deserialize_results = result_obj.deserialize()
        result_obj.mark_result_received()

        # Some logging
        logger.info(f'Client received a {result_obj.method} result with topic {topic}')

        return result_obj

    @_allowed_roles(QueueRole.CLIENT)
    def send_inputs(self,
                    *input_args: Any,
                    method: str = None,
                    input_kwargs: Optional[Dict[str, Any]] = None,
                    keep_inputs: Optional[bool] = None,
                    topic: str = 'default',
                    task_info: Optional[Dict[str, Any]] = None):
        """Send a task request

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
        self._send_request(result.json(exclude_unset=True), topic)
        logger.info(f'Client sent a {method} task with topic {topic}')

    @_allowed_roles(QueueRole.SERVER)
    def get_task(self, timeout: float = None) -> Tuple[str, Result]:
        """Get a task object

        Args:
            timeout (float): Timeout for waiting for a task
        Returns:
            - (str) Topic of the calculation. Used in defining which queue to use to send the results
            - (Result) Task description
        Raises:
            TimeoutException: If the timeout on the queue is reached
            KillSignalException: If the queue receives a kill signal
        """

        # Pull a record off of the queue
        topic, message = self._get_request(timeout)
        logger.debug(f'Received a task message with topic {topic} inbound queue')

        # Get the message
        task = Result.parse_raw(message)
        task.mark_input_received()
        return topic, task

    @_allowed_roles(QueueRole.CLIENT)
    def send_kill_signal(self):
        """Send the kill signal to the task server"""
        self._send_request("null", topic='default')

    @_allowed_roles(QueueRole.SERVER)
    def send_result(self, result: Result, topic: str):
        """Send a value to a client

        Args:
            result (Result): Result object to communicate back
            topic (str): Topic of the calculation
        """
        result.mark_result_sent()
        self._send_result(result.json(), topic=topic)

    @abstractmethod
    def _get_request(self, timeout: int = None) -> Tuple[str, str]:
        """Get a task request from the client

        Args:
            timeout: Timeout for the blocking get in seconds
        Returns:
            - (str) Topic of the item
            - (str) Serialized version of the task request object
        Raises:
            (TimeoutException) if the timeout is reached
            (KillSignalException) if a kill signal (the string ``null``) is received
        """
        pass

    @abstractmethod
    def _send_request(self, message: str, topic: str):
        """Push a task request to the task server

        Args:
            message (str): JSON-serialized version of the task request object
            topic (str): Topic of the queue
        """
        pass

    @abstractmethod
    def _get_result(self, topic: str, timeout: int = None) -> str:
        """Get a result from the task server

        Returns:
            A serialized form of the result method
        Raises:
            (TimeoutException) if the timeout is reached
        """
        pass

    @abstractmethod
    def _send_result(self, message: str, topic: str):
        """Push a result object from task server to thinker

        Args:
            message (str): Serialized version of the task request object
            topic (str): Topic of the queue
        """

    @abstractmethod
    def flush(self):
        """Remove all existing results from the queues"""
        pass
