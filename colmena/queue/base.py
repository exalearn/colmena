"""Base classes for queues and related functions"""
import warnings
from abc import abstractmethod
from enum import Enum
from threading import Lock, Event
from typing import Optional, Tuple, Any, Collection, Union, Dict, Set
import logging

import proxystore.store

from colmena.models import Result, SerializationMethod, ResourceRequirements

logger = logging.getLogger(__name__)


class QueueRole(str, Enum):
    """Role a queue is used for"""

    ANY = 'any'
    SERVER = 'server'
    CLIENT = 'client'


class ColmenaQueues:
    """Base class for a queue used in Colmena.

    Follows the basic ``get`` and ``put`` semantics of most queues,
    with the addition of a "topic" used by Colmena to separate
    task requests or objects used for different purposes."""

    def __init__(self,
                 topics: Collection[str],
                 serialization_method: Union[str, SerializationMethod] = SerializationMethod.JSON,
                 keep_inputs: bool = True,
                 proxystore_name: Optional[Union[str, Dict[str, str]]] = None,
                 proxystore_threshold: Optional[Union[int, Dict[str, int]]] = None):
        """
        Args:
            topics: Names of topics that are known for this queue
            serialization_method: Method used to serialize task inputs and results
            keep_inputs: Whether to return task inputs with the result object
            proxystore_name (str, dict): Name of a registered ProxyStore
                `Store` instance. This can be a single name such that the
                corresponding `Store` is used for all topics or a mapping of
                topics to registered `Store` names. If a mapping is provided
                but a topic is not in the mapping, ProxyStore will not be used.
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
            self.proxystore_name = {t: proxystore_name for t in self.topics}
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
            store = proxystore.store.get_store(ps_name)
            if store is None:
                raise ValueError(
                    f'A Store with name "{ps_name}" has not been registered. '
                    'This is likely because the store needs to be '
                    'initialized prior to initializing the Colmena queue.'
                )

        # Log the ProxyStore configuration
        for topic in self.topics:
            ps_name = self.proxystore_name[topic]
            ps_threshold = self.proxystore_threshold[topic]

            if ps_name is None or ps_threshold is None:
                logger.debug(f'Topic {topic} will not use ProxyStore')
            else:
                logger.debug(
                    f'Topic {topic} will use ProxyStore backend "{ps_name}" '
                    f'with a threshold of {ps_threshold} bytes'
                )

        # Create a collection that holds the task which have been sent out, and an event that is triggered
        #  when the last task being sent out hits zero
        self._active_lock = Lock()
        self._active_tasks: Set[str] = set()
        self._all_complete = Event()

    def __getstate__(self):
        state = self.__dict__.copy()
        # We do not send the lock or event over pickle
        state.pop('_active_lock')
        state.pop('_all_complete')
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self._active_lock = Lock()
        self._all_complete = Event()

    def _check_role(self, allowed_role: QueueRole, calling_function: str):
        """Check whether the queue is in an appropriate role for a requested function

        Emits a warning if the queue is in the wrong role

        Args:
            allowed_role: Role to check for
            calling_function: Name of the calling function
        """
        if self.role != QueueRole.ANY and self.role != allowed_role:
            warnings.warn(f'{calling_function} is intended for {allowed_role} not a {self.role}')

    @property
    def active_count(self) -> int:
        """Number of active tasks"""
        return len(self._active_tasks)

    def set_role(self, role: Union[QueueRole, str]):
        """Define the role of this queue.

        Controls whether users will be warned away from performing actions that are disallowed by
        a certain queue role, such as sending results from a client or issuing requests from a server"""
        role = QueueRole(role)
        self.role = role

    def get_result(self, topic: str = 'default', timeout: Optional[float] = None) -> Optional[Result]:
        """Get a completed result

        Args:
            topic: Which topic of task to wait for
            timeout: Timeout for waiting for a value
        Returns:
            (Result) Result from a computation
        Raises:
            TimeoutException if the timeout is met
        """
        self._check_role(QueueRole.CLIENT, 'get_result')

        # Get a value
        message = self._get_result(timeout=timeout, topic=topic)
        logger.debug(f'Received value: {str(message)[:25]}')

        # Parse the value and mark it as complete
        result_obj = Result.parse_raw(message)
        result_obj.time.deserialize_results = result_obj.deserialize()
        result_obj.mark_result_received()

        # Some logging
        logger.info(f'Client received a {result_obj.method} result with topic {topic}')

        # Update the list of active tasks
        with self._active_lock:
            self._active_tasks.discard(result_obj.task_id)
            if len(self._active_tasks) == 0:
                self._all_complete.set()

        return result_obj

    def send_inputs(self,
                    *input_args: Any,
                    method: str = None,
                    input_kwargs: Optional[Dict[str, Any]] = None,
                    keep_inputs: Optional[bool] = None,
                    resources: Optional[Union[ResourceRequirements, dict]] = None,
                    topic: str = 'default',
                    task_info: Optional[Dict[str, Any]] = None) -> str:
        """Send a task request

        Args:
            *input_args (Any): Positional arguments to a function
            method (str): Name of the method to run. Optional
            input_kwargs (dict): Any keyword arguments for the function being run
            keep_inputs (bool): Whether to override the
            topic (str): Topic for the queue, which sets the topic for the result
            resources: Suggestions for how many resources to use for the task
            task_info (dict): Any information used for task tracking
        Returns:
            Task ID
        """
        self._check_role(QueueRole.CLIENT, 'send_inputs')

        # Make sure the queue topic exists
        if topic not in self.topics:
            raise ValueError(f'Unknown topic: {topic}. Known are: {", ".join(self.topics)}')

        # Make fake kwargs, if needed
        if input_kwargs is None:
            input_kwargs = dict()

        # Determine whether to override the default "keep_inputs"
        _keep_inputs = self.keep_inputs
        if keep_inputs is not None:
            _keep_inputs = keep_inputs

        # Gather ProxyStore info if we are using it with this topic
        ps_name = self.proxystore_name[topic]
        ps_threshold = self.proxystore_threshold[topic]
        ps_kwargs = {}
        if ps_name is not None and ps_threshold is not None:
            store = proxystore.store.get_store(ps_name)
            # proxystore_kwargs contains all the information we would need to
            # reconnect to the ProxyStore backend on any worker
            ps_kwargs.update({
                'proxystore_name': ps_name,
                'proxystore_threshold': ps_threshold,
                'proxystore_config': store.config(),
            })

        # Create a new Result object
        result = Result(
            (input_args, input_kwargs),
            method=method,
            keep_inputs=_keep_inputs,
            serialization_method=self.serialization_method,
            task_info=task_info,
            resources=resources or ResourceRequirements(),  # Takes either the user specified or a default,
            topic=topic,
            **ps_kwargs
        )

        # Push the serialized value to the task server
        result.time.serialize_inputs, proxies = result.serialize()
        self._send_request(result.json(exclude_none=True), topic)
        logger.info(f'Client sent a {method} task with topic {topic}. Created {len(proxies)} proxies for input values')

        # Store the task ID in the active list
        with self._active_lock:
            self._active_tasks.add(result.task_id)
            self._all_complete.clear()
        return result.task_id

    def wait_until_done(self, timeout: Optional[float] = None):
        """Wait until all out-going tasks have completed

        Returns:
            Whether the event was set within the timeout
        """
        self._check_role(QueueRole.CLIENT, 'wait_until_done')
        return self._all_complete.wait(timeout=timeout)

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
        self._check_role(QueueRole.SERVER, 'get_task')

        # Pull a record off of the queue
        topic, message = self._get_request(timeout)
        logger.debug(f'Received a task message with topic {topic} inbound queue')

        # Get the message
        task = Result.parse_raw(message)
        task.mark_input_received()
        return topic, task

    def send_kill_signal(self):
        """Send the kill signal to the task server"""
        self._check_role(QueueRole.CLIENT, 'send_kill_signal')
        self._send_request("null", topic='default')

    def send_result(self, result: Result):
        """Send a value to a client

        Args:
            result (Result): Result object to communicate back
            topic (str): Topic of the calculation
        """
        self._check_role(QueueRole.SERVER, 'send_result')
        result.mark_result_sent()
        self._send_result(result.json(), topic=result.topic)

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
