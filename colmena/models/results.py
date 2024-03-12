"""Data models for requests for and results from compuations"""
import json
import logging
import sys
from math import nan
import pickle as pkl
from datetime import datetime
from enum import Enum
from functools import partial
from time import perf_counter
from traceback import TracebackException
from typing import Any, Tuple, Dict, Optional, Union, List, Sequence
from uuid import uuid4

from pydantic import BaseModel, Field, Extra
from proxystore.proxy import Proxy

from colmena.proxy import get_store, store_proxy_stats
from colmena.proxy import proxy_json_encoder

logger = logging.getLogger(__name__)


class SerializationMethod(str, Enum):
    """Serialization options"""

    JSON = "json"  # Serialize using JSONf
    PICKLE = "pickle"  # Pickle serialization

    @staticmethod
    def serialize(method: 'SerializationMethod', data: Any) -> str:
        """Serialize an object using a specified method

        Args:
            method: Method used to serialize the object
            data: Object to be serialized
        Returns:
            Serialized data
        """

        if method == "json":
            return json.dumps(data)
        elif method == "pickle":
            return pkl.dumps(data).hex()
        else:
            raise NotImplementedError(f'Method {method} not yet implemented')

    @staticmethod
    def deserialize(method: 'SerializationMethod', message: str) -> Any:
        """Deserialize an object

        Args:
            method: Method used to serialize
            message: Message to deserialize
        Returns:
            Result object
        """

        if method == "json":
            return json.loads(message)
        elif method == "pickle":
            return pkl.loads(bytes.fromhex(message))
        else:
            raise NotImplementedError(f'Method {method} not yet implemented')


def _serialized_str_to_bytes_shim(
        s: str,
        method: Union[str, SerializationMethod],
) -> bytes:
    """Shim between Colmena serialized objects and bytes.

    Colmena's serialization mechanisms produce strings but ProxyStore
    serializes to bytes, so this shim takes an object serialized by Colmena
    and converts it to bytes.

    Args:
        s: Serialized string object
        method: Serialization method used to produce s

    Returns:
        bytes representation of s
    """
    if method == "json":
        return s.encode('utf-8')
    elif method == "pickle":
        # In this case the conversion goes from obj > bytes > str > bytes
        # which results in an unnecessary conversion to a string but this is
        # an unavoidable side effect of converting between the Colmena
        # and ProxyStore serialization formats.
        return bytes.fromhex(s)
    else:
        raise NotImplementedError(f'Method {method} not yet implemented')


def _serialized_bytes_to_obj_wrapper(
        b: str,
        method: Union[str, SerializationMethod],
) -> Any:
    """Wrapper which converts bytes to strings before deserializing.

    Args:
        b: Byte string of serialized object
        method: Serialization method used to produce b

    Returns:
        Deserialized object
    """
    if method == "json":
        s = b.decode('utf-8')
    elif method == "pickle":
        # In this case the conversion goes from bytes > str > bytes > obj
        # which results in an unecessary conversion to a string but this is
        # an unavoidable side effect of converting between the Colmena
        # and ProxyStore serialization formats.
        s = b.hex()
    else:
        raise NotImplementedError(f'Method {method} not yet implemented')

    return SerializationMethod.deserialize(method, s)


class FailureInformation(BaseModel):
    """Stores information about a task failure"""

    exception: str = Field(..., description="The exception returned by the failed task")
    traceback: Optional[str] = Field(None, description="Full stack trace for exception, if available")

    @classmethod
    def from_exception(cls, exc: BaseException) -> 'FailureInformation':
        tb = TracebackException.from_exception(exc)
        return cls(exception=repr(exc), traceback="".join(tb.format()))


class WorkerInformation(BaseModel, extra=Extra.allow):
    """Information about the worker that executed this task"""

    hostname: Optional[str] = Field(None, description='Hostname of the worker who executed this task')


class ResourceRequirements(BaseModel):
    """Resource requirements for tasks. Used by some Colmena backends to allocate resources to the task

    Follows the naming conventions of
    `RADICAL-Pilot <https://radicalpilot.readthedocs.io/en/stable/apidoc.html#taskdescription>`_.
    """

    # Defining how we use CPU resources
    node_count: int = Field(1, description='Total number of nodes to use for the task')
    cpu_processes: int = Field(1, description='Total number of MPI ranks per node')
    cpu_threads: int = Field(1, description='Number of threads per process')

    @property
    def total_ranks(self) -> int:
        """Total number of MPI ranks"""
        return self.node_count * self.cpu_processes


class Timestamps(BaseModel):
    """A class which records the system times at which key events in a task occurred

    All should be in UTC.
    """

    created: float = Field(description="Time this value object was created",
                           default_factory=lambda: datetime.now().timestamp())
    input_received: float = Field(nan, description="Time the inputs was received by the task server")
    compute_started: float = Field(nan, description="Time workflow process began executing a task")
    compute_ended: float = Field(nan, description="Time workflow process finished executing a task")
    result_sent: float = Field(nan, description="Time message was sent from the server")
    result_received: float = Field(nan, description="Time value was received by client")
    start_task_submission: float = Field(nan, description="Time marking the start of the task submission to workflow engine")
    task_received: float = Field(nan, description="Time task result received from workflow engine")


class TimeSpans(BaseModel):
    """Amount of time elapsed between major events

    All are recorded in seconds
    """

    running: float = Field(nan, description="Runtime of the method, if available")
    serialize_inputs: float = Field(nan, description="Time required to serialize inputs on client")
    deserialize_inputs: float = Field(nan, description="Time required to deserialize inputs on worker")
    serialize_results: float = Field(nan, description="Time required to serialize results on worker")
    deserialize_results: float = Field(nan, description="Time required to deserialize results on client")
    async_resolve_proxies: float = Field(nan, description="Time required to start async resolves of proxies")
    proxy: Dict[str, Dict[str, dict]] = Field(default_factory=dict,
                                              description='Timings related to resolving ProxyStore proxies on the compute worker')

    additional: Dict[str, float] = Field(default_factory=dict,
                                         description="Additional timings reported by a task server")


class Result(BaseModel):
    """A class which describes the inputs and results of the calculations evaluated by the MethodServer

    Each instance of this class stores the inputs and outputs to the function along with some tracking
    information allowing for performance analysis (e.g., time submitted to Queue, time received by client).
    All times are listed as Unix timestamps.

    The Result class also handles serialization of the data to be transmitted over a RedisQueue
    """

    # Core result information
    task_id: str = Field(default_factory=lambda: str(uuid4()), description='Unique identifier for each task')
    inputs: Union[Tuple[Tuple[Any, ...], Dict[str, Any]], str] = \
        Field(None, description="Input to a function. Positional and keyword arguments. The `str` data type "
                                "is for internal use and is used when communicating serialized objects.")
    value: Any = Field(None, description="Output of a function")
    method: Optional[str] = Field(None, description="Name of the method to run.")
    success: Optional[bool] = Field(None, description="Whether the task completed successfully")
    complete: Optional[bool] = Field(None, description="Whether this result is the last for a task instead of an intermediate result")

    # Store task information
    task_info: Optional[Dict[str, Any]] = Field(default_factory=dict,
                                                description="Task tracking information to be transmitted "
                                                            "along with inputs and results. User provided")
    resources: ResourceRequirements = Field(default_factory=ResourceRequirements, help='List of the resources required for a task, if desired')
    failure_info: Optional[FailureInformation] = Field(None, description="Messages about task failure. Provided by Task Server")
    worker_info: Optional[WorkerInformation] = Field(None, description="Information about the worker which executed a task. Provided by Task Server")
    message_sizes: Dict[str, int] = Field(default_factory=dict, description='Sizes of the inputs and results in bytes')

    # Timings
    timestamp: Timestamps = Field(default_factory=Timestamps, help='Times at which major events occurred')
    time: TimeSpans = Field(default_factory=TimeSpans, help='Elapsed time between major events')

    # Serialization options
    serialization_method: SerializationMethod = Field(SerializationMethod.JSON,
                                                      description="Method used to serialize input data")
    keep_inputs: bool = Field(True, description="Whether to keep the inputs with the result object or delete "
                                                "them after the method has completed")
    proxystore_name: Optional[str] = Field(None, description="Name of ProxyStore backend you use for transferring large objects")
    proxystore_config: Optional[Dict] = Field(None, description="ProxyStore backend configuration")
    proxystore_threshold: Optional[int] = Field(None,
                                                description="Proxy all input/output objects larger than this threshold in bytes")

    # Task routing information
    topic: Optional[str] = Field(None, description='Label used to group results in queue between Thinker and Task Server')

    def __init__(self, inputs: Tuple[Tuple[Any], Dict[str, Any]], **kwargs):
        """
        Args:
             inputs (Any, Dict): Inputs to a function. Separated into positional and keyword arguments
        """
        super().__init__(inputs=inputs, **kwargs)

    @classmethod
    def from_args_and_kwargs(cls, fn_args: Sequence[Any], fn_kwargs: Dict[str, Any] = None, **kwargs):
        """Create a result object form a the arguments and kwargs for the function

        Keyword arguments to this function are passed to the initializer for `Result`.

        Args:
            fn_args: Positional arguments to the function
            fn_kwargs: Keyword arguments to the function
        Returns:
            Result object with the results object
        """
        return cls(inputs=(tuple(fn_args), fn_kwargs or dict()), **kwargs)

    @property
    def args(self) -> Tuple[Any]:
        return tuple(self.inputs[0])

    @property
    def kwargs(self) -> Dict[str, Any]:
        return self.inputs[1]

    def json(self, **kwargs: Dict[str, Any]) -> str:
        """Override json encoder to use a custom encoder with proxy support"""
        if 'exclude' in kwargs:
            # Make a shallow copy of the user passed excludes
            user_exclude = kwargs['exclude'].copy()
            if isinstance(kwargs['exclude'], dict):
                kwargs['exclude'].update({'inputs': True, 'value': True})
            if isinstance(kwargs['exclude'], set):
                kwargs['exclude'].update({'inputs', 'value'})
            else:
                raise ValueError(
                    f'Unsupported type {type(kwargs["exclude"])} for argument "exclude". Expected set or dict')
        else:
            user_exclude = set()
            kwargs['exclude'] = {'inputs', 'value'}

        # Use pydantic's encoding for everything except `inputs` and `values`
        data = super().dict(**kwargs)

        # Add inputs/values back to data unless the user excluded them
        if isinstance(user_exclude, set):
            if 'inputs' not in user_exclude:
                data['inputs'] = self.inputs
            if 'value' not in user_exclude:
                data['value'] = self.value
        elif isinstance(user_exclude, dict):
            if not user_exclude['inputs']:
                data['inputs'] = self.inputs
            if not user_exclude['value']:
                data['value'] = self.value

        # Jsonify with custom proxy encoder
        return json.dumps(data, default=proxy_json_encoder)

    def mark_result_received(self):
        """Mark that a completed computation was received by a client"""
        self.timestamp.result_received = datetime.now().timestamp()

    def mark_input_received(self):
        """Mark that a task server has received a value"""
        self.timestamp.input_received = datetime.now().timestamp()

    def mark_compute_started(self):
        """Mark that the compute for a method has started"""
        self.timestamp.compute_started = datetime.now().timestamp()

    def mark_result_sent(self):
        """Mark when a result is sent from the task server"""
        self.timestamp.result_sent = datetime.now().timestamp()

    def mark_start_task_submission(self):
        """Mark when the Task Server submits a task to the engine"""
        self.timestamp.start_task_submission = datetime.now().timestamp()

    def mark_task_received(self):
        """Mark when the Task Server receives the task from the engine"""
        self.timestamp.task_received = datetime.now().timestamp()

    def mark_compute_ended(self):
        """Mark when the task finished executing"""
        self.timestamp.compute_ended = datetime.now().timestamp()

    def set_result(self, result: Any, runtime: float = nan, intermediate: bool = False):
        """Set the value of this computation

        Automatically sets the "time_result_completed" field and, if known, defines the runtime.

        Will delete the inputs to the function if the user specifies ``self.return_inputs == False``.
        Removing the inputs once the result is known can save communication time

        Args:
            result: Result to be stored
            runtime: Runtime for the function
            intermediate: If this result is not the final one in a workflow
        """
        self.value = result
        if not self.keep_inputs:
            self.inputs = ((), {})
        self.time.running = runtime
        self.success = True
        self.complete = not intermediate

    def serialize(self) -> Tuple[float, List[Proxy]]:
        """Stores the input and value fields as a pickled objects

        Returns:
            - (float) Time to serialize
            - List of any proxies that were created
        """
        start_time = perf_counter()
        _value = self.value
        _inputs = self.inputs
        proxies = []

        if self.proxystore_name is not None:
            store = get_store(name=self.proxystore_name, config=self.proxystore_config)
        else:
            store = None

        def _serialize_and_proxy(value, evict=False) -> Tuple[str, int]:
            """Helper function for serializing and proxying

            Args:
                value: Value to be serialized
                evict: Whether to evict from proxy store on reading
            Returns:
                - Serialized representation, which is either the object or a proxy to it
                - Size of the serialized object (not the size of the proxy)
            """
            # Serialized object before proxying to compare size of serialized
            # object to value server threshold. Using sys.getsizeof would be
            # faster but sys.getsizeof does not account for the memory
            # consumption of objects that value refers to
            value_str = SerializationMethod.serialize(
                self.serialization_method, value
            )
            value_size = sys.getsizeof(value_str)

            if (
                    store is not None and
                    self.proxystore_threshold is not None and
                    not isinstance(value, Proxy) and
                    value_size >= self.proxystore_threshold
            ):
                # Override ProxyStore's default serialization with these shims
                # to Colmena's serialization mechanisms. This avoids value
                # being serialized twice: once to get the size of the
                # serialized object and once by proxy().
                deserializer = partial(
                    _serialized_bytes_to_obj_wrapper,
                    method=self.serialization_method,
                )
                serializer = partial(
                    _serialized_str_to_bytes_shim,
                    method=self.serialization_method,
                )

                value_proxy = store.proxy(
                    value_str,
                    evict=evict,
                    deserializer=deserializer,
                    serializer=serializer,
                )
                logger.debug(f'Proxied object of type {type(value)} with id={id(value)}')
                proxies.append(value_proxy)

                # Update the statistics
                store_proxy_stats(value_proxy, self.time.proxy)

                # Serialize the proxy with Colmena's utilities. This is
                # efficient since the proxy is just a reference and metadata
                value_str = SerializationMethod.serialize(
                    self.serialization_method, value_proxy
                )

            return value_str, value_size

        try:
            # Each value in *args and **kwargs is serialized independently
            if len(_inputs[0]) > 0:
                args, args_sizes = zip(*map(_serialize_and_proxy, _inputs[0]))
            else:
                args = args_sizes = []
            kwargs = {}
            kwarg_sizes = []
            for k, v in _inputs[1].items():
                _kwarg_str, _kwarg_size = _serialize_and_proxy(v)
                kwargs[k] = _kwarg_str
                kwarg_sizes.append(_kwarg_size)
            self.inputs = (args, kwargs)

            # Store the size of the input if not already there
            if 'inputs' not in self.message_sizes:
                self.message_sizes['inputs'] = sum(args_sizes) + sum(kwarg_sizes)

            # The entire result is serialized as one object. Pass evict=True
            # so the value is evicted from the value server once it is resolved
            # by the thinker.
            if _value is not None:
                self.value, size = _serialize_and_proxy(_value, evict=True)
                if 'value' not in self.message_sizes:
                    self.message_sizes['value'] = size

            return perf_counter() - start_time, proxies
        except Exception as e:
            # Put the original values back
            self.inputs = _inputs
            self.value = _value
            raise e

    def deserialize(self) -> float:
        """De-serialize the input and value fields

        Returns:
            (float) The time required to deserialize
        """
        # Check that the data is actually a string
        start_time = perf_counter()
        _value = self.value
        _inputs = self.inputs

        def _deserialize(value):
            if not isinstance(value, str):
                return value
            return SerializationMethod.deserialize(self.serialization_method, value)

        if isinstance(_inputs, str):
            _inputs = SerializationMethod.deserialize(self.serialization_method, _inputs)

        try:
            # Deserialize each value in *args and **kwargs
            args = tuple(map(_deserialize, _inputs[0]))
            kwargs = {k: _deserialize(v) for k, v in _inputs[1].items()}
            self.inputs = (args, kwargs)

            # Deserialize result if it exists
            if _value is not None:
                self.value = _deserialize(_value)

            return perf_counter() - start_time
        except Exception as e:
            # Put the original values back
            self.inputs = _inputs
            self.value = _value
            raise e
