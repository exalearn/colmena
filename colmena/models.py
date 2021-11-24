import json
import logging
import pickle as pkl
import sys
from datetime import datetime
from enum import Enum
from time import perf_counter
from traceback import TracebackException
from typing import Any, Tuple, Dict, Optional, Union

from pydantic import BaseModel, Field

import colmena
import proxystore as ps

logger = logging.getLogger(__name__)


# TODO (wardlt): Merge with FuncX's approach?
class SerializationMethod(str, Enum):
    """Serialization options"""

    JSON = "json"  # Serialize using JSON
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
            method: Method used to serialize the message
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


class FailureInformation(BaseModel):
    """Stores information about a task failure"""

    exception: str = Field(..., description="The exception returned by the failed task")
    traceback: Optional[str] = Field(None, description="Full stack trace for exception, if available")

    @classmethod
    def from_exception(cls, exc: BaseException) -> 'FailureInformation':
        tb = TracebackException.from_exception(exc)
        return cls(exception=repr(exc), traceback="".join(tb.format()))


class Result(BaseModel):
    """A class which describes the inputs and results of the calculations evaluated by the MethodServer

    Each instance of this class stores the inputs and outputs to the function along with some tracking
    information allowing for performance analysis (e.g., time submitted to Queue, time received by client).
    All times are listed as Unix timestamps.

    The Result class also handles serialization of the data to be transmitted over a RedisQueue
    """

    # Core result information
    inputs: Union[Tuple[Tuple[Any, ...], Dict[str, Any]], str] = \
        Field(None, description="Input to a function. Positional and keyword arguments. The `str` data type "
                                "is for internal use and is used when communicating serialized objects.")
    value: Any = Field(None, description="Output of a function")
    method: Optional[str] = Field(None, description="Name of the method to run.")
    success: Optional[bool] = Field(None, description="Whether the task completed successfully")

    # Store task information
    task_info: Optional[Dict[str, Any]] = Field(default_factory=dict, description="Task tracking information to be transmitted "
                                                                                  "along with inputs and results. User provided")
    failure_info: Optional[FailureInformation] = Field(None, description="Messages about task failure. Provided by Task Server")

    # Performance tracking
    time_created: float = Field(None, description="Time this value object was created")
    time_input_received: float = Field(None, description="Time the inputs was received by the task server")
    time_compute_started: float = Field(None, description="Time workflow process began executing a task")
    time_result_sent: float = Field(None, description="Time message was sent from the server")
    time_result_received: float = Field(None, description="Time value was received by client")

    time_running: float = Field(None, description="Runtime of the method, if available")
    time_serialize_inputs: float = Field(None, description="Time required to serialize inputs on client")
    time_deserialize_inputs: float = Field(None, description="Time required to deserialize inputs on worker")
    time_serialize_results: float = Field(None, description="Time required to serialize results on worker")
    time_deserialize_results: float = Field(None, description="Time required to deserialize results on client")
    time_async_resolve_proxies: float = Field(None, description="Time required to scan function inputs and start async resolves of proxies")

    # Serialization options
    serialization_method: SerializationMethod = Field(SerializationMethod.JSON,
                                                      description="Method used to serialize input data")
    keep_inputs: bool = Field(True, description="Whether to keep the inputs with the result object or delete "
                                                "them after the method has completed")
    value_server_hostname: Optional[str] = Field(None, description="Value server hostname")
    value_server_port: Optional[str] = Field(None, description="Value server port")
    value_server_threshold: int = Field(
        None, description="Object size threshold (bytes) at which input/value "
                          "objects are stored in value server before serialization")

    def __init__(self, inputs: Tuple[Tuple[Any], Dict[str, Any]], **kwargs):
        """
        Args:
             inputs (Any, Dict): Inputs to a function. Separated into positional and keyword arguments
        """
        super().__init__(inputs=inputs, **kwargs)

        # Mark "created" only if the value is not already set
        if 'time_created' not in kwargs:
            self.time_created = datetime.now().timestamp()

    @property
    def args(self) -> Tuple[Any]:
        return tuple(self.inputs[0])

    @property
    def kwargs(self) -> Dict[str, Any]:
        return self.inputs[1]

    def mark_result_received(self):
        """Mark that a completed computation was received by a client"""
        self.time_result_received = datetime.now().timestamp()

    def mark_input_received(self):
        """Mark that a task server has received a value"""
        self.time_input_received = datetime.now().timestamp()

    def mark_compute_started(self):
        """Mark that the compute for a method has started"""
        self.time_compute_started = datetime.now().timestamp()

    def mark_result_sent(self):
        """Mark when a result is sent from the task server"""
        self.time_result_sent = datetime.now().timestamp()

    def set_result(self, result: Any, runtime: float = None):
        """Set the value of this computation

        Automatically sets the "time_result_completed" field and, if known, defines the runtime.

        Will delete the inputs to the function if the user specifies ``self.return_inputs == False``.
        Removing the inputs once the result is known can save communication time

        Args:
            result: Result to be stored
            runtime (float): Runtime for the function
        """
        self.value = result
        if not self.keep_inputs:
            self.inputs = ((), {})
        self.time_running = runtime
        self.success = True

    def serialize(self) -> float:
        """Stores the input and value fields as a pickled objects

        Returns:
            (float) Time to serialize
        """
        start_time = perf_counter()
        _value = self.value
        _inputs = self.inputs

        def _serialize_and_proxy(value, evict=False):
            """Helper function for serializing and proxying"""
            # Serialized object before proxying to compare size of serialized
            # object to value server threshold
            value_str = SerializationMethod.serialize(
                self.serialization_method, value
            )

            if (
                    self.value_server_threshold is not None and
                    sys.getsizeof(value_str) >= self.value_server_threshold and
                    not isinstance(value, ps.proxy.Proxy)
            ):
                # Proxy the value. Note: we use the id of the object as the key
                # so calling proxy() on the same object multiple times
                # does not create multiple copies in the value server.
                value_proxy = colmena.proxy.proxy(
                    value_str,
                    key=str(id(value)),
                    is_serialized=True,
                    serialization_method=self.serialization_method,
                    evict=evict
                )
                logger.debug(f'Proxied object of type {type(value)} with id={id(value)}')
                # Serialize the proxy. This is efficient since the proxy is
                # just a reference + metadata about the value
                value_str = SerializationMethod.serialize(
                    self.serialization_method, value_proxy
                )

            return value_str

        try:
            # Each value in *args and **kwargs is serialized independently
            args = tuple(map(_serialize_and_proxy, _inputs[0]))
            kwargs = {k: _serialize_and_proxy(v) for k, v in _inputs[1].items()}
            self.inputs = (args, kwargs)

            # The entire result is serialized as one object. Pass evict=True
            # so the value is evicted from the value server once it is resolved
            # by the thinker.
            if _value is not None:
                self.value = _serialize_and_proxy(_value, evict=True)

            return perf_counter() - start_time
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
