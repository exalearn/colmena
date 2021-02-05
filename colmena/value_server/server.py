import logging
import os
import redis

from typing import Any, Optional, Union

from colmena.models import SerializationMethod


logger = logging.getLogger(__name__)

VALUE_SERVER_HOST_ENV_VAR = 'COLMENA_VALUE_SERVER_HOST'
VALUE_SERVER_PORT_ENV_VAR = 'COLMENA_VALUE_SERVER_PORT'

_server = None


class ValueServerReference:
    """Reference to a value in the value server

    Manages metadata about an object in the value server and acts as an
    indicator to Colmena that in input/output to a function is found in the
    value server. ValueServerReference objects are created by
    `ValueServer.put()` and can be passed to `ValueServer.get()` or
    `dereference()` to get the value back.

    Colmena will automatically parse inputs to a `target_function` for
    `ValueServerReference` instances and use the references to get the true
    value that needs to be passed to the `target_function`. The dereferencing
    is performed in `run_and_record_timing()`, the wrapper for functions
    executed on workers.
    """
    def __init__(self,
                 value: Any,
                 key: Optional[str] = None,
                 serialization_method: Union[str, SerializationMethod] = SerializationMethod.PICKLE):
        """
        Args:
            value: value being stored in the value server
            key (str): optional key for the value. If the key is not specified
                one will be generated.
            serialization_method (str): serialization method used for
                storing the value in the value server.
        """
        # TODO(gpauloski): in the future we want a more robust way of getting
        # a unique key for an object to use at its value server reference.
        self.key = str(id(value)) if key is None else key
        self.serialization_method = serialization_method


class ValueServer:
    """Wrapper around a Redis Client for interacting with the value server"""
    def __init__(self, hostname: str, port: int):
        """
        Args:
            hostname (str): hostname of Redis server
            port (int): port of Redis server
        """
        self.redis_client = redis.StrictRedis(
            host=hostname, port=port, decode_responses=True)

    def get(self, ref: ValueServerReference) -> Any:
        value = self.redis_client.get(ref.key)
        return SerializationMethod.deserialize(ref.serialization_method, value)

    def put(self, value: Any,
            key: str = None,
            serialization_method: Union[str, SerializationMethod] = SerializationMethod.PICKLE
            ) -> ValueServerReference:
        ref = ValueServerReference(value, key, serialization_method)
        value = SerializationMethod.serialize(ref.serialization_method, value)
        self.redis_client.set(ref.key, value)
        return ref


def init_value_server(hostname: Optional[str] = None,
                      port: Optional[int] = None) -> None:
    """Attempt to establish a Redis client connection to the value server

    Attempt to initialize the global variable `_server` to a `ValueServer`
    instance using the Redis server hostname and port that are provided as
    arguments or via the environment variables defined by
    `VALUE_SERVER_HOST_ENV_VAR` and `VALUE_SERVER_PORT_ENV_VAR`. If the hostname
    and port are not provided via arguments or environment variables, we
    assume the value server is not being used.

    Args:
        hostname (str): optional Redis server hostname for the value server
        port (int): optional Redis server port for the value server
    """
    global _server

    if _server is not None:
        return

    if hostname is None:
        if VALUE_SERVER_HOST_ENV_VAR in os.environ:
            hostname = os.environ.get(VALUE_SERVER_HOST_ENV_VAR)
        else:
            # Note: for now we just assume if the env var is not set that is
            # because we are not using the value server
            return

    if port is None:
        if VALUE_SERVER_PORT_ENV_VAR in os.environ:
            port = int(os.environ.get(VALUE_SERVER_PORT_ENV_VAR))
        else:
            return

    _server = ValueServer(hostname, port)


def dereference(possible_references: Union[object, list, tuple, dict]) -> Any:
    """Dereference ValueServerReference objects

    Scans all arguments for any ValueServerReference objects and retrieves
    the corresponding values from the value server. If the value server
    is not available, the arguments are returned as is.

    Args:
        possible_references (object, list, tuple, dict): possible object or
            iterable of objects that may be ValueServerReference objects
    Returns:
        An object or iterable of objects in the same format as the arguments
        with all ValueServerReference objects replaced with the corresponding
        values from the value server
    """
    init_value_server()

    if _server is None:
        return possible_references

    def get_if_reference(obj: Any) -> Any:
        if isinstance(obj, ValueServerReference):
            return _server.get(obj)
        return obj

    if isinstance(possible_references, list):
        return [get_if_reference(obj) for obj in possible_references]

    if isinstance(possible_references, tuple):
        return tuple([get_if_reference(obj) for obj in possible_references])

    if isinstance(possible_references, dict):
        return {key: get_if_reference(value)
                for key, value in possible_references.items()}

    return get_if_reference(possible_references)


def put(value: Any,
        key: Optional[str] = None,
        serialization_method: Union[str, SerializationMethod] = SerializationMethod.PICKLE
        ) -> ValueServerReference:
    """Put an object in the value server

    Args:
        value: object to place in value server
        key (str): optionally specify the key to use with this object
        serialization_method (str): serialization method to use
    Returns:
        ValueServerReference: reference object that can be given to
        `dereference()` to get the original value back
    Raises:
        RuntimeError if the value server is not (or unable to be) initialized
    """

    init_value_server()

    if _server is None:
        raise RuntimeError('Value server is not initialized')

    return _server.put(value, key, serialization_method)
