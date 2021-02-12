from __future__ import annotations

import copy
import logging
import os
import redis

from typing import Any, Dict, Optional, Union
from wrapt import ObjectProxy

from colmena.models import SerializationMethod


logger = logging.getLogger(__name__)

VALUE_SERVER_HOST_ENV_VAR = 'COLMENA_VALUE_SERVER_HOST'
VALUE_SERVER_PORT_ENV_VAR = 'COLMENA_VALUE_SERVER_PORT'

value_server = None


def _self_get_wrapped(self):
    if self._self_wrapped_obj is None:
        self._self_wrapped_obj = value_server.get(self)
    return self._self_wrapped_obj


#ObjectProxy.__wrapped__ = property(_self_get_wrapped)


class ObjectDelegate(ObjectProxy):
    """
    
    TODO(gpauloski):
      - async_get() function that will asynchrously get the wrapped object
        from the value store. We can let self._self_wrapped_obj be a future
        in this case that _self_get_wrapped() can look for
    """
    def __init__(self,
                 obj: Any,
                 key: Optional[str] = None,
                 serialization_method: Union[str, SerializationMethod] =
                        SerializationMethod.PICKLE
        ) -> None:
        """
        Args:
            value: value being stored in the value server
            key (str): optional key for the value. If the key is not specified
                one will be generated.
            serialization_method (str): serialization method used for
                storing the value in the value server.
        """
        try:
            # This will raise an exception because ObjectProxy.__init__()
            # will attempt to set self.__wrapper__ to obj; however we
            # have manually set __wrapper__ to _self_get_wrapped
            super(ObjectDelegate, self).__init__(obj)
        except AttributeError:
            pass
        self._self_wrapped_obj = obj
        self._self_key = str(id(obj)) if key is None else key
        self._self_serialization_method = serialization_method
        self.__wrapped__ = property(_self_get_wrapped)

        if not value_server.exists(self):
            value_server.put(self)

    def __copy__(self) -> ObjectDelegate:
        return ObjectDelegate(
            copy.copy(self.__wrapped__),
            copy.copy(self._self_key),
            copy.copy(self._self_serialization_method)
        )

    def __deepcopy__(self) -> ObjectDelegate:
        return ObjectDelegate(
            copy.deepcopy(self.__wrapped__),
            copy.deepcopy(self._self_key),
            copy.deepcopy(self._self_serialization_method)
        )

    #def __getstate__(self):
    #    return (self._self_key, self._self_serialization_method)     

    #def __setstate__(self, state) -> None:
    #    self._self_key = state[0]
    #    self._self_serialization_method = state[1]

    def __reduce__(self):
        return (
            ObjectDelegate,
            (None, self._self_key, self._self_serialization_method)
        )

    def __reduce_ex__(self, protocol):
        return (
            ObjectDelegate,
            (None, self._self_key, self._self_serialization_method),
        )

    def reference(self) -> ObjectDelegate:
        obj = self.__deepcopy__()
        obj._self_wrapped_obj = None
        return obj


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

    def exists(self, obj: ObjectDelegate) -> bool:
        return self.redis_client.exists(obj._self_key)

    def get(self, obj: ObjectDelegate) -> Any:
        value = self.redis_client.get(self.key(obj))
        return SerializationMethod.deserialize(
                self.serialization_method(obj), value)
    
    def key(self, obj: ObjectDelegate) -> str:
        return obj._self_key

    def put(self,
            obj: ObjectDelegate,
            key: str = None,
            serialization_method: Optional[Union[str, SerializationMethod]] = None
        ) -> None:

        if key is None:
            key = self.key(obj)
        if serialization_method is None:
            serialization_method = self.serialization_method(obj)

        value = self.wrapped_obj(obj)
        value = SerializationMethod.serialize(serialization_method, value)

        self.redis_client.set(key, value)

    def serialization_method(self, obj: ObjectDelegate) -> SerializationMethod:
        return obj._self_serialization_method

    def wrapped_obj(self, obj: ObjectDelegate) -> Any:
        return obj._self_wrapped_obj


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
    global value_server

    if value_server is not None:
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

    value_server = ValueServer(hostname, port)

