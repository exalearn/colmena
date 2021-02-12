from __future__ import annotations

import copy
import logging
import os
import redis

from typing import Any, Dict, Optional, Union
from lazy_object_proxy import Proxy

from colmena.models import SerializationMethod


logger = logging.getLogger(__name__)

VALUE_SERVER_HOST_ENV_VAR = 'COLMENA_VALUE_SERVER_HOST'
VALUE_SERVER_PORT_ENV_VAR = 'COLMENA_VALUE_SERVER_PORT'

value_server = None


class Factory():
    """Factory class for retrieving objects from the value server"""
    def __init__(self,
                 key: str,
                 serialization_method: Union[str, SerializationMethod] = 
                    SerializationMethod.PICKLE
        ):
        """
        Args:
            key (str): key used to retrive object from value server
            serialization_method (SerializationMethod): serialization method
                used to store object in value server
        """
        self.key = key
        self.serialization_method = serialization_method

    def __call__(self):
        """Retrive object from value server
        
        Note: 
            `__call__` is generally only called once by the ObjectProxy
            unless ObjectProxy.reset_proxy() is called.
        """
        return value_server.get(self.key)

    def async_get(self):
        pass


class ObjectProxy(Proxy):
    """Lazy proxy object wrapping a factory function

    An ObjectProxy transparently wraps any arbitrary object via an object
    `Factory`. The factory is callable and returns the true object; however,
    the factory is not called until the proxy is accessed in some way.
    """
    def __init__(self, factory: Factory) -> None:
        """
        Args:
            factory (Factor): factory class that when called will return
                the true object being wrapped
        """
        super(ObjectProxy, self).__init__(factory)

    def __reduce__(self):
        """See `__reduce_ex__`"""
        return ObjectProxy, (self.__factory__,)

    def __reduce_ex__(self, protocol):
        """Helper method for pickling

        Override `Proxy.__reduce_ex__` so that we only pickle the Factory
        and not the object itself to reduce size of the pickle.
        """
        return ObjectProxy, (self.__factory__,)
    
    def async_resolve_proxy(self) -> None:
        raise NotImplementedError()
        #self.__factory__.async_get()
    
    def reset_proxy(self) -> None:
        """Reset wrapped object so that the factory is called on next access"""
        if hasattr(self, '__target__'):
            object.__delattr__(self, '__target__')


def to_proxy(obj: Any,
             key=None,
             serialization_method: Union[str, SerializationMethod] =
                SerializationMethod.PICKLE
    ):
    """Put object in value server and return proxy object

    Args:
        obj (object)
        key (str, optional): key to use for value server
        serialization_method (SerializationMethod): serialization method
    
    Returns:
        ObjectProxy
    """
    if key is None:
        key = str(id(obj))
    value_server.put(obj, key, serialization_method)
    return ObjectProxy(Factory(key, serialization_method))


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

    def exists(self, key: str) -> bool:
        """Check if key exists

        Args:
            key (str)

        Returns:
            bool
        """
        return self.redis_client.exists(key)

    def get(self,
            key: str,
            serialization_method: Union[str, SerializationMethod] =
                SerializationMethod.PICKLE
        ) -> Any:
        """Get object by key from value server

        Args:
            key (str)
            serialization_method (SerializationMethod): serialization method
                to use for deserializing object

        Returns:
            deserialized object corresponding to key
        """
        value = self.redis_client.get(key)
        return SerializationMethod.deserialize(serialization_method, value)

    def put(self,
            obj: Any,
            key: str,
            serialization_method: Union[str, SerializationMethod] =
                SerializationMethod.PICKLE
        ) -> None:
        """Put object in value server

        Args:
            obj (object)
            key (str)
            serialization_method (SerializationMethod): serialization method
                to use for serializing object before putting in value server
        """
        value = SerializationMethod.serialize(serialization_method, obj)
        self.redis_client.set(key, value)


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

