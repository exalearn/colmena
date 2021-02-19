from __future__ import annotations

import logging
import os
import redis
import warnings

from collections import OrderedDict
from typing import Any, Optional, Union

from colmena import value_server
from colmena.models import SerializationMethod


logger = logging.getLogger(__name__)

LRU_CACHE_SIZE = 16
VALUE_SERVER_HOST_ENV_VAR = 'COLMENA_VALUE_SERVER_HOST'
VALUE_SERVER_PORT_ENV_VAR = 'COLMENA_VALUE_SERVER_PORT'


class LRUCache:
    """Simple LRU Cache"""
    def __init__(self, maxsize: int = 16) -> None:
        """
        Args:
            maxsize (int): maximum number of value to cache
        """
        self.maxsize = maxsize
        self.cache = OrderedDict()

    def exists(self, key: Any) -> bool:
        """Check if key is cached"""
        return key in self.cache

    def get(self, key: Any, default: Any = None) -> Any:
        """Get value for key if it exists else returns `default`"""
        if self.exists(key):
            # Move to front b/c most recently used
            self.cache.move_to_end(key, last=False)
            return self.cache[key]
        else:
            return default

    def set(self, key: Any, value: Any) -> None:
        """Set key to value"""
        if len(self.cache) >= self.maxsize:
            self.cache.popitem()
        self.cache[key] = value


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
        self.cache = LRUCache(LRU_CACHE_SIZE)

    def exists(self, key: str) -> bool:
        """Check if key exists

        Args:
            key (str)

        Returns:
            bool
        """
        return self.redis_client.exists(key)

    def is_cached(self, key: str) -> bool:
        """Check if key is cached locally

        Args:
            key (str)

        Returns:
            bool
        """
        return self.cache.exists(key)

    def get(self,
            key: str,
            serialization_method: Union[str, SerializationMethod] = SerializationMethod.PICKLE
    ) -> Optional[object]:
        """Get object by key from value server

        Args:
            key (str)
            serialization_method (SerializationMethod): serialization method
                to use for deserializing object

        Returns:
            deserialized object corresponding to key or None if key does not
            exist
        """
        if self.is_cached(key):
            return self.cache.get(key)
        value = self.redis_client.get(key)
        if value is not None:
            value = SerializationMethod.deserialize(serialization_method, value)
            self.cache.set(key, value)
            return value
        return None

    def put(self,
            key: str,
            obj: Any,
            serialization_method: Union[str, SerializationMethod] = SerializationMethod.PICKLE
    ) -> None:
        """Put object in value server

        Args:
            key (str)
            obj (object)
            serialization_method (SerializationMethod): serialization method
                to use for serializing object before putting in value server
        """
        if self.exists(key):
            warnings.warn('Object with key={} is already in the value server')
            return

        value = SerializationMethod.serialize(serialization_method, obj)
        self.redis_client.set(key, value)


def init_value_server(hostname: Optional[str] = None,
                      port: Optional[int] = None) -> None:
    """Attempt to establish a Redis client connection to the value server

    Attempt to initialize the global variable `server` to a `ValueServer`
    instance using the Redis server hostname and port that are provided as
    arguments or via the environment variables defined by
    `VALUE_SERVER_HOST_ENV_VAR` and `VALUE_SERVER_PORT_ENV_VAR`. If the hostname
    and port are not provided via arguments or environment variables, we
    assume the value server is not being used.

    Args:
        hostname (str): optional Redis server hostname for the value server
        port (int): optional Redis server port for the value server
    """
    if value_server.server is not None:
        return

    if hostname is None:
        if VALUE_SERVER_HOST_ENV_VAR in os.environ:
            hostname = os.environ.get(VALUE_SERVER_HOST_ENV_VAR)
        else:
            raise ValueError('hostname was not passed to init_value_server ',
                             'and the {} environment variable is not '
                             'set'.format(VALUE_SERVER_HOST_ENV_VAR))

    if port is None:
        if VALUE_SERVER_PORT_ENV_VAR in os.environ:
            port = int(os.environ.get(VALUE_SERVER_PORT_ENV_VAR))
        else:
            raise ValueError('port was not passed to init_value_server ',
                             'and the {} environment variable is not '
                             'set'.format(VALUE_SERVER_PORT_ENV_VAR))

    value_server.server = ValueServer(hostname, port)
