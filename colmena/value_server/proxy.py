from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from lazy_object_proxy import Proxy
from typing import Any, Optional, Union

import colmena.value_server as value_server
from colmena.models import SerializationMethod

default_pool = ThreadPoolExecutor()


class Factory():
    """Factory class for retrieving objects from the value server"""
    def __init__(self,
                 key: str,
                 serialization_method: Union[str, SerializationMethod] = SerializationMethod.PICKLE
    ) -> None:
        """
        Args:
            key (str): key used to retrive object from value server
            serialization_method (SerializationMethod): serialization method
                used to store object in value server
        """
        self.key = key
        self.serialization_method = serialization_method
        self.async_get_future = None

    def __call__(self):
        """Retrive object from value server
        Note:
            `__call__` is generally only called once by the ObjectProxy
            unless ObjectProxy.reset_proxy() is called.
        """
        if value_server.server is None:
            value_server.init_value_server()

        if self.async_get_future is not None:
            return self.async_get_future.result()

        return value_server.server.get(self.key)

    def async_get(self):
        """Asynchrously get the object for the next call to `__call__`"""
        if value_server.server is None:
            value_server.init_value_server()

        self.async_get_future = default_pool.submit(
                value_server.server.get, self.key)


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
        self.__factory__.async_get()

    def reset_proxy(self) -> None:
        """Reset wrapped object so that the factory is called on next access"""
        if hasattr(self, '__target__'):
            object.__delattr__(self, '__target__')


def to_proxy(obj: Any,
             key=None,
             serialization_method: Union[str, SerializationMethod] = SerializationMethod.PICKLE
) -> None:
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
    if not value_server.server.exists(key):
        value_server.server.put(key, obj, serialization_method)
    return ObjectProxy(Factory(key, serialization_method))

def async_get_args(args: Union[object, list, tuple, dict]) -> None:
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
    def async_get_if_proxy(obj: Any) -> Any:
        if isinstance(obj, ObjectProxy):
            obj.async_resolve_proxy()

    if isinstance(args, list) or isinstance(args, tuple):
        map(async_get_if_proxy, args)

    if isinstance(args, dict):
        map(async_get_if_proxy, args.values())
