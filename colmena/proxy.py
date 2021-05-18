"""Utilities for interacting with ProxyStore"""
import proxystore as ps

from typing import Any, Optional, Union

from colmena.models import SerializationMethod


class ColmenaSerializationFactory(ps.store.redis.RedisFactory):
    """Custom Factory for using Colmena serialization utilities"""
    def __init__(self,
                 key: str,
                 name: str,
                 hostname: str,
                 port: int,
                 serialization_method: Union[str, SerializationMethod] = SerializationMethod.PICKLE,
                 **kwargs) -> None:
        """Init ColmenaSerialization Factory

        Args:
            key (str): key corresponding to object in Redis.
            name (str): name of store to retrive objects from.
            hostname (str): hostname of Redis server containing object.
            port (int): port of Redis server containing object.
            serialization_method (str): Colmena serialization method to use
                for deserializing the object when resolved from Redis.
            kwargs: keyword arguments to pass to the RedisFactory.
        """
        self.serialization_method = serialization_method
        self.kwargs = kwargs
        super(ColmenaSerializationFactory, self).__init__(
            key, name, hostname, port, **kwargs
        )

    def __getnewargs_ex__(self):
        """Helper method for pickling

        Note:
            We override default pickling behavior because a Factory may contain
            a Future if it is being asynchronously resolved and Futures cannot
            be pickled.
        """
        return (self.key, self.name, self.hostname, self.port), {
            'serialization_method': self.serialization_method,
            **self.kwargs
        }

    def resolve(self) -> Any:
        obj_str = super(ColmenaSerializationFactory, self).resolve()
        return SerializationMethod.deserialize(self.serialization_method, obj_str)


def proxy(obj: Any,
          key: Optional[str] = None,
          is_serialized: bool = False,
          serialization_method: Union[str, SerializationMethod] = SerializationMethod.PICKLE,
          **kwargs) -> ps.proxy.Proxy:
    """Place object in Value Server and return Proxy

    Args:
        obj: object to be placed in Value Server and proxied.
        key (str): optional key to associate with object. By default, ProxyStore
            will create a key for the object (default: None).
        is_serialized (bool): True if obj is already serialized (default: False).
        serialization_method (str): serialization method to use for the object
            (default: SerializationMethod.PICKLE).
        kwargs (dict): keyword arguments to pass to ProxyStore.store.redis.RedisStore.proxy().

    Returns:
        ps.proxy.Proxy
    """
    store = ps.store.get_store('redis')
    if not is_serialized:
        obj = SerializationMethod.serialize(serialization_method, obj)
    return store.proxy(
        obj,
        key,
        serialize=False,  # Do not use ProxyStore serialization utilities
        serialization_method=serialization_method,
        factory=ColmenaSerializationFactory,
        **kwargs
    )


def resolve_proxies_async(args: Union[object, list, tuple, dict]) -> None:
    """Begin asynchronously resolving all proxies in input

    Scan inputs for instances of `Proxy` and begin asynchronously resolving.
    This is useful if you have one or more proxies that will be needed soon
    so the underlying objects can be asynchronously resolved to reduce the
    cost of the first access to the proxy.

    Args:
        args (object, list, tuple, dict): possible object or
            iterable of objects that may be ObjectProxy instances
    """
    def resolve_async_if_proxy(obj: Any) -> None:
        if isinstance(obj, ps.proxy.Proxy):
            ps.proxy.resolve_async(obj)

    if isinstance(args, ps.proxy.Proxy):
        resolve_async_if_proxy(args)
    elif isinstance(args, list) or isinstance(args, tuple):
        for x in args:
            resolve_async_if_proxy(x)
    elif isinstance(args, dict):
        for x in args:
            resolve_async_if_proxy(args[x])
