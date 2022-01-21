"""Utilities for interacting with ProxyStore"""
import warnings
import proxystore as ps

from typing import Any, Union


class ProxyJSONSerializationWarning(Warning):
    pass


def proxy_json_encoder(proxy: ps.proxy.Proxy) -> Any:
    """Custom encoder function for proxies

    Proxy objects are not JSON serializable so this function, when passed to
    `json.dumps()`, will attempt to JSON serialize the wrapped object. If the
    proxy is not resolved, a warning will be raised for the user and the
    proxy will be replaced with a placeholder string for the proxy. This
    1) prevents JSON serialization from failing and 2) avoid unintended
    resolutions of proxies that may invoke expensive communication operations
    without the user being aware.

    Usage:
        >>> # With JSON dump/dumps
        >>> json.dumps(json_obj_containing_proxy, default=proxy_json_encoder)
        >>> # With Pydantic
        >>> my_basemodel_instance.json(encoder=proxy_json_encoder)

    Args:
        proxy (Proxy): proxy to convert to JSON encodable object

    Returns:
        The object wrapped by the proxy if the proxy has already been resolved
        otherwise a placeholder string.

    Raises:
        TypeError:
            if `proxy` is not an instance of a Proxy.
    """
    if not isinstance(proxy, ps.proxy.Proxy):
        # The JSON encoder will catch this TypeError and handle appropriately
        raise TypeError

    if ps.proxy.is_resolved(proxy):
        # Proxy is already resolved so encode the underlying object
        # rather than the proxy
        return ps.proxy.extract(proxy)

    warnings.warn(
        "Attemping to JSON serialize an unresolved proxy. To prevent "
        "an unintended proxy resolve, the resulting JSON object will "
        "have unresolved proxies replaced with a placeholder string.",
        category=ProxyJSONSerializationWarning
    )
    return f"<Unresolved Proxy at {hex(id(proxy))}>"


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
