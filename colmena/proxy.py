"""Utilities for interacting with ProxyStore"""
import proxystore as ps

from typing import Any, Union

to_proxy = ps.to_proxy
"""Alias of proxystore.to_proxy"""


def extract_and_evict(proxy: ps.proxy.Proxy) -> object:
    """Extract wrapped object from proxy and evict from value server"""
    ps.utils.evict(proxy)
    return ps.utils.extract(proxy)


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
            ps.utils.resolve_async(obj)

    if isinstance(args, ps.proxy.Proxy):
        resolve_async_if_proxy(args)
    elif isinstance(args, list) or isinstance(args, tuple):
        for x in args:
            resolve_async_if_proxy(x)
    elif isinstance(args, dict):
        for x in args:
            resolve_async_if_proxy(args[x])
