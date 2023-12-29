"""Utilities for interacting with ProxyStore"""
import logging
import warnings

import proxystore
from proxystore.proxy import extract
from proxystore.proxy import is_resolved
from proxystore.proxy import Proxy
from proxystore.store.base import Store
from proxystore.store.utils import resolve_async, get_key

from typing import Any, Dict, Union, List, Optional

logger = logging.getLogger(__name__)


class ProxyJSONSerializationWarning(Warning):
    pass


def get_store(
    name: str,
    config: Optional[Dict[str, Any]] = None,
) -> Optional[Store]:
    """Get a Store by name or create one if it does not already exist.

    Args:
        name (str): name of the store.
        config: ``Store`` configuration that can be used to reinitialize the
            ``Store`` if provided and a store with `name` is not found.

    Returns:
        The store registered as `name` or a newly intialized and registered
        store if `kind` is not ``None``.
    """
    store = proxystore.store.get_store(name)
    if store is None and config is not None:
        store = Store.from_config(config)
        proxystore.store.register_store(store)
    return store


def proxy_json_encoder(proxy: Proxy) -> Any:
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
    if not isinstance(proxy, Proxy):
        # The JSON encoder will catch this TypeError and handle appropriately
        logger.error(f'Passed a series of objects that are not serializable: {type(proxy)}. {proxy}')
        raise TypeError(f'Unserializable type: {type(proxy)}')

    if is_resolved(proxy):
        # Proxy is already resolved so encode the underlying object
        # rather than the proxy
        return extract(proxy)

    warnings.warn(
        "Attemping to JSON serialize an unresolved proxy. To prevent "
        "an unintended proxy resolve, the resulting JSON object will "
        "have unresolved proxies replaced with a placeholder string.",
        category=ProxyJSONSerializationWarning
    )
    return f"<Unresolved Proxy at {hex(id(proxy))}>"


def resolve_proxies_async(args: Union[object, list, tuple, dict]) -> List[Proxy]:
    """Begin asynchronously resolving all proxies in input

    Scan inputs for instances of `Proxy` and begin asynchronously resolving.
    This is useful if you have one or more proxies that will be needed soon
    so the underlying objects can be asynchronously resolved to reduce the
    cost of the first access to the proxy.

    Args:
        args (object, list, tuple, dict): possible object or
            iterable of objects that may be ObjectProxy instances

    Returns:
        List of the proxies that are being resolved
    """

    # Create a list to store the keys
    output = []

    # Make a function that will resolve proxies
    def resolve_async_if_proxy(obj: Any) -> None:
        if isinstance(obj, Proxy):
            output.append(obj)
            resolve_async(obj)

    if isinstance(args, Proxy):
        resolve_async_if_proxy(args)
    elif isinstance(args, list) or isinstance(args, tuple):
        for x in args:
            resolve_async_if_proxy(x)
    elif isinstance(args, dict):
        for x in args:
            resolve_async_if_proxy(args[x])
    return output


def store_proxy_stats(proxy: Proxy, proxy_timing: dict):
    """Store the timings associated with a proxy, if available

    Args:
        proxy: Proxy to evaluate
        proxy_timing: Dictionary in which to store timings to be updated
    """
    # Get the key associated with this proxy
    key = get_key(proxy)

    # ProxyStore keys are NamedTuples, so we cast to a string to use as a JSON key.
    key = str(key)

    # Get the store associated with this proxy
    store = proxystore.store.get_store(proxy)
    if store.metrics is not None:
        # Get the stats and convert them to a JSON-serializable form
        metrics = store.metrics.get_metrics(proxy)
        stats = metrics.as_dict() if metrics is not None else {}
    else:
        stats = {}

    # Update existing timings
    if key not in proxy_timing:
        proxy_timing[key] = {}
    proxy_timing[key].update(stats)
