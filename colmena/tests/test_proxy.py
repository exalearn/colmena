"""Test for ProxyStore utilities."""
import json

import pytest

from proxystore.proxy import Proxy
from proxystore.store import register_store
from proxystore.store import unregister_store
from proxystore.store.base import Store
from proxystore.connectors.local import LocalConnector

from colmena.proxy import get_store
from colmena.proxy import proxy_json_encoder
from colmena.proxy import resolve_proxies_async


class ExampleStore(Store):
    pass


@pytest.fixture
def proxy() -> Proxy:
    with Store('proxy-fixture-store', LocalConnector()) as store:
        yield store.proxy('test-value')


def test_get_store_already_registered() -> None:
    store = Store('test-store', LocalConnector())
    register_store(store)
    assert get_store('test-store') is store
    unregister_store(store)


def test_get_store_missing() -> None:
    assert get_store('test-store') is None


def test_get_store_initialize_from_config() -> None:
    # Create a temp store just to get the config
    config = Store('test-store', LocalConnector()).config()
    store = get_store('test-store', config)
    # Verify get_store registered the store globally by getting it again
    assert get_store('test-store') is store
    unregister_store(store)


def test_proxy_json_encoder() -> None:
    p = Proxy(lambda: 'test-value')
    result = json.dumps({'proxy': p}, default=proxy_json_encoder)
    reconstructed_data = json.loads(result)
    assert 'proxy' in reconstructed_data


def test_proxy_json_encoder_no_proxies() -> None:
    # proxy_json_encoder should raise TypeError on non-Proxy types which
    # were unable to be serialized by the default serializer
    with pytest.raises(TypeError):
        json.dumps({'a': lambda: 1}, default=proxy_json_encoder)


def test_proxy_json_encoder_resolved_proxy(proxy) -> None:
    # force the proxy to resolve
    assert isinstance(proxy, str)
    result = json.dumps({'proxy': proxy}, default=proxy_json_encoder)
    assert result == '{"proxy": "test-value"}'


def test_resolve_proxy_async_object_arg(proxy) -> None:
    assert resolve_proxies_async(proxy) == [proxy]
    assert resolve_proxies_async('not a proxy') == []


def test_resolve_proxy_async_sequence_arg(proxy) -> None:
    assert resolve_proxies_async([proxy, 'not a proxy']) == [proxy]
    assert resolve_proxies_async((proxy, 'not a proxy')) == [proxy]


def test_resolve_proxy_async_dict_arg(proxy) -> None:
    assert resolve_proxies_async(
        {'proxy': proxy, 'other': 'not a proxy'},
    ) == [proxy]
