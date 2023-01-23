"""Test for ProxyStore utilities."""
import json
from typing import Any
from typing import Type

import pytest

from proxystore.proxy import Proxy
from proxystore.store import register_store
from proxystore.store import unregister_store
from proxystore.store.base import Store
from proxystore.store.file import FileStore
from proxystore.store.local import LocalStore

from colmena.proxy import get_class_path
from colmena.proxy import import_class
from colmena.proxy import get_store
from colmena.proxy import proxy_json_encoder
from colmena.proxy import resolve_proxies_async


class ExampleStore(Store):
    pass


@pytest.fixture
def proxy() -> Proxy:
    with LocalStore('proxy-fixture-store') as store:
        yield store.proxy('test-value')


@pytest.mark.parametrize(
    'cls,expected',
    (
        (FileStore, 'proxystore.store.file.FileStore'),
        (LocalStore, 'proxystore.store.local.LocalStore'),
        # This directory has no __init__.py so the module is test_proxy
        # rather than colmena.tests.test_proxy.
        (ExampleStore, 'test_proxy.ExampleStore'),
    ),
)
def test_get_class_path(cls: Type[Any], expected: str) -> None:
    assert get_class_path(cls) == expected


@pytest.mark.parametrize(
    'path,expected',
    (
        ('proxystore.store.file.FileStore', FileStore),
        ('proxystore.store.local.LocalStore', LocalStore),
        ('test_proxy.ExampleStore', ExampleStore),
        ('typing.Any', Any),
    ),
)
def test_import_class(path: str, expected: Type[Any]) -> None:
    assert import_class(path) == expected


def test_import_class_missing_path() -> None:
    with pytest.raises(ImportError):
        import_class('FileStore')


def test_get_store_already_registered() -> None:
    store = LocalStore('test-store')
    register_store(store)
    assert get_store('test-store') is store
    unregister_store(store.name)


def test_get_store_missing() -> None:
    assert get_store('test-store') is None


def test_get_store_initialize_by_type() -> None:
    store = get_store('test-store', LocalStore, cache_size=0)
    # Verify get_store registered the store globally by getting it again
    assert get_store('test-store') is store
    unregister_store(store.name)


def test_get_store_initialize_by_str() -> None:
    store = get_store(
        'test-store',
        'proxystore.store.local.LocalStore',
        cache_size=0,
    )
    # Verify get_store registered the store globally by getting it again
    assert get_store('test-store') is store
    unregister_store(store.name)


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
