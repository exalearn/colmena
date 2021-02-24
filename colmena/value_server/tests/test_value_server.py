"""Tests for Colmena Value Server"""
import numpy as np
import os

from pytest import raises, mark

from colmena import value_server
from colmena.value_server import init_value_server
from colmena.value_server import to_proxy
from colmena.value_server import to_proxy_threshold
from colmena.value_server import ObjectProxy
from colmena.value_server import LRUCache
from colmena.value_server import VALUE_SERVER_HOST_ENV_VAR
from colmena.value_server import VALUE_SERVER_PORT_ENV_VAR


class DisablePyTestCollectionMixin(object):
    __test__ = False


class TestClass(DisablePyTestCollectionMixin):
    def __init__(self, x=1) -> None:
        self.x = x

    def __reduce_ex__(self, version):
        # Object must be json or pickleable
        return TestClass, (self.x,)


@mark.timeout(30)
def test_init_value_server() -> None:
    """Test initializing value server from ENV variables"""
    with raises(ValueError):
        init_value_server()

    os.environ[VALUE_SERVER_HOST_ENV_VAR] = 'localhost'
    os.environ[VALUE_SERVER_PORT_ENV_VAR] = '6379'

    init_value_server()
    assert value_server.server is not None


@mark.timeout(30)
def test_value_server() -> None:
    """Test value server interactions"""
    assert not value_server.server.exists('test_key')
    assert value_server.server.get('test_key') is None
    value_server.server.put('test_key', [1, 2, 3])
    assert value_server.server.exists('test_key')
    assert value_server.server.get('test_key') == [1, 2, 3]

    # Value server stores object as immutable so this
    # does not work
    # value_server.server.put('test_key', [1, 2, 3, 4])
    assert value_server.server.exists('test_key')
    assert value_server.server.get('test_key') == [1, 2, 3]


@mark.timeout(30)
def test_proxy() -> None:
    """Test proxy object behaves like wrapped object"""
    x = to_proxy(1)
    assert isinstance(x, ObjectProxy)
    assert isinstance(x, int)
    assert x == 1
    x += 1
    assert x == 2

    x = to_proxy(TestClass())
    assert isinstance(x, TestClass)
    assert x.x == 1
    x.x += 1
    assert x.x == 2

    x = to_proxy(np.array([1, 2, 3]))
    assert isinstance(x, np.ndarray)
    assert len(x) == 3
    assert x.shape == (3, )
    assert np.sum(x) == 6
    x = x + x
    assert np.array_equal(x, [2, 4, 6])


@mark.timeout(30)
def test_proxy_serialize() -> None:
    """Test ObjectProxy serialization"""
    x = to_proxy([1, 2, 3], serialization_method='pickle')
    assert isinstance(x, list)
    x = to_proxy([1, 2, 3], serialization_method='json')
    assert isinstance(x, list)

    # Should fail because np array not jsonable
    with raises(TypeError):
        x = to_proxy(np.array([1, 2, 3]), serialization_method='json')


@mark.timeout(30)
def test_to_proxy_threshold() -> None:
    """Test to proxy by size threshold"""
    assert to_proxy_threshold(None, 100) is None
    x = to_proxy_threshold(1, 0)
    assert x == 1
    assert isinstance(x, ObjectProxy)

    x = to_proxy_threshold(1, 1000)
    assert not isinstance(x, ObjectProxy)

    # list
    x = to_proxy_threshold([1, np.empty(int(1000 * 1000 * 50 / 4))], 1000 * 1000)
    assert isinstance(x, list)
    assert isinstance(x[0], int)
    assert not isinstance(x[0], ObjectProxy)
    assert isinstance(x[1], ObjectProxy)

    # tuple
    x = to_proxy_threshold((1, np.empty(1000)), 1000)
    assert isinstance(x, tuple)
    assert isinstance(x[0], int)
    assert not isinstance(x[0], ObjectProxy)
    assert isinstance(x[1], ObjectProxy)

    # dict
    x = to_proxy_threshold({'1': 1, '2': np.empty(1000)}, 1000)
    assert isinstance(x, dict)
    assert isinstance(x['1'], int)
    assert not isinstance(x['1'], ObjectProxy)
    assert isinstance(x['2'], ObjectProxy)


@mark.timeout(30)
def test_lru_cache() -> None:
    """Test LRU Cache"""
    c = LRUCache(4)
    # Put 1, 2, 3, 4 in cache
    for i in range(1, 5):
        c.set(str(i), i)
    for i in range(4, 0, -1):
        assert c.get(str(i)) == i
    # 4 is now least recently used
    c.set('5', 5)
    # 4 should now be evicted
    assert c.exists('1')
    assert not c.exists('4')
    assert c.exists('5')
