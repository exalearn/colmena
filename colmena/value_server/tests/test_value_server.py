"""Tests for Colmena Value Server"""
import numpy as np
import os

from pytest import fixture, raises, mark

from colmena import value_server
from colmena.value_server import init_value_server
from colmena.value_server import to_proxy
from colmena.value_server import ObjectProxy
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
    value_server.server.put('test_key', [1, 2, 3, 4])
    assert value_server.server.exists('test_key')
    assert value_server.server.get('test_key') == [1, 2, 3, 4]


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
