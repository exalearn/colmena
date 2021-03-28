"""Tests for ObjectProxy"""
import numpy as np
import pickle as pkl

from pytest import raises, mark, fixture

from colmena import value_server
from colmena.value_server import to_proxy
from colmena.value_server import to_proxy_threshold
from colmena.value_server import ObjectProxy
from colmena.value_server import init_value_server


class DisablePyTestCollectionMixin(object):
    __test__ = False


class TestClass(DisablePyTestCollectionMixin):
    def __init__(self, x=1) -> None:
        self.x = x

    def __reduce_ex__(self, version):
        # Object must be json or pickleable
        return TestClass, (self.x,)


@fixture(scope='session', autouse=True)
def init() -> None:
    init_value_server('localhost', 6379)


@mark.timeout(30)
def test_proxy() -> None:
    """Test proxy object behaves like wrapped object"""
    x = to_proxy(1)
    assert not x.is_resolved()

    assert isinstance(x, ObjectProxy)
    assert not x.is_resolved()

    assert isinstance(x, int)
    assert x.is_resolved()

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
def test_proxy_strict() -> None:
    """Test strict proxy guarentees"""
    x = np.array([1, 2, 3])
    p1 = to_proxy(x, strict=True)
    x += 1
    assert np.array_equal(p1, [1, 2, 3])
    p2 = to_proxy(x, strict=False)
    p3 = to_proxy(x, strict=True)
    assert p1.__factory__.key == p2.__factory__.key
    assert np.array_equal(p2, [1, 2, 3])
    assert np.array_equal(p3, [2, 3, 4])


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
def test_serialize_proxy() -> None:
    """Test serializing ObjectProxy"""
    x = to_proxy([1, 2, 3])
    # Currently, object proxies cannot be JSON serialized
    # x_json = json.dumps(x)
    # assert x == json.loads(x_json)
    x_pickle = pkl.dumps(x).hex()
    assert x == pkl.loads(bytes.fromhex(x_pickle))


@mark.timeout(30)
def test_async_resolve() -> None:
    """Test async proxy resolving"""
    key = str(np.random.randint(0, 100000))
    x = to_proxy([1, 2, 3], key=key)
    assert x.__factory__.async_get_future is None
    assert not value_server.server.is_cached(key)
    x.async_resolve()
    assert not x.is_resolved()
    assert x.__factory__.async_get_future is not None
    assert isinstance(x, list)
    assert x.__factory__.async_get_future is None
    assert x.is_resolved()
    # x is already resolved so this should be a no-op
    x.async_resolve()
    assert x.__factory__.async_get_future is None
    assert isinstance(x, list)


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
def test_proxy_proxy() -> None:
    """Test proxying a proxy"""
    x = to_proxy([1, 2, 3])
    y = to_proxy(x)
    assert id(x) == id(y)
