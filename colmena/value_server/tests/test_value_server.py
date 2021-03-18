"""Tests for Colmena Value Server"""
import multiprocessing as mp
import os

from pytest import raises, mark

from colmena import value_server
from colmena.value_server import init_value_server
from colmena.value_server import LRUCache
from colmena.value_server import VALUE_SERVER_HOST_ENV_VAR
from colmena.value_server import VALUE_SERVER_PORT_ENV_VAR


@mark.timeout(30)
def test_init_value_server() -> None:
    """Test initializing value server from ENV variables"""
    value_server.server = None

    with raises(ValueError):
        init_value_server()

    os.environ[VALUE_SERVER_HOST_ENV_VAR] = 'localhost'
    os.environ[VALUE_SERVER_PORT_ENV_VAR] = '6379'

    init_value_server()
    assert value_server.server is not None

    # The below test assume empty DB
    value_server.server.redis_client.flushdb()


@mark.timeout(30)
def test_value_server() -> None:
    """Test value server interactions"""
    assert value_server.server.get('test_key') is None
    value_server.server.set('test_key', [1, 2, 3])
    assert value_server.server.get('test_key') == [1, 2, 3]


@mark.timeout(30)
def test_value_server_eviction() -> None:
    """Test value server evictions"""
    value_server.server.set('test_key', [1, 2, 3])
    assert value_server.server.get('test_key') == [1, 2, 3]
    value_server.server.evict('test_key')
    # Value still exists in local cache
    assert value_server.server.get('test_key') == [1, 2, 3]
    # Value will not exists in Redis
    assert value_server.server.redis_client.get('test_key') is None


@mark.timeout(30)
def test_value_server_strict() -> None:
    """Test value server strict timestamp guarentees"""
    value_server.server.set('test_key', [1, 2, 3])
    assert value_server.server.get('test_key') == [1, 2, 3]
    value_server.server.set('test_key', [2, 3, 4])
    # cache will still have [1, 2, 3] without strict flag set
    assert value_server.server.get('test_key') == [1, 2, 3]
    # setting strict flag will force getting most recent version
    assert value_server.server.get('test_key', strict=True) == [2, 3, 4]


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


@mark.timeout(30)
def test_lru_cache_mp() -> None:
    """Test LRU Cache with Multiprocessing"""
    return
    c = LRUCache(1)
    c.set('test_key', 'test_value')

    def f(x):
        _c = LRUCache(1)
        assert _c.hits == 0
        assert _c.misses == 0
        assert _c.get('test_key') == 'test_value'
        assert _c.hits == 1
        assert _c.misses == 0
        assert _c.get(x) is None
        assert _c.hits == 1
        assert _c.misses == 1

    with mp.Pool(2) as p:
        p.map(f, ['1', '2', '3'])
