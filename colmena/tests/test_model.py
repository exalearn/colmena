"""Tests for the data models"""
import sys

from proxystore.proxy import Proxy
from proxystore.store.utils import get_key

from colmena.models import ResourceRequirements, Result


def test_resources():
    res = ResourceRequirements(node_count=2, cpu_processes=4)
    assert res.total_ranks == 8


def test_message_sizes():
    result = Result(
        (('0' * 8,), {'test': '0' * 8}),
        method='test',
        keep_inputs=False,
        serialization_method='json',
    )

    # Make sure the size of the inputs is stored
    result.serialize()
    assert result.message_sizes['inputs'] >= 2 * sys.getsizeof('0' * 8)
    assert 'value' not in result.message_sizes

    # Add a result
    result.deserialize()
    result.set_result(1, 1)
    result.serialize()
    assert result.message_sizes['inputs'] >= 2 * sys.getsizeof('0' * 8)
    assert result.message_sizes['inputs'] >= sys.getsizeof(1)


def test_proxy(store):
    result = Result.from_args_and_kwargs(
        ('a' * 1000,),
        serialization_method='pickle',
        proxystore_name='store',
        proxystore_threshold=10,
        proxystore_config=store.config()
    )
    result.serialize()
    result.deserialize()
    assert isinstance(result.inputs[0][0], Proxy)
    proxy = result.inputs[0][0]
    key = get_key(proxy)

    assert len(result.args[0]) == 1000
    assert not store.exists(key)
