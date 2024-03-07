"""Tests for the data models"""
import sys

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
