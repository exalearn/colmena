from pytest import fixture

from colmena.thinker.resources import ResourceCounter


@fixture
def rec() -> ResourceCounter:
    return ResourceCounter(8, ['ml', 'sim'])


def test_initialize(rec):
    assert rec.unallocated_nodes == 8


def test_allocations(rec):
    assert rec.transfer_nodes(None, "ml", 8)
    assert rec.unallocated_nodes == 0
    assert rec.count_available_nodes("ml") == 8

    rec.request_nodes("ml", 8)
    assert rec.count_available_nodes("ml") == 0

    assert not rec.request_nodes("ml", 1, timeout=0.02)

    assert rec.register_completion("ml", 4, rerequest=False) is None
    assert rec.count_available_nodes("ml") == 4

    assert rec.register_completion("ml", 4, rerequest=True)
    assert rec.count_available_nodes("ml") == 4
