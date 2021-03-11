from time import sleep

from pytest import fixture

from colmena.thinker.resources import ResourceCounter


@fixture
def rec() -> ResourceCounter:
    return ResourceCounter(8, ['ml', 'sim'])


def test_initialize(rec):
    assert rec.unallocated_nodes == 8


def test_allocations(rec):
    # Move 8 nodes to the "ml" pool
    assert rec.transfer_nodes(None, "ml", 8)
    assert rec.unallocated_nodes == 0
    assert rec.count_available_nodes("ml") == 8

    # Checkout all of them
    assert rec.request_nodes("ml", 8, timeout=1)
    assert rec.count_available_nodes("ml") == 0

    # Request unavailable nodes to test a timeout
    assert not rec.request_nodes("ml", 1, timeout=0.02)

    # Release nodes
    assert rec.register_completion("ml", 4, rerequest=False) is None
    assert rec.count_available_nodes("ml") == 4

    # Release and re-request
    assert rec.register_completion("ml", 4, rerequest=True, timeout=1)
    assert rec.count_available_nodes("ml") == 4

    # Attempt a transfer that times out
    assert not rec.transfer_nodes("ml", "sim", n_nodes=5, timeout=1)
    sleep(1)  # Wait until the fulfiller will discover the request was vacated
    assert rec.count_available_nodes("ml") == 4

    # Attempt a transfer that completes
    assert rec.transfer_nodes("ml", "sim", n_nodes=4, timeout=4)
    assert rec.count_available_nodes("sim") == 4
    assert rec.count_available_nodes("ml") == 0
    assert rec.unallocated_nodes == 0
