from threading import Timer, Event
from time import sleep

from pytest import fixture, mark

from colmena.thinker.resources import ResourceCounter, ReallocatorThread


@fixture
def rec() -> ResourceCounter:
    return ResourceCounter(8, ['ml', 'sim'])


def test_initialize(rec):
    assert rec.unallocated_slots == 8
    assert rec.available_slots(None) == 8


def test_allocations(rec):
    # Move 8 nodes to the "ml" pool
    assert rec.reallocate(None, "ml", 8)
    assert rec.unallocated_slots == 0
    assert rec.available_slots("ml") == 8

    # Checkout all of them
    assert rec.acquire("ml", 8, timeout=1)
    assert rec.available_slots("ml") == 0
    assert rec.allocated_slots("ml") == 8

    # Request unavailable nodes to test a timeout
    assert not rec.acquire("ml", 1, timeout=0.02)

    # Release nodes
    assert rec.release("ml", 4, rerequest=False) is None
    assert rec.available_slots("ml") == 4

    # Release and re-request
    assert rec.release("ml", 4, rerequest=True, timeout=1)
    assert rec.available_slots("ml") == 4
    assert rec.available_slots("ml") == 4

    # Attempt a transfer that times out
    assert not rec.reallocate("ml", "sim", n_slots=5, timeout=0.01)
    assert rec.available_slots("ml") == 4

    # Attempt a transfer that completes
    assert rec.reallocate("ml", "sim", n_slots=4, timeout=4)
    assert rec.available_slots("sim") == 4
    assert rec.available_slots("ml") == 0
    assert rec.allocated_slots("sim") == 4
    assert rec.unallocated_slots == 0

    # Test out the "cancel if" events
    stop = Event()
    stop.set()
    assert not rec.acquire("sim", n_slots=5, cancel_if=stop)
    assert rec.available_slots("sim") == 4

    stop.clear()
    t = Timer(0.2, function=stop.set)
    t.start()
    assert not rec.acquire("sim", n_slots=5, cancel_if=stop)
    assert rec.available_slots("sim") == 4
    assert stop.is_set()

    # Test out reallocate all
    rec.reallocate("sim", "ml", "all")
    assert rec.allocated_slots("sim") == 0
    assert rec.allocated_slots("ml") == 8

    # Test out non-blocking re-allocator
    trigger = Event()
    rec.reallocate("ml", "sim", 8, block=False, callback=trigger.set)  # Will not immediately complete as there are reserved ML slotes
    sleep(0.1)
    assert not trigger.is_set()
    assert rec.allocated_slots("ml") == 8

    rec.release("ml", 4)  # Release so that the reallocation can complete
    sleep(0.1)
    assert trigger.is_set()
    assert rec.allocated_slots("sim") == 8


def test_reallocator(rec):
    # Start with everything allocated to "simulation"
    rec.reallocate(None, "sim", 8)

    # Test allocating up to the maximum
    stop = Event()
    alloc = ReallocatorThread(rec, stop_event=stop, gather_from="sim", gather_to="ml", disperse_to=None, max_slots=2)
    alloc.start()
    sleep(0.2)
    assert alloc.is_alive()
    assert rec.allocated_slots("ml") == 2

    # Once you set "stop," the thread should move resources to "unallocated"
    stop.set()
    sleep(0.2)
    assert not alloc.is_alive()
    assert rec.unallocated_slots == 2

    # Test without a maximum allocation
    stop.clear()
    alloc = ReallocatorThread(rec, stop_event=stop, gather_from="sim", gather_to="ml", disperse_to=None)
    alloc.start()
    sleep(0.2)
    assert alloc.is_alive()
    assert rec.available_slots("sim") == 0

    # Once you trigger the "stop event," the thread should move all resources to "None" and then exit
    stop.set()
    sleep(2)  # We check for the flag every 1s
    assert not alloc.is_alive()
    assert rec.unallocated_slots == 8


@mark.timeout(2)
@mark.repeat(4)
def test_reallocator_deadlock(rec):
    """Creates the deadlock reported in https://github.com/exalearn/colmena/issues/43"""
    rec.reallocate(None, "sim", 8)
    assert rec.available_slots("sim") == 8

    # Create two allocators: One that pulls from sim and another that pulls from ml
    ml_alloc = ReallocatorThread(rec, gather_from="sim", gather_to="ml", disperse_to="sim", max_slots=8)
    sim_alloc = ReallocatorThread(rec, gather_from="ml", gather_to="sim", disperse_to="ml", max_slots=8)

    # Start the ML allocator, which will pull resources from sim to ml
    ml_alloc.start()
    sleep(0.010)  # Give enough time for allocation to finish
    assert not ml_alloc.stop_event.is_set()
    assert rec.available_slots("ml") == 8
    assert rec.acquire("ml", 8)
    assert rec.available_slots("ml") == 0

    # Start the sim allocator, which will ask to pull resources from ml over to sim
    sim_alloc.start()
    sleep(0.001)
    assert rec.available_slots("ml") == 0

    # Send the stop signal and wait for ml_alloc to exit
    ml_alloc.stop_event.set()
    rec.release("ml", 8)
    ml_alloc.join(1)
    assert not ml_alloc.is_alive()
