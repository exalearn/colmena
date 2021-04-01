"""Utilities for tracking resources"""
from threading import Semaphore, Lock, Event
from typing import List, Dict, Optional
from time import monotonic
from math import inf
import logging

logger = logging.getLogger(__name__)

_CANCEL_CHECK_FREQ = 1.0


class ResourceCounter:
    """Utility class for keeping track of resources available for different tasks.

    The class manages two pieces of state: the amount of resources allocated to a certain task,
    and the amount of resources that are currently available for that task.
    Users of this class can change either state using a series of thread-safe methods.

    *Tracking Allocations*: The resource counter is initialized with a certain count of resources,
    which represent the total number of a certain computing device available (e.g., node, GPU).
    They all begin as "unallocated" for any task.

    Users change the amount of resources dedicated to tasks by "reallocating" them from one task to another.
    The :meth:`reallocate` method achieves this by requesting a certain number of units from one task
    and adding them to a second task's available resources once those units are marked as available.

    *Tracking Utilization*: The amount of resources in use for a certain task is tracked by an internal counter.
    Users of this class request the use a certain number of resources by calling the :meth:`acquire` method.
    The method blocks until either the request is completely fulfilled (i.e., the specified amount of resources
    are marked as available) or the operation times out.

    Resources are marked as available again using the :meth:`release` method.
    The release method marks those resources as available to be re-used for other tasks of the same type.
    Resources must be re-allocated using :meth:`reallocate`.

    **Implementation**: All of the operations described above are thread-safe.
    Resource utilization is tracked using a semaphore so that threads can acquire and release resources simultaneously.
    Resources are acquired as first-come-first-served by using a lock to control access to the "acquire" function
    of the resource utilization semaphore.
    """

    def __init__(self, total_slots: int, task_types: List[str]):
        """
        Args:
            total_slots: Total number of nodes available to the resources
            task_types: Names of task types
        """
        # Save the total number of nodes available
        self._total_slots = total_slots

        # Add a "global" task type
        my_tasks: List[Optional[str]] = task_types.copy()
        if None in my_tasks:
            raise ValueError("`None` is reserved as the global task name")
        my_tasks.append(None)

        # Create semaphores that track the resources available tasks
        self._availability: Dict[Optional[str], Semaphore] = dict(
            (t, Semaphore(value=0)) for t in my_tasks
        )
        self._availability_lock: Dict[Optional[str], Lock] = dict(
            (t, Lock()) for t in my_tasks
        )

        # Create counters to represent the amount of resources allocated to each task
        self._allocation: Dict[Optional[str], int] = dict(
            (t, 0) for t in my_tasks
        )
        self._allocation_lock: Dict[Optional[str], Lock] = dict(
            (t, Lock()) for t in my_tasks
        )

        # Mark the number of unallocated slots
        for _ in range(self._total_slots):
            self._availability[None].release()
        self._allocation[None] = self._total_slots

        logger.info(f'Created a resource tracker with {total_slots} slots and {len(task_types)} task types: {", ".join(task_types)}')

    @property
    def unallocated_slots(self) -> int:
        """Number of unallocated slots"""
        return self._allocation[None]

    def allocated_slots(self, task: str) -> int:
        """Number of slots allocated to a certain task

        Args:
            task: Name of the task
        """
        if task not in self._availability:
            raise KeyError(f'Unknown task name: {task}')
        return self._allocation[task]

    def available_slots(self, task: Optional[str]) -> int:
        """Get the number of nodes available for a certain task

        Args:
            task: Name of the task
        Returns:
            Number of slots available for that task
        """
        if task not in self._availability:
            raise KeyError(f'Unknown task name: {task}')
        return self._availability[task]._value

    def release(self, task: Optional[str], n_slots: int, rerequest: bool = False, timeout: float = -1) -> Optional[bool]:
        """Register that nodes for a particular task are available
        and, optionally, re-request those nodes for the same task.

        Blocks until the task request completes

        Args:
            task: Name of the task
            n_slots: Number of slots to mark as available
            rerequest: Whether to re-request nodes immediately after releasing them
            timeout: Maximum time to wait for the request to be filled
        Returns:
            Whether the re-request was fulfilled
        """

        for _ in range(n_slots):
            self._availability[task].release()  # TODO (wardlt): Py3.9 lets you release counter by >1
        if rerequest:
            return self.acquire(task, n_slots, timeout=timeout)
        return None

    def acquire(self, task: Optional[str], n_slots: int, timeout: float = -1, cancel_if: Optional[Event] = None) -> bool:
        """Request a certain number of nodes for a particular task

        Draws only from the pool of nodes allocated to this task

        Args:
            task: Name of the task
            n_slots: Number of slots to request
            timeout: Maximum time to wait for the request to be filled
            cancel_if: Cancel the request if this event happens
        Returns:
            Whether the request was fulfilled
        """

        # Determine when this operation will time out
        if timeout > 0:
            end_time = monotonic() + timeout
        else:
            end_time = inf

        # Set a small timeout if we are also checking for a condition
        if cancel_if:
            timeout = _CANCEL_CHECK_FREQ

        # Acquire the lock for getting the hold
        lock_acquired = False
        while not lock_acquired:
            lock_acquired = self._availability_lock[task].acquire(timeout=timeout)
            if (cancel_if is not None and cancel_if.is_set()) or monotonic() > end_time:
                if lock_acquired:
                    self._availability_lock[task].release()
                return False

        # Wait until all nodes are acquired
        n_acquired = 0
        success = True
        while n_acquired < n_slots:
            acq_success = self._availability[task].acquire(timeout=timeout)
            if (cancel_if is not None and cancel_if.is_set()) or monotonic() > end_time:
                success = False
                break
            if acq_success:
                n_acquired += 1

        # Let another thread use this class
        self._availability_lock[task].release()

        # If you were not successful, give the resources back
        if not success:
            for _ in range(n_acquired):
                self._availability[task].release()

        return success

    def reallocate(self, task_from: Optional[str], task_to: Optional[str], n_slots: int,
                   timeout: float = -1, cancel_if: Optional[Event] = None) -> bool:
        """Transfer computer resources from one task to another

        Args:
            task_from: Which task to pull resources from (None to request un-allocated nodes)
            task_to: Which task to add resources to (None to de-allocate nodes)
            n_slots: Number of nodes to request
            timeout: Maximum time to wait for the request to be filled
            cancel_if: Cancel the request if this event happens
        Returns:
            Whether request was fulfilled
        """

        # Pull nodes from the remaining
        acq_success = self.acquire(task_from, n_slots, timeout, cancel_if)

        # If successful, push those resources to the target pool
        if acq_success:
            # Mark resources as available
            for _ in range(n_slots):
                self._availability[task_to].release()

            # Record changes to the total pool size
            with self._allocation_lock[task_from], self._allocation_lock[task_to]:
                self._allocation[task_from] -= n_slots
                self._allocation[task_to] += n_slots
            logger.info(f'Transferred {n_slots} slots from {task_from} to {task_to}')
            # TODO (wardlt): Eventually provide some mechanism to inform a batch
            #   system that resources allocated to ``from_task`` should be released

        return acq_success
