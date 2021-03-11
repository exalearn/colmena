"""Utilities for tracking resources"""
from threading import Semaphore, Lock
from typing import List, Dict, Optional
import logging

logger = logging.getLogger(__name__)


# TODO (wardlt): Document the logic for each of the function requests (e.g., request then validate)
# TODO (wardlt): Add timeouts to requests. You can mark whether a request is vacated by passing an event
#  along with the request amount and objects
class ResourceCounter:
    """Utility class for keeping track of resources available for different tasks.

    The primary use of this class is to TODO: Logan finish your sentence!

    The main benefit of this class is when changing the number of nodes allocated to a particular task.
    Users employ the :meth:`transfer_nodes` method request an certain number of nodes to be re-allocated
    from one task (``from_task``) to another (``to_task``).
    This method places a request to the system such that when nodes from ``from_task`` are marked as "available"
    they are added to the pool of workers for ``to_task`` rather than being re-allocated to ``from_task``.
    """

    def __init__(self, total_nodes: int, task_types: List[str]):
        """
        Args:
            total_nodes: Total number of nodes available to the resources
            task_types: Names of task types
        """
        # Save the total number of nodes available
        self._total_nodes = total_nodes

        # Add a "global" task type
        my_tasks: List[Optional[str]] = task_types.copy()
        if None in my_tasks:
            raise ValueError("`None` is reserved as the global task name")
        my_tasks.append(None)

        # Create semaphores that track the resources allocated to individual tasks
        #  TODO (wardlt): Will eventually need to keep track of the total number of slots available
        #   to allow for interfacing with an execution provider (e.g., to tell it to release holds on resources)
        self._task_allocations: Dict[Optional[str], Semaphore] = dict(
            (t, Semaphore(value=0)) for t in my_tasks
        )
        self._pulling_lock: Dict[Optional[str], Lock] = dict(
            (t, Lock()) for t in my_tasks
        )

        # Mark the number of unallocated nodes
        for _ in range(self._total_nodes):
            self._task_allocations[None].release()

    @property
    def unallocated_nodes(self) -> int:
        """Number of unallocated nodes"""
        return self._task_allocations[None]._value

    def count_available_nodes(self, task: str) -> int:
        """Get the number of nodes available for a certain task

        Args:
            task: Name of the task
        Returns:
            Number of nodes available for that task
        """
        if task not in self._task_allocations:
            raise KeyError(f'Unknown task name: {task}')
        return self._task_allocations[task]._value

    def register_completion(self, task: str, n_nodes: int, rerequest: bool = True, timeout: float = -1)\
            -> Optional[bool]:
        """Register that nodes for a particular task are available 
        and, by default, re-request those nodes for the same task.

        Blocks until the task request completes
        
        Args:
            task: Name of the task
            n_nodes: Number of nodes to mark as available
            rerequest: Whether to re-request
            timeout: Maximum time to wait for the request to be filled
        Returns:
            Whether the re-request was fulfilled
        """

        for _ in range(n_nodes):
            self._task_allocations[task].release()  # TODO (wardlt): Py3.9 lets you release counter by >1
        if rerequest:
            return self.request_nodes(task, n_nodes, timeout=timeout)
        return None

    # TODO (warlt): Allow partial fulfillment?
    def request_nodes(self, task: str, n_nodes: int, timeout: float = -1) -> bool:
        """Request a certain number of nodes for a particular task

        Draws only from the pool of nodes allocated to this task

        Args:
            task: Name of the task
            n_nodes: Number of nodes to request
            timeout: Maximum time to wait for the request to be filled
        Returns:
            Whether the request was fulfilled
        """

        # Acquire the lock for getting the hold over the
        lock_acquired = self._pulling_lock[task].acquire(timeout=timeout)
        if not lock_acquired:
            return lock_acquired

        # Wait until all nodes are acquired
        n_acquired = 0
        success = True
        for _ in range(n_nodes):
            success = self._task_allocations[task].acquire(timeout=timeout)
            if not success:
                break
            n_acquired += 1

        # Let another thread use this class
        self._pulling_lock[task].release()

        # If you were not successful, give the resources back
        if not success:
            for _ in range(n_acquired):
                self._task_allocations[task].release()

        return success

    def transfer_nodes(self, task_from: Optional[str], task_to: Optional[str], n_nodes: int,
                       timeout: float = -1) -> bool:
        """Request for nodes to be transferred from one allocation pool to another

        Args:
            task_from: Which task pool to pull from (None to request un-allocated nodes)
            task_to: Which task pool to add to (None to de-allocate nodes)
            n_nodes: Number of nodes to request
            timeout: Maximum time to wait for the request to be filled
        Returns:
            Whether request was fulfilled
        """

        # Pull nodes from the remaining
        acq_success = self.request_nodes(task_from, n_nodes, timeout)

        # If successful, push those resources to the target pool
        if acq_success:
            for _ in range(n_nodes):
                self._task_allocations[task_to].release()
            # TODO (wardlt): Eventually provide some mechanism to inform a batch
            #   system that resources allocated to ``from_task`` should be released

        return acq_success
