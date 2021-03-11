"""Utilities for tracking resources"""
from threading import Semaphore, Lock, Thread, Event
from typing import List, Dict, Optional, Tuple
from queue import Queue
import logging

logger = logging.getLogger(__name__)


# TODO (wardlt): Add timeouts to requests. You can mark whether a request is vacated by passing an event
#  along with the request amount and objects
class ResourceCounter:
    """Utility class for keeping track of resources available for different tasks.

    The primary use of this class is to

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

        # Settings currently hidden from user
        self._acquire_timeout = 5

        # Save the total number of nodes available
        self._total_nodes = total_nodes

        # Add a "global" task type
        my_tasks: List[Optional[str]] = task_types.copy()
        if None in my_tasks:
            raise ValueError("`None` is reserved as the global task name")
        my_tasks.append(None)

        # Create semaphores that track the resources allocated to individual tasks
        self._task_allocations: Dict[Optional[str], Semaphore] = dict(
            (t, Semaphore(value=0)) for t in my_tasks
        )
        self._service_requests: Dict[Optional[str], Queue[Tuple[int, Lock, Event]]] = dict(
            (t, Queue()) for t in my_tasks
        )

        # Mark the number of unallocated nodes
        for _ in range(self._total_nodes):
            self._task_allocations[None].release()

        # Launch all of the fulfillers
        self._fulfillers = dict(
            (t, Thread(target=self._fulfill_requests, args=(t,), daemon=True)) for t in my_tasks
        )
        for t in self._fulfillers.values():
            t.start()

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

    def _fulfill_requests(self, task: Optional[str]):
        """Background thread that handles requests to allocate from different pools

        Args:
            task: Task name (None for the unallocated pool)
        """
        logger.debug(f'Started fulfiller for task {task}')
        while True:
            # Retrieve the next request
            n_requested, lock, vacate = self._service_requests[task].get()

            # Wait until either the request is fully completed or the request is vacated
            was_vacated = False
            n_acquired = 0
            while n_acquired < n_requested:
                while not self._task_allocations[task].acquire(timeout=self._acquire_timeout):
                    was_vacated = vacate.is_set()  # Check if the requester has vacated
                    if was_vacated:  # If so, no longer acquire nodes
                        break
                if was_vacated:
                    break
                n_acquired += 1

            # Indicate to the requester that their request is fulfilled
            lock.release()

            # If the request was vacated, toss the nodes back into the poll
            if vacate.is_set():
                for _ in range(n_acquired):
                    self._task_allocations[task].release()

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

        # Create a lock that is held until the request is fulfilled and an event to signal it was vacated
        acq_lock = Lock()
        acq_lock.acquire()
        req_vacate = Event()

        # Push the request to the pool
        self._service_requests[task].put((n_nodes, acq_lock, req_vacate))

        # Wait until the request is fulfilled
        was_fulfilled = acq_lock.acquire(timeout=timeout)
        if not was_fulfilled:
            req_vacate.set()
        return was_fulfilled

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

        # Create a lock that is held until the request is fulfilled and an event to signal it was vacated
        acq_lock = Lock()
        acq_lock.acquire()
        req_vacate = Event()

        # Push the request to the pool
        self._service_requests[task_from].put((n_nodes, acq_lock, req_vacate))

        # Wait until the request was fulfilled
        was_fulfilled = acq_lock.acquire(timeout=timeout)
        if was_fulfilled:
            logger.info(f"Reallocated {n_nodes} from {task_from} to {task_to}")
            for _ in range(n_nodes):
                self._task_allocations[task_to].release()
        else:
            req_vacate.set()

        return was_fulfilled
