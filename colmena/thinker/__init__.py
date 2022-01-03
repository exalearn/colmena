"""Base classes for 'thinking' applications that respond to tasks completing"""
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import partial, update_wrapper
from threading import Event, local, Thread
from traceback import TracebackException
from typing import Optional, Callable, List, Union

import os

import logging

from colmena.redis.queue import ClientQueues
from colmena.thinker.resources import ResourceCounter, ReallocatorThread

logger = logging.getLogger(__name__)

_DONE_REACTION_TIME = 1


def agent(func: Optional[Callable] = None, critical: bool = True):
    """Decorator that denotes a function as an "agent" thread that is launched when a Thinker process is started

    Args:
        func: Do not directly pass this variable. It is used as an argument to the decorator
        critical: Whether the "done" flag should be set once this thread finishes
    """
    def decorator(f: Callable):
        f._colmena_agent = True
        f._colmena_critical = critical
        return f
    if func is None:
        return decorator
    return decorator(func)


def _result_event_agent(thinker: 'BaseThinker', process_func: Callable, topic: Optional[str]):
    """Wrapper function for result processing agents"""
    # Wait until we get a result
    while not thinker.done.is_set():
        result = thinker.queues.get_result(timeout=_DONE_REACTION_TIME, topic=topic)
        if result is not None:
            process_func(thinker, result)


def result_processor(func: Optional[Callable] = None, topic: Optional[str] = None):
    """Decorator that builds agents which respond to results becoming available in a queue

    Decorated functions must take a single argument: a result object

    Args:
        func: Do not directly pass this variable. It is used as an argument to the decorator
        topic: Topic of the queue to pull from
    """

    def decorator(f: Callable):
        output = partial(_result_event_agent, process_func=f, topic=topic)
        output = agent(output)
        return update_wrapper(output, f)
    if func is None:
        return decorator
    return decorator(func)


def _task_submitter_agent(thinker: 'BaseThinker', process_func: Callable, task_type: str, n_slots: Union[int, str]):
    """Wrapper function for task submission agents"""
    # Determine the number of threads
    if not isinstance(n_slots, int):
        n_slots = getattr(thinker, n_slots)

    while not thinker.done.is_set():
        # Wait until resources are free or thinker.done is set
        acq_success = thinker.rec.acquire(task_type, n_slots, cancel_if=thinker.done)
        if acq_success:
            process_func(thinker)


def task_submitter(func: Optional[Callable] = None, task_type: str = None, n_slots: Union[int, str] = 1):
    """Decorator that builds agents which respond to computing resources becoming available

    Decorated functions should assume that resources are available and reserved when the function is called

    Args:
        func: Do not directly pass this variable. It is used as an argument to the decorator
        task_type: Name of task pool from which to request resources
        n_slots: Number of resources to request. Must be either a integer or the name of a class attribute
    """

    def decorator(f: Callable):
        output = partial(_task_submitter_agent, process_func=f, task_type=task_type, n_slots=n_slots)
        output = agent(output)
        return update_wrapper(output, f)
    if func is None:
        return decorator
    return decorator(func)


def _event_responder_agent(thinker: 'BaseThinker', process_func: Callable, event_name: str,
                           reallocate_resources: bool, gather_from: Optional[str],
                           gather_to: Optional[str], disperse_to: Optional[str],
                           max_slots: Union[int, str, None], slot_step: int):
    """Wrapper for event processing agents"""

    # Get the event
    if not hasattr(thinker, event_name):
        raise ValueError(f'Thinker lacks an event named {event_name}')
    event: Event = getattr(thinker, event_name)

    # Get the max_slots if set to a class attribute
    if not isinstance(max_slots, int) and max_slots is not None:
        max_slots = getattr(thinker, max_slots)

    # Loop until the thinker is completed
    reallocator_thread: Optional[ReallocatorThread] = None
    while not thinker.done.is_set():
        if event.wait(_DONE_REACTION_TIME):
            # If desired, launch the resource-allocation thread
            if reallocate_resources:
                reallocator_thread = ReallocatorThread(
                    thinker.rec, gather_from=gather_from, gather_to=gather_to,
                    disperse_to=disperse_to, max_slots=max_slots,
                    slot_step=slot_step, logger_name=thinker.logger.name + ".allocate"
                )
                reallocator_thread.start()

            # Launch the function
            process_func(thinker)

            # If we are using resource re-allocation, set the stop condition and wait for resources to be freed
            if reallocator_thread is not None:
                reallocator_thread.stop_event.set()
                reallocator_thread.join()


def event_responder(func: Optional[Callable] = None, event_name: str = None,
                    reallocate_resources: bool = False,
                    gather_from: Optional[str] = None, gather_to: Optional[str] = None,
                    disperse_to: Optional[str] = None, max_slots: Union[int, str, None] = None,
                    slot_step: int = 1):
    """Decorator that builds agents which respond to an event being set.

    The event responder can launch a thread to acquire resource temporarily.
    The thread is created if you set ``reallocate_resources=True`` in the decorator
    and transfers resources to a specific pool until the decorated function completes
    or a user-defined resource cap is set.
    You must configure from where these resources are acquired, in which resource pool
    they are placed, and where they are re-allocated after the thread completes.
    The thread will allocate up to the maximum number of slots defined and
    then reallocate _all slots available to that pool_ to the designated resource.


    The Thinker associated with this agent must have a class attribute that is an :class:`Event`
    with the same name as ``event_name``.

    Args:
        func: Do not directly pass this variable. It is used as an argument to the decorator
        event_name: Name of the event to watch
        reallocate_resources: Whether to re-allocate resources while function is running
        gather_from: Name of a resource pool from which to acquire resources
        gather_to: Name of the resource pool to place re-allocated resources
        disperse_to: Name of the resource pool to move resources to after function completes
        max_slots: Maximum number of resource slots to acquire. Can be an integer,
            the name of a class attribute of the thinker, or 'none' it no maximum is needed
        slot_step: Number of slots to acquire per request
    """

    # Make sure the re-allocation logic is configured reasonably
    if reallocate_resources and gather_to == gather_from:
        raise ValueError('Resources should be drawn and stored in different pools')

    def decorator(f: Callable):
        output = partial(_event_responder_agent, process_func=f, event_name=event_name,
                         reallocate_resources=reallocate_resources, gather_from=gather_from, gather_to=gather_to,
                         disperse_to=disperse_to, max_slots=max_slots, slot_step=slot_step)
        output = agent(output)
        return update_wrapper(output, f)
    if func is None:
        return decorator
    return decorator(func)


def _launch_agent(func: Callable, thinker: 'BaseThinker'):
    """Shim function for launching an agent

    Sets the thread-local variables for a class, such as its name and default topic
    """

    # Set the thread-local options for this agent
    name = func.__name__
    thinker.local_details.name = name
    thinker.local_details.logger = thinker.make_logger(name)

    # Mark that this thread has launched
    thinker.logger.info(f'{name} started')

    # Launch it
    try:
        func(thinker)
    finally:
        # If a "critical" function, set the "done" flag
        if getattr(func, '_colmena_critical', False):
            thinker.done.set()

        # Mark that the thread has crashed
        thinker.logger.info(f'{name} completed')


class _AgentData(local):
    """Data local to a certain agent thread

    Attributes:
        logger: Logger for this thread
        name (str): Name of the thread
    """

    def __init__(self, logger: logging.Logger):
        """
        Args:
            logger: Logger to use for this thread
        """
        self.logger = logger
        self.name: Optional[str] = None


class BaseThinker(Thread):
    """Base class for dataflow program that steers a Colmena application

    The intent of this class is to simplify writing an dataflow programs using Colmena.
    When implementing a subclass, write each operation in the program as class method.
    Each method should take no inputs and produce no output, and could be thought of as
    an "operation" or "agent" that will run as a thread.

    Each agent communicates with others via `queues <https://docs.python.org/3/library/queue.html>`_
    or other `threading objects <https://docs.python.org/3/library/threading.html#>`_ and
    the Colmena task server via the :class:`ClientQueues`.
    The only communication method available by default is a class attribute named ``done``
    that is used to signal that the program should terminate.

    Denote each of these agents with the :meth:`agent` decorator, as in:

    .. code-block: python

        class ExampleThinker(BaseThinker):

            @agent
            def function(self):
                return True

    The decorator will tell Colmena to launch that method as a separate thread
    when the "Thinker" thread is started.
    Colmena will also create a distinct logger for each of the agents to that is
    accessible as the :meth:`logger` property.

    Start the thinker by calling ``.start()``
    """

    def __init__(self, queue: ClientQueues, resource_counter: Optional[ResourceCounter] = None,
                 daemon: bool = True, **kwargs):
        """
            Args:
                queue: Queue wrapper used to communicate with task server
                resource_counter: Utility to used track resource utilization
                daemon: Whether to launch this as a daemon thread
                **kwargs: Options passed to :class:`Thread`
        """
        super().__init__(daemon=daemon, **kwargs)

        # Define thinker-wide collectives
        self.rec = resource_counter
        self.queues = queue

        # Create some basic events and locks
        self.done: Event = Event()

        # Thread-local stuff, like the default queue and name
        self.local_details = _AgentData(self.make_logger())

    @property
    def logger(self) -> logging.Logger:
        """Get the logger for the active thread"""
        return self.local_details.logger

    def make_logging_handler(self) -> Optional[logging.Handler]:
        """Override to create a distinct logging handler for log messages emitted from this object"""
        return None

    def make_logger(self, name: Optional[str] = None):
        """Make a sub-logger for our application

        Args:
            name: Name to use for the sub-logger

        Returns:
            Logger with an appropriate name
        """
        # Create the logger
        my_name = self.__class__.__name__.lower()
        if name is not None:
            my_name += "." + name
        new_logger = logging.getLogger(my_name)

        # Assign the handler to the root logger
        if name is None:
            hnd = self.make_logging_handler()
            if hnd is not None:
                new_logger.addHandler(hnd)
        return new_logger

    @classmethod
    def list_agents(cls) -> List[Callable]:
        """List all functions that map to operations within the thinker application

        Returns:
            List of methods that define agent threads
        """
        agents = []
        for n in dir(cls):
            o = getattr(cls, n)
            if hasattr(o, '_colmena_agent'):
                agents.append(o)
        return agents

    def run(self):
        """Launch all operation threads and wait until all complete

        Sets the ``done`` flag when a thread completes, then waits for all other flags to finish.

        Does not raise exceptions if a thread exits with an exception. Exception and traceback information
        are printed using logging at the ``WARNING`` level.
        """
        self.logger.info(f"{self.__class__.__name__} started. Process id: {os.getpid()}")

        threads = []
        functions = self.list_agents()
        with ThreadPoolExecutor(max_workers=len(functions)) as executor:
            # Submit all of the worker threads
            for f in functions:
                threads.append(executor.submit(_launch_agent, f, self))
            self.logger.info(f'Launched all {len(functions)} functions')

            # Wait until any one completes, then set the "gen_done" event to
            #  signal all remaining threads to finish after completing their work
            for finished in as_completed(threads):
                exc = finished.exception()
                if exc is None:
                    self.logger.info('Thread completed without problems')
                else:
                    tb = TracebackException.from_exception(exc)
                    self.logger.warning(f'Thread failed: {exc}.\nTraceback: {"".join(tb.format())}')

        self.logger.info(f"{self.__class__.__name__} completed")
