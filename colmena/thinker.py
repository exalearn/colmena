"""Base classes for 'thinking' applications that respond to tasks completing"""
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Event, local, Thread
from traceback import TracebackException
from typing import Optional, Callable, List
import os

import logging

from colmena.redis.queue import ClientQueues

logger = logging.getLogger(__name__)


def agent(func):
    """Denote a function as an "agent" thread that is launched when
    a Thinker process is started"""
    func._colmena_agent = True
    return func


def _launch_agent(func: Callable, worker: 'BaseThinker'):
    """Shim function for launching an agent

    Sets the thread-local variables for a class, such as its name and default topic
    """

    # Set the thread-local options for this agent
    name = func.__name__
    worker.local_details.name = name
    worker.local_details.logger = worker.make_logger(name)

    # Mark that this thread has launched
    worker.logger.info(f'{name} started')

    # Launch it
    func(worker)

    # Mark that the thread has crashed
    worker.logger.info(f'{name} completed')


class AgentData(local):
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
    the Colmena method server via the :class:`ClientQueues`.
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
    Colmena will also create a distinct logger for the

    Start the thread by calling `.start()`, as in:

    .. code-block: python

        t = ExampleThinker(queue)
        t.start()
        t.join()  # Wait until work completes

    Attributes:
         done (threading.Event): Event used to mark that a thread has completed
    """

    def __init__(self, queue: ClientQueues, daemon: bool = True, **kwargs):
        """
        Args:
            queue: Queue wrapper used to communicate with
            daemon: Whether to launch this as a daemon thread
            **kwargs: Options passed to :class:`Thread`
        """
        super().__init__(daemon=daemon, **kwargs)

        # Create the base logger
        self.queues = queue

        # Create some basic events and locks
        self.done: Event = Event()

        # Thread-local stuff, like the default queue and name
        self.local_details = AgentData(self.make_logger())

    @property
    def logger(self) -> logging.Logger:
        """Get the logger for the active thread"""
        return self.local_details.logger

    def make_logging_handler(self) -> Optional[logging.Handler]:
        """Override to create a distinct logging handler for log messages emitted
        from this object"""
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
            finished = next(as_completed(threads))
            self.done.set()
            exc = finished.exception()
            if exc is None:
                self.logger.info('Thread completed without problems')
            else:
                tb = TracebackException.from_exception(exc)
                self.logger.warning(f'Thread failed: {exc}.\nTraceback: {"".join(tb.format())}')

            # Cycle through the threads until all exit
            for t in as_completed(threads):
                t.result()

        self.logger.info(f"{self.__class__.__name__} completed")
