"""Base classes for 'thinking' applictions that respond to tasks completing"""
from concurrent.futures import ThreadPoolExecutor, as_completed
from multiprocessing import Process
from threading import Event
from traceback import TracebackException
from typing import Optional, Callable, List

import logging

logger = logging.getLogger(__name__)


def agent(func):
    """Denote a function as an "agent" thread that is launched when
    a Thinker process is started"""
    func._colmena_agent = True
    return func


class BaseThinker(Process):
    """Base class for steering applications

    Attributes:
         logger (logging.Logger): Base logger for general log messages
         done (threading.Event): Event used to mark that a thread has completed
    """

    logger: logging.Logger  # Base logger for the class

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        # Create the base logger
        self.logger = self._make_logger()

        # Create some basic events and locks
        self.done = Event()

    def _make_logging_handler(self) -> logging.Handler:
        """Create the logging handler for your class"""
        return logging.StreamHandler()

    def _make_logger(self, name: Optional[str] = None):
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
            hnd = self._make_logging_handler()
            new_logger.addHandler(hnd)
        return new_logger

    @classmethod
    def list_agents(cls) -> List[Callable]:
        agents = []
        for n in dir(cls):
            o = getattr(cls, n)
            if hasattr(o, '_colmena_agent'):
                agents.append(o)
        return agents

    def run(self):
        self.logger.info(f"{self.__class__.__name__} started")

        threads = []
        functions = self.list_agents()
        with ThreadPoolExecutor(max_workers=len(functions)) as executor:
            # Submit all of the worker threads
            for f in functions:
                threads.append(executor.submit(f, self))
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
