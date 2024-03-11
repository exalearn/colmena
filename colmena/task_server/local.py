"""Use Python's :class:`~concurrent.futures.Executor` to run workers on a local system"""
from typing import Optional, Union, Callable, Collection
from concurrent.futures import Future, Executor, ThreadPoolExecutor, ProcessPoolExecutor

from colmena.models.results import Result

from .base import FutureBasedTaskServer, convert_to_colmena_method
from colmena.queue.base import ColmenaQueues
from colmena.models.methods import ColmenaMethod


class LocalTaskServer(FutureBasedTaskServer):
    """Use Python's native concurrent libraries to execute tasks"""

    def __init__(self,
                 queues: ColmenaQueues,
                 methods: Collection[Union[Callable, ColmenaMethod]],
                 threads: bool = True,
                 num_workers: Optional[int] = None):
        """
        Args:
            methods: Methods to be served
            queues: Queues used to commmunicate with thinker
            threads: Use threads instead of workers
            num_workers: Number of workers to deploy.
        """
        self._methods = dict(
            (m.name, m) for m in map(convert_to_colmena_method, methods)
        )
        self._executor: Optional[Executor] = None
        self.num_workers = num_workers
        self.threads = threads
        super().__init__(queues=queues, method_names=list(self._methods.keys()))

    def _submit(self, task: Result, topic: str) -> Optional[Future]:
        return self._executor.submit(self._methods[task.method], task)

    def _setup(self):
        self._executor = (
            ThreadPoolExecutor(self.num_workers)
            if self.threads else
            ProcessPoolExecutor(self.num_workers)
        )

    def _cleanup(self):
        self._executor.shutdown()
