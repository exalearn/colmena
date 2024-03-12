"""Base classes used by Colmena to describe functions being executed by a workflow engine"""
import os
import shlex
import logging
import platform
from io import StringIO
from pathlib import Path
from subprocess import run
from tempfile import TemporaryDirectory
from time import perf_counter
from inspect import signature, isgeneratorfunction
from typing import Any, Dict, List, Tuple, Optional, Callable, Generator

from colmena.models.results import ResourceRequirements, Result, FailureInformation
from colmena.proxy import resolve_proxies_async, store_proxy_stats
from colmena.queue import ColmenaQueues


logger = logging.getLogger(__name__)


class ColmenaMethod:
    """Base wrapper for a Python function run as part of a Colmena workflow

    The wrapper handles the parts of running a Colmena task that are beyond running the function,
    such as serialization, timing, interfaces to ProxyStore.
    """

    name: str
    """Name used to identify the function"""

    @property
    def __name__(self):
        return self.name

    def function(self, *args, **kwargs) -> Any:
        """Function provided by the Colmena user"""
        raise NotImplementedError()

    def __call__(self, result: Result) -> Result:
        """Invoke a Colmena task request

        Args:
            result: Request, which inclues the arguments and will hold the result
        Returns:
            The input result object, populated with the results
        """
        # Mark that compute has started on the worker
        result.mark_compute_started()

        # Unpack the inputs
        result.time.deserialize_inputs = result.deserialize()

        # Start resolving any proxies in the input asynchronously
        start_time = perf_counter()
        input_proxies = []
        for arg in result.args:
            input_proxies.extend(resolve_proxies_async(arg))
        for value in result.kwargs.values():
            input_proxies.extend(resolve_proxies_async(value))
        result.time.async_resolve_proxies = perf_counter() - start_time

        # Add the worker information into the tasks, if available
        worker_info = {}
        # TODO (wardlt): Move this information into a separate, parsl-specific wrapper
        for tag in ['PARSL_WORKER_RANK', 'PARSL_WORKER_POOL_ID']:
            if tag in os.environ:
                worker_info[tag] = os.environ[tag]
        worker_info['hostname'] = platform.node()
        result.worker_info = worker_info

        # Determine additional kwargs to provide to the function
        additional_kwargs = {}
        for k, v in [('_resources', result.resources), ('_result', result)]:
            if k in result.kwargs:
                logger.warning(f'`{k}` provided as a kwargs. Unexpected things are about to happen')
            if k in signature(self.function).parameters:
                additional_kwargs[k] = v

        # Execute the function
        start_time = perf_counter()
        success = True
        try:
            output = self.function(*result.args, **result.kwargs, **additional_kwargs)
        except BaseException as e:
            output = None
            success = False
            result.failure_info = FailureInformation.from_exception(e)
        finally:
            end_time = perf_counter()

        # Store the results
        result.set_result(output, end_time - start_time)
        if not success:
            result.success = False
        result.mark_compute_ended()

        # Re-pack the results. Will store the proxy statistics
        result.time.serialize_results, _ = result.serialize()

        # Get the statistics for the proxy resolution
        for proxy in input_proxies:
            store_proxy_stats(proxy, result.time.proxy)

        return result


class PythonMethod(ColmenaMethod):
    """A Python function to be executed on a single worker

    Args:
        function: Generator function to be executed
        name: Name of the function. Defaults to `function.__name__`
    """

    function: Callable

    def __init__(self, function: Callable, name: Optional[str] = None) -> None:
        if isgeneratorfunction(function):
            raise ValueError('Function is a generator function. Use `PythonGeneratorTask` instead.')
        self.name = name or function.__name__
        self.function = function


class PythonGeneratorMethod(ColmenaMethod):
    """Python function which runs on a single worker and generates results iteratively

    Generator functions support streaming each iteration of the generator
    to the Thinker when a `streaming_queue` is provided.

    Args:
        function: Generator function to be executed
        name: Name of the function. Defaults to `function.__name__`
        store_return_value: Whether to capture the `return value <https://docs.python.org/3/reference/simple_stmts.html#the-return-statement>`_
            of the generator and store it in the Result object.
    """

    def __init__(self,
                 function: Callable[..., Generator],
                 name: Optional[str] = None,
                 store_return_value: bool = False,
                 streaming_queue: Optional[ColmenaQueues] = None) -> None:
        if not isgeneratorfunction(function):
            raise ValueError('Function is not a generator function. Use `PythonTask` instead.')
        self._function = function
        self.name = name or function.__name__
        self.store_return_value = store_return_value
        self.streaming_queue = streaming_queue

    def stream_result(self, y: Any, result: Result, start_time: float):
        """Send an intermediate result using the task queue

        Args:
            y: Intermediate result
            result: Result package carrying task metadata
            start_time: Start time of the algorithm, used to report
        """

        # Store the intermediate result in a copy of the input object
        result = result.copy(deep=True)
        result.set_result(
            y, perf_counter() - start_time, intermediate=True,
        )
        result.time.serialize_results, _ = result.serialize()

        # Send it back to the queue
        self.streaming_queue.send_result(result)

    def function(self, *args, _result: Result, **kwargs) -> Any:
        """Run the Colmena task and collect intermediate results to provide as a list"""

        # TODO (wardlt): Make push to task queue asynchronous
        gen = self._function(*args, **kwargs)
        iter_results = []
        start_time = perf_counter()
        while True:
            try:
                y = next(gen)
                if self.streaming_queue is None:
                    iter_results.append(y)
                else:
                    self.stream_result(y, _result, start_time)
            except StopIteration as e:
                if self.streaming_queue is not None:
                    if self.store_return_value:
                        return e.value
                elif self.store_return_value:
                    return iter_results, e.value
                else:
                    return iter_results


class ExecutableMethod(ColmenaMethod):
    """Task that involves running an executable using a system call.

    Such tasks often include a "pre-processing" step in Python that prepares inputs for the executable
    and a "post-processing" step which stores the outputs (either produced from stdout or written to files)
    as Python objects.

    Separating the task into these two functions and a system call for launching the program
    simplifies development (shorter functions that ar easier to test), and allows some workflow
    engines to improve performance by running processing and execution tasks separately.

    Implement a new ExecutableTask by defining the executable, a preprocessing method (:meth:`preprocess`),
    and a postprocessing method (:meth:`postprocess`).

    Use the ExecutableTask by instantiating a copy of your new class and then passing it to the task server
    as you would with any other function.

    **MPI Executables**

    Launching an MPI executable requires two parts: a path to an executable and a preamble defining how to launch it.
    Defining an MPI application using the instructions described above and then set the :attr:`mpi` attribute to ``True``.
    This will tell the Colmena task server to look for a "preamble" for how to launch the application.

    You may need to supply an MPI command invocation recipe for your particular cluster, depending on your choice of task server.
    Supply a template as the ``mpi_command_string`` field, which will be converted
    by `Python's string format function <https://docs.python.org/3/library/string.html#format-string-syntax>`_
    to produce a version of the command with the specific resource requirements of your task
    by the :meth:`render_mpi_launch` method.
    The attributes of this class (e.g., ``node_count``, ``total_ranks``) will be used as arguments to `format`.
    For example, a template of ``aprun -N {total_ranks} -n {cpu_process}`` will produce ``aprun -N 6 -n 3`` if you
    specify ``node_count=2`` and ``cpu_processes=3``.

    Args:
        executable: List of executable arguments
        name: Name used for the task. Defaults to ``executable[0]``
        mpi: Whether to use MPI to launch the exectuable
        mpi_command_string: Template for MPI launcher. See :attr:`mpi_command_string`.
    """

    executable: List[str]
    """Command used to launch the executable"""

    mpi: bool = False
    """Whether this is an MPI executable"""

    mpi_command_string: Optional[str] = None
    """Template string defining how to launch this application using MPI.
    Should include placeholders named after the fields in ResourceRequirements marked using {}'s.
    Example: `mpirun -np {total_ranks}`"""

    def __init__(self, executable: List[str], name: Optional[str] = None,
                 mpi: bool = False, mpi_command_string: Optional[str] = None) -> None:
        super().__init__()
        self.name = name or executable[0]
        self.executable = executable
        self.mpi = mpi
        self.mpi_command_string = mpi_command_string

    def render_mpi_launch(self, resources: ResourceRequirements) -> str:
        """Create an MPI launch command given the configuration

        Returns:
            MPI launch configuration
        """
        return self.mpi_command_string.format(total_ranks=resources.total_ranks,
                                              **resources.dict(exclude={'mpi_command_string'}))

    def preprocess(self, run_dir: Path, args: Tuple[Any], kwargs: Dict[str, Any]) -> Tuple[List[str], Optional[str]]:
        """Perform preprocessing steps necessary to prepare for executable to be started.

        These may include writing files to the local directory, creating CLI arguments,
        or standard input to be passed to the executable

        Args:
            run_dir: Path to a directory in which to write files used by an executable
            args: Arguments to the task, control how the run is set up
            kwargs: Keyword arguments to the function
        Returns:
            - Options to be passed as command line arguments to the executable
            - Values to pass to the standard in of the executable
        """
        raise NotImplementedError()

    def execute(self, run_dir: Path, arguments: List[str], stdin: Optional[str],
                resources: Optional[ResourceRequirements] = None) -> float:
        """Run an executable

        Args:
            run_dir: Directory in which to execute the code
            arguments: Command line arguments
            stdin: Content to pass in via standard in
            resources: Amount of resources to use for the application
        Returns:
            Runtime (unit: s)
        """

        # Make the shell command to be launched
        shell_cmd = self.assemble_shell_cmd(arguments, resources)
        logger.debug(f'Launching shell command: {" ".join(shell_cmd)}')

        # Launch it, routing the stdout and stderr as appropriate
        start_time = perf_counter()
        with open(run_dir / 'colmena.stdout', 'w') as fo, open(run_dir / 'colmena.stderr', 'w') as fe:
            if stdin is not None:
                stdin = StringIO(stdin)
            run(shell_cmd, stdout=fo, stderr=fe, stdin=stdin, cwd=run_dir)
        return perf_counter() - start_time

    def assemble_shell_cmd(self, arguments: List[str], resources: ResourceRequirements) -> List[str]:
        """Assemble the shell command to be launched

        Args:
            arguments: Command line arguments
            resources: Resource requirements
        Returns:
            Components of the shell command
        """

        # If resources are provided and the task is an MPI, generate the MPI executor
        if self.mpi:
            assert resources is not None, "Resources must be specified for MPI tasks"
            preamble = shlex.split(self.render_mpi_launch(resources))
        else:
            preamble = []

        # Get the full shell command
        shell_cmd = preamble + self.executable + arguments
        return shell_cmd

    def postprocess(self, run_dir: Path) -> Any:
        """Extract results after execution completes

        Args:
            run_dir: Run directory for the executable. Stdout will be written to `run_dir/colmena.stdout`
                    and stderr to `run_dir/colmena.stderr`
        """
        raise NotImplementedError()

    def function(self, *args, _resources: Optional[ResourceRequirements] = None, **kwargs):
        """Execute the function

        Args:
            args: Positional arguments
            kwargs: Keyword arguments
            _resources: Resources available. Optional. Only used for MPI tasks.
        """
        # Launch everything inside a temporary directory
        with TemporaryDirectory() as run_dir:
            run_dir = Path(run_dir)

            # Prepare the run directory
            cli_args, stdin = self.preprocess(run_dir, args, kwargs)

            # Execute everything
            self.execute(run_dir, cli_args, stdin, resources=_resources)

            # Return the post-processed results
            return self.postprocess(run_dir)
