import json
import logging
import pickle as pkl
import shlex
import sys
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from io import StringIO
from pathlib import Path
from subprocess import run
from tempfile import TemporaryDirectory
from time import perf_counter
from traceback import TracebackException
from typing import Any, Tuple, Dict, Optional, Union, List
from uuid import uuid4

from pydantic import BaseModel, Field, Extra
import proxystore as ps

from colmena.proxy import proxy_json_encoder

logger = logging.getLogger(__name__)


# TODO (wardlt): Merge with FuncX's approach?
class SerializationMethod(str, Enum):
    """Serialization options"""

    JSON = "json"  # Serialize using JSON
    PICKLE = "pickle"  # Pickle serialization

    @staticmethod
    def serialize(method: 'SerializationMethod', data: Any) -> str:
        """Serialize an object using a specified method

        Args:
            method: Method used to serialize the object
            data: Object to be serialized
        Returns:
            Serialized data
        """

        if method == "json":
            return json.dumps(data)
        elif method == "pickle":
            return pkl.dumps(data).hex()
        else:
            raise NotImplementedError(f'Method {method} not yet implemented')

    @staticmethod
    def deserialize(method: 'SerializationMethod', message: str) -> Any:
        """Deserialize an object

        Args:
            method: Method used to serialize the message
            message: Message to deserialize
        Returns:
            Result object
        """

        if method == "json":
            return json.loads(message)
        elif method == "pickle":
            return pkl.loads(bytes.fromhex(message))
        else:
            raise NotImplementedError(f'Method {method} not yet implemented')


class FailureInformation(BaseModel):
    """Stores information about a task failure"""

    exception: str = Field(..., description="The exception returned by the failed task")
    traceback: Optional[str] = Field(None, description="Full stack trace for exception, if available")

    @classmethod
    def from_exception(cls, exc: BaseException) -> 'FailureInformation':
        tb = TracebackException.from_exception(exc)
        return cls(exception=repr(exc), traceback="".join(tb.format()))


class WorkerInformation(BaseModel, extra=Extra.allow):
    """Information about the worker that executed this task"""

    hostname: Optional[str] = Field(None, description='Hostname of the worker who executed this task')


class ResourceRequirements(BaseModel):
    """Resource requirements for tasks. Used by some Colmena backends to allocate resources to the task

    Follows the naming conventions of
    `RADICAL-Pilot <https://radicalpilot.readthedocs.io/en/stable/apidoc.html#taskdescription>`_.
    """

    # Defining how we use CPU resources
    node_count: int = Field(1, description='Total number of nodes to use for the task')
    cpu_processes: int = Field(1, description='Total number of MPI ranks per node')
    cpu_threads: int = Field(1, description='Number of threads per process')

    @property
    def total_ranks(self) -> int:
        """Total number of MPI ranks"""
        return self.node_count * self.cpu_processes


class Result(BaseModel):
    """A class which describes the inputs and results of the calculations evaluated by the MethodServer

    Each instance of this class stores the inputs and outputs to the function along with some tracking
    information allowing for performance analysis (e.g., time submitted to Queue, time received by client).
    All times are listed as Unix timestamps.

    The Result class also handles serialization of the data to be transmitted over a RedisQueue
    """

    # Core result information
    task_id: str = Field(default_factory=lambda: str(uuid4()), description='Unique identifier for each task')
    inputs: Union[Tuple[Tuple[Any, ...], Dict[str, Any]], str] = \
        Field(None, description="Input to a function. Positional and keyword arguments. The `str` data type "
                                "is for internal use and is used when communicating serialized objects.")
    value: Any = Field(None, description="Output of a function")
    method: Optional[str] = Field(None, description="Name of the method to run.")
    success: Optional[bool] = Field(None, description="Whether the task completed successfully")

    # Store task information
    task_info: Optional[Dict[str, Any]] = Field(default_factory=dict,
                                                description="Task tracking information to be transmitted "
                                                            "along with inputs and results. User provided")
    resources: ResourceRequirements = Field(default_factory=ResourceRequirements, help='List of the resources required for a task, if desired')
    failure_info: Optional[FailureInformation] = Field(None, description="Messages about task failure. Provided by Task Server")
    worker_info: Optional[WorkerInformation] = Field(None, description="Information about the worker which executed a task. Provided by Task Server")

    # Performance tracking
    time_created: float = Field(None, description="Time this value object was created")
    time_input_received: float = Field(None, description="Time the inputs was received by the task server")
    time_compute_started: float = Field(None, description="Time workflow process began executing a task")
    time_compute_ended: float = Field(None, description="Time workflow process finished executing a task")
    time_result_sent: float = Field(None, description="Time message was sent from the server")
    time_result_received: float = Field(None, description="Time value was received by client")
    time_start_task_submission: float = Field(None, description="Time marking the start of the task submission to workflow engine")
    time_task_received: float = Field(None, description="Time task result received from workflow engine")

    time_running: float = Field(None, description="Runtime of the method, if available")
    time_serialize_inputs: float = Field(None, description="Time required to serialize inputs on client")
    time_deserialize_inputs: float = Field(None, description="Time required to deserialize inputs on worker")
    time_serialize_results: float = Field(None, description="Time required to serialize results on worker")
    time_deserialize_results: float = Field(None, description="Time required to deserialize results on client")
    time_async_resolve_proxies: float = Field(None,
                                              description="Time required to scan function inputs and start async resolves of proxies")

    additional_timing: dict = Field(default_factory=dict,
                                    description="Timings recorded by a TaskServer that are not defined by above")
    proxy_timing: Dict[str, Dict[str, dict]] = Field(default_factory=dict,
                                                     description='Timings related to resolving ProxyStore proxies on the compute worker')

    # Serialization options
    serialization_method: SerializationMethod = Field(SerializationMethod.JSON,
                                                      description="Method used to serialize input data")
    keep_inputs: bool = Field(True, description="Whether to keep the inputs with the result object or delete "
                                                "them after the method has completed")
    proxystore_name: Optional[str] = Field(None, description="Name of ProxyStore backend you use for transferring large objects")
    proxystore_type: Optional[str] = Field(None, description="Type of ProxyStore backend being used")
    proxystore_kwargs: Optional[Dict] = Field(None, description="Kwargs to reinitialize ProxyStore backend")
    proxystore_threshold: Optional[int] = Field(None,
                                                description="Proxy all input/output objects larger than this threshold in bytes")

    def __init__(self, inputs: Tuple[Tuple[Any], Dict[str, Any]], **kwargs):
        """
        Args:
             inputs (Any, Dict): Inputs to a function. Separated into positional and keyword arguments
        """
        super().__init__(inputs=inputs, **kwargs)

        # Mark "created" only if the value is not already set
        if 'time_created' not in kwargs:
            self.time_created = datetime.now().timestamp()

    @property
    def args(self) -> Tuple[Any]:
        return tuple(self.inputs[0])

    @property
    def kwargs(self) -> Dict[str, Any]:
        return self.inputs[1]

    def json(self, **kwargs: Dict[str, Any]) -> str:
        """Override json encoder to use a custom encoder with proxy support"""
        if 'exclude' in kwargs:
            # Make a shallow copy of the user passed excludes
            user_exclude = kwargs['exclude'].copy()
            if isinstance(kwargs['exclude'], dict):
                kwargs['exclude'].update({'inputs': True, 'value': True})
            if isinstance(kwargs['exclude'], set):
                kwargs['exclude'].update({'inputs', 'value'})
            else:
                raise ValueError(
                    f'Unsupported type {type(kwargs["exclude"])} for argument "exclude". Expected set or dict')
        else:
            user_exclude = set()
            kwargs['exclude'] = {'inputs', 'value'}

        # Use pydantic's encoding for everything except `inputs` and `values`
        data = super().dict(**kwargs)

        # Add inputs/values back to data unless the user excluded them
        if isinstance(user_exclude, set):
            if 'inputs' not in user_exclude:
                data['inputs'] = self.inputs
            if 'value' not in user_exclude:
                data['value'] = self.value
        elif isinstance(user_exclude, dict):
            if not user_exclude['inputs']:
                data['inputs'] = self.inputs
            if not user_exclude['value']:
                data['value'] = self.value

        # Jsonify with custom proxy encoder
        return json.dumps(data, default=proxy_json_encoder)

    def mark_result_received(self):
        """Mark that a completed computation was received by a client"""
        self.time_result_received = datetime.now().timestamp()

    def mark_input_received(self):
        """Mark that a task server has received a value"""
        self.time_input_received = datetime.now().timestamp()

    def mark_compute_started(self):
        """Mark that the compute for a method has started"""
        self.time_compute_started = datetime.now().timestamp()

    def mark_result_sent(self):
        """Mark when a result is sent from the task server"""
        self.time_result_sent = datetime.now().timestamp()

    def mark_start_task_submission(self):
        """Mark when the Task Server submits a task to the engine"""
        self.time_start_task_submission = datetime.now().timestamp()

    def mark_task_received(self):
        """Mark when the Task Server receives the task from the engine"""
        self.time_task_received = datetime.now().timestamp()

    def mark_compute_ended(self):
        """Mark when the task finished executing"""
        self.time_compute_ended = datetime.now().timestamp()

    def set_result(self, result: Any, runtime: float = None):
        """Set the value of this computation

        Automatically sets the "time_result_completed" field and, if known, defines the runtime.

        Will delete the inputs to the function if the user specifies ``self.return_inputs == False``.
        Removing the inputs once the result is known can save communication time

        Args:
            result: Result to be stored
            runtime (float): Runtime for the function
        """
        self.value = result
        if not self.keep_inputs:
            self.inputs = ((), {})
        self.time_running = runtime
        self.success = True

    def serialize(self) -> float:
        """Stores the input and value fields as a pickled objects

        Returns:
            (float) Time to serialize
        """
        start_time = perf_counter()
        _value = self.value
        _inputs = self.inputs

        def _serialize_and_proxy(value, evict=False):
            """Helper function for serializing and proxying"""
            # Serialized object before proxying to compare size of serialized
            # object to value server threshold. Using sys.getsizeof would be
            # faster but sys.getsizeof does not account for the memory
            # consumption of objects that value refers to
            value_str = SerializationMethod.serialize(
                self.serialization_method, value
            )

            if (
                    self.proxystore_name is not None and
                    self.proxystore_threshold is not None and
                    not isinstance(value, ps.proxy.Proxy) and
                    sys.getsizeof(value_str) >= self.proxystore_threshold
            ):
                # Proxy the value. We use the id of the object as the key
                # so multiple copies of the object are not added to ProxyStore,
                # but the value in ProxyStore will still be updated.
                store = ps.store.get_store(self.proxystore_name)
                if store is None:
                    store = ps.store.init_store(
                        self.proxystore_type,
                        name=self.proxystore_name,
                        **self.proxystore_kwargs
                    )
                value_proxy = store.proxy(value, evict=evict)
                logger.debug(f'Proxied object of type {type(value)} with id={id(value)}')
                # Serialize the proxy with Colmena's utilities. This is
                # efficient since the proxy is just a reference and metadata
                value_str = SerializationMethod.serialize(
                    self.serialization_method, value_proxy
                )

            return value_str

        try:
            # Each value in *args and **kwargs is serialized independently
            args = tuple(map(_serialize_and_proxy, _inputs[0]))
            kwargs = {k: _serialize_and_proxy(v) for k, v in _inputs[1].items()}
            self.inputs = (args, kwargs)

            # The entire result is serialized as one object. Pass evict=True
            # so the value is evicted from the value server once it is resolved
            # by the thinker.
            if _value is not None:
                self.value = _serialize_and_proxy(_value, evict=True)

            return perf_counter() - start_time
        except Exception as e:
            # Put the original values back
            self.inputs = _inputs
            self.value = _value
            raise e

    def deserialize(self) -> float:
        """De-serialize the input and value fields

        Returns:
            (float) The time required to deserialize
        """
        # Check that the data is actually a string
        start_time = perf_counter()
        _value = self.value
        _inputs = self.inputs

        def _deserialize(value):
            if not isinstance(value, str):
                return value
            return SerializationMethod.deserialize(self.serialization_method, value)

        if isinstance(_inputs, str):
            _inputs = SerializationMethod.deserialize(self.serialization_method, _inputs)

        try:
            # Deserialize each value in *args and **kwargs
            args = tuple(map(_deserialize, _inputs[0]))
            kwargs = {k: _deserialize(v) for k, v in _inputs[1].items()}
            self.inputs = (args, kwargs)

            # Deserialize result if it exists
            if _value is not None:
                self.value = _deserialize(_value)

            return perf_counter() - start_time
        except Exception as e:
            # Put the original values back
            self.inputs = _inputs
            self.value = _value
            raise e


@dataclass
class ExecutableTask:
    """Base class for a Colmena task that involves running an executable using a system call.

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
    """

    executable: List[str]
    """Command used to launch the executable"""

    mpi: bool = False
    """Whether this is an MPI executable"""

    mpi_command_string: Optional[str] = None
    """Template string defining how to launch this application using MPI.
    Should include placeholders named after the fields in ResourceRequirements marked using {}'s.
    Example: `mpirun -np {total_ranks}`"""

    @property
    def __name__(self):
        return self.__class__.__name__.lower()

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
            kwargs: Keyword arguments to
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

    def __call__(self, *args, _resources: Optional[ResourceRequirements] = None, **kwargs):
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
