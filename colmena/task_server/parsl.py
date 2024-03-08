"""Parsl task server and related utilities"""
import os
import shlex
import logging
import platform
import shutil
from concurrent.futures import Future
from functools import partial
from pathlib import Path
from tempfile import mkdtemp
from time import perf_counter
from datetime import datetime
from typing import Optional, List, Callable, Tuple, Dict, Union

import parsl
from parsl.app.app import AppBase
from parsl.app.bash import BashApp
from parsl.config import Config
from parsl.app.python import PythonApp
from colmena.models.methods import ExecutableMethod

from colmena.queue.base import ColmenaQueues
from colmena.models import Result, FailureInformation, ResourceRequirements
from colmena.proxy import resolve_proxies_async
from colmena.task_server.base import convert_to_colmena_method, FutureBasedTaskServer

logger = logging.getLogger(__name__)


# Functions related to splitting "ExecutableTasks" into multiple steps
def _execute_preprocess(task: ExecutableMethod, result: Result) -> Tuple[Result, Path, Tuple[List[str], Optional[str]]]:
    """Perform the pre-processing step for an executable task

    Must execute on the remote system

    Manages pulling inputs down from the ProxyStore,
     creating a temporary run directory,
     and calling ExecutableTask.preprocess to store
     any needed items in that run directory

    Args:
        task: Description of task to be executed
        result: Object holding the inputs
    Returns:
        - Updated result object
        - Path to the temporary directory
        - A tuple of the inputs to the executable
            - List of CLI arguments
            - Contents of stdin, if desired
    """

    # Mark that compute has started
    result.mark_compute_started()

    # Unpack the inputs
    result.time.deserialize_inputs = result.deserialize()

    # Start resolving any proxies in the input asynchronously
    start_time = perf_counter()
    resolve_proxies_async(result.args)
    resolve_proxies_async(result.kwargs)
    result.time.async_resolve_proxies = perf_counter() - start_time

    # Create a temporary directory
    #  TODO (wardlt): Figure out how to allow users to define a path for temporary directories
    temp_dir = Path(mkdtemp(prefix='colmena_'))

    # Execute the function
    start_time = perf_counter()
    try:
        output = task.preprocess(temp_dir, result.args, result.kwargs)
    except BaseException as e:
        output = ([], None)  # Set a null value that still matches the output type
        result.success = False
        result.failure_info = FailureInformation.from_exception(e)
    finally:
        end_time = perf_counter()

    # Record the time required to perform the pre-processing
    result.time.additional['exec_preprocess'] = end_time - start_time

    # Remove the inputs. We don't need to send them back to the manager (the manager already knows what it sent out)
    result.inputs = ((), {})

    return result, temp_dir, output


def _execute_postprocess(task: ExecutableMethod, exit_code: int, result: Result, temp_dir: Path, serialized_inputs: str):
    """Execute the post-processing function after an executable completes

    Args:
        task: Task description, which contains details on how to post-process the results
        exit_code: Exit code the application (should be 0)
        result: Storage for the result data
        temp_dir: Path to the run directory on the remote system
        serialized_inputs: Copy of the serialized inputs
            The ``Result`` object is currently without a copy of the inputs
    """

    # Execute the function
    start_time = perf_counter()
    try:
        output = task.postprocess(temp_dir)
        result.success = True
        shutil.rmtree(temp_dir)
    except BaseException as e:
        output = None
        result.success = False
        result.failure_info = FailureInformation.from_exception(e)
    finally:
        end_time = perf_counter()
    result.time.additional['exec_postprocess'] = end_time - start_time

    # Store the results
    if result.success:
        result.set_result(output, datetime.now().timestamp() - result.timestamp.compute_started)

    # Store the run time in the result object
    result.time.additional['exec_execution'] = (result.time.running -
                                                result.time.additional['exec_postprocess'] -
                                                result.time.additional['exec_preprocess'])

    # Add the worker information into the tasks, if available
    worker_info = {'hostname': platform.node()}
    result.worker_info = worker_info

    # Re-pack the results (will use proxystore, if able)
    result.time.serialize_results, _ = result.serialize()

    # Put the serialized inputs back, if desired
    if result.keep_inputs:
        result.inputs = serialized_inputs

    return result


def _execute_execute(task: ExecutableMethod, task_path: Path, arguments: List[str],
                     stdin: Optional[str], cpu_process_type: str, *,
                     stdout: str, stderr: str, pre_exec: str = None, **kwargs) -> str:
    """Execute the executable step of an executable task

    This function is executed after :meth:`__execute_preprocess` has completed, which means
    any necessary input files are in `task_path` and any necessary CLI arguments have been determined.

    It will be executed as a :class:`BashApp`, so it returns a run command given the arguments provided.

    The kwargs for the argument include resource requirements and other information used to communicate
    the task requirements to the Parsl Executor.
    The primary user of the such kwargs is the RCT backend for Parsl.

    Args:
        task: General task information. Includes the path to the executable
        task_path: Path to the run directory
        arguments: List of arguments to add to the function execution
        stdin: Data to be passed to the stdin (not currently supported)
        cpu_process_type: Process type "MPI" or "SINGLE." Used by RCT to determine how to execute the program
        pre_exec: List of environment variables to set
        stdout: Path to the stdout for the function (should be ``task_task_path // 'colmena.stdout`).
            Provided as a kwargs so that the Parsl executor knows where to write the file
        stderr: Path to the stderr for the function (should be ``task_task_path // 'colmena.stdout`).
            Provided as a kwargs so that the Parsl executor knows where to write the file
        kwargs: Extra keyword arguments define the resource requirements. See :class:`ResourceRequirements`.
    Returns:
        The function to invoke as a string
    """

    assert stdin is None or len(stdin) == 0, "Standard in is not supported yet"

    # Make the launch command
    resources = ResourceRequirements.parse_obj(kwargs)
    shell_cmd = task.assemble_shell_cmd(arguments, resources)

    # Create the shell command
    #  TODO (wardlt): This is shlex.join, which is only available in Py3.8+
    shell_cmd = " ".join(shlex.quote(str(s)) for s in shell_cmd)

    # Move to the run directory
    os.chdir(task_path)

    return shell_cmd


def _preprocess_callback(
        preprocess_future: Future,
        serialized_inputs: str,
        result: Result,
        task_server: 'ParslTaskServer',
        topic: str,
        execute_fun: AppBase,
        postprocess_fun: AppBase
):
    """Perform the next steps in an executable workflow

    If preprocessing was unsuccessful, send failed result back to client.
    If successful, submit the "execute task" to Parsl and attach a callback
    to that function which will submit the postprocess task.

    Args:
        preprocess_future: Future provided when submitting pre-process task to Parsl
        serialized_inputs: Original inputs, still in serialized form.
            We deserialize the inputs in `_execute_preprocess` and do not pass the input data back to this callback function to minimize data sent.
        result: Result object to be gradually updated
        task_server: Connection to the Parsl task server. Used to send results back to client
        topic: Topic of the task
        execute_fun: Parsl App that submits the execution task
        postprocess_fun: Parsl App that performs post-processing
    """

    # If the Parsl execution was unsuccessful, send the result back to the client
    if preprocess_future.exception() is not None:
        return task_server.perform_callback(preprocess_future, result, topic)

    # If successful, unpack the outputs
    result, temp_dir, (exec_args, exec_stdin) = preprocess_future.result()

    # If unsuccessful, send the results back to the client
    if result.success is not None and not result.success:

        logger.info('Result failed during preprocessing. Sending back to client.')

        # Send the serialized inputs back to the client if they are expected
        if result.keep_inputs:
            result.inputs = serialized_inputs

        # Store the time it took to run the preprocessing
        result.time.running = result.time.additional.get('exec_preprocess', 0)
        return task_server.queues.send_result(result)

    # If successful, submit the execute step and pass its result to Parsl
    logger.info(f'Preprocessing was successful for {result.method} task. Submitting to execute')

    exec_future: Future = execute_fun(temp_dir, exec_args, exec_stdin,
                                      stdout=str(temp_dir / 'colmena.stdout'),
                                      stderr=str(temp_dir / 'colmena.stderr'),
                                      **result.resources.dict())

    # Submit post-process to collect the results of the exec_function
    post_future: Future = postprocess_fun(exec_future, result, temp_dir, serialized_inputs if result.keep_inputs else None)

    # Once that function completes, you are ready to submit the task back to the client
    def _send_back(future: Future):
        # Send the results back to the client, if desired
        #  This is only used if the task fails. (Otherwise, the copy of "result" held in the future is used)
        if result.keep_inputs:
            result.inputs = serialized_inputs
        return task_server.perform_callback(future, result, topic)

    post_future.add_done_callback(_send_back)


class ParslTaskServer(FutureBasedTaskServer):
    """Task server based on Parsl

    Create a Parsl task server by first creating a resource configuration following
    the recommendations in `the Parsl documentation
    <https://parsl.readthedocs.io/en/stable/userguide/configuring.html>`_.
    Then instantiate a task server with a list of Python functions,
    configurations defining on which Parsl executors each function can run,
    and the Parsl resource configuration.
    The executor(s) for each function can be defined with a combination
    of per method specifications

    .. code-block:: python

        ParslTaskServer([(f, {'executors': ['a']})], queue, config)

    and also using a default executor

    .. code-block:: python

        ParslTaskServer([f], queue, config, default_executors=['a'])

    Further configuration options for each method can be defined
    in the list of methods.

    **Technical Details**

    The task server stores each of the supplied methods as Parsl "Apps".
    Tasks are launched on remote workers by calling these Apps,
    and results are placed in the result queue by callbacks attached the resultant Parsl Futures.

    The behavior of an :class:`ExecutableTask` involves several Apps and callbacks.

    1. A :class:`PythonApp` to invoke the "preprocessing" function that is given the :class:`Result`.

       The app produces a path to a temporary run directory containing the input files,
       content for the standard input of the executable,
       and an updated copy of the :class:`Result` object containing timing information.

       Note that the :class:`Result` object returned by this app lacks the inputs to reduce communication costs.

       Once complete (successfully or not), it invokes a callback which launches the next two tasks
       and creates the next callback.
       In the even of an unsuccessful execution, the callback function returns the failure information to the client and exits.

    2. A :class:`BashApp` to run the executable that is given the path to the run directory
       and the list of resources required for executing the task.

       There is no callback for app.

    3. A :class:`PythonApp` to store the results of the execution that is given the exit code of the executable (should be 0),
       a copy of the :class:`Result` object produced by the preprocessing,
       the path to the run directory,
       and a serialized version of the inputs to the app.

       The application parses the outputs from the execution, stores them in the :class:`Result` object,
       and then serializes results for transmission back to the client.
       The application also re-inserts the inputs if they are required to be sent back to the client.

       The callback for this function submits the outputs, if successful, or any failure information, if not, to the result queue.

    Every one of the Apps is run on the remote system as they may involve manipulating files
    on the remote system.
    """

    def __init__(self, methods: List[Union[Callable, Tuple[Callable, Dict]]],
                 queues: ColmenaQueues,
                 config: Config,
                 timeout: Optional[int] = None,
                 default_executors: Union[str, List[str]] = 'all'):
        """

        Args:
            methods (list): List of methods to be served.
                Each element in the list is either a function or a tuple where the first element
                is a function and the second is a dictionary of the arguments being used to create
                the Parsl ParslApp see `Parsl documentation
                <https://parsl.readthedocs.io/en/stable/stubs/parsl.app.app.python_app.html#parsl.app.app.python_app>`_.
            queues: Queues for the task server
            config: Parsl configuration
            timeout (int): Timeout, if desired
            default_executors: Executor or list of executors to use by default.
        """
        # Get a list of default executors
        if default_executors == 'all':
            default_executors = [e.label for e in config.executors]

        # Store the Parsl configuration
        self.config = config

        # Assemble the list of methods
        self.methods_: Dict[str, Tuple[AppBase, str]] = {}  # Store the method and its type
        self.exec_apps_: Dict[str, Tuple[AppBase, AppBase]] = {}  # Stores the execute and post-process apps for ExecutableTasks
        for method in methods:
            # Get the options or use the defaults
            if isinstance(method, (tuple, list)):
                if len(method) != 2:
                    raise ValueError('Method description should a tuple of length 2')
                function, options = method
            else:
                function = method
                options = {'executors': default_executors}
                logger.info(f'Using default executors for {function.__name__}: {default_executors}')

            # Convert the function to a Colmena task
            function = convert_to_colmena_method(method)
            name = function.name

            # If the function is not an executable, submit it as a single task
            if not isinstance(function, ExecutableMethod):
                app = PythonApp(function, **options)
                self.methods_[name] = (app, 'basic')
            else:
                logger.info(f'Building a chain of apps for an ExecutableTask, {function.__name__}')
                # If it is an executable, the function we launch initially is a "preprocess inputs" function
                preprocess_fun = partial(_execute_preprocess, function)
                preprocess_fun.__name__ = f'{name}_preprocess'
                preprocess_app = PythonApp(preprocess_fun, **options)

                # Make executable app, which is just to invoke the executable
                execute_fun = partial(_execute_execute, function, cpu_process_type='MPI' if function.mpi else 'SINGLE')
                execute_fun.__name__ = f'{name}_execute'
                execute_app = BashApp(execute_fun, **options)

                # Make the post-process app, which gathers the results and puts them in the "result object"
                postprocess_fun = partial(_execute_postprocess, function)
                postprocess_fun.__name__ = f'{name}_postprocess'
                postprocess_app = PythonApp(postprocess_fun, **options)

                # Store them for use during submission phase
                self.methods_[name] = (preprocess_app, 'exec')
                self.exec_apps_[name] = (execute_app, postprocess_app)

        logger.info(f'Defined {len(self.methods_)} methods: {", ".join(self.methods_.keys())}')

        # Initialize the base class
        super().__init__(queues, self.methods_.keys(), timeout)

    def _submit(self, task: Result, topic: str) -> Optional[Future]:
        # Determine which method to run
        method = task.method

        # Submit the application
        task.mark_start_task_submission()
        function, func_type = self.methods_[method]
        serialized_inputs = task.inputs  # Hold a copy of the original inputs. Used for "exec" apps to minimize
        future: Future = function(task)
        logger.debug('Pushed task to Parsl')
        # TODO (wardlt): Implement "resubmit if task returns a new future." or the ability to launch Parsl workflows with >1 step

        # Depending on the task type, return a different future
        if func_type == 'basic':
            # For most functions, just return the future so the task server will handle the output
            return future
        elif func_type == 'exec':
            # For executable functions, we have a different route for returning results
            exec_app, post_app = self.exec_apps_[method]
            # TODO (wardlt): Use a join_app rather than callback?
            future.add_done_callback(lambda x: _preprocess_callback(x, serialized_inputs, task, self, topic, exec_app, post_app))
            return None  # `None` prevents the Task Server from adding its own callback
        else:
            raise ValueError(f'Unrecognized function type: {func_type}')

    def _cleanup(self):
        """Close out any resources needed by the task server"""
        # Wait until all tasks have finished
        dfk = parsl.dfk()
        dfk.wait_for_current_tasks()
        dfk.cleanup()
        logger.info(f"All tasks have completed for {self.__class__.__name__} on {self.ident}")

    def _setup(self):
        # Launch the Parsl workflow engine
        parsl.load(self.config)
        logger.info(f"Launched Parsl DFK. Process id: {os.getpid()}")
