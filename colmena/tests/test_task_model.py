from typing import Tuple, List, Optional, Dict, Any, Iterator
from pathlib import Path
from math import isnan

from pytest import fixture
from proxystore.connectors.file import FileConnector
from proxystore.store import Store
from proxystore.store import register_store
from proxystore.store import unregister_store

from colmena.models.methods import ExecutableMethod, PythonMethod, PythonGeneratorMethod
from colmena.models import ResourceRequirements, Result, SerializationMethod
from colmena.queue import PipeQueues


class EchoTask(ExecutableMethod):
    def __init__(self):
        super().__init__(executable=['echo'])

    def preprocess(self, run_dir: Path, args: Tuple[Any], kwargs: Dict[str, Any]) -> Tuple[List[str], Optional[str]]:
        return list(map(str, args)), None

    def postprocess(self, run_dir: Path) -> Any:
        return (run_dir / 'colmena.stdout').read_text()


@fixture
def result() -> Result:
    """Result object ready to be executed by a Task"""
    result = Result.from_args_and_kwargs((1,), method='echo')
    result.serialize()
    return result


@fixture
def store(tmpdir):
    with Store('store', FileConnector(tmpdir), metrics=True) as store:
        register_store(store)
        yield store
        unregister_store(store)


def echo(x: Any) -> Any:
    return x


def generator(i: int) -> Iterator[int]:
    """Generator which iterates over a list then returns a string saying "done" """
    yield from range(i)
    return "done"


def test_python_task(result):
    """A standard Python function"""

    # Make sure name resolution functions as intended
    task = PythonMethod(function=echo)
    assert task.name == 'echo'

    task = PythonMethod(function=echo, name='test')
    assert task.name == 'test'

    # Ensure it sets the task timings while running
    assert isnan(result.timestamp.compute_started)
    result = task(result)
    assert not isnan(result.timestamp.compute_ended)

    assert isinstance(result.value, str)  # Data is still serialized
    result.deserialize()
    assert result.value == 1


def test_generator(result):
    task = PythonGeneratorMethod(function=generator, store_return_value=False)
    assert task.name == 'generator'

    result = task(result)
    assert result.success, result.failure_info.traceback
    result.deserialize()
    assert result.value == [0,]


def test_generator_with_return(result):
    task = PythonGeneratorMethod(function=generator, name='with_return', store_return_value=True)
    assert task.name == 'with_return'

    result = task(result)
    assert result.success, result.failure_info.traceback
    result.deserialize()
    assert result.value == [[0,], 'done']


def test_generator_streaming(result):
    """Trigger streaming by adding a queue to the task definition"""

    queue = PipeQueues()
    task = PythonGeneratorMethod(function=generator, name='stream', store_return_value=True, streaming_queue=queue)

    result.topic = 'default'
    result = task(result)
    assert result.success, result.failure_info.traceback
    result.deserialize()
    assert result.value == 'done'
    assert result.complete

    intermediate = queue.get_result(timeout=1)
    assert intermediate.success
    intermediate.deserialize()
    assert not intermediate.complete
    assert intermediate.value == 0

    assert result.time.running >= intermediate.time.running


def test_executable_task(result):
    # Run a basic task
    task = EchoTask()
    assert task.name == 'echo'
    assert task.executable == ['echo']

    result = task(result)
    result.deserialize()
    assert result.success, result.failure_info.traceback
    assert result.value == '1\n'

    # Run an "MPI task"
    task.mpi = True
    task.mpi_command_string = 'aprun -N {total_ranks} -n {cpu_processes} --cc depth'
    assert task.render_mpi_launch(ResourceRequirements(node_count=2, cpu_processes=4)) == 'aprun -N 8 -n 4 --cc depth'

    task.mpi_command_string = 'echo -N {total_ranks} -n {cpu_processes} --cc depth'
    result.resources = ResourceRequirements(node_count=2, cpu_processes=3)
    result.serialize()
    result = task(result)
    result.deserialize()
    assert result.value == '-N 6 -n 3 --cc depth echo 1\n'


def test_run_function(store):
    """Make sure the run function behaves as expected:

    - Records runtimes
    - Tracks proxy statistics
    """

    # Make the result and configure it to use the store
    result = Result(inputs=(('a' * 1024,), {}))
    result.proxystore_name = store.name
    result.proxystore_threshold = 128
    result.proxystore_config = store.config()

    # Serialize it
    result.serialization_method = SerializationMethod.PICKLE
    result.serialize()

    # Run the function
    task = PythonMethod(lambda x: x.upper(), name='upper')
    result = task(result)

    # Make sure the timings are all set
    assert result.time.running > 0
    assert result.time.async_resolve_proxies > 0
    assert result.time.deserialize_inputs > 0
    assert result.time.serialize_results > 0
    assert result.timestamp.compute_ended >= result.timestamp.compute_started

    # Make sure we have stats for both proxies
    assert len(result.time.proxy) == 2
    assert all('store.proxy' in v['times'] for v in result.time.proxy.values())
