from typing import Tuple, List, Optional, Dict, Any
from collections.abc import Generator
from pathlib import Path
from math import isnan

from pytest import fixture

from colmena.models.tasks import ExecutableTask, PythonTask, PythonGeneratorTask
from colmena.models import ResourceRequirements, Result


class EchoTask(ExecutableTask):
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


def echo(x: Any) -> Any:
    return x


def generator(i: int) -> Generator[int, None, str]:
    """Generator which iterates over a list then returns a string saying "done" """
    yield from range(i)
    return "done"


def test_python_task(result):
    """A standard Python function"""

    # Make sure name resolution functions as intended
    task = PythonTask(function=echo)
    assert task.name == 'echo'

    task = PythonTask(function=echo, name='test')
    assert task.name == 'test'

    # Ensure it sets the task timings while running
    assert isnan(result.timestamp.compute_started)
    result = task(result)
    assert not isnan(result.timestamp.compute_ended)

    assert isinstance(result.value, str)  # Data is still serialized
    result.deserialize()
    assert result.value == 1


def test_generator(result):
    task = PythonGeneratorTask(function=generator, store_return_value=False)
    assert task.name == 'generator'

    result = task(result)
    assert result.success, result.failure_info.traceback
    result.deserialize()
    assert result.value == [0,]


def test_generator_with_return(result):
    task = PythonGeneratorTask(function=generator, name='with_return', store_return_value=True)
    assert task.name == 'with_return'

    result = task(result)
    assert result.success, result.failure_info.traceback
    result.deserialize()
    assert result.value == [[0,], 'done']


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
