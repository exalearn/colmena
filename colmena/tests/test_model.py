"""Tests for the data models"""
import sys
from typing import Any, Tuple, Dict, List, Optional
from pathlib import Path

from colmena.models import ResourceRequirements, ExecutableTask, Result


class EchoTask(ExecutableTask):
    def __init__(self):
        super().__init__(executable=['echo'])

    def preprocess(self, run_dir: Path, args: Tuple[Any], kwargs: Dict[str, Any]) -> Tuple[List[str], Optional[str]]:
        return list(map(str, args)), None

    def postprocess(self, run_dir: Path) -> Any:
        return (run_dir / 'colmena.stdout').read_text()


def test_resources():
    res = ResourceRequirements(node_count=2, cpu_processes=4)
    assert res.total_ranks == 8


def test_message_sizes():
    result = Result(
        (('0' * 8,), {'test': '0' * 8}),
        method='test',
        keep_inputs=False,
        serialization_method='json',
    )

    # Make sure the size of the inputs is stored
    result.serialize()
    assert result.message_sizes['inputs'] >= 2 * sys.getsizeof('0' * 8)
    assert 'value' not in result.message_sizes

    # Add a result
    result.deserialize()
    result.set_result(1, 1)
    result.serialize()
    assert result.message_sizes['inputs'] >= 2 * sys.getsizeof('0' * 8)
    assert result.message_sizes['inputs'] >= sys.getsizeof(1)


def test_executable_task():
    # Run a basic tak
    task = EchoTask()
    assert task.executable == ['echo']
    assert task(1) == '1\n'

    # Run an "MPI task"
    task.mpi = True
    task.mpi_command_string = 'aprun -N {total_ranks} -n {cpu_processes} --cc depth'
    assert task.render_mpi_launch(ResourceRequirements(node_count=2, cpu_processes=4)) == 'aprun -N 8 -n 4 --cc depth'

    task.mpi_command_string = 'echo -N {total_ranks} -n {cpu_processes} --cc depth'
    assert task(1, _resources=ResourceRequirements(node_count=2, cpu_processes=3)) == '-N 6 -n 3 --cc depth echo 1\n'
