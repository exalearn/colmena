from typing import Any, Dict, Tuple, List, Optional
from pathlib import Path

from colmena.models import Result, ExecutableTask

# TODO (wardlt): Figure how to import this from test_models
from colmena.task_server.base import run_and_record_timing


class EchoTask(ExecutableTask):
    def __init__(self):
        super().__init__(executable=['echo'])

    def preprocess(self, run_dir: Path, args: Tuple[Any], kwargs: Dict[str, Any]) -> Tuple[List[str], Optional[str]]:
        return list(map(str, args)), None

    def postprocess(self, run_dir: Path) -> Any:
        return (run_dir / 'colmena.stdout').read_text()


class FakeMPITask(ExecutableTask):
    def __init__(self):
        super().__init__(executable=['echo', '-n'],
                         mpi=True,
                         mpi_command_string='echo -N {total_ranks} -n {cpu_processes} --cc depth')

    def preprocess(self, run_dir: Path, args: Tuple[Any], kwargs: Dict[str, Any]) -> Tuple[List[str], Optional[str]]:
        return list(map(str, args)), None

    def postprocess(self, run_dir: Path) -> Any:
        return (run_dir / 'colmena.stdout').read_text()


def test_run_with_executable():
    result = Result(inputs=((1,), {}))
    func = EchoTask()
    run_and_record_timing(func, result)
    result.deserialize()
    assert result.value == '1\n'
