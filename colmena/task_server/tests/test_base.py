from typing import Any, Dict, Tuple, List, Optional
from pathlib import Path

from proxystore.store import unregister_store
from proxystore.store.file import FileStore
from pytest import fixture

from colmena.models import Result, ExecutableTask, SerializationMethod
from colmena.task_server.base import run_and_record_timing


# TODO (wardlt): Figure how to import this from test_models
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


@fixture
def store(tmpdir):
    store = FileStore(name='store', store_dir=tmpdir, stats=True)
    yield store
    unregister_store('store')


def test_run_function(store):
    """Make sure the run function behaves as expected:

    - Records runtimes
    - Tracks proxy statistics
    """

    # Make the result and configure it to use the store
    result = Result(inputs=(('a' * 1024,), {}))
    result.proxystore_name = store.name
    result.proxystore_type = f'{store.__class__.__module__}.{store.__class__.__name__}'
    result.proxystore_threshold = 128
    result.proxystore_kwargs = store.kwargs

    # Serialize it
    result.serialization_method = SerializationMethod.PICKLE
    result.serialize()

    # Run the function
    run_and_record_timing(lambda x: x.upper(), result)

    # Make sure the timings are all set
    assert result.time_running > 0
    assert result.time_async_resolve_proxies > 0
    assert result.time_deserialize_inputs > 0
    assert result.time_serialize_results > 0
    assert result.time_compute_ended > result.time_compute_started

    # Make sure we have stats for both proxies
    assert len(result.proxy_timing) == 2
    assert all('set_bytes' in v for v in result.proxy_timing.values())
