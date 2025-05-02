from tempfile import TemporaryDirectory

from proxystore.connectors.file import FileConnector
from proxystore.store import Store, register_store, unregister_store
from pytest import fixture


@fixture
def store(tmp_path):
    with TemporaryDirectory() as tmpdir:
        with Store('store', FileConnector(tmpdir), metrics=True) as store:
            register_store(store)
            yield store
            unregister_store(store)
