"""Implementation of the value server"""

from colmena.value_server.proxy import async_resolve_proxies  # noqa
from colmena.value_server.proxy import to_proxy  # noqa
from colmena.value_server.proxy import to_proxy_threshold  # noqa
from colmena.value_server.proxy import ObjectProxy  # noqa

from colmena.value_server.server import init_value_server  # noqa
from colmena.value_server.server import LRUCache  # noqa
from colmena.value_server.server import VALUE_SERVER_HOST_ENV_VAR  # noqa
from colmena.value_server.server import VALUE_SERVER_PORT_ENV_VAR  # noqa

global server
server = None
