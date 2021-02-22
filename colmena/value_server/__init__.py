"""Implementation of the value server"""

from colmena.value_server.proxy import async_resolve_proxies
from colmena.value_server.proxy import to_proxy
from colmena.value_server.proxy import to_proxy_threshold
from colmena.value_server.proxy import ObjectProxy

from colmena.value_server.server import init_value_server
from colmena.value_server.server import LRUCache
from colmena.value_server.server import VALUE_SERVER_HOST_ENV_VAR
from colmena.value_server.server import VALUE_SERVER_PORT_ENV_VAR

global server
server = None

__all__ = [
    'async_resolve_proxies',
    'to_proxy',
    'to_proxy_threshold',
    'ObjectProxy',
    'init_value_server',
    'server',
    'LRUCache',
    'VALUE_SERVER_HOST_ENV_VAR',
    'VALUE_SERVER_PORT_ENV_VAR'
]
