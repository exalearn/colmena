import logging
import os
import redis

from typing import Any, Union


logger = logging.getLogger(__name__)

VALUE_SERVER_HOST_ENV_VAR = 'COLMENA_VALUE_SERVER_HOST'
VALUE_SERVER_PORT_ENV_VAR = 'COLMENA_VALUE_SERVER_PORT'

_server = None


class ValueServerReference:
    def __init__(self, obj: Any):
        # TODO(gpauloski): in the future we want a more robust way of getting
        # a unique key for an object to use at its value server reference.
        self.key = id(obj)


class ValueServer:
    def __init__(self, hostname: str, port: int):
        self.redis_client = redis.StrictRedis(
            host=hostname, port=port, decode_responses=True)

    def get(self, ref: ValueServerReference) -> Any:
        return self.redis_client.get(ref.key)

    def put(self, value: Any) -> ValueServerReference:
        ref = ValueServerReference(value)
        self.redis_client.set(ref.key, value)
        return ref


def init_value_server(hostname: str = None, port: int = None) -> None:
    global _server

    if _server is not None:
        return

    if hostname is None:
        if VALUE_SERVER_HOST_ENV_VAR in os.environ:
            hostname = os.environ.get(VALUE_SERVER_HOST_ENV_VAR)
        else:
            raise ValueError('The value server hostname must be passed as '
                             'an argument or set as an environment variable')

    if port is None:
        if VALUE_SERVER_PORT_ENV_VAR in os.environ:
            port = int(os.environ.get(VALUE_SERVER_PORT_ENV_VAR))
        else:
            raise ValueError('The value server port must be passed as '
                             'an argument or set as an environment variable')
    
    _server = ValueServer(hostname, port)


def dereference(possible_references: Union[object, list, tuple, dict]) -> Any:
    if _server is None:
        init_value_server()

    def get_if_reference(obj: Any):
        if isinstance(obj, ValueServerReference):
            return _server.get(obj)
        return obj

    if isinstance(possible_references, list):
        return [get_if_reference(obj) for obj in possible_references]
    
    if isinstance(possible_references, tuple):
        return tuple([get_if_reference(obj) for obj in possible_references])

    if isinstance(possible_references, dict):
        return {key: get_if_reference(value) 
                for key, value in possible_references.items()}

    return get_if_reference(possible_references)


def put(value: Any) -> ValueServerReference:
    if _server is None:
        init_value_server()

    return _server.put(value)

