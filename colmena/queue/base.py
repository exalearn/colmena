"""Base classes for queues and related functions"""
from abc import abstractmethod
from typing import Optional, Tuple


class BaseQueue:
    """Base class for a queue used in Colmena.

    Follows the basic ``get`` and ``put`` semantics of most queues,
    with the addition of a "topic" used by Colmena to separate
    task requests or objects used by """

    @abstractmethod
    def get(self, timeout: int = None, topic: str = None) -> Optional[Tuple[str, str]]:
        """Get an item from the redis queue

        Args:
            timeout (int): Timeout for the blocking get in seconds
            topic (str): Which topical queue to read from. If ``None``, wait for all topics
        Returns:
            If timeout occurs, output is ``None``. Else:
            - (str) Topic of the item
            - (str) Value from the redis queue
        """
        pass

    @abstractmethod
    def put(self, input_data: str, topic: str = 'default'):
        """Push data to a Redis queue

        Args:
            input_data (str): Message to be sent
            topic (str): Topic of the queue. Use ``None
        """
        pass

    @abstractmethod
    def flush(self):
        """Flush the Redis queue"""
        pass
