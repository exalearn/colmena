"""Exceptions type specific to the colmena"""


class KillSignalException(BaseException):
    """Server has received a singal to stop"""


class TimeoutException(BaseException):
    """Timeout on listening to a queue has occurred"""
