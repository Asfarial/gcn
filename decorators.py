from typing import Callable
from core import Verbose


def message(msg: str) -> Callable:
    """
       Decorator to print
       Message on start and skip row on end of a
       function execution
    """
    def decorator(func: Callable) -> Callable:
        def inner(*args, **kwargs):
            Verbose.message(msg)
            results = func(*args, **kwargs)
            Verbose.message("")
            return results
        return inner
    return decorator
