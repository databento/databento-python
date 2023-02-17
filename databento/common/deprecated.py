import functools
import warnings
from typing import Any


def deprecated(func: Any) -> Any:
    @functools.wraps(func)
    def new_func(*args: Any, **kwargs: Any) -> Any:
        warnings.simplefilter("always", DeprecationWarning)
        warnings.warn(func.__doc__, category=DeprecationWarning)
        return func(*args, **kwargs)

    return new_func
