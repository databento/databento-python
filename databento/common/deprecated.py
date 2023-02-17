import functools
import warnings
from typing import Any


def deprecated(func: Any) -> Any:
    @functools.wraps(func)
    def new_func(*args: Any, **kwargs: Any) -> Any:
        warnings.simplefilter("always", DeprecationWarning)
        warnings.warn(
            func.__doc__,
            category=DeprecationWarning,
            stacklevel=3,  # This makes the error happen in user code
        )
        return func(*args, **kwargs)

    return new_func
