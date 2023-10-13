from __future__ import annotations

import itertools
from collections.abc import Iterable
from typing import TypeVar


_C = TypeVar("_C")


def chunk(iterable: Iterable[_C], size: int) -> Iterable[tuple[_C, ...]]:
    """
    Break an iterable into chunks with a length of at most `size`.

    Parameters
    ----------
    iterable: Iterable[_C]
        The iterable to break up.
    size : int
        The maximum size of each chunk.

    Returns
    -------
    Iterable[_C]

    Raises
    ------
    ValueError
        If `size` is less than 1.

    """
    if size < 1:
        raise ValueError("size must be at least 1")

    it = iter(iterable)
    return iter(lambda: tuple(itertools.islice(it, size)), ())
