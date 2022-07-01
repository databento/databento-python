from types import TracebackType
from typing import IO, AnyStr, Iterable, Iterator, List, Optional, Type


class BinaryBuffer(IO[bytes]):
    """
    Provides a clearable in-memory buffer for streaming binary data.

    References
    ----------
    # https://docs.python.org/3/library/io.html
    """

    def __init__(self):
        self.raw = b""

    def __len__(self):
        return len(self.raw)

    def __next__(self) -> AnyStr:
        pass

    def __iter__(self) -> Iterator[AnyStr]:
        pass

    def __enter__(self) -> IO[AnyStr]:
        pass

    def __exit__(
        self,
        __t: Optional[Type[BaseException]],
        __value: Optional[BaseException],
        __traceback: Optional[TracebackType],
    ) -> None:
        pass

    @property
    def len(self):
        return len(self.raw)

    def write(self, b: bytes):
        self.raw += b

    def clear(self):
        self.raw = b""

    def fileno(self) -> int:
        pass

    def flush(self) -> None:
        pass

    def isatty(self) -> bool:
        pass

    def read(self, __n: int = ...) -> AnyStr:
        pass

    def readable(self) -> bool:
        pass

    def readline(self, __limit: int = ...) -> AnyStr:
        pass

    def readlines(self, __hint: int = ...) -> List[AnyStr]:
        pass

    def seek(self, __offset: int, __whence: int = ...) -> int:
        pass

    def seekable(self) -> bool:
        pass

    def tell(self) -> int:
        pass

    def truncate(self, __size: Optional[int] = ...) -> int:
        pass

    def writable(self) -> bool:
        pass

    def writelines(self, __lines: Iterable[AnyStr]) -> None:
        pass

    def close(self) -> None:
        pass
