from collections.abc import Generator
from collections.abc import Iterator
from pathlib import Path
from typing import IO
from typing import Final
from typing import Protocol

import zstandard
from databento.common.dbnstore import is_zstandard
from databento_dbn import Compression


FILE_READ_SIZE: Final = 2**10


class ReplayProtocol(Protocol):
    @property
    def name(self) -> str: ...

    def __iter__(self) -> Iterator[bytes]: ...


class FileReplay(ReplayProtocol):
    def __init__(self, dbn_file: Path):
        self._dbn_file = dbn_file
        self._compression = Compression.NONE

        with self._dbn_file.open("rb") as dbn:
            if is_zstandard(dbn):
                self._compression = Compression.ZSTD

    @property
    def name(self) -> str:
        return self._dbn_file.name

    def __iter__(self) -> Generator[bytes, None, None]:
        with self._dbn_file.open("rb") as dbn:
            if self._compression == Compression.ZSTD:
                reader: IO[bytes] = zstandard.ZstdDecompressor().stream_reader(dbn)
            else:
                reader = dbn
            while next_bytes := reader.read(FILE_READ_SIZE):
                yield next_bytes
