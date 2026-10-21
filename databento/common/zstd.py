from __future__ import annotations

import sys
from io import BytesIO
from os import PathLike
from typing import IO
from typing import Literal
from typing import Protocol
from typing import cast


class IncrementalCompressor(Protocol):
    """
    A compressor that accepts uncompressed data in chunks.
    """

    def compress(self, data: bytes) -> bytes: ...

    def flush(self) -> bytes: ...


if sys.version_info >= (3, 14):
    from compression import zstd as _zstd

    ZstdError = _zstd.ZstdError

    def is_frame_header(header: bytes) -> bool:
        """
        Determine if `header` is the start of a zstandard frame.

        Parameters
        ----------
        header : bytes
            The first bytes of the data to check, at least 18 bytes to cover
            the largest frame header.

        Returns
        -------
        bool

        """
        try:
            _zstd.get_frame_info(header)
        except _zstd.ZstdError:
            return False
        return True

    def stream_reader(reader: IO[bytes]) -> IO[bytes]:
        """
        Return a reader that decompresses zstandard data from `reader`.

        The returned reader decompresses across all frames in `reader`.

        Parameters
        ----------
        reader : IO[bytes]
            The compressed data to read from.

        Returns
        -------
        IO[bytes]

        """
        return cast(IO[bytes], _zstd.ZstdFile(reader, mode="rb"))

    def stream_writer(
        path: PathLike[str] | str,
        mode: Literal["w", "x"],
    ) -> IO[bytes]:
        """
        Open the file at `path` for writing zstandard compressed data.

        Closing the returned writer finishes the frame and closes the file.
        Frames are written with a content checksum.

        Parameters
        ----------
        path : PathLike[str] or str
            The file path to write to.
        mode : str
            The file write mode to use, either "x" or "w".

        Returns
        -------
        IO[bytes]

        """
        return cast(
            IO[bytes],
            _zstd.ZstdFile(
                path,
                mode="wb" if mode == "w" else "xb",
                options={_zstd.CompressionParameter.checksum_flag: 1},
            ),
        )

    def compress(data: bytes) -> bytes:
        """
        Compress `data` into a single zstandard frame.

        Parameters
        ----------
        data : bytes
            The data to compress.

        Returns
        -------
        bytes

        """
        return _zstd.compress(data)

    def decompress(data: bytes) -> bytes:
        """
        Decompress all zstandard frames in `data`.

        Parameters
        ----------
        data : bytes
            The compressed data.

        Returns
        -------
        bytes

        """
        return _zstd.decompress(data)

    def incremental_compressor() -> IncrementalCompressor:
        """
        Create a compressor that accepts uncompressed data in chunks.

        Returns
        -------
        IncrementalCompressor

        """
        return _zstd.ZstdCompressor()

else:
    import zstandard

    ZstdError = zstandard.ZstdError

    def is_frame_header(header: bytes) -> bool:
        """
        Determine if `header` is the start of a zstandard frame.

        Parameters
        ----------
        header : bytes
            The first bytes of the data to check, at least 18 bytes to cover
            the largest frame header.

        Returns
        -------
        bool

        """
        try:
            zstandard.get_frame_parameters(header)
        except zstandard.ZstdError:
            return False
        return True

    def stream_reader(reader: IO[bytes]) -> IO[bytes]:
        """
        Return a reader that decompresses zstandard data from `reader`.

        The returned reader decompresses across all frames in `reader`.

        Parameters
        ----------
        reader : IO[bytes]
            The compressed data to read from.

        Returns
        -------
        IO[bytes]

        """
        return zstandard.ZstdDecompressor().stream_reader(
            reader,
            read_across_frames=True,
        )

    def stream_writer(
        path: PathLike[str] | str,
        mode: Literal["w", "x"],
    ) -> IO[bytes]:
        """
        Open the file at `path` for writing zstandard compressed data.

        Closing the returned writer finishes the frame and closes the file.
        Frames are written with a content checksum.

        Parameters
        ----------
        path : PathLike[str] or str
            The file path to write to.
        mode : str
            The file write mode to use, either "x" or "w".

        Returns
        -------
        IO[bytes]

        """
        return zstandard.ZstdCompressor(write_checksum=True).stream_writer(
            open(path, mode=f"{mode}b"),
            closefd=True,
        )

    def compress(data: bytes) -> bytes:
        """
        Compress `data` into a single zstandard frame.

        Parameters
        ----------
        data : bytes
            The data to compress.

        Returns
        -------
        bytes

        """
        return zstandard.compress(data)

    def decompress(data: bytes) -> bytes:
        """
        Decompress all zstandard frames in `data`.

        Parameters
        ----------
        data : bytes
            The compressed data.

        Returns
        -------
        bytes

        """
        return stream_reader(BytesIO(data)).read()

    def incremental_compressor() -> IncrementalCompressor:
        """
        Create a compressor that accepts uncompressed data in chunks.

        Returns
        -------
        IncrementalCompressor

        """
        return zstandard.ZstdCompressor().compressobj()
