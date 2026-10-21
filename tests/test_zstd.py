"""
Unit tests for the zstandard compression helpers.
"""

from io import BytesIO
from pathlib import Path

import pytest

from databento.common import zstd
from databento.common.dbnstore import is_zstandard


def test_compress_decompress_round_trip() -> None:
    """
    Test that compressed data decompresses to the original bytes.
    """
    # Arrange
    data = b"hello" * 1000

    # Act
    compressed = zstd.compress(data)

    # Assert
    assert compressed != data
    assert zstd.decompress(compressed) == data


def test_decompress_multiple_frames() -> None:
    """
    Test that decompress handles concatenated frames.
    """
    # Arrange
    first = b"a" * 1000
    second = b"b" * 1000
    data = zstd.compress(first) + zstd.compress(second)

    # Act, Assert
    assert zstd.decompress(data) == first + second


def test_stream_reader_reads_across_frames() -> None:
    """
    Test that stream_reader reads across concatenated frames.
    """
    # Arrange
    first = b"a" * 1000
    second = b"b" * 1000
    data = zstd.compress(first) + zstd.compress(second)

    # Act
    reader = zstd.stream_reader(BytesIO(data))

    # Assert
    assert reader.read(10) == b"a" * 10
    assert reader.read() == b"a" * 990 + second
    assert reader.read() == b""


@pytest.mark.parametrize(
    "header,expected",
    [
        pytest.param(zstd.compress(b"x")[:18], True, id="standard_frame"),
        pytest.param(b"DBN\x01" + bytes(14), False, id="dbn_header"),
        pytest.param(b"", False, id="empty"),
    ],
)
def test_is_frame_header(header: bytes, expected: bool) -> None:
    """
    Test that is_frame_header identifies the start of a zstandard frame.
    """
    # Act, Assert
    assert zstd.is_frame_header(header) == expected


def test_stream_writer(tmp_path: Path) -> None:
    """
    Test that stream_writer writes a complete frame with a content checksum and
    closes the file.
    """
    # Arrange
    path = tmp_path / "data.zst"
    data = b"hello" * 1000

    # Act
    writer = zstd.stream_writer(path, "w")
    writer.write(data)
    writer.close()

    # Assert
    raw = path.read_bytes()
    assert raw[4] & 0x04  # frame header descriptor: content checksum flag
    assert zstd.decompress(raw) == data
    with path.open("rb") as reader:
        assert is_zstandard(reader)


def test_stream_writer_exclusive_mode_existing_file(tmp_path: Path) -> None:
    """
    Test that stream_writer in exclusive mode refuses to overwrite a file.
    """
    # Arrange
    path = tmp_path / "data.zst"
    path.write_bytes(b"")

    # Act, Assert
    with pytest.raises(FileExistsError):
        zstd.stream_writer(path, "x")


def test_incremental_compressor() -> None:
    """
    Test that the incremental compressor produces a single decompressible frame
    from several chunks.
    """
    # Arrange
    chunks = [b"a" * 1000, b"b" * 1000, b"c" * 1000]
    cctx = zstd.incremental_compressor()

    # Act
    compressed = b"".join(cctx.compress(chunk) for chunk in chunks) + cctx.flush()

    # Assert
    assert zstd.decompress(compressed) == b"".join(chunks)
