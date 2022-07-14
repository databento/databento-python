import os

from databento.common.bento import FileBento
from databento.common.logging import log_debug
from databento.common.metadata import MetadataDecoder


def from_dbz_file(path: str) -> FileBento:
    """
    Load data from a DBZ file at the given path.

    We recommend you organize your data with a file name extension which includes
    the schema value e.g. for data with MBO schema use 'my_data.mbo.bin'.

    Parameters
    ----------
    path : str
        The path to the data.
    schema : Schema, optional
        The schema for the data. If ``None`` then will be inferred.

    Returns
    -------
    FileBento

    Raises
    ------
    FileNotFoundError
        If no file is found at the given path.
    RuntimeError
        If an empty file exists at the given path.
    RuntimeError
        If schema is None and cannot infer schema from path.

    Warnings
    --------
    If you do not provide a `schema` and a valid schema value does not appear in
    the `path` file name extensions, then loading will fail.

    """
    if not os.path.exists(path):
        raise FileNotFoundError(f"no file found at `path` '{path}'")
    if os.stat(path).st_size == 0:
        raise RuntimeError(f"the file at `path` '{path}' was empty")

    with open(path, mode="rb") as f:
        meta_chunk = f.read(8)
        if not meta_chunk.startswith(b"Q*M\x18"):
            raise RuntimeError("invalid DBZ file")

        log_debug("Decoding metadata...")
        magic = int.from_bytes(meta_chunk[:4], byteorder="little")
        frame_size = int.from_bytes(meta_chunk[4:8], byteorder="little")
        log_debug(f"magic={magic}, frame_size={frame_size}")

        metadata = MetadataDecoder.decode_to_json(f.read(frame_size + 8))
        log_debug(f"metadata={metadata}")  # TODO(cs): Temporary logging

        bento = FileBento(path=path)
        bento.set_metadata(metadata)

        return bento
