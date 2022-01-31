import os
from typing import Optional, Union

from databento.common.enums import Schema
from databento.historical.bento import FileBento


def from_file(path: str, schema: Optional[Union[Schema, str]] = None) -> FileBento:
    """
    Load data from a file at the given path.

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

    if schema is not None:
        schema = Schema(schema)
    return FileBento(path=path, schema=schema)
