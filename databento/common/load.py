import os

from databento.common.bento import FileBento


def from_file(path: str) -> FileBento:
    """
    Load data from a DBZ file at the given path.

    We recommend you organize your data with a file name extension which includes
    the schema value e.g. for data with MBO schema use 'my_data.mbo.dbz'.

    Parameters
    ----------
    path : str
        The path to the data.

    Returns
    -------
    FileBento

    Raises
    ------
    FileNotFoundError
        If no file is found at the given path.
    RuntimeError
        If an empty file exists at the given path.

    """
    if not os.path.exists(path):
        raise FileNotFoundError(f"no file found at `path` '{path}'")
    if os.stat(path).st_size == 0:
        raise RuntimeError(f"the file at `path` '{path}' was empty")

    bento = FileBento(path=path)

    metadata = bento.source_metadata()
    bento.set_metadata(metadata)

    return bento
