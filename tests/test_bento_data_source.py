import pathlib
from typing import Callable

import pytest
from databento.common.dbnstore import FileDataSource
from databento.common.dbnstore import MemoryDataSource
from databento_dbn import Schema


@pytest.mark.parametrize("schema", [pytest.param(x) for x in Schema.variants()])
def test_memory_data_source(
    test_data: Callable[[Schema], bytes],
    schema: Schema,
) -> None:
    """
    Test create of MemoryDataSource.
    """
    data = test_data(schema)
    data_source = MemoryDataSource(data)

    assert len(data) == data_source.nbytes
    assert repr(data) == data_source.name


@pytest.mark.parametrize("schema", [pytest.param(x) for x in Schema.variants()])
def test_file_data_source(
    test_data_path: Callable[[Schema], pathlib.Path],
    schema: Schema,
) -> None:
    """
    Test create of FileDataSource.
    """
    path = test_data_path(schema)
    data_source = FileDataSource(path)

    assert path.stat().st_size == data_source.nbytes
    assert path.name == data_source.name
