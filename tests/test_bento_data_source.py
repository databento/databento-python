import pathlib
from typing import Callable

import pytest
from databento.common.dbnstore import FileDataSource, MemoryDataSource
from databento.common.enums import Schema


@pytest.mark.parametrize("schema", [pytest.param(x) for x in Schema])
def test_memory_data_source(
    test_data: Callable[[Schema], bytes],
    schema: Schema,
) -> None:
    """Test create of MemoryDataSource"""
    if schema in (
        Schema.STATUS,
        Schema.STATISTICS,
        Schema.SYMBOL_MAPPING,
        Schema.GATEWAY_ERROR,
    ):
        pytest.skip(f"untested schema {schema}")

    data = test_data(schema)
    data_source = MemoryDataSource(data)

    assert len(data) == data_source.nbytes
    assert repr(data) == data_source.name


@pytest.mark.parametrize("schema", [pytest.param(x) for x in Schema])
def test_file_data_source(
    test_data_path: Callable[[Schema], pathlib.Path],
    schema: Schema,
) -> None:
    """Test create of FileDataSource"""
    if schema in (
        Schema.STATUS,
        Schema.STATISTICS,
        Schema.SYMBOL_MAPPING,
        Schema.GATEWAY_ERROR,
    ):
        pytest.skip(f"untested schema {schema}")

    path = test_data_path(schema)
    data_source = FileDataSource(path)

    assert path.stat().st_size == data_source.nbytes
    assert path.name == data_source.name
